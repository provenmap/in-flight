import { setTimeout as sleep } from 'node:timers/promises';
import { DeadLetter } from '../pricing/cloudevents.js';
import { showRef, type Catalog, type DeadLetterHeaders } from '../pricing/catalog.js';
import { Pricing } from '../pricing/pricing.js';
import { decode } from '../pricing/records.js';
import { Admin, Consumer, tpKey, type Rec } from '../kafka.js';
import { Ledger, msgJson, statsJson, stuckJson, type Msg, type Stuck } from './ledger.js';
import type { Poller, Polled } from './poller.js';

/** A viewer cannot show more red blocks than this usefully; the counter still counts them all. */
export const MaxStuck = 3000;

/** The map: districts, and each flow topic with where it comes from and who reads it. */
export function flowJson(c: Catalog) {
  return {
    name: c.flow,
    currency: c.currency,
    districts: c.districts.map(d => ({ id: d.id, name: d.name, sink: d.sink })),
    topics: c.topics.map(t => ({
      topic: t,
      from: c.districtOf(t) ?? '',
      to: c.readersOf(t).map(k => k.district),
      events: c.events.filter(e => e.topic === t).map(e => showRef(e.ref)),
    })),
    dlqs: c.services.flatMap(k => (k.dlq ? [{ dlq: k.dlq, district: k.district, group: k.name, redrive: k.redrive ?? null }] : [])),
    groups: c.services.map(k => ({ group: k.group, district: k.district })),
  };
}

export type Frame = Record<string, unknown>[];

/**
 * Where a dead letter says why it failed, when, and where it came from: the catalog's own names first, then this
 * app's, Kafka Connect's (with `errors.deadletterqueue.context.headers.enable`) and Spring Kafka's.
 */
export function deadLetterHeaders(own: DeadLetterHeaders): Record<keyof DeadLetterHeaders, string[]> {
  const with_ = (name: string | undefined, known: string[]) => (name ? [name, ...known] : known);
  return {
    error: with_(own.error, [DeadLetter.Error, '__connect.errors.exception.message', 'kafka_dlt-exception-message']),
    failedAt: with_(own.failedAt, [DeadLetter.FailedAt]),
    sourceTopic: with_(own.sourceTopic, [DeadLetter.SourceTopic, '__connect.errors.topic', 'kafka_dlt-original-topic']),
  };
}

const first = (headers: Map<string, string>, names: string[]): string | undefined =>
  names.map(n => headers.get(n)).find(v => v !== undefined && v !== '');

/**
 * Reads every dead-letter queue from the start and every flow topic from the end, prices each record, and feeds the
 * ledger. It owns the ledger: stats, and snapshots for new viewers, are made between its batches, so a viewer that
 * joins sees every message after its snapshot exactly once.
 */
export class Reader {
  readonly ledger: Ledger;
  private readonly pricing: Pricing;
  private readonly dlqs: Set<string>;
  private readonly headers: Record<keyof DeadLetterHeaders, string[]>;
  private readonly joins: ((snapshot: Record<string, unknown>) => void)[] = [];
  private running = true;
  private done: Promise<void> | null = null;
  /** True once the reader has its partitions; until then a snapshot would be empty, so joins wait. */
  reading = false;

  constructor(
    private readonly bootstrap: string,
    private readonly catalog: Catalog,
    private readonly publish: (frame: Frame) => void,
    private readonly poller: Poller,
    /** True with no simulator: the snapshot says so, so the view shows no controls. */
    private readonly watching = false,
  ) {
    this.ledger = new Ledger(catalog);
    this.pricing = new Pricing(catalog);
    this.dlqs = new Set(catalog.dlqs);
    this.headers = deadLetterHeaders(catalog.deadLetterHeaders);
  }

  start(): void {
    this.done = this.run().catch(e => console.error(`reader: ${(e as Error).message}`));
  }

  /** `send` gets a snapshot between two batches; every frame published after it is the viewer's to receive. */
  join(send: (snapshot: Record<string, unknown>) => void): void {
    this.joins.push(send);
  }

  private async run(): Promise<void> {
    const admin = Admin.connect(this.bootstrap);
    const c = await Consumer.connect(this.bootstrap, null, 100);
    try {
      const topics = [...this.catalog.topics, ...this.catalog.dlqs];
      if (!(await this.waitFor(admin, topics))) return;
      for (const dlq of this.catalog.dlqs) {
        const group = this.catalog.redriveGroupOf(dlq);
        if (!group) continue;
        for (const [k, o] of await admin.committed(group)) {
          const at = k.lastIndexOf(':');
          if (k.slice(0, at) === dlq) this.ledger.redriven(dlq, Number(k.slice(at + 1)), o);
        }
      }
      const parts = (await Promise.all(topics.map(t => c.partitionsFor(t)))).flat();
      // Dead letters from the start; flow topics from the end, pinned now, so nothing produced from here on is skipped.
      c.assign(await Promise.all(parts.map(async tp => {
        const { low, high } = await c.watermarks(tp);
        return { ...tp, offset: this.dlqs.has(tp.topic) ? low : high };
      })));
      console.log(`observer: reading ${this.catalog.dlqs.join(', ')} from the start and ${this.catalog.topics.length} flow topics from the end`);
      this.reading = true;

      let seen: Polled | null = null;
      let lastStats = 0;
      while (this.running) {
        const msgs = (await c.poll()).flatMap(r => this.read(r));
        const polled = this.poller.latest;
        if (polled && polled !== seen) {
          seen = polled;
          for (const [g, offs] of polled.committed) this.ledger.onOffsets(g, offs, polled.ends);
          for (const [dlq, offs] of polled.redriven) msgs.push(...this.ledger.onRedriveOffsets(dlq, offs, new Date()));
        }
        const frame: Frame = msgs.map(msgJson);
        if (Date.now() - lastStats >= 500) {
          lastStats = Date.now();
          frame.push(this.statsJson());
        }
        if (frame.length) this.publish(frame);
        for (const send of this.joins.splice(0)) send(this.snapshot());
      }
    } finally {
      this.reading = false;
      await c.close();
      admin.close();
    }
  }

  async stop(): Promise<void> {
    this.running = false;
    await this.done;
  }

  private read(r: Rec): Msg[] {
    const pos = { topic: r.topic, partition: r.partition, offset: r.offset };
    const dead = this.dlqs.has(r.topic);
    // A dead letter came from the topic its headers name, or else from one of the topics its service reads.
    const source = dead ? first(r.headers, this.headers.sourceTopic) : r.topic;
    const topics = source !== undefined ? [source] : (this.catalog.ownerOfDlq(r.topic)?.reads ?? []);
    const decoded = decode(r.value, r.headers);
    const priced = decoded.ok ? this.pricing.priceDecoded(decoded.value, topics, `${tpKey(r.topic, r.partition)}:${r.offset}`) : decoded;
    if (!dead) {
      if (!priced.ok) {
        this.ledger.unreadable();
        return [];
      }
      return this.ledger.onFlow(pos, priced.value, new Date(r.timestamp));
    }
    const failedAt = first(r.headers, this.headers.failedAt);
    const at = failedAt !== undefined && !Number.isNaN(Date.parse(failedAt)) ? new Date(failedAt) : new Date(r.timestamp);
    const error = first(r.headers, this.headers.error) ?? 'no error given';
    if (priced.ok) return [this.ledger.onDead(r.topic, pos, priced.value, error, at)];
    // Stuck all the same: a dead letter nobody can price is still an order that did not get through.
    const d = decoded.ok ? decoded.value : null;
    const event = d?.type ? (d.version !== null ? `${d.type}@v${d.version}` : d.type) : 'an unreadable record';
    return [this.ledger.onDeadUnpriced(r.topic, pos, event, priced.error, error, at)];
  }

  statsJson(): Record<string, unknown> {
    return { ...statsJson(this.ledger.stats()), sim: this.poller.latest?.sim ?? null };
  }

  stuck(): Stuck[] {
    return this.ledger.stuckNow();
  }

  snapshot(): Record<string, unknown> {
    const stuck = this.ledger.stuckNow();
    return {
      type: 'snapshot',
      mode: this.watching ? 'watch' : 'demo',
      flow: flowJson(this.catalog),
      stuck: stuck.slice(-MaxStuck).map(stuckJson),
      stuckShown: Math.min(stuck.length, MaxStuck),
      stats: this.statsJson(),
    };
  }

  private async waitFor(admin: Admin, topics: string[]): Promise<boolean> {
    let told = false;
    while (this.running) {
      const have = new Set(await admin.listTopics().catch(() => [] as string[]));
      const missing = topics.filter(t => !have.has(t));
      if (!missing.length) return true;
      if (!told) console.log(`observer: waiting for topics ${missing.join(', ')}${this.watching ? ': the catalog names them, and the observer reads them all' : ' (start the simulator)'}`);
      told = true;
      await sleep(2000);
    }
    return false;
  }
}

