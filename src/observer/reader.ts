import { setTimeout as sleep } from 'node:timers/promises';
import { DeadLetter } from '../pricing/cloudevents.js';
import { showRef, type Catalog } from '../pricing/catalog.js';
import { Pricing } from '../pricing/pricing.js';
import { Admin, Consumer, type Rec } from '../kafka.js';
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
    dlqs: c.services.flatMap(k => (k.dlq ? [{ dlq: k.dlq, district: k.district, group: k.name }] : [])),
  };
}

export type Frame = Record<string, unknown>[];

/**
 * Reads every dead-letter queue from the start and every flow topic from the end, prices each record, and feeds the
 * ledger. It owns the ledger: stats, and snapshots for new viewers, are made between its batches, so a viewer that
 * joins sees every message after its snapshot exactly once.
 */
export class Reader {
  readonly ledger: Ledger;
  private readonly pricing: Pricing;
  private readonly dlqs: Set<string>;
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
  ) {
    this.ledger = new Ledger(catalog);
    this.pricing = new Pricing(catalog);
    this.dlqs = new Set(catalog.dlqs);
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
    const priced = this.pricing.priceBytes(r.value);
    if (!priced.ok) {
      this.ledger.unreadable();
      return [];
    }
    if (this.dlqs.has(r.topic)) {
      const failedAt = r.headers.get(DeadLetter.FailedAt);
      const at = failedAt !== undefined && !Number.isNaN(Date.parse(failedAt)) ? new Date(failedAt) : new Date(r.timestamp);
      return [this.ledger.onDead(r.topic, pos, priced.value, r.headers.get(DeadLetter.Error) ?? 'unknown', at)];
    }
    return this.ledger.onFlow(pos, priced.value, new Date(r.timestamp));
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
      if (topics.every(t => have.has(t))) return true;
      if (!told) console.log(`observer: waiting for topics ${topics.join(', ')} (start the simulator)`);
      told = true;
      await sleep(2000);
    }
    return false;
  }
}

