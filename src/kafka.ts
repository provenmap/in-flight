// A thin promise layer over the Confluent client's librdkafka API. That API, not the client's KafkaJS-style one,
// has what this app needs: manual assign with no group rebalance, per-partition pause, commits of chosen offsets.
import rdkafka from '@confluentinc/kafka-javascript';
import type { RdKafka } from '@confluentinc/kafka-javascript';

const { KafkaConsumer, Producer, AdminClient } = (rdkafka as unknown as { default?: typeof rdkafka }).default ?? rdkafka;

export const Partitions = 3;

/** A partition of a topic. */
export interface TP {
  topic: string;
  partition: number;
}

/** A position in a partition. */
export interface TPO extends TP {
  offset: number;
}

/** A record as this app reads it. Headers keep the last value given for each key. */
export interface Rec extends TPO {
  key: string | null;
  value: Buffer;
  timestamp: number;
  headers: Map<string, string>;
}

/** A record to write. */
export interface OutRecord {
  topic: string;
  key: string | null;
  value: Buffer | string;
  headers?: Record<string, string>;
}

export const tpKey = (topic: string, partition: number): string => `${topic}:${partition}`;

function promised<T>(run: (cb: (err: unknown, value: T) => void) => void): Promise<T> {
  return new Promise((resolve, reject) => run((err, value) => (err ? reject(err) : resolve(value))));
}

const quiet = { 'log_level': 3 }; // librdkafka logs every connection at INFO; keep warnings and errors

/**
 * A consumer. With a group it can subscribe, commit and read its group's commits; without one it only reads, so it
 * commits nothing and shows up in nobody's lag.
 */
export class Consumer {
  private constructor(private readonly c: RdKafka.KafkaConsumer, readonly group: string | null) {}

  /** `pollMs` is how long a poll waits for its first record. */
  static async connect(bootstrap: string, group: string | null = null, pollMs = 200): Promise<Consumer> {
    // librdkafka wants a group id even to read by assignment; one that never commits and never joins is invisible.
    const c = new KafkaConsumer({
      ...quiet,
      'bootstrap.servers': bootstrap,
      'group.id': group ?? `rif-reader-${process.pid}-${Date.now()}`,
      'enable.auto.commit': false,
      'enable.auto.offset.store': false,
    }, { 'auto.offset.reset': 'earliest' });
    await promised(cb => c.connect({ timeout: 10_000 }, cb));
    c.setDefaultConsumeTimeout(pollMs);
    // Otherwise a batch waits `pollMs` for every record, and a steady trickle holds it open until it is full.
    (c as unknown as { setDefaultIsTimeoutOnlyForFirstMessage(on: boolean): void }).setDefaultIsTimeoutOnlyForFirstMessage(true);
    return new Consumer(c, group);
  }

  subscribe(topics: string[]): void {
    this.c.subscribe(topics);
  }

  /** Reads these partitions from these offsets, outside any rebalance. */
  assign(from: TPO[]): void {
    this.c.assign(from);
  }

  /** Up to `max` records: whatever is ready once the first arrives, or none after `pollMs`. */
  async poll(max = 500): Promise<Rec[]> {
    const msgs = await promised<RdKafka.Message[]>(cb => this.c.consume(max, cb));
    return msgs.map(m => ({
      topic: m.topic,
      partition: m.partition,
      offset: m.offset,
      key: m.key == null ? null : m.key.toString(),
      value: m.value ?? Buffer.alloc(0),
      timestamp: m.timestamp ?? Date.now(),
      headers: new Map((m.headers ?? []).flatMap(h => Object.entries(h)).map(([k, v]) => [k, v.toString()])),
    }));
  }

  pause(tps: TP[]): void {
    this.c.pause(tps);
  }

  resume(tps: TP[]): void {
    this.c.resume(tps);
  }

  /** Commits exactly these offsets (the next record to read), and resolves once the broker has them. */
  commit(offsets: TPO[]): Promise<void> {
    if (!offsets.length) return Promise.resolve();
    const c = this.c as unknown as { commitCb(tps: TPO[], cb: (err: unknown) => void): void };
    return promised(cb => c.commitCb(offsets, err => cb(err, undefined)));
  }

  /** This group's committed offsets for these partitions; a partition with no commit is left out. */
  async committed(tps: TP[]): Promise<Map<string, number>> {
    const got = await promised<RdKafka.TopicPartitionOffset[]>(cb => this.c.committed(tps, 10_000, cb));
    return new Map(got.filter(t => t.offset >= 0).map(t => [tpKey(t.topic, t.partition), t.offset]));
  }

  /** The first offset still in the log and the offset the next record will get. */
  watermarks(tp: TP): Promise<{ low: number; high: number }> {
    return promised(cb => this.c.queryWatermarkOffsets(tp.topic, tp.partition, 10_000, (err, o) => cb(err, o && { low: o.lowOffset, high: o.highOffset })));
  }

  async partitionsFor(topic: string): Promise<TP[]> {
    const md = await promised<RdKafka.Metadata>(cb => this.c.getMetadata({ topic, timeout: 10_000 }, cb));
    const t = md.topics.find(x => x.name === topic);
    return (t?.partitions ?? []).map(p => ({ topic, partition: p.id })).sort((a, b) => a.partition - b.partition);
  }

  async close(): Promise<void> {
    if (!this.c.isConnected()) return;
    await promised(cb => this.c.disconnect(cb));
  }
}

/** A producer with acks from all replicas. `send` queues; `flush` waits until everything queued is acknowledged. */
export class Writer {
  private constructor(private readonly p: RdKafka.Producer) {}

  static async connect(bootstrap: string): Promise<Writer> {
    const p = new Producer({
      ...quiet,
      'bootstrap.servers': bootstrap,
      'acks': -1, // all replicas
      'linger.ms': 5,
      'dr_cb': false,
    });
    await promised(cb => p.connect({ timeout: 10_000 }, cb));
    p.setPollInterval(100);
    return new Writer(p);
  }

  send(out: OutRecord): void {
    const headers = out.headers ? Object.entries(out.headers).map(([k, v]) => ({ [k]: v })) : undefined;
    const value = typeof out.value === 'string' ? Buffer.from(out.value) : out.value;
    this.p.produce(out.topic, null, value, out.key, Date.now(), undefined, headers);
  }

  flush(timeoutMs = 10_000): Promise<void> {
    return promised(cb => this.p.flush(timeoutMs, err => cb(err, undefined)));
  }

  async close(): Promise<void> {
    if (!this.p.isConnected()) return;
    await this.flush().catch(() => undefined);
    await promised(cb => this.p.disconnect(cb));
  }
}

/** Topics, group offsets and log ends. */
export class Admin {
  private constructor(private readonly a: RdKafka.IAdminClient) {}

  static connect(bootstrap: string): Admin {
    return new Admin(AdminClient.create({ ...quiet, 'bootstrap.servers': bootstrap }));
  }

  listTopics(): Promise<string[]> {
    return promised(cb => this.a.listTopics({ timeout: 10_000 }, cb));
  }

  async ensureTopics(topics: string[], partitions = Partitions): Promise<void> {
    const existing = new Set(await this.listTopics());
    for (const topic of topics.filter(t => !existing.has(t))) {
      await promised(cb => this.a.createTopic({ topic, num_partitions: partitions, replication_factor: 1 }, 10_000, err => cb(err, undefined)))
        .catch((e: { code?: number }) => {
          if (e.code !== rdkafka.CODES.ERRORS.ERR_TOPIC_ALREADY_EXISTS) throw e;
        });
    }
  }

  /** A group's committed offsets, keyed by `topic:partition`. */
  async committed(group: string): Promise<Map<string, number>> {
    const [res] = await promised<RdKafka.GroupResults[]>(cb => this.a.listConsumerGroupOffsets([{ groupId: group }], { timeout: 10_000 }, cb));
    if (res?.error) throw res.error;
    return new Map((res?.partitions ?? []).filter(p => p.offset >= 0 && !p.error).map(p => [tpKey(p.topic, p.partition), p.offset]));
  }

  async partitions(topics: string[]): Promise<TP[]> {
    const ds = await promised<RdKafka.TopicDescription[]>(cb => this.a.describeTopics(topics, { timeout: 10_000 }, cb));
    return ds.flatMap(d => d.partitions.map(p => ({ topic: d.name, partition: p.partition })));
  }

  /** The end of each partition: the offset the next record will get. */
  async ends(tps: TP[]): Promise<Map<string, number>> {
    if (!tps.length) return new Map();
    const res = await promised<RdKafka.ListOffsetsResult[]>(cb =>
      this.a.listOffsets(tps.map(tp => ({ ...tp, offset: rdkafka.OffsetSpec.LATEST })), { timeout: 10_000 }, cb));
    return new Map(res.filter(r => !r.error).map(r => [tpKey(r.topic, r.partition), r.offset]));
  }

  /** How many members a group has: none for a group that only commits by assignment. */
  async members(group: string): Promise<number> {
    const res = await promised<RdKafka.GroupDescriptions>(cb => this.a.describeGroups([group], { timeout: 10_000 }, cb));
    return res.groups[0]?.members.length ?? 0;
  }

  async deleteTopics(topics: string[]): Promise<void> {
    for (const t of topics) await promised(cb => this.a.deleteTopic(t, 10_000, err => cb(err, undefined)));
  }

  close(): void {
    this.a.disconnect();
  }
}
