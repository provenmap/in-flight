// Phase 0: the Kafka client does everything the simulator and observer need, against the Compose Kafka.
import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import { Admin, Consumer, Writer, tpKey, type Rec } from '../../src/kafka.js';

const bootstrap = process.env.BOOTSTRAP_SERVERS ?? 'localhost:9092';
const run = Date.now();
const topic = `rif-spike-${run}`;
const group = `rif-spike-group-${run}`;

async function drain(c: Consumer, want: number, ms = 15_000): Promise<Rec[]> {
  const got: Rec[] = [];
  const until = Date.now() + ms;
  while (got.length < want && Date.now() < until) got.push(...await c.poll());
  return got;
}

describe('the Kafka client', () => {
  const admin = Admin.connect(bootstrap);
  let writer: Writer;

  beforeAll(async () => {
    await admin.ensureTopics([topic]);
    writer = await Writer.connect(bootstrap);
    for (let i = 0; i < 30; i++) {
      writer.send({ topic, key: `k-${i}`, value: JSON.stringify({ i }), headers: { 'rif-error': `e-${i}`, 'rif-failed-at': '2026-10-30T12:00:00Z' } });
    }
    await writer.flush();
  });

  afterAll(async () => {
    await writer?.close();
    await admin.deleteTopics([topic]).catch(() => undefined);
    admin.close();
  });

  test('admin: topic partitions and end offsets', async () => {
    const parts = await admin.partitions([topic]);
    expect(parts.map(p => p.partition).sort()).toEqual([0, 1, 2]);
    const ends = await admin.ends(parts);
    expect([...ends.values()].reduce((a, b) => a + b, 0)).toBe(30);
  });

  test('a reader outside any group assigns, starts at chosen offsets, and reads headers and timestamps', async () => {
    const c = await Consumer.connect(bootstrap);
    try {
      const parts = await c.partitionsFor(topic);
      c.assign(parts.map(p => ({ ...p, offset: 0 })));
      const recs = await drain(c, 30);
      expect(recs).toHaveLength(30);
      const r = recs.find(x => x.key === 'k-7')!;
      expect(r.headers.get('rif-error')).toBe('e-7');
      expect(JSON.parse(r.value.toString())).toEqual({ i: 7 });
      expect(Math.abs(r.timestamp - Date.now())).toBeLessThan(120_000);

      // Start a partition partway: the end, pinned now, reads nothing old.
      const { high } = await c.watermarks(parts[0]!);
      c.assign([{ ...parts[0]!, offset: high }]);
      expect(await drain(c, 1, 1500)).toHaveLength(0);
    } finally {
      await c.close();
    }
  });

  test('a group consumer assigns without a rebalance, pauses a partition, and commits a chosen offset', async () => {
    const c = await Consumer.connect(bootstrap, group);
    try {
      const parts = await c.partitionsFor(topic);
      const ends = new Map(await Promise.all(parts.map(async p => [p.partition, (await c.watermarks(p)).high] as const)));
      const busiest = parts.reduce((a, b) => (ends.get(a.partition)! >= ends.get(b.partition)! ? a : b));
      expect(ends.get(busiest.partition)!).toBeGreaterThan(2);
      // Paused from the start, the partition yields nothing; resumed, it does.
      c.assign([{ ...busiest, offset: 0 }]);
      c.pause([busiest]);
      expect(await drain(c, 1, 1500)).toHaveLength(0);
      c.resume([busiest]);
      const recs = await drain(c, ends.get(busiest.partition)!);
      expect(recs.map(r => r.offset).slice(0, 3)).toEqual([0, 1, 2]);

      // Commit a chosen offset, not the position: the group's commit is exactly it, seen by the group and by Admin,
      // and the group never joined, so it has no members and no rebalance happened.
      await c.commit([{ ...busiest, offset: 1 }]);
      expect((await c.committed([busiest])).get(tpKey(topic, busiest.partition))).toBe(1);
      expect((await admin.committed(group)).get(tpKey(topic, busiest.partition))).toBe(1);
      expect(await admin.members(group)).toBe(0);
    } finally {
      await c.close();
    }
  });

  test('a subscribed group consumer gets assigned and commits after it flushes', async () => {
    const g = `${group}-sub`;
    const c = await Consumer.connect(bootstrap, g);
    try {
      c.subscribe([topic]);
      const recs = await drain(c, 30, 30_000);
      expect(recs).toHaveLength(30);
      const next = new Map<string, { topic: string; partition: number; offset: number }>();
      for (const r of recs) next.set(tpKey(r.topic, r.partition), { topic: r.topic, partition: r.partition, offset: r.offset + 1 });
      await c.commit([...next.values()]);
      const committed = await admin.committed(g);
      expect([...committed.values()].reduce((a, b) => a + b, 0)).toBe(30);
    } finally {
      await c.close();
    }
  });
});
