import { CloudEvents, type CloudEvent } from '../pricing/cloudevents.js';
import { Consumer, Writer, tpKey, type TP, type TPO } from '../kafka.js';
import type { Result } from '../result.js';
import { record, type Out } from './events.js';

/** How the last redrive went. */
export interface RedriveStatus {
  running: boolean;
  redriven: number;
  remaining: number;
  error: string | null;
}

export const idle: RedriveStatus = { running: false, redriven: 0, remaining: 0, error: null };

/**
 * One pass over a dead-letter queue, up to the end it had when the pass began, in its own consumer group. Each
 * record goes through `handle` again; what it returns is published carrying the original event id. The first
 * record that still fails stops its partition, and the committed offset stays on it, so a later pass retries it.
 * Nothing is re-published to the source topic, so the other readers of that topic never see an event twice.
 *
 * The group never subscribes: it assigns the partitions itself, so there is no rebalance and no member to wait for.
 */
export class Redrive {
  constructor(
    private readonly bootstrap: string,
    private readonly dlq: string,
    private readonly group: string,
    private readonly handle: (e: CloudEvent) => Result<Out[]>,
  ) {}

  async run(progress: (s: RedriveStatus) => void, giveUpMs = 60_000): Promise<RedriveStatus> {
    const c = await Consumer.connect(this.bootstrap, this.group);
    const p = await Writer.connect(this.bootstrap);
    try {
      const parts = await c.partitionsFor(this.dlq);
      const marks = new Map(await Promise.all(parts.map(async tp => [tpKey(tp.topic, tp.partition), await c.watermarks(tp)] as const)));
      const committed = await c.committed(parts);
      const key = (tp: TP) => tpKey(tp.topic, tp.partition);
      const end = new Map(parts.map(tp => [key(tp), marks.get(key(tp))!.high]));
      const next = new Map(parts.map(tp => [key(tp), committed.get(key(tp)) ?? marks.get(key(tp))!.low]));
      c.assign(parts.map(tp => ({ ...tp, offset: next.get(key(tp))! })));

      const open = new Set(parts.filter(tp => next.get(key(tp))! < end.get(key(tp))!).map(key));
      let done = 0;
      let error: string | null = null;
      const giveUp = Date.now() + giveUpMs;
      const remaining = () => parts.reduce((n, tp) => n + end.get(key(tp))! - next.get(key(tp))!, 0);

      while (open.size && Date.now() < giveUp) {
        const commits = new Map<string, TPO>();
        for (const r of await c.poll()) {
          const k = tpKey(r.topic, r.partition);
          if (!open.has(k) || r.offset >= end.get(k)!) continue;
          const e = CloudEvents.parse(r.value);
          const res = e.ok ? this.handle(e.value) : e;
          if (res.ok) {
            for (const o of res.value) p.send(record(o));
            next.set(k, r.offset + 1);
            commits.set(k, { topic: r.topic, partition: r.partition, offset: r.offset + 1 });
            done++;
            if (r.offset + 1 >= end.get(k)!) open.delete(k);
          } else {
            error = `offset ${r.offset} on partition ${r.partition}: ${res.error}`;
            c.pause([{ topic: r.topic, partition: r.partition }]);
            open.delete(k);
          }
        }
        if (commits.size) {
          await p.flush();
          await c.commit([...commits.values()]);
        }
        progress({ running: true, redriven: done, remaining: remaining(), error });
      }
      return { running: false, redriven: done, remaining: remaining(), error };
    } finally {
      await c.close();
      await p.close();
    }
  }
}
