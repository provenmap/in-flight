import { CloudEvents, DeadLetter, type CloudEvent } from '../pricing/cloudevents.js';
import { Consumer, Writer, tpKey, type OutRecord, type Rec, type TPO } from '../kafka.js';
import type { Result } from '../result.js';
import { record, type Out } from './events.js';

/** The record a service writes when it cannot read one: the original bytes and key, and why. */
export function deadLetter(dlq: string, group: string, r: Rec, error: string, at: Date): OutRecord {
  return {
    topic: dlq,
    key: r.key,
    value: r.value,
    headers: {
      [DeadLetter.Error]: error,
      [DeadLetter.SourceTopic]: r.topic,
      [DeadLetter.Group]: group,
      [DeadLetter.FailedAt]: at.toISOString(),
    },
  };
}

/** The offset to commit for each partition a batch touched: one past the last record handled. */
export function nextOffsets(recs: Rec[]): TPO[] {
  const next = new Map<string, TPO>();
  for (const r of recs) next.set(tpKey(r.topic, r.partition), { topic: r.topic, partition: r.partition, offset: r.offset + 1 });
  return [...next.values()];
}

export type Handler = (e: CloudEvent) => Result<Out[]> | Promise<Result<Out[]>>;

/**
 * A Kafka service: reads its topics in its own consumer group, publishes what `handle` returns, and dead-letters
 * what it cannot read. Offsets are committed after each batch's output is flushed, so consumer lag is real.
 */
export class Service {
  processed = 0;
  deadLettered = 0;
  private running = true;
  private done: Promise<void> | null = null;

  constructor(
    readonly name: string,
    private readonly bootstrap: string,
    readonly group: string,
    private readonly topics: string[],
    private readonly dlq: string,
    private readonly handle: Handler,
  ) {}

  start(): void {
    this.done = this.run().catch(e => console.error(`${this.name}: ${(e as Error).message}`));
  }

  private async run(): Promise<void> {
    const c = await Consumer.connect(this.bootstrap, this.group);
    const p = await Writer.connect(this.bootstrap);
    try {
      c.subscribe(this.topics);
      while (this.running) {
        const recs = await c.poll();
        for (const r of recs) {
          const e = CloudEvents.parse(r.value);
          const res = e.ok ? await this.handle(e.value) : e;
          if (res.ok) {
            for (const o of res.value) p.send(record(o));
            this.processed++;
          } else {
            p.send(deadLetter(this.dlq, this.group, r, res.error, new Date()));
            this.deadLettered++;
          }
        }
        if (recs.length) {
          await p.flush();
          await c.commit(nextOffsets(recs));
        }
      }
    } finally {
      await c.close();
      await p.close();
      console.log(`${this.name} stopped`);
    }
  }

  async stop(): Promise<void> {
    this.running = false;
    await this.done;
  }

  get stats() {
    return { processed: this.processed, deadLettered: this.deadLettered };
  }
}
