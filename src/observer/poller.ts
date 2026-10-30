import { setTimeout as sleep } from 'node:timers/promises';
import type { Catalog, Json } from '../pricing/catalog.js';
import { Admin } from '../kafka.js';

/** The simulator's control API, as the observer, the view and the MCP server reach it. */
export class SimulatorClient {
  constructor(readonly url: string) {}

  /** The simulator's answer, or a 502 when it cannot be reached. */
  async call(method: 'GET' | 'POST', path: string, timeoutMs = 5000): Promise<{ status: number; body: string }> {
    try {
      const res = await fetch(`${this.url}${path}`, { method, signal: AbortSignal.timeout(timeoutMs) });
      return { status: res.status, body: await res.text() };
    } catch {
      return { status: 502, body: JSON.stringify({ error: `the simulator is not reachable at ${this.url}` }) };
    }
  }

  /** The knobs and counts, or null when the simulator is down. */
  async state(timeoutMs = 1000): Promise<Json | null> {
    const res = await this.call('GET', '/state', timeoutMs);
    if (res.status !== 200) return null;
    try {
      return JSON.parse(res.body) as Json;
    } catch {
      return null;
    }
  }
}

/** What the poller last saw: each service group's committed offsets, each partition's end, and the simulator's state. */
export interface Polled {
  committed: Map<string, Map<string, number>>;
  ends: Map<string, number>;
  sim: Json | null;
}

/**
 * Polls, once a second, what the reader cannot see in the records themselves: the service groups' committed
 * offsets, the end of every partition they read, and the simulator's knobs.
 */
export class Poller {
  latest: Polled | null = null;
  private running = true;
  private done: Promise<void> | null = null;

  constructor(private readonly bootstrap: string, private readonly catalog: Catalog, private readonly simulator: SimulatorClient) {}

  start(): void {
    this.done = this.run();
  }

  private async run(): Promise<void> {
    const admin = Admin.connect(this.bootstrap);
    try {
      while (this.running) {
        try {
          const groups = this.catalog.services.map(s => s.group);
          const committed = new Map(await Promise.all(groups.map(async g => [g, await admin.committed(g)] as const)));
          const topics = [...new Set(this.catalog.services.flatMap(s => s.reads))];
          const ends = await admin.ends(await admin.partitions(topics));
          this.latest = { committed, ends, sim: await this.simulator.state() };
        } catch (e) {
          console.error(`poller: ${(e as Error).message}`);
        }
        await sleep(1000);
      }
    } finally {
      admin.close();
    }
  }

  async stop(): Promise<void> {
    this.running = false;
    await this.done;
  }
}
