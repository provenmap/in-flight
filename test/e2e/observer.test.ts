// Milestone 3: money at risk, as a viewer of the observer's WebSocket sees it. A healthy flow reads $0, a jam counts
// every stuck order, patch and redrive recover them all into one episode, and a restarted observer counts them as
// recovered. Then the flow at 500 orders a second, to see the observer keep up.
import { setTimeout as sleep } from 'node:timers/promises';
import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import WebSocket from 'ws';
import { Admin } from '../../src/kafka.js';
import { startServer, type Server } from '../../src/server/server.js';
import { control, settle, spawnSimulator, state } from './helpers.js';

const bootstrap = process.env.BOOTSTRAP_SERVERS ?? 'localhost:9092';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type M = Record<string, any>;

/** A viewer: every message it was sent, in order. */
class Viewer {
  readonly msgs: M[] = [];
  readonly ws: WebSocket;
  readonly open: Promise<void>;

  constructor(url: string) {
    this.ws = new WebSocket(url.replace('http', 'ws') + '/ws');
    this.ws.on('message', d => this.msgs.push(...(JSON.parse(d.toString()) as M[])));
    this.open = new Promise(resolve => this.ws.once('open', () => resolve()));
  }

  of(type: string): M[] {
    return this.msgs.filter(m => m.type === type);
  }

  get stats(): M | undefined {
    return this.of('stats').at(-1);
  }

  async until(pred: (v: Viewer) => boolean, ms = 30_000): Promise<void> {
    const end = Date.now() + ms;
    while (!pred(this)) {
      if (Date.now() > end) throw new Error(`timed out; last stats ${JSON.stringify(this.stats)}`);
      await sleep(200);
    }
  }

  close(): void {
    this.ws.close();
  }
}

describe('the observer', () => {
  let sim: Awaited<ReturnType<typeof spawnSimulator>>;
  let server: Server;
  const start = () => startServer({ bootstrap, port: 38088, simulatorUrl: 'http://localhost:38090' });

  beforeAll(async () => {
    sim = await spawnSimulator(bootstrap, 38090);
    await settle(sim.url);
    server = await start();
  });

  afterAll(async () => {
    await server?.close();
    await sim?.close();
  });

  test('a new viewer gets a snapshot first, with the flow', async () => {
    const v = new Viewer(server.url);
    await v.until(x => x.msgs.length > 0);
    expect(v.msgs[0]!.type).toBe('snapshot');
    expect(v.msgs[0]!.flow.districts.map((d: M) => d.id)).toEqual(['orders', 'payments', 'inventory', 'shipping', 'customers']);
    expect(v.msgs[0]!.flow.topics.find((t: M) => t.topic === 'orders.placed').to).toEqual(['payments', 'inventory']);
    v.close();
  });

  test('a healthy flow reads $0 at risk, nothing stuck', async () => {
    await control(sim.url, 'rate', '?perSecond=8');
    const v = new Viewer(server.url);
    await v.until(x => x.of('stats').length >= 12 && x.of('event').length > 20);
    const stats = v.of('stats').slice(2);
    expect(stats.every(s => s.stuck.events === 0)).toBe(true);
    // Lag is bounded by the polled end, so a flowing pipeline mostly reads exactly $0.
    expect(stats.filter(s => s.atRisk.value === 0).length).toBeGreaterThanOrEqual(stats.length / 2);
    expect(v.of('event').find(e => e.topic === 'shipping.dispatched')?.to).toEqual(['customers']);
    v.close();
  });

  test('a jam counts every stuck order; patch and redrive recover them all in one episode', async () => {
    const v = new Viewer(server.url);
    await v.until(x => !!x.stats);
    await control(sim.url, 'rate', '?perSecond=20');
    await control(sim.url, 'break');
    await sleep(4000);
    await control(sim.url, 'restore');
    await v.until(x => x.stats!.stuck.events > 0 && x.stats!.stuck.events === x.of('dead').length && x.stats!.sim.services.payments.processed > 0);
    await sleep(1500);
    const jammed = v.stats!;
    const dead = v.of('dead');
    expect(dead.length).toBeGreaterThan(20);
    expect(jammed.stuck.events).toBe(dead.length);
    expect(jammed.stuck.byDistrict.payments).toBeCloseTo(dead.reduce((a, d) => a + d.value, 0), 2);
    expect(jammed.atRisk.value).toBeGreaterThanOrEqual(jammed.stuck.value - 0.005);
    expect(dead[0]).toMatchObject({ dlq: 'payments.dlq', district: 'payments', event: 'OrderPlaced@v2', error: "'total' is an object, expected a decimal string" });

    await control(sim.url, 'patch');
    await control(sim.url, 'redrive');
    await v.until(x => x.of('recovered').length === dead.length && x.stats!.stuck.events === 0 && !!x.stats!.lastEpisode);
    const done = v.stats!;
    expect(done.stuck.value).toBe(0);
    expect(done.lastEpisode.events).toBe(dead.length);
    expect(done.lastEpisode.value).toBeCloseTo(jammed.stuck.value, 2);
    expect(new Set(v.of('recovered').map(r => r.id))).toEqual(new Set(dead.map(d => d.id)));
    expect(v.of('recovered').every(r => r.afterMs > 0)).toBe(true);
    expect(v.of('event').filter(e => e.redrive)).toHaveLength(dead.length);
    await control(sim.url, 'rate', '?perSecond=8');
    await v.until(x => x.stats!.atRisk.value === 0);
    await control(sim.url, 'unpatch');
    v.close();
  });

  test('a restarted observer counts every dead letter as recovered, and nothing as stuck', async () => {
    await server.close();
    server = await start();
    const admin = Admin.connect(bootstrap);
    const ends = await admin.ends(await admin.partitions(['payments.dlq']));
    admin.close();
    const all = [...ends.values()].reduce((a, b) => a + b, 0);

    const v = new Viewer(server.url);
    await v.until(x => x.msgs.length > 0);
    await v.until(x => (x.stats ?? x.msgs[0]!.stats).recovered.events >= all);
    const s = v.stats ?? v.msgs[0]!.stats;
    expect(s.stuck.events).toBe(0);
    expect(s.recovered.events).toBe(all);
    expect([...v.msgs[0]!.stuck, ...v.of('dead')]).toEqual([]);
    expect(v.of('recovered').every(r => r.afterMs === null)).toBe(true);
    v.close();
  });

  test('at 500 orders a second the observer keeps up', async () => {
    const v = new Viewer(server.url);
    await v.open;
    await v.until(x => !!x.stats);
    const placedBefore = (await state(sim.url)).placed;
    await control(sim.url, 'rate', '?perSecond=500');
    const from = v.msgs.length;
    await sleep(5000);
    await control(sim.url, 'rate', '?perSecond=0');
    const placed = (await state(sim.url)).placed - placedBefore;
    await sleep(1500);
    const seen = v.msgs.slice(from).filter(m => m.type === 'event' && m.topic === 'orders.placed').length;
    const statsFrames = v.msgs.slice(from).filter(m => m.type === 'stats').length;
    console.log(`500/s for 5 s: ${placed} orders placed, ${seen} seen by the viewer, ${statsFrames} stats messages`);
    expect(placed).toBeGreaterThan(2000);
    expect(seen).toBeGreaterThanOrEqual(placed * 0.95);
    expect(statsFrames).toBeGreaterThanOrEqual(10);
    // Payments charges one order at a time, so the flood is real lag, valued from what the observer saw.
    expect(v.of('stats').some(s => s.lag.byGroup['payments-service'].events > 500 && s.lag.byGroup['payments-service'].value > 0)).toBe(true);

    // Leave the pipeline drained for whoever runs next.
    for (let i = 0; i < 150; i++) {
      const s = await state(sim.url);
      if (s.services.payments.processed + s.services.payments.deadLettered >= s.placed) break;
      await sleep(1000);
    }
    await control(sim.url, 'rate', '?perSecond=8');
    v.close();
  }, 240_000);
});
