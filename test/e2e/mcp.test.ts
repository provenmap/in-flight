// The MCP server, end to end, over stdio and over HTTP: an agent breaks the schema, sees money at risk, patches,
// redrives, and reads it back to $0 with the last episode matching what was stuck.
import { setTimeout as sleep } from 'node:timers/promises';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';
import type { Transport } from '@modelcontextprotocol/sdk/shared/transport.js';
import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import { startServer, type Server } from '../../src/server/server.js';
import { control, settle, spawnSimulator } from './helpers.js';

const bootstrap = process.env.BOOTSTRAP_SERVERS ?? 'localhost:9092';
const simPort = 48090;

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Data = Record<string, any>;

async function call(client: Client, name: string, args: Record<string, unknown> = {}): Promise<{ data: Data; text: string; isError: boolean }> {
  const res = await client.callTool({ name, arguments: args });
  const text = (res.content as { type: string; text: string }[]).map(c => c.text).join('\n');
  return { data: (res.structuredContent ?? {}) as Data, text, isError: res.isError === true };
}

async function until<T>(f: () => Promise<T>, ok: (t: T) => boolean, ms = 60_000): Promise<T> {
  const end = Date.now() + ms;
  for (;;) {
    const t = await f();
    if (ok(t)) return t;
    if (Date.now() > end) throw new Error(`timed out; last ${JSON.stringify(t)}`);
    await sleep(500);
  }
}

describe.each([
  ['stdio', () => new StdioClientTransport({
    command: process.execPath,
    args: ['--import', 'tsx', 'src/cli.ts', 'mcp'],
    env: { ...process.env as Record<string, string>, BOOTSTRAP_SERVERS: bootstrap, SIMULATOR_URL: `http://localhost:${simPort}` },
    stderr: 'inherit',
  })],
  ['http', () => new StreamableHTTPClientTransport(new URL('http://localhost:48088/mcp'))],
] as const)('the MCP server over %s', (kind, transport: () => Transport) => {
  let sim: Awaited<ReturnType<typeof spawnSimulator>>;
  let server: Server | undefined;
  const client = new Client({ name: 'rif-e2e', version: '1.0.0' });

  beforeAll(async () => {
    sim = await spawnSimulator(bootstrap, simPort);
    await settle(sim.url);
    if (kind === 'http') server = await startServer({ bootstrap, port: 48088, simulatorUrl: sim.url });
    await client.connect(transport());
  });

  afterAll(async () => {
    await client.close();
    await server?.close();
    await control(sim.url, 'restore');
    await control(sim.url, 'unpatch');
    await sim?.close();
  });

  test('tools and the flow resource', async () => {
    const tools = (await client.listTools()).tools.map(t => t.name).sort();
    expect(tools).toEqual(['break_schema', 'last_episode', 'patch_payments', 'redrive', 'restore_schema', 'set_rate',
      'simulator_state', 'stuck_events', 'unpatch_payments', 'value_at_risk'].sort());
    const flow = JSON.parse(((await client.readResource({ uri: 'rif://flow' })).contents[0] as { text: string }).text);
    expect(flow.districts.map((d: Data) => d.id)).toEqual(['orders', 'payments', 'inventory', 'shipping', 'customers']);
    expect(flow.dlqs.map((d: Data) => d.dlq)).toEqual(['payments.dlq', 'inventory.dlq', 'shipping.dlq']);
  });

  test('break, see money at risk, patch, redrive, back to $0 with the episode matching', async () => {
    await until(() => call(client, 'value_at_risk'), r => r.data.observing === true && r.data.stuck.events === 0);
    expect((await call(client, 'simulator_state')).data.orderVersion).toBe(1);

    expect((await call(client, 'set_rate', { perSecond: 20 })).data.rate).toBe(20);
    const broke = await call(client, 'break_schema');
    expect(broke.isError).toBe(false);
    expect(broke.data.orderVersion).toBe(2);
    await sleep(4000);

    const jammed = await until(() => call(client, 'value_at_risk'), r => r.data.stuck.events > 20);
    expect(jammed.data.atRisk.value).toBeGreaterThan(0);
    expect(jammed.data.stuck.byDistrict.payments).toBeGreaterThan(0);
    expect(jammed.text).toMatch(/^\$[\d,.]+ at risk: [\d,]+ orders/);

    const stuck = await call(client, 'stuck_events', { district: 'payments', limit: 5 });
    expect(stuck.data.shown).toBe(5);
    expect(stuck.data.events[0]).toMatchObject({ dlq: 'payments.dlq', event: 'OrderPlaced@v2', error: "'total' is an object, expected a decimal string" });
    expect((await call(client, 'stuck_events', { district: 'inventory' })).data.total.events).toBe(0);

    expect((await call(client, 'patch_payments')).data.patched).toBe(true);
    await sleep(1500);
    const before = (await call(client, 'stuck_events', { limit: 3000 })).data.total;
    expect((await call(client, 'redrive')).isError).toBe(false);
    const again = await call(client, 'redrive');
    expect(again.isError).toBe(true);
    expect(again.text).toContain('a redrive is already running (409)');

    await call(client, 'set_rate', { perSecond: 8 });
    const drained = await until(() => call(client, 'value_at_risk'), r => r.data.stuck.events === 0 && r.data.atRisk.value === 0);
    expect(drained.text).toMatch(/^Nothing held up: \$0\.00 at risk\.\n/);
    const episode = (await call(client, 'last_episode')).data.lastEpisode;
    expect(episode.events).toBe(before.events);
    expect(episode.value).toBeCloseTo(before.value, 2);
    expect(episode.tookMs).toBeGreaterThan(4000);
    expect((await call(client, 'simulator_state')).data.redrive).toMatchObject({ running: false, redriven: before.events, remaining: 0, error: null });

    await call(client, 'restore_schema');
    await call(client, 'unpatch_payments');
  });

  test('a bad rate is refused the way the simulator refuses it', async () => {
    const res = await client.callTool({ name: 'set_rate', arguments: { perSecond: 900 } });
    expect(res.isError).toBe(true);
  });
});
