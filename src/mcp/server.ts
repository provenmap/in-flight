import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import { z } from 'zod';
import type { Observer } from '../observer/observer.js';
import { stuckJson } from '../observer/ledger.js';

const usd = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' });
const count = (n: number, noun: string) => `${n.toLocaleString('en-US')} ${noun}${n === 1 ? '' : 's'}`;

/** A tool answer: a line for a person to read, and the same facts as data. */
function answer(line: string, data: Record<string, unknown>): CallToolResult {
  return { content: [{ type: 'text', text: `${line}\n\n${JSON.stringify(data, null, 2)}` }], structuredContent: data };
}

function failure(text: string): CallToolResult {
  return { content: [{ type: 'text', text }], isError: true };
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type StatsJson = Record<string, any>;

/**
 * The MCP server: what the view shows and the controls it has, on the same observer and the same simulator API,
 * never by driving the view.
 */
export function createMcpServer(observer: Observer, version = '0.1.0'): McpServer {
  const mcp = new McpServer(
    { name: 'revenue-in-flight', version },
    {
      instructions:
        'Order-to-cash, live from Kafka. value_at_risk is the headline: money held up in dead-letter queues (stuck) and in ' +
        'consumer lag. The demo: break_schema ships OrderPlaced v2, which payments cannot read, so orders pile up stuck; ' +
        'patch_payments makes payments read both shapes; redrive replays the dead letters; last_episode reports the recovery.',
    },
  );
  const stats = (): StatsJson => observer.reader.statsJson();
  const notReady = () => (observer.reader.reading ? null : 'The observer is still connecting to Kafka; its figures will start at zero.');

  mcp.registerTool('value_at_risk', {
    title: 'Money at risk',
    description: 'The headline: money, orders and customers held up right now, split into stuck (dead-lettered, not yet recovered) and consumer lag, by district and by consumer group.',
    annotations: { readOnlyHint: true },
  }, async () => {
    const s = stats();
    const data = {
      atRisk: s.atRisk,
      stuck: s.stuck,
      lag: s.lag,
      recovered: s.recovered,
      observing: observer.reader.reading,
    };
    const line = s.atRisk.orders
      ? `${usd.format(s.atRisk.value)} at risk: ${count(s.atRisk.orders, 'order')}, ${count(s.atRisk.customers, 'customer')} waiting (stuck ${usd.format(s.stuck.value)}, in lag ${usd.format(s.lag.value)}).`
      : 'Nothing held up: $0.00 at risk.';
    return answer([line, notReady()].filter(Boolean).join(' '), data);
  });

  mcp.registerTool('stuck_events', {
    title: 'Stuck events',
    description: 'The dead letters still stuck, newest last, each with its value, customer, order and the error that stopped it. Filter by the district that dead-lettered it.',
    inputSchema: {
      district: z.string().optional().describe('Only dead letters from this district, such as payments'),
      limit: z.number().int().min(1).max(3000).optional().describe('At most this many, the newest (default 100)'),
    },
    annotations: { readOnlyHint: true },
  }, async ({ district, limit }) => {
    const all = observer.reader.stuck().filter(s => !district || s.district === district);
    const shown = all.slice(-(limit ?? 100));
    const total = all.reduce((sum, s) => sum + Number(s.priced.value.toString()), 0);
    const data = { total: { events: all.length, value: Math.round(total * 100) / 100 }, shown: shown.length, events: shown.map(stuckJson) };
    const line = all.length
      ? `${count(all.length, 'event')} stuck${district ? ` at ${district}` : ''}, worth ${usd.format(total)}; showing the newest ${shown.length}.`
      : `Nothing stuck${district ? ` at ${district}` : ''}.`;
    return answer(line, data);
  });

  mcp.registerTool('last_episode', {
    title: 'Last recovery',
    description: 'The latest stuck spell that was fully recovered: the money and events it held, and how long from the first event stuck to the last one recovered.',
    annotations: { readOnlyHint: true },
  }, async () => {
    const e = stats().lastEpisode as { value: number; events: number; tookMs: number } | null;
    if (!e) return answer('No recovery yet since the observer started.', { lastEpisode: null });
    return answer(`Recovered ${usd.format(e.value)}: ${count(e.events, 'order')}, stuck ${(e.tookMs / 1000).toFixed(1)} s.`, { lastEpisode: e });
  });

  mcp.registerResource('flow', 'rif://flow', {
    title: 'The order-to-cash flow',
    description: 'Districts, topics, which district produces each topic and which read it, the event types on each, and the dead-letter queues.',
    mimeType: 'application/json',
  }, async uri => ({ contents: [{ uri: uri.href, mimeType: 'application/json', text: JSON.stringify(observer.flow, null, 2) }] }));

  mcp.registerTool('simulator_state', {
    title: 'Simulator state',
    description: 'The demo system: order traffic rate, which OrderPlaced version the orders team ships (2 is the breaking one), whether payments is patched, each service\'s counts, and the last redrive.',
    annotations: { readOnlyHint: true },
  }, async () => {
    const res = await observer.simulator.call('GET', '/state');
    if (res.status !== 200) return failure(JSON.parse(res.body).error ?? `the simulator answered ${res.status}`);
    const s = JSON.parse(res.body);
    const line = `OrderPlaced v${s.orderVersion}${s.orderVersion > 1 ? ' (broken for payments unless patched)' : ''}, payments ${s.patched ? 'patched' : 'strict'}, ` +
      `${s.rate} orders/s${s.redrive.running ? ', redrive running' : ''}.`;
    return answer(line, s);
  });

  const control = (name: string, action: string, title: string, description: string, line: (s: StatsJson) => string) =>
    mcp.registerTool(name, { title, description, annotations: { readOnlyHint: false, idempotentHint: action !== 'redrive' } }, async () => {
      const res = await observer.simulator.call('POST', `/${action}`);
      const body = JSON.parse(res.body);
      if (res.status !== 200) return failure(`${body.error ?? `the simulator answered ${res.status}`} (${res.status})`);
      return answer(line(body), body);
    });

  control('break_schema', 'break', 'Ship OrderPlaced v2',
    'The orders team ships OrderPlaced v2, which writes total as { amount, currency }. Payments cannot read it and dead-letters every new order; inventory keeps working.',
    () => 'Orders now ship OrderPlaced v2. Unpatched payments dead-letters every one.');
  control('restore_schema', 'restore', 'Back to OrderPlaced v1', 'The orders team goes back to OrderPlaced v1.', () => 'Orders ship OrderPlaced v1 again.');
  control('patch_payments', 'patch', 'Patch payments', 'Payments learns to read both OrderPlaced shapes. New orders flow again; the dead letters stay stuck until a redrive.',
    () => 'Payments reads v1 and v2. Dead letters already stuck need a redrive.');
  control('unpatch_payments', 'unpatch', 'Unpatch payments', 'Payments goes back to reading only OrderPlaced v1.', () => 'Payments reads only v1 again.');
  control('redrive', 'redrive', 'Redrive the payments DLQ',
    'One pass over payments.dlq with the current payments code, up to its end when the pass starts. It stops a partition at the first record that still fails, without moving its offset. Fails with 409 while one is running.',
    () => 'Redrive started. Watch value_at_risk drain and read last_episode when it reaches $0.');

  mcp.registerTool('set_rate', {
    title: 'Set order traffic',
    description: 'Orders placed per second, 0 to 500.',
    inputSchema: { perSecond: z.number().min(0).max(500).describe('Orders a second') },
    annotations: { readOnlyHint: false, idempotentHint: true },
  }, async ({ perSecond }) => {
    const res = await observer.simulator.call('POST', `/rate?perSecond=${perSecond}`);
    const body = JSON.parse(res.body);
    if (res.status !== 200) return failure(`${body.error ?? `the simulator answered ${res.status}`} (${res.status})`);
    return answer(`Order traffic set to ${perSecond}/s.`, body);
  });

  return mcp;
}
