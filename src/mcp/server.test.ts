import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import Fastify from 'fastify';
import { afterEach, describe, expect, test } from 'vitest';
import { Observer } from '../observer/observer.js';
import { registerMcpHttp } from './http.js';
import { createMcpServer } from './server.js';

// An observer that is never started: no Kafka, an empty ledger, and a simulator nobody runs.
const observer = () => new Observer({ bootstrap: 'localhost:1', simulatorUrl: 'http://localhost:1' });

async function connected() {
  const [a, b] = InMemoryTransport.createLinkedPair();
  const client = new Client({ name: 'test', version: '1.0.0' });
  await createMcpServer(observer()).connect(a);
  await client.connect(b);
  return client;
}

const text = (r: object) => ((r as { content?: { text: string }[] }).content ?? []).map(c => c.text).join('\n');

describe('the MCP server', () => {
  test('an empty ledger is $0 at risk, and says it is not observing yet', async () => {
    const c = await connected();
    const r = await c.callTool({ name: 'value_at_risk' });
    expect(text(r)).toMatch(/^Nothing held up: \$0.00 at risk\. The observer is still connecting/);
    expect(r.structuredContent).toMatchObject({ atRisk: { value: 0, orders: 0, customers: 0 }, observing: false });
    expect(text(await c.callTool({ name: 'last_episode' }))).toBe('No recovery yet since the observer started.\n\n{\n  "lastEpisode": null\n}');
  });

  test('controls fail with the 502 when the simulator is down', async () => {
    const c = await connected();
    for (const name of ['break_schema', 'redrive', 'simulator_state']) {
      const r = await c.callTool({ name });
      expect(r.isError, name).toBe(true);
      expect(text(r)).toContain('the simulator is not reachable at http://localhost:1');
    }
  });

  test('tool inputs are validated', async () => {
    const c = await connected();
    expect((await c.callTool({ name: 'set_rate', arguments: { perSecond: -1 } })).isError).toBe(true);
    expect((await c.callTool({ name: 'stuck_events', arguments: { limit: 0 } })).isError).toBe(true);
  });
});

describe('MCP over HTTP', () => {
  const saved = process.env.RIF_MCP_TOKEN;
  afterEach(() => {
    if (saved === undefined) delete process.env.RIF_MCP_TOKEN;
    else process.env.RIF_MCP_TOKEN = saved;
  });

  const init = { jsonrpc: '2.0', id: 1, method: 'initialize', params: { protocolVersion: '2025-06-18', capabilities: {}, clientInfo: { name: 't', version: '1' } } };
  const headers = { 'content-type': 'application/json', accept: 'application/json, text/event-stream' };

  function app(bindHost: string) {
    const a = Fastify();
    registerMcpHttp(a, observer(), bindHost);
    return a;
  }

  test('on localhost it answers requests to localhost without a token', async () => {
    delete process.env.RIF_MCP_TOKEN;
    const res = await app('localhost').inject({ method: 'POST', url: '/mcp', headers: { ...headers, host: 'localhost:8088' }, payload: init });
    expect(res.statusCode).toBe(200);
    expect(res.json().result.serverInfo.name).toBe('revenue-in-flight');
  });

  test('on localhost it refuses other hosts and origins, which stops DNS rebinding', async () => {
    delete process.env.RIF_MCP_TOKEN;
    const a = app('localhost');
    expect((await a.inject({ method: 'POST', url: '/mcp', headers: { ...headers, host: 'evil.example:8088' }, payload: init })).statusCode).toBe(403);
    expect((await a.inject({ method: 'POST', url: '/mcp', headers: { ...headers, host: 'localhost:8088', origin: 'https://evil.example' }, payload: init })).statusCode).toBe(403);
  });

  test('beyond localhost it is off without a token, and needs the token once one is set', async () => {
    delete process.env.RIF_MCP_TOKEN;
    expect((await app('0.0.0.0').inject({ method: 'POST', url: '/mcp', headers: { ...headers, host: 'demo.example' }, payload: init })).statusCode).toBe(403);
    process.env.RIF_MCP_TOKEN = 's3cret';
    const a = app('0.0.0.0');
    expect((await a.inject({ method: 'POST', url: '/mcp', headers: { ...headers, host: 'demo.example' }, payload: init })).statusCode).toBe(401);
    expect((await a.inject({ method: 'POST', url: '/mcp', headers: { ...headers, host: 'demo.example', authorization: 'Bearer nope' }, payload: init })).statusCode).toBe(401);
    expect((await a.inject({ method: 'POST', url: '/mcp', headers: { ...headers, host: 'demo.example', authorization: 'Bearer s3cret' }, payload: init })).statusCode).toBe(200);
  });
});
