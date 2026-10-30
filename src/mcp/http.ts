import { timingSafeEqual } from 'node:crypto';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import type { FastifyInstance } from 'fastify';
import type { Observer } from '../observer/observer.js';
import { createMcpServer } from './server.js';

const loopback = new Set(['localhost', '127.0.0.1', '::1', '[::1]']);

const hostname = (hostHeader: string): string => (hostHeader.startsWith('[') ? hostHeader.slice(0, hostHeader.indexOf(']') + 1) : hostHeader.split(':')[0]!);

function sameToken(given: string, want: string): boolean {
  const a = Buffer.from(given), b = Buffer.from(want);
  return a.length === b.length && timingSafeEqual(a, b);
}

/**
 * MCP over Streamable HTTP on `/mcp`, stateless: each request gets a fresh server on the shared observer.
 *
 * Bound to localhost it needs no token, but only answers requests addressed to localhost, which stops a web page from
 * reaching it through DNS rebinding. Bound beyond localhost it needs `RIF_MCP_TOKEN` as a bearer token, and is off
 * until one is set. A token, once set, is always required.
 */
export function registerMcpHttp(app: FastifyInstance, observer: Observer, bindHost: string): void {
  const token = process.env.RIF_MCP_TOKEN || null;
  const local = loopback.has(bindHost);

  app.addHook('onRequest', async (req, reply) => {
    if (req.url !== '/mcp' && !req.url.startsWith('/mcp?')) return;
    if (!token && !local) {
      return reply.code(403).send({ error: 'MCP over HTTP is off when the server listens beyond localhost: set RIF_MCP_TOKEN' });
    }
    if (local && !loopback.has(hostname(req.headers.host ?? ''))) {
      return reply.code(403).send({ error: 'MCP over HTTP answers only requests to localhost' });
    }
    const origin = req.headers.origin;
    if (local && origin && !loopback.has(new URL(origin).hostname)) {
      return reply.code(403).send({ error: 'MCP over HTTP refuses requests from other origins' });
    }
    if (token) {
      const auth = req.headers.authorization ?? '';
      if (!auth.startsWith('Bearer ') || !sameToken(auth.slice(7), token)) {
        return reply.code(401).header('www-authenticate', 'Bearer').send({ error: 'a bearer token is required' });
      }
    }
  });

  app.all('/mcp', async (req, reply) => {
    const mcp = createMcpServer(observer);
    const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined, enableJsonResponse: true });
    reply.hijack();
    reply.raw.on('close', () => {
      void transport.close();
      void mcp.close();
    });
    await mcp.connect(transport);
    await transport.handleRequest(req.raw, reply.raw, req.body);
  });
}
