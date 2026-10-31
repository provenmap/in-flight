import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import fastifyStatic from '@fastify/static';
import fastifyWebsocket from '@fastify/websocket';
import Fastify from 'fastify';
import { registerMcpHttp } from '../mcp/http.js';
import { Observer, type ObserverOptions } from '../observer/observer.js';
import { Hub } from './hub.js';

export interface ServerOptions extends ObserverOptions {
  port?: number;
  host?: string;
  /** The built view. Defaults to `dist/ui`. */
  uiDir?: string;
}

export interface Server {
  url: string;
  observer: Observer;
  hub: Hub;
  close(): Promise<void>;
}

const here = (path: string) => fileURLToPath(new URL(path, import.meta.url));

/** The built view: `RIF_UI_DIR`, or `dist/ui` in the package. */
export function defaultUiDir(): string | null {
  for (const dir of [process.env.RIF_UI_DIR, here('../../dist/ui')]) {
    if (dir && existsSync(`${dir}/index.html`)) return dir;
  }
  return null;
}

/**
 * Prices every event in the flow, tracks money at risk, and streams it to the view, which it also serves.
 *
 *   GET  /                         the view
 *   WS   /ws                       a snapshot, then frames of event, dead, recovered and stats messages
 *   GET  /api/sim/state            the simulator's state
 *   POST /api/sim/{action}         break | restore | patch | unpatch | redrive | rate?perSecond=N, passed to the simulator
 *                                  (both 404 when the observer only watches)
 *   POST /mcp                      the MCP server, over Streamable HTTP
 */
export async function startServer(opts: ServerOptions = {}): Promise<Server> {
  const port = opts.port ?? Number(process.env.PORT ?? 8088);
  const host = opts.host ?? process.env.HOST ?? 'localhost';
  const observer = new Observer(opts).start();
  const hub = new Hub(observer.reader);
  observer.onFrame(frame => hub.publish(frame));

  const app = Fastify();
  await app.register(fastifyWebsocket);
  app.get('/ws', { websocket: true }, socket => hub.join(socket));

  const sim = observer.simulator;
  const noSimulator = { error: 'there is no simulator: this observer only watches' };
  app.get('/api/sim/state', async (_req, reply) => {
    if (!sim) return reply.code(404).send(noSimulator);
    const res = await sim.call('GET', '/state');
    return reply.code(res.status).type('application/json').send(res.body);
  });
  app.post<{ Params: { action: string } }>('/api/sim/:action', async (req, reply) => {
    if (!sim) return reply.code(404).send(noSimulator);
    const q = req.url.indexOf('?');
    const res = await sim.call('POST', `/${encodeURIComponent(req.params.action)}${q >= 0 ? req.url.slice(q) : ''}`);
    return reply.code(res.status).type('application/json').send(res.body);
  });

  registerMcpHttp(app, observer, host);

  const ui = opts.uiDir ?? defaultUiDir();
  if (ui) await app.register(fastifyStatic, { root: ui });
  else app.get('/', async (_req, reply) => reply.type('text/plain').send('The view is not built: run npm run build.\n'));

  await app.listen({ port, host });
  console.log(`observer: Kafka at ${observer.bootstrap}, ${sim ? `simulator at ${sim.url}` : `watching ${observer.catalog.flow} with no simulator`}, view on http://localhost:${port}`);

  return {
    url: `http://localhost:${port}`,
    observer,
    hub,
    async close() {
      hub.close();
      await app.close();
      await observer.close();
    },
  };
}
