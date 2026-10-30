import { randomInt } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import Fastify, { type FastifyInstance } from 'fastify';
import { Admin, Writer } from '../kafka.js';
import { defaultCatalog } from '../pricing/catalog.js';
import { Emitter, record } from './events.js';
import { Service } from './loop.js';
import { Orders, unseeded } from './orders.js';
import { Redrive, idle, type RedriveStatus } from './redrive.js';
import { Inventory, Payments, Shipping } from './services.js';

/** The knobs the control API turns. */
export class Controls {
  orderVersion = 1;
  patched = false;
  rate = 8;
  redrive: RedriveStatus = idle;
}

/** The orders team at work: orders arrive as a Poisson process at `rate` a second, in the current schema version. */
class Traffic {
  placed = 0;
  private running = true;
  private done: Promise<void> | null = null;

  constructor(private readonly bootstrap: string, private readonly controls: Controls) {}

  start(): void {
    this.done = this.run().catch(e => console.error(`traffic: ${(e as Error).message}`));
  }

  private async run(): Promise<void> {
    const p = await Writer.connect(this.bootstrap);
    const emit = new Emitter('/orders');
    const rng = unseeded();
    // Gaps are scheduled against the clock, not slept one by one, so timer granularity does not slow fast traffic.
    let due = performance.now();
    try {
      while (this.running) {
        const rate = this.controls.rate;
        if (rate <= 0) {
          await sleep(200);
          due = performance.now();
          continue;
        }
        p.send(record(Orders.placed(emit, rng, this.controls.orderVersion, new Date())));
        this.placed++;
        due += Math.min((-Math.log(1 - rng.next()) / rate) * 1000, 5000);
        const wait = due - performance.now();
        if (wait > 0) await sleep(wait);
        else {
          if (wait < -1000) due = performance.now(); // fell far behind: carry on from now, no burst
          await new Promise(setImmediate);
        }
      }
    } finally {
      await p.close();
    }
  }

  async stop(): Promise<void> {
    this.running = false;
    await this.done;
  }
}

export interface SimulatorOptions {
  bootstrap?: string;
  port?: number;
  host?: string;
  /** Orders a second at start. */
  rate?: number;
}

export interface Simulator {
  controls: Controls;
  url: string;
  close(): Promise<void>;
}

/**
 * Order traffic and the three services, with a control API on port 8090:
 *
 *   GET  /state                   the knobs, each service's counts and the last redrive
 *   POST /break | /restore        orders switch to OrderPlaced v2 | back to v1
 *   POST /patch | /unpatch        payments reads both shapes | only v1
 *   POST /redrive                 one pass over payments.dlq with the current payments code
 *   POST /rate?perSecond=N        order traffic
 */
export async function startSimulator(opts: SimulatorOptions = {}): Promise<Simulator> {
  const bootstrap = opts.bootstrap ?? process.env.BOOTSTRAP_SERVERS ?? 'localhost:9092';
  const port = opts.port ?? Number(process.env.CONTROL_PORT ?? 8090);
  const host = opts.host ?? process.env.HOST ?? 'localhost';
  const catalog = defaultCatalog();

  const admin = Admin.connect(bootstrap);
  try {
    await admin.ensureTopics([...catalog.topics, ...catalog.dlqs]);
    // Two simulators on one Kafka would split the services' partitions between them, and one's redrive would drain
    // the other's dead letters. A group with members means another simulator is attached.
    for (const group of catalog.services.map(s => s.group)) {
      const members = await admin.members(group);
      if (members) {
        throw new Error(`another simulator is attached to the Kafka at ${bootstrap}: consumer group '${group}' has ${members} ` +
          'member(s). Stop it, or use another Kafka. A simulator that was killed rather than stopped holds its groups ' +
          'until their 45 s session timeout.');
      }
    }
  } finally {
    admin.close();
  }

  const controls = new Controls();
  if (opts.rate !== undefined) controls.rate = opts.rate;
  const traffic = new Traffic(bootstrap, controls);
  const paymentsE = new Emitter('/payments');
  const payments = new Service('payments-service', bootstrap, Payments.Group, [Orders.Topic], Payments.Dlq, async e => {
    // An external card authorisation takes a few milliseconds; a flood of orders builds real lag.
    await sleep(10 + randomInt(15));
    return Payments.handle(paymentsE, e, controls.patched, new Date());
  });
  const inventoryE = new Emitter('/inventory');
  const inventory = new Service('inventory-service', bootstrap, Inventory.Group, [Orders.Topic], Inventory.Dlq, e =>
    Inventory.handle(inventoryE, e, new Date()));
  const shipping = new Shipping(new Emitter('/shipping'));
  const shipper = new Service('shipping-service', bootstrap, Shipping.Group, Shipping.Reads, Shipping.Dlq, e => shipping.handle(e, new Date()));
  const services = { payments, inventory, shipping: shipper };

  const redriveGroup = catalog.redriveGroupOf(Payments.Dlq);
  if (!redriveGroup) throw new Error(`the catalog names no redrive group for ${Payments.Dlq}`);
  const redrive = new Redrive(bootstrap, Payments.Dlq, redriveGroup, e => Payments.handle(paymentsE, e, controls.patched, new Date(), true));
  let redriving: Promise<void> = Promise.resolve();

  traffic.start();
  for (const s of Object.values(services)) s.start();

  const state = () => ({
    orderVersion: controls.orderVersion,
    patched: controls.patched,
    rate: controls.rate,
    placed: traffic.placed,
    services: Object.fromEntries(Object.entries(services).map(([n, s]) => [n, s.stats])),
    shippingWaiting: shipping.pending,
    redrive: controls.redrive,
  });

  function startRedrive(): string | null {
    if (controls.redrive.running) return 'a redrive is already running';
    controls.redrive = { running: true, redriven: 0, remaining: 0, error: null };
    redriving = redrive.run(s => { controls.redrive = s; })
      .catch((e: unknown): RedriveStatus => ({ running: false, redriven: 0, remaining: 0, error: String(e) }))
      .then(result => {
        controls.redrive = result;
        console.log(`redrive: ${result.redriven} redriven, ${result.remaining} remaining${result.error ? `, stopped at ${result.error}` : ''}`);
      });
    return null;
  }

  type Outcome = { code: number; error: string } | null;
  const actions: Record<string, (query: Record<string, string>) => Outcome> = {
    '/break': () => { controls.orderVersion = 2; return null; },
    '/restore': () => { controls.orderVersion = 1; return null; },
    '/patch': () => { controls.patched = true; return null; },
    '/unpatch': () => { controls.patched = false; return null; },
    '/redrive': () => {
      const busy = startRedrive();
      return busy ? { code: 409, error: busy } : null;
    },
    '/rate': query => {
      const r = Number(query.perSecond);
      if (query.perSecond === undefined || query.perSecond === '' || !(r >= 0 && r <= 500)) {
        return { code: 400, error: 'rate needs perSecond between 0 and 500' };
      }
      controls.rate = r;
      return null;
    },
  };

  const app: FastifyInstance = Fastify();
  for (const path of ['/state', ...Object.keys(actions)]) {
    app.all(path, async (req, reply) => {
      const query = req.query as Record<string, string>;
      const ok = path === '/state' ? req.method === 'GET' : req.method === 'POST';
      const outcome: Outcome = !ok ? { code: 405, error: `${req.method} not allowed` } : path === '/state' ? null : actions[path]!(query);
      if (req.method === 'POST' && !outcome) {
        console.log(`control: ${path} ${Object.entries(query).map(([k, v]) => `${k}=${v}`).join(' ')}`.trim());
      }
      return outcome ? reply.code(outcome.code).send({ error: outcome.error }) : reply.send(state());
    });
  }
  app.setNotFoundHandler((req, reply) => reply.code(404).send({ error: `no such endpoint ${req.url.split('?')[0]}` }));
  await app.listen({ port, host });
  console.log(`simulator: Kafka at ${bootstrap}, ${controls.rate} orders/s, control API on http://localhost:${port}`);

  return {
    controls,
    url: `http://localhost:${port}`,
    async close() {
      await app.close();
      await traffic.stop();
      await Promise.all(Object.values(services).map(s => s.stop()));
      await redriving;
    },
  };
}
