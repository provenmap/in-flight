// Watch mode: a flow of its own on topics of its own, written the way other producers write, with no simulator.
// Plain JSON events, Kafka Connect's dead-letter headers, a recovery with no redrive marker, and a dead letter no
// version in the catalog can price, recovered when its redrive group commits past it.
import { setTimeout as sleep } from 'node:timers/promises';
import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import WebSocket from 'ws';
import { Admin, Consumer, Writer } from '../../src/kafka.js';
import { Catalog } from '../../src/pricing/catalog.js';
import { startServer, type Server } from '../../src/server/server.js';

const bootstrap = process.env.BOOTSTRAP_SERVERS ?? 'localhost:9092';
const run = `w${Date.now().toString(36)}`;
const t = (name: string) => `${run}.${name}`;

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type M = Record<string, any>;

const catalog = Catalog.fromJson({
  flow: 'checkout',
  currency: 'USD',
  districts: [{ id: 'cart', name: 'Cart' }, { id: 'billing', name: 'Billing' }, { id: 'buyers', name: 'Buyers', sink: true }],
  events: [
    { type: 'CartCheckedOut', version: 1, topic: t('carts'), district: 'cart', value: 'price', customer: 'buyer', order: 'cartId' },
    { type: 'InvoicePaid', version: 1, topic: t('invoices'), district: 'billing', value: 'paid', customer: 'buyer', order: 'cartId' },
  ],
  consumers: [
    { group: `${run}-billing`, district: 'billing', reads: [t('carts')], dlq: t('billing.dlq'), redrive: `${run}-billing-redrive` },
    { district: 'buyers', reads: [t('invoices')] },
  ],
});

describe('watching a flow with no simulator', () => {
  let server: Server;
  let writer: Writer;
  const msgs: M[] = [];
  let ws: WebSocket;
  const stats = (): M => msgs.filter(m => m.type === 'stats').at(-1) ?? {};
  const until = async (pred: () => boolean, ms = 20_000) => {
    const end = Date.now() + ms;
    while (!pred()) {
      if (Date.now() > end) throw new Error(`timed out; last stats ${JSON.stringify(stats())}`);
      await sleep(200);
    }
  };

  beforeAll(async () => {
    if (!catalog.ok) throw new Error(catalog.error.join('; '));
    const admin = Admin.connect(bootstrap);
    await admin.ensureTopics([t('carts'), t('invoices'), t('billing.dlq')], 1);
    admin.close();
    writer = await Writer.connect(bootstrap);
    server = await startServer({ bootstrap, port: 58088, catalog: catalog.value, simulatorUrl: null });
    ws = new WebSocket('ws://localhost:58088/ws');
    ws.on('message', d => msgs.push(...(JSON.parse(d.toString()) as M[])));
    await until(() => msgs.some(m => m.type === 'snapshot'));
  });

  afterAll(async () => {
    ws?.close();
    await server?.close();
    await writer?.close();
    const admin = Admin.connect(bootstrap);
    await admin.deleteTopics([t('carts'), t('invoices'), t('billing.dlq')]).catch(() => undefined);
    admin.close();
  });

  test('the snapshot says it watches, and the controls are not there', async () => {
    const snap = msgs.find(m => m.type === 'snapshot')!;
    expect(snap.mode).toBe('watch');
    expect(snap.flow.name).toBe('checkout');
    expect(snap.stats.sim).toBeNull();
    expect((await fetch('http://localhost:58088/api/sim/state')).status).toBe(404);
  });

  test('plain JSON flows, a Kafka Connect dead letter is stuck, and the district producing for that order recovers it', async () => {
    const cart = (id: string, price: unknown) => JSON.stringify({ cartId: id, buyer: `b-${id}`, price });
    writer.send({ topic: t('carts'), key: 'k1', value: cart('k1', '25.00') });
    writer.send({ topic: t('billing.dlq'), key: 'k2', value: cart('k2', '40.00'), headers: {
      '__connect.errors.topic': t('carts'), '__connect.errors.exception.message': 'Unknown currency',
    } });
    await writer.flush();
    await until(() => msgs.some(m => m.type === 'event' && m.orderId === 'k1') && stats().stuck?.events === 1);
    const dead = msgs.find(m => m.type === 'dead')!;
    expect([dead.event, dead.district, dead.error, dead.value, dead.priced]).toEqual(['CartCheckedOut@v1', 'billing', 'Unknown currency', 40, true]);
    expect(stats().atRisk.value).toBe(40);

    writer.send({ topic: t('invoices'), key: 'k2', value: JSON.stringify({ cartId: 'k2', buyer: 'b-k2', paid: '40.00' }) });
    await writer.flush();
    await until(() => stats().stuck?.events === 0);
    expect(msgs.some(m => m.type === 'recovered' && m.id === dead.id)).toBe(true);
    expect(stats().lastEpisode.value).toBe(40);
  });

  test('a dead letter nothing in the catalog can price is stuck with its value unknown, until the redrive group passes it', async () => {
    writer.send({ topic: t('billing.dlq'), key: 'k3', value: 'not json at all', headers: { '__connect.errors.exception.message': 'Bad bytes' } });
    await writer.flush();
    await until(() => stats().stuck?.unpriced === 1);
    expect(stats().atRisk).toMatchObject({ value: 0, unpriced: 1 });
    const dead = msgs.filter(m => m.type === 'dead').at(-1)!;
    expect([dead.event, dead.priced]).toEqual(['an unreadable record', false]);

    // A redrive tool of one's own, as far as the observer can tell: its group commits past the dead letter.
    const redrive = await Consumer.connect(bootstrap, `${run}-billing-redrive`);
    await redrive.commit([{ topic: t('billing.dlq'), partition: 0, offset: Number(dead.id.split(':').at(-1)) + 1 }]);
    await redrive.close();
    await until(() => stats().stuck?.events === 0);
    expect(msgs.some(m => m.type === 'recovered' && m.id === dead.id)).toBe(true);
  });
});
