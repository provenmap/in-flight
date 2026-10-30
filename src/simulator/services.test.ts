import Big from 'big.js';
import { describe, expect, test } from 'vitest';
import type { JsonObject } from '../pricing/catalog.js';
import type { CloudEvent } from '../pricing/cloudevents.js';
import { defaultPricing } from '../pricing/pricing.js';
import { Emitter, formatMoney, javaHash, scalaStringHash } from './events.js';
import { Orders, seeded } from './orders.js';
import { Inventory, Payments, Shipping } from './services.js';

const at = new Date('2026-10-30T12:00:00Z');
const plus = (d: Date, s: number) => new Date(d.getTime() + s * 1000);
const emit = new Emitter('/test');
const pricing = defaultPricing();

const order = (version: number, seed = 1): CloudEvent => Orders.placed(emit, seeded(seed), version, at).event;

function first(pred: (id: string) => boolean): string {
  for (let i = 1; ; i++) if (pred(`ord-${i}`)) return `ord-${i}`;
}
/** An order id that the issuer authorises, and one it declines. */
const approved = first(id => !Payments.declines(id));
const declined = first(id => Payments.declines(id));

const withOrderId = (e: CloudEvent, id: string): CloudEvent => ({ ...e, data: { ...e.data, orderId: id } });

function value<T>(r: { ok: true; value: T } | { ok: false; error: string }): T {
  if (!r.ok) throw new Error(r.error);
  return r.value;
}
const priced = (e: CloudEvent) => {
  const p = pricing.price(e);
  return p.ok ? p.value.value.toString() : p.error;
};

describe('services', () => {
  test('the hashes match the JVM, so decisions match the Scala build', () => {
    const jvm: [string, number, number][] = [
      ['ord-1', -1725563276, 106004549], ['ord-2', 1990441921, 106004550], ['ord-17', -1068601923, -1008826222],
      ['ord-00a1b2c3d4', 1402889169, 339870956], ['x', 659534019, 120], ['', 377927480, 0],
    ];
    for (const [s, murmur, java] of jvm) {
      expect(scalaStringHash(s), s).toBe(murmur);
      expect(javaHash(s), s).toBe(java);
    }
  });

  test('orders: v1 has a decimal total, v2 an amount object, and both agree with the items', () => {
    for (let seed = 1; seed <= 50; seed++) {
      const v1 = order(1, seed);
      const v2 = order(2, seed);
      expect(typeof v1.data.total).toBe('string');
      expect((v2.data.total as JsonObject).currency).toBe('USD');
      const items = (v1.data.items as JsonObject[]).reduce((sum, i) => sum.plus(new Big(i.price as string).times(i.qty as number)), new Big(0));
      expect(new Big(v1.data.total as string).eq(items)).toBe(true);
      expect(priced(v2)).toEqual(priced(v1));
    }
  });

  test('orders: about 2,000 customers and believable totals', () => {
    const rng = seeded(7);
    const orders = Array.from({ length: 5000 }, () => Orders.placed(emit, rng, 1, at).event);
    const totals = orders.map(e => Number(e.data.total));
    const mean = totals.reduce((a, b) => a + b, 0) / totals.length;
    expect(new Set(orders.map(e => e.data.customerId)).size).toBeGreaterThan(1800);
    expect(mean, `mean order ${mean}`).toBeGreaterThan(40);
    expect(mean).toBeLessThan(300);
    expect(Math.max(...totals), 'some orders are big ones').toBeGreaterThan(1000);
  });

  test('payments authorises a v1 order for its total', () => {
    const e = withOrderId(order(1), approved);
    const out = value(Payments.handle(emit, e, false, at));
    expect(out.map(o => [o.topic, o.key, o.event.type])).toEqual([[Payments.Authorized, approved, 'PaymentAuthorized']]);
    expect(out[0]!.event.data.amount).toBe(e.data.total);
    expect(out[0]!.event.redriveOf).toBeNull();
  });

  test('payments declines about one order in twenty, the same way every time', () => {
    const ids = Array.from({ length: 10000 }, (_, i) => `ord-${i + 1}`);
    const rate = ids.filter(Payments.declines).length / ids.length;
    expect(rate).toBeGreaterThan(0.04);
    expect(rate).toBeLessThan(0.06);
    const out = value(Payments.handle(emit, withOrderId(order(1), declined), false, at));
    expect(out.map(o => o.topic)).toEqual([Payments.Declined]);
    expect(out[0]!.event.data.reason).toBe('card declined');
  });

  test('unpatched payments cannot read v2, and says why', () => {
    expect(Payments.handle(emit, order(2), false, at)).toEqual({ ok: false, error: "'total' is an object, expected a decimal string" });
  });

  test('patched payments reads both shapes', () => {
    for (const v of [1, 2]) {
      const e = withOrderId(order(v), approved);
      const amount = value(Payments.handle(emit, e, true, at))[0]!.event.data.amount;
      expect(amount, `v${v}`).toBe(formatMoney(value(pricing.price(e)).value));
    }
  });

  test('a redriven payment carries the id of the order event it recovers', () => {
    const e = withOrderId(order(2), approved);
    const out = value(Payments.handle(emit, e, true, at, true));
    expect(out[0]!.event.redriveOf).toBe(e.id);
  });

  test('payments refuses events that are not orders', () => {
    const stock = value(Inventory.handle(emit, order(1), at))[0]!.event;
    expect(Payments.handle(emit, stock, true, at)).toEqual({ ok: false, error: 'payments cannot read StockReserved@v1' });
  });

  test('inventory reserves stock for both versions, valued from the items', () => {
    for (const v of [1, 2]) {
      const e = order(v, 3);
      const out = value(Inventory.handle(emit, e, at));
      expect(out.map(o => o.topic)).toEqual([Inventory.Reserved]);
      expect(priced(out[0]!.event), `v${v}`).toEqual(priced(e));
    }
  });

  test('inventory dead-letters an order without items', () => {
    const e = { ...order(1), data: { orderId: 'o', customerId: 'c', items: [] } };
    expect(Inventory.handle(emit, e, at)).toEqual({ ok: false, error: 'no items' });
    const bad = { ...e, data: { orderId: 'o', customerId: 'c', items: [{ sku: 'x', qty: 0, price: '1.00' }] } };
    expect(Inventory.handle(emit, bad, at)).toEqual({ ok: false, error: "'qty' is not a positive whole number" });
  });

  function paymentAndStock(id: string): [CloudEvent, CloudEvent] {
    const e = withOrderId(order(1), id);
    return [value(Payments.handle(emit, e, false, at))[0]!.event, value(Inventory.handle(emit, e, at))[0]!.event];
  }

  test('shipping dispatches once it has payment and stock, in either order', () => {
    const [pay, stock] = paymentAndStock(approved);
    for (const [a, b] of [[pay, stock], [stock, pay]] as const) {
      const s = new Shipping(emit);
      expect(s.handle(a, at)).toEqual({ ok: true, value: [] });
      expect(s.pending).toBe(1);
      const out = value(s.handle(b, at));
      expect(out.map(o => [o.topic, o.key])).toEqual([[Shipping.Dispatched, approved]]);
      expect(out[0]!.event.data.orderTotal).toBe(pay.data.amount);
      expect(s.pending).toBe(0);
    }
  });

  test('shipping waits for a payment held up in a dead-letter queue, then dispatches the redriven one', () => {
    const e = withOrderId(order(2), approved);
    const stock = value(Inventory.handle(emit, e, at))[0]!.event;
    const s = new Shipping(emit);
    expect(s.handle(stock, at)).toEqual({ ok: true, value: [] });
    const redriven = value(Payments.handle(emit, e, true, plus(at, 120), true))[0]!.event;
    expect(value(s.handle(redriven, plus(at, 120))).map(o => o.topic)).toEqual([Shipping.Dispatched]);
  });

  test('shipping forgets orders that never complete', () => {
    const [, stock] = paymentAndStock(declined);
    const s = new Shipping(emit, 5 * 60_000);
    s.handle(stock, at);
    const [, other] = paymentAndStock(approved);
    s.handle(other, plus(at, 301));
    expect(s.pending).toBe(1);
  });

  test('shipping dead-letters what it does not read', () => {
    expect(new Shipping(emit).handle(order(1), at)).toEqual({ ok: false, error: 'shipping cannot read OrderPlaced@v1' });
  });
});
