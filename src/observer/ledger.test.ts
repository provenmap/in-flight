import Big from 'big.js';
import { describe, expect, test } from 'vitest';
import { defaultCatalog } from '../pricing/catalog.js';
import type { Priced } from '../pricing/pricing.js';
import { Ledger, msgJson, type GroupLag, type Msg } from './ledger.js';

const catalog = defaultCatalog();
const t0 = new Date('2026-10-30T12:00:00Z');
const plus = (s: number) => new Date(t0.getTime() + s * 1000);
const pos = (topic: string, partition: number, offset: number) => ({ topic, partition, offset });

function placed(id: string, order: string, customer: string, value: string, version = 2): Priced {
  return { eventId: id, ref: { name: 'OrderPlaced', version }, topic: 'orders.placed', district: 'orders', value: new Big(value), customer, orderId: order, redriveOf: null };
}

function authorized(id: string, order: string, customer: string, value: string, redriveOf: string | null = null): Priced {
  return { eventId: id, ref: { name: 'PaymentAuthorized', version: 1 }, topic: 'payments.authorized', district: 'payments', value: new Big(value), customer, orderId: order, redriveOf };
}

const dead = (l: Ledger, offset: number, p: Priced, at = t0): Msg =>
  l.onDead('payments.dlq', pos('payments.dlq', 0, offset), p, "'total' is an object", at);

const json = (m: Msg) => msgJson(m) as Record<string, unknown>;
const lag = (g: GroupLag | undefined) => g && [g.events, g.value.toString(), g.customers];
const money = (b: Big) => b.toString();

describe('the ledger', () => {
  test('a dead letter is stuck money at payments', () => {
    const l = new Ledger(catalog);
    const m = dead(l, 0, placed('e1', 'o1', 'c1', '100.00'));
    expect(m.kind).toBe('dead');
    expect(json(m).district).toBe('payments');
    dead(l, 1, placed('e2', 'o2', 'c1', '50.50'), plus(1));
    const s = l.stats();
    expect([money(s.stuckValue), s.stuckEvents, s.stuckCustomers]).toEqual(['150.5', 2, 1]);
    expect(s.stuckSince).toEqual(t0);
    expect([...s.stuckByDistrict].map(([d, v]) => [d, money(v)])).toEqual([['payments', '150.5']]);
    expect([money(s.atRiskValue), s.atRiskOrders, s.atRiskCustomers]).toEqual(['150.5', 2, 1]);
  });

  test('a redriven success recovers its dead letter, with the time it was stuck', () => {
    const l = new Ledger(catalog);
    dead(l, 0, placed('e1', 'o1', 'c1', '100.00'));
    dead(l, 1, placed('e2', 'o2', 'c2', '20.00'), plus(5));
    const msgs = l.onFlow(pos('payments.authorized', 0, 7), authorized('p1', 'o1', 'c1', '100.00', 'e1'), plus(30));
    expect(msgs.map(m => json(m).type)).toEqual(['event', 'recovered']);
    expect(json(msgs[1]!).afterMs).toBe(30000);
    expect(json(msgs[0]!).to).toEqual(['shipping']);
    expect(json(msgs[0]!).redrive).toBe(true);
    let s = l.stats();
    expect([money(s.stuckValue), money(s.recoveredValue), s.recoveredEvents]).toEqual(['20', '100', 1]);
    expect(s.lastEpisode).toBeNull();

    l.onFlow(pos('payments.authorized', 0, 8), authorized('p2', 'o2', 'c2', '20.00', 'e2'), plus(45));
    s = l.stats();
    expect(s.lastEpisode && [money(s.lastEpisode.value), s.lastEpisode.events, s.lastEpisode.tookMs]).toEqual(['120', 2, 45000]);
    expect(money(s.atRiskValue)).toBe('0');
  });

  test('after a restart, dead letters the redrive already passed count as recovered, not stuck', () => {
    const l = new Ledger(catalog);
    l.redriven('payments.dlq', 0, 2);
    const msgs = [0, 1, 2].map(o => dead(l, o, placed(`e${o}`, `o${o}`, 'c', '10.00')));
    expect(msgs.map(m => json(m).type)).toEqual(['recovered', 'recovered', 'dead']);
    expect(json(msgs[0]!).afterMs).toBeNull();
    expect([l.stats().stuckEvents, l.stats().recoveredEvents]).toEqual([1, 2]);
  });

  test('a redrive offset on one partition says nothing about another', () => {
    const l = new Ledger(catalog);
    l.redriven('payments.dlq', 1, 100);
    expect(json(dead(l, 0, placed('e', 'o', 'c', '1.00'))).type).toBe('dead');
  });

  test('a redriven event that overtakes its dead letter still recovers it', () => {
    const l = new Ledger(catalog);
    expect(l.onFlow(pos('payments.authorized', 0, 0), authorized('p1', 'o1', 'c1', '9.00', 'e1'), t0)).toHaveLength(1);
    expect(json(dead(l, 0, placed('e1', 'o1', 'c1', '9.00'))).type).toBe('recovered');
    expect(l.stats().stuckEvents).toBe(0);
  });

  test('lag: what a group has not committed, valued from the events the observer saw', () => {
    const l = new Ledger(catalog);
    for (let o = 0; o <= 4; o++) l.onFlow(pos('orders.placed', 0, o), placed(`e${o}`, `o${o}`, `c${o % 2}`, '10.00', 1), t0);
    l.onOffsets('payments-service', new Map([['orders.placed:0', 2]]), new Map([['orders.placed:0', 5]]));
    l.onOffsets('inventory-service', new Map([['orders.placed:0', 5]]), new Map());
    const s = l.stats();
    expect(lag(s.lag.get('payments-service'))).toEqual([3, '30', 2]);
    expect(lag(s.lag.get('inventory-service'))).toEqual([0, '0', 0]);
    expect(s.lag.get('shipping-service')!.events).toBe(0);
    expect([money(s.atRiskValue), s.atRiskOrders, s.atRiskCustomers]).toEqual(['30', 3, 2]);
  });

  test('events past the polled end are in flight, not lag', () => {
    const l = new Ledger(catalog);
    for (let o = 0; o <= 4; o++) l.onFlow(pos('orders.placed', 0, o), placed(`e${o}`, `o${o}`, 'c', '10.00', 1), t0);
    l.onOffsets('payments-service', new Map([['orders.placed:0', 2]]), new Map([['orders.placed:0', 3]]));
    expect(lag(l.stats().lag.get('payments-service'))).toEqual([1, '10', 1]);
  });

  test('lag counts events the observer did not see, but cannot value them', () => {
    const l = new Ledger(catalog);
    l.onOffsets('payments-service', new Map([['orders.placed:1', 10]]), new Map([['orders.placed:1', 25]]));
    expect(lag(l.stats().lag.get('payments-service'))).toEqual([15, '0', 0]);
  });

  test('an order held up in two places is at risk once, at its largest value', () => {
    const l = new Ledger(catalog);
    dead(l, 0, placed('e1', 'o1', 'c1', '100.00'));
    l.onFlow(pos('orders.placed', 0, 0), placed('e1', 'o1', 'c1', '100.00'), t0);
    l.onFlow(pos('inventory.reserved', 0, 0),
      { eventId: 's1', ref: { name: 'StockReserved', version: 1 }, topic: 'inventory.reserved', district: 'inventory', value: new Big('95.00'), customer: 'c1', orderId: 'o1', redriveOf: null }, t0);
    l.onOffsets('inventory-service', new Map([['orders.placed:0', 0]]), new Map([['orders.placed:0', 1]]));
    l.onOffsets('shipping-service', new Map([['inventory.reserved:0', 0]]), new Map([['inventory.reserved:0', 1]]));
    const s = l.stats();
    expect(money(s.stuckValue.plus(s.lagValue))).toBe('295');
    expect([money(s.atRiskValue), s.atRiskOrders, s.atRiskCustomers]).toEqual(['100', 1, 1]);
  });

  test('events every reader has committed are forgotten', () => {
    const l = new Ledger(catalog);
    for (let o = 0; o <= 9; o++) l.onFlow(pos('orders.placed', 0, o), placed(`e${o}`, `o${o}`, 'c', '1.00', 1), t0);
    l.onOffsets('payments-service', new Map([['orders.placed:0', 10]]), new Map([['orders.placed:0', 10]]));
    l.onOffsets('inventory-service', new Map([['orders.placed:0', 6]]), new Map());
    expect(lag(l.stats().lag.get('inventory-service'))).toEqual([4, '4', 1]);
    // Payments rewinds (a reset, say): only what inventory still holds can be valued.
    l.onOffsets('payments-service', new Map([['orders.placed:0', 0]]), new Map());
    expect(lag(l.stats().lag.get('payments-service'))).toEqual([10, '4', 1]);
  });

  test('flow topics nobody reads are not remembered, and go nowhere', () => {
    const l = new Ledger(catalog);
    const declined: Priced = { eventId: 'd1', ref: { name: 'PaymentDeclined', version: 1 }, topic: 'payments.declined', district: 'payments', value: new Big('5.00'), customer: 'c', orderId: 'o', redriveOf: null };
    expect(json(l.onFlow(pos('payments.declined', 0, 0), declined, t0)[0]!).to).toEqual([]);
  });

  test('the remembered window is bounded, oldest first', () => {
    const l = new Ledger(catalog, 3);
    for (let o = 0; o <= 9; o++) l.onFlow(pos('orders.placed', 0, o), placed(`e${o}`, `o${o}`, 'c', '1.00', 1), t0);
    l.onOffsets('payments-service', new Map([['orders.placed:0', 0]]), new Map([['orders.placed:0', 10]]));
    expect(lag(l.stats().lag.get('payments-service'))).toEqual([10, '3', 1]);
  });
});
