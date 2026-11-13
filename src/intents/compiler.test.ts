import { describe, expect, test } from 'vitest';
import { showRef } from '../pricing/paths.js';
import { parseSemantics } from './semantics.js';
import { raw } from './fixture.test-data.js';

const load = (edit: (r: ReturnType<typeof raw>) => void = () => {}) => {
  const r = raw();
  edit(r);
  return parseSemantics(r);
};
const errors = (res: ReturnType<typeof load>) => (res.ok ? [] : res.error);
const sound = () => {
  const res = load();
  if (!res.ok || !res.value) throw new Error(errors(res).join('; '));
  return res.value;
};

describe('compiling intents', () => {
  test('changes() means exactly the event types with an effect on the attribute, from whichever topics', () => {
    const t = sound().table('order-total-changes')!;
    expect(t.entity).toBe('Order');
    expect(t.routes.map(r => showRef(r.event))).toEqual(['OrderAmended@v1', 'OrderPlaced@v1', 'RefundIssued@v1']);
    expect(t.sourceTopics).toEqual(['billing.refunds', 'orders.amended', 'orders.placed']);
    expect(t.routes[2]).toMatchObject({
      reasons: ['subtract Order.total from refund.amount'],
      reads: [
        { what: 'Order key', source: 'from refund.orderId', type: 'string' },
        { what: 'Order.total', source: 'subtract from refund.amount', type: 'decimal' },
      ],
    });
  });

  test('emits, about, and, or, not', () => {
    const names = (predicate: string, by?: string) => {
      const t = sound().explain(predicate, by);
      return t.ok ? t.value.routes.map(r => r.event.name) : t.error;
    };
    expect(names('emits(OrderShipped)', 'Order')).toEqual(['OrderShipped']);
    expect(names('about(Shipment)')).toEqual(['OrderShipped']);
    expect(names('about(Order) and not changes(Order.total)')).toEqual(['OrderShipped']);
    expect(names('changes(Order.status) or emits(RefundIssued)')).toEqual(['OrderPlaced', 'OrderShipped', 'RefundIssued']);
    // An event the catalog gives no meaning to is outside every intent, even a negated one.
    expect(names('not emits(OrderPlaced)', 'Order')).not.toContain('Heartbeat');
  });

  test('it fails on what it cannot resolve, and guesses nothing', () => {
    const explain = (predicate: string, by?: string) => {
      const t = sound().explain(predicate, by);
      return t.ok ? [] : t.error;
    };
    expect(explain('changes(Invoice.total)')).toEqual(["unknown entity 'Invoice' in changes(Invoice.total)"]);
    expect(explain('changes(Order.totl)')).toEqual(["Order has no attribute 'totl' (has: total, status)"]);
    expect(explain('emits(OrderLost)')).toEqual(["unknown event type 'OrderLost' in emits(OrderLost)"]);
    expect(explain('changes(Order.total) and emits(OrderShipped)')).toEqual(['changes(Order.total) and emits(OrderShipped) matches no event type in the catalog']);
    expect(explain('emits(OrderShipped)')).toEqual(["key entity is ambiguous (Order, Shipment); say which with 'by'"]);
    expect(explain('about(Shipment) or about(Order)', 'Shipment')[0]).toMatch(/OrderAmended@v1 matches but carries no Shipment key/);
  });

  test('a declared intent that does not compile fails the catalog', () => {
    expect(errors(load(r => r.intents.push({ name: 'bad', predicate: 'changes(Order.colour)' })))).toEqual(["intent 'bad': Order has no attribute 'colour' (has: total, status)"]);
    expect(errors(load(r => r.intents.push({ name: 'Bad Name', predicate: 'about(Order)' })))).toEqual(["intent name 'Bad Name' must be lowercase letters, digits and hyphens"]);
    expect(errors(load(r => r.intents.push({ name: 'order-total-changes', predicate: 'about(Order)' })))).toEqual(["intent 'order-total-changes' is declared twice"]);
  });

  test('the meaning is typed and checked', () => {
    expect(errors(load(r => { r.events[0]!.keys = { Order: 'note' }; }))).toEqual(["OrderPlaced@v1: key path 'note' for Order is optional"]);
    expect(errors(load(r => { r.events[0]!.keys = { Parcel: 'orderId' }; }))).toContain("OrderPlaced@v1: key for unknown entity 'Parcel'");
    expect(errors(load(r => { r.events[1]!.effects = [{ op: 'assign', attribute: 'Order.total', from: 'orderId' }]; })))
      .toEqual(["OrderAmended@v1: effect 'assign Order.total from orderId': 'orderId' is string, Order.total is decimal"]);
    expect(errors(load(r => { r.events[1]!.effects = [{ op: 'add', attribute: 'Order.status', value: 'x' }]; })))
      .toEqual(expect.arrayContaining(["OrderAmended@v1: effect 'add Order.status = \"x\"': add needs a numeric attribute, Order.status is string"]));
    expect(errors(load(r => { r.events[1]!.effects = [{ op: 'assign', attribute: 'Shipment.carrier', from: 'orderId' }]; })))
      .toEqual(["OrderAmended@v1: effect 'assign Shipment.carrier from orderId': the event carries no Shipment key, so it cannot say which Shipment it changes"]);
    expect(errors(load(r => { r.events[1]!.keys = undefined; }))).toContain('OrderAmended@v1: declares no entity key, so no intent could key it');
    expect(errors(load(r => { r.entities.Order!.total = 'money'; }))[0]).toMatch(/^entity Order: total: unknown field type 'money'/);
  });

  test('a catalog that says nothing about meaning has none', () => {
    expect(parseSemantics({ events: [{ ref: { name: 'A', version: 1 }, topic: 't' }] })).toEqual({ ok: true, value: undefined });
  });
});
