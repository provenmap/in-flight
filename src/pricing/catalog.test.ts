import { readFileSync } from 'node:fs';
import { describe, expect, test } from 'vitest';
import { Catalog, catalogFile, defaultCatalog, type Json } from './catalog.js';

type Obj = Record<string, Json> & { [k: string]: any }; // eslint-disable-line @typescript-eslint/no-explicit-any

describe('the catalog', () => {
  const file = Catalog.load(catalogFile);
  if (!file.ok) throw new Error(file.error.join('; '));
  const c = file.value;

  function edit(f: (json: Obj) => void) {
    const json = JSON.parse(readFileSync(catalogFile, 'utf8')) as Obj;
    f(json);
    return Catalog.fromJson(json);
  }
  const errors = (res: ReturnType<typeof edit>) => (res.ok ? [] : res.error);

  test('the shipped catalog loads, and the default is that file', () => {
    expect(file.ok).toBe(true);
    expect(defaultCatalog()).toEqual(c);
  });

  test('districts, topics and dead-letter queues', () => {
    expect(c.districts.map(d => d.id)).toEqual(['orders', 'payments', 'inventory', 'shipping', 'customers']);
    expect(c.districts.filter(d => d.sink).map(d => d.id)).toEqual(['customers']);
    expect(c.topics).toEqual(['orders.placed', 'payments.authorized', 'payments.declined', 'inventory.reserved', 'shipping.dispatched']);
    expect(c.dlqs).toEqual(['payments.dlq', 'inventory.dlq', 'shipping.dlq']);
    expect(c.services.map(s => s.group)).toEqual(['payments-service', 'inventory-service', 'shipping-service']);
  });

  test('who reads what', () => {
    expect(c.readersOf('orders.placed').map(k => k.district)).toEqual(['payments', 'inventory']);
    expect(c.readersOf('payments.authorized').map(k => k.district)).toEqual(['shipping']);
    expect(c.readersOf('payments.declined')).toEqual([]);
    expect(c.readersOf('shipping.dispatched').map(k => k.name)).toEqual(['customers']);
    expect(c.ownerOfDlq('payments.dlq')?.group).toBe('payments-service');
    expect(c.districtOf('inventory.reserved')).toBe('inventory');
    expect(c.redriveGroupOf('payments.dlq')).toBe('payments-service-redrive');
    expect(c.redriveGroupOf('inventory.dlq')).toBeUndefined();
  });

  test("a redrive group needs a dead-letter queue, and cannot reuse a service's group", () => {
    expect(errors(edit(j => { j.consumers[3].redrive = 'customers-redrive'; })))
      .toEqual(['consumer customers: a redrive group needs a dead-letter queue']);
    expect(errors(edit(j => { j.consumers[0].redrive = 'inventory-service'; })))
      .toEqual(["consumer group 'inventory-service' is declared twice"]);
  });

  test('both OrderPlaced versions share a topic but declare their value in different places', () => {
    const v1 = c.find({ name: 'OrderPlaced', version: 1 })!;
    const v2 = c.find({ name: 'OrderPlaced', version: 2 })!;
    expect(v1.topic).toBe(v2.topic);
    expect(v1.value.show).toBe('total');
    expect(v2.value.show).toBe('total.amount');
  });

  test('an unknown district is rejected', () => {
    expect(errors(edit(j => { j.events[2].district = 'billing'; })))
      .toEqual(["PaymentAuthorized@v1: unknown district 'billing'"]);
  });

  test('a consumer of an unknown topic is rejected', () => {
    expect(errors(edit(j => { j.consumers[2].reads.push('billing.invoices'); })))
      .toEqual(["consumer shipping-service: reads unknown topic 'billing.invoices'"]);
  });

  test('a version declared twice is rejected', () => {
    expect(errors(edit(j => { j.events.push(j.events[0]); }))).toEqual(['OrderPlaced@v1 is declared twice']);
  });

  test('a dead-letter queue without a consumer group is rejected', () => {
    expect(errors(edit(j => { j.consumers[3].dlq = 'customers.dlq'; })))
      .toEqual(['consumer customers: a dead-letter queue needs a consumer group']);
  });

  test('a sink cannot produce', () => {
    expect(errors(edit(j => { j.events[5].district = 'customers'; })))
      .toEqual(['OrderDispatched@v1: a sink cannot produce events']);
  });

  test('a bad value path and a missing version are both reported', () => {
    const res = edit(j => {
      j.events[0].value = 'total..amount';
      delete j.events[1].version;
    });
    expect(new Set(errors(res))).toEqual(new Set([
      "events[0]: value: 'total..amount' is not a dotted field path",
      "events[1]: 'version' must be a whole number",
    ]));
  });

  test('a missing string and a missing array are named', () => {
    expect(errors(edit(j => { delete j.districts[1].name; }))).toEqual(["districts[1]: 'name' must be a string"]);
    expect(errors(edit(j => { delete j.flow; }))).toEqual(["catalog: 'flow' must be a string"]);
    expect(errors(edit(j => { j.consumers = {}; }))).toEqual(["'consumers' must be an array"]);
  });

  test('consumers read by intent, and the intent supplies the topics when none are listed', () => {
    expect(c.consumersOf('order-total-changes').map(k => k.name)).toEqual(['payments-service']);
    expect(c.semantics?.table('ready-to-ship')?.sourceTopics).toEqual(['inventory.reserved', 'payments.authorized']);

    const byIntent = edit(j => { delete j.consumers[2].reads; });
    expect(byIntent.ok && byIntent.value.consumer('shipping-service')?.reads).toEqual(['inventory.reserved', 'payments.authorized']);

    expect(errors(edit(j => { j.consumers[0].intent = 'refunds'; })))
      .toEqual(["consumer payments-service: reads by intent 'refunds', which the catalog does not declare"]);
    expect(errors(edit(j => { j.consumers[2].reads = ['payments.authorized']; })))
      .toEqual(["consumer shipping-service: intent 'ready-to-ship' draws on topic 'inventory.reserved', which it does not read"]);
    expect(errors(edit(j => { j.intents[0].predicate = 'changes(Order.colour)'; })))
      .toEqual(["intent 'order-total-changes': Order has no attribute 'colour' (has: total, status, payment, stock)"]);
  });

  test('a catalog with no meaning declared still loads, with no intents', () => {
    const plain = edit(j => {
      delete j.entities;
      delete j.intents;
      for (const e of j.events) { delete e.fields; delete e.keys; delete e.effects; }
      for (const k of j.consumers) delete k.intent;
    });
    expect(plain.ok && plain.value.semantics).toBeUndefined();
    expect(plain.ok && plain.value.topics).toEqual(c.topics);
  });
});
