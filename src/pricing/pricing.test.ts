import Big from 'big.js';
import { describe, expect, test } from 'vitest';
import { CloudEvents, toJson, type CloudEvent } from './cloudevents.js';
import { showRef, type JsonObject } from './catalog.js';
import { defaultPricing, type Priced } from './pricing.js';
import { decode } from './records.js';

describe('pricing', () => {
  const pricing = defaultPricing();
  const at = new Date('2026-10-30T12:00:00Z');

  const event = (type: string, version: number, data: JsonObject): CloudEvent =>
    ({ id: 'e-1', source: '/test', type, version, time: at, sequence: 1, data, redriveOf: null });

  const placedV1 = () => event('OrderPlaced', 1, { orderId: 'o-1', customerId: 'c-42', total: '129.90', currency: 'USD' });
  const placedV2 = () => event('OrderPlaced', 2, { orderId: 'o-2', customerId: 'c-7', total: { amount: '1049.00', currency: 'USD' } });

  const value = (r: ReturnType<typeof pricing.price>) => (r.ok ? { ok: true, value: r.value.value.toString() } : r);

  test('OrderPlaced v1 is worth its total', () => {
    const want: Priced = {
      eventId: 'e-1', ref: { name: 'OrderPlaced', version: 1 }, topic: 'orders.placed', district: 'orders',
      value: new Big('129.90'), customer: 'c-42', orderId: 'o-1', redriveOf: null,
    };
    expect(pricing.price(placedV1())).toEqual({ ok: true, value: want });
  });

  test('OrderPlaced v2 is worth total.amount', () => {
    const p = pricing.price(placedV2());
    if (!p.ok) throw new Error(p.error);
    expect([p.value.district, p.value.value.toFixed(2), p.value.customer, p.value.orderId]).toEqual(['orders', '1049.00', 'c-7', 'o-2']);
  });

  test('every downstream event is priced in its own district', () => {
    const cases = [
      ['PaymentAuthorized', 'amount', 'payments'],
      ['PaymentDeclined', 'amount', 'payments'],
      ['StockReserved', 'value', 'inventory'],
      ['OrderDispatched', 'orderTotal', 'shipping'],
    ] as const;
    for (const [type, field, district] of cases) {
      const p = pricing.price(event(type, 1, { orderId: 'o-3', customerId: 'c-1', [field]: '15.25' }));
      expect(p.ok && [p.value.district, p.value.value.toString()], type).toEqual([district, '15.25']);
    }
  });

  test('a JSON number is an amount too', () => {
    const p = pricing.price(event('PaymentAuthorized', 1, { orderId: 'o', customerId: 'c', amount: 12.5 }));
    expect(value(p)).toEqual({ ok: true, value: '12.5' });
  });

  test('v2 data read with the v1 path is not guessed at', () => {
    const mislabelled = { ...placedV2(), version: 1 };
    expect(pricing.price(mislabelled)).toEqual({ ok: false, error: "OrderPlaced@v1 (e-1): no amount at 'total'" });
  });

  test('an undeclared type is an error', () => {
    expect(pricing.price({ ...placedV1(), type: 'OrderAmended' })).toEqual({ ok: false, error: 'OrderAmended@v1 (e-1) is not in the catalog' });
  });

  test('an undeclared version is priced by the newest version of its type that reads it, and says so', () => {
    const p = pricing.price({ ...placedV1(), version: 3 });
    if (!p.ok) throw new Error(p.error);
    expect([p.value.ref, p.value.pricedAs, p.value.value.toString()]).toEqual([{ name: 'OrderPlaced', version: 3 }, { name: 'OrderPlaced', version: 1 }, '129.9']);
    // A shape no declared version reads stays unpriced: the value is never guessed.
    const v3 = event('OrderPlaced', 3, { orderId: 'o-3', customerId: 'c', total: { cents: 12990 } });
    expect(pricing.price(v3)).toEqual({ ok: false, error: 'OrderPlaced@v3 (e-1) is not in the catalog, and no version of OrderPlaced it has can read it' });
  });

  test('a missing customer or order is an error', () => {
    const noCustomer = event('StockReserved', 1, { orderId: 'o', value: '1.00' });
    expect(pricing.price(noCustomer)).toEqual({ ok: false, error: "StockReserved@v1 (e-1): no customer at 'customerId'" });
  });

  test('a redriven event keeps the id of the one it recovers, through the wire format', () => {
    const redriven = { ...event('PaymentAuthorized', 1, { orderId: 'o', customerId: 'c', amount: '9.99' }), id: 'e-2', redriveOf: 'e-1' };
    expect(CloudEvents.parse(toJson(redriven))).toEqual({ ok: true, value: redriven });
    const p = pricing.priceBytes(Buffer.from(toJson(redriven)));
    expect(p.ok && p.value.redriveOf).toBe('e-1');
  });

  test('a fresh event round-trips and has no redrive marker', () => {
    const e = CloudEvents.create('/orders', 'OrderPlaced', 1, at, 7, { orderId: 'o', customerId: 'c', total: '1.00' });
    expect(CloudEvents.parse(toJson(e))).toEqual({ ok: true, value: e });
    expect(toJson(e)).not.toContain('redriveof');
  });

  test('an envelope without a schemaversion or data is rejected', () => {
    expect(CloudEvents.parse('{"id":"x","source":"s","type":"T","data":{}}')).toEqual({ ok: false, error: 'T (x) has no whole schemaversion' });
    expect(CloudEvents.parse('{"id":"x","source":"s","type":"T","schemaversion":1}')).toEqual({ ok: false, error: 'T (x) has no data object' });
    expect(CloudEvents.parse('not json').ok).toBe(false);
  });

  describe('records from other producers', () => {
    const none = new Map<string, string>();
    const data = { orderId: 'o-9', customerId: 'c-9', amount: '42.00' };

    test('plain JSON is priced by its topic', () => {
      const p = pricing.priceRecord(Buffer.from(JSON.stringify(data)), none, ['payments.authorized'], 'payments.authorized:0:12');
      if (!p.ok) throw new Error(p.error);
      expect([showRef(p.value.ref), p.value.eventId, p.value.value.toString()]).toEqual(['PaymentAuthorized@v1', 'payments.authorized:0:12', '42']);
    });

    test('plain JSON on a topic with several versions takes the newest one that reads it', () => {
      const v1 = pricing.priceRecord(Buffer.from('{"orderId":"o","customerId":"c","total":"10.00"}'), none, ['orders.placed'], 'x');
      const v2 = pricing.priceRecord(Buffer.from('{"orderId":"o","customerId":"c","total":{"amount":"11.00"}}'), none, ['orders.placed'], 'x');
      expect([v1.ok && showRef(v1.value.ref), v2.ok && showRef(v2.value.ref)]).toEqual(['OrderPlaced@v1', 'OrderPlaced@v2']);
    });

    test('a binary-mode CloudEvent has its attributes in ce_ headers', () => {
      const headers = new Map([['ce_id', 'b-1'], ['ce_type', 'PaymentAuthorized'], ['ce_schemaversion', '1'], ['ce_redriveof', 'e-0']]);
      const p = pricing.priceRecord(Buffer.from(JSON.stringify(data)), headers, ['payments.authorized'], 'x');
      expect(p.ok && [p.value.eventId, p.value.redriveOf]).toEqual(['b-1', 'e-0']);
    });

    test('Schema Registry framing is dropped', () => {
      const framed = Buffer.concat([Buffer.from([0, 0, 0, 0, 7]), Buffer.from(JSON.stringify(data))]);
      expect(pricing.priceRecord(framed, none, ['payments.authorized'], 'x').ok).toBe(true);
    });

    test('a structured CloudEvent without a schemaversion is priced by its type', () => {
      const e = { specversion: '1.0', id: 's-1', source: '/s', type: 'StockReserved', data: { orderId: 'o', customerId: 'c', value: '3.00' } };
      const p = pricing.priceRecord(Buffer.from(JSON.stringify(e)), none, [], 'x');
      expect(p.ok && showRef(p.value.ref)).toBe('StockReserved@v1');
    });

    test('decoding errors say what was wrong', () => {
      expect(decode(Buffer.from('nope'), none).ok).toBe(false);
      expect(decode(Buffer.from('[1]'), none)).toEqual({ ok: false, error: 'the record is not a JSON object' });
      expect(pricing.priceRecord(Buffer.from('{"a":1}'), none, ['nowhere'], 'x')).toEqual({ ok: false, error: 'a record on nowhere (x) has no event type in the catalog' });
    });
  });
});
