import Big from 'big.js';
import { describe, expect, test } from 'vitest';
import { CloudEvents, toJson, type CloudEvent } from './cloudevents.js';
import type { JsonObject } from './catalog.js';
import { defaultPricing, type Priced } from './pricing.js';

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

  test('an undeclared type or version is an error', () => {
    expect(pricing.price({ ...placedV1(), version: 3 })).toEqual({ ok: false, error: 'OrderPlaced@v3 (e-1) is not in the catalog' });
    expect(pricing.price({ ...placedV1(), type: 'OrderAmended' })).toEqual({ ok: false, error: 'OrderAmended@v1 (e-1) is not in the catalog' });
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
});
