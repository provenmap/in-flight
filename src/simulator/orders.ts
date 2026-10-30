import { randomInt } from 'node:crypto';
import Big from 'big.js';
import type { JsonObject } from '../pricing/catalog.js';
import type { CloudEvent } from '../pricing/cloudevents.js';
import { formatMoney, type Emitter, type Out } from './events.js';

/** Random numbers from a seed, so tests are repeatable. */
export interface Rng {
  /** In [0, 1). */
  next(): number;
}

/** mulberry32: small, fast and good enough for synthetic shoppers. */
export function seeded(seed: number): Rng {
  let a = seed | 0;
  return {
    next() {
      a = (a + 0x6d2b79f5) | 0;
      let t = Math.imul(a ^ (a >>> 15), 1 | a);
      t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    },
  };
}

export const unseeded = (): Rng => seeded(randomInt(2 ** 31));

const int = (rng: Rng, n: number) => Math.floor(rng.next() * n);

interface Product {
  sku: string;
  price: Big;
}

const product = (sku: string, price: string): Product => ({ sku, price: new Big(price) });

/**
 * The orders team: synthetic shoppers placing orders. Version 1 writes `total` as a decimal string; version 2,
 * the breaking change, writes it as `{ amount, currency }`.
 */
export const Orders = {
  Topic: 'orders.placed',
  Customers: 2000,
  Currency: 'USD',

  /** A shop's range, cheap things far more often than dear ones. Weight, then product. */
  range: [
    [30, product('SKU-SOCKS-3PK', '12.99')],
    [28, product('SKU-MUG-CERAMIC', '16.50')],
    [25, product('SKU-TSHIRT', '24.00')],
    [22, product('SKU-PHONE-CASE', '29.95')],
    [18, product('SKU-BOOK-HC', '34.99')],
    [15, product('SKU-SNEAKERS', '89.00')],
    [12, product('SKU-BACKPACK', '119.00')],
    [10, product('SKU-HEADPHONES', '149.00')],
    [6, product('SKU-SMARTWATCH', '329.00')],
    [4, product('SKU-ESPRESSO', '549.00')],
    [2, product('SKU-LAPTOP-14', '1299.00')],
    [1, product('SKU-TV-65', '1899.00')],
  ] as [number, Product][],

  customerId: (i: number): string => `cus-${String(i).padStart(4, '0')}`,

  pick(rng: Rng): Product {
    let n = int(rng, totalWeight);
    return Orders.range.find(([w]) => (n -= w) < 0)![1];
  },

  /** One to four distinct lines, usually one, mostly one of each; now and then a business buys ten of something cheap. */
  basket(rng: Rng): [Product, number][] {
    const r = rng.next();
    const lines = r < 0.5 ? 1 : r < 0.8 ? 2 : r < 0.95 ? 3 : 4;
    const picked = [...new Set(Array.from({ length: lines }, () => Orders.pick(rng)))];
    return picked.map(p => {
      const q = rng.next();
      const qty = q < 0.03 && p.price.lt(50) ? 10 : q < 0.2 ? 2 + int(rng, 2) : 1;
      return [p, qty];
    });
  },

  placed(emit: Emitter, rng: Rng, version: number, at: Date): Out {
    // 40 random bits, as ten hex digits.
    const orderId = `ord-${int(rng, 256).toString(16).padStart(2, '0')}${int(rng, 2 ** 32).toString(16).padStart(8, '0')}`;
    const customer = Orders.customerId(1 + int(rng, Orders.Customers));
    const lines = Orders.basket(rng);
    const total = lines.reduce((sum, [p, q]) => sum.plus(p.price.times(q)), new Big(0));
    const items = lines.map(([p, q]) => ({ sku: p.sku, qty: q, price: formatMoney(p.price) }));
    const data: JsonObject = version === 1
      ? { orderId, customerId: customer, total: formatMoney(total), currency: Orders.Currency, items, placedAt: at.toISOString() }
      : { orderId, customerId: customer, total: { amount: formatMoney(total), currency: Orders.Currency }, items, placedAt: at.toISOString() };
    return { topic: Orders.Topic, key: orderId, event: emit.emit('OrderPlaced', version, at, data) };
  },

  isPlaced: (e: CloudEvent): boolean => e.type === 'OrderPlaced',
};

const totalWeight = Orders.range.reduce((a, [w]) => a + w, 0);
