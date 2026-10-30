import Big from 'big.js';
import { err, ok, type Result } from '../result.js';
import { showRef, type Json, type JsonObject } from '../pricing/catalog.js';
import { refOf, type CloudEvent } from '../pricing/cloudevents.js';
import { Read, floorMod, formatMoney, javaHash, scalaStringHash, type Emitter, type Out } from './events.js';
import { Orders } from './orders.js';

/** What payments needs from an order to charge it. */
interface Charge {
  orderId: string;
  customerId: string;
  amount: Big;
  currency: string;
}

/** Runs reads in order and stops at the first that fails, the way a for-comprehension over Either does. */
function all<T extends unknown[]>(...reads: { [K in keyof T]: () => Result<T[K]> }): Result<T> {
  const out: unknown[] = [];
  for (const read of reads) {
    const r = read();
    if (!r.ok) return r;
    out.push(r.value);
  }
  return ok(out as T);
}

const stripPrefix = (s: string, prefix: string) => (s.startsWith(prefix) ? s.slice(prefix.length) : s);

/**
 * payments-service: charges every order placed. It reads `total` as a decimal string, so an `OrderPlaced` v2 fails
 * and is dead-lettered. Patched, it reads both shapes. About one charge in twenty is declined.
 */
export const Payments = {
  Group: 'payments-service',
  Dlq: 'payments.dlq',
  Authorized: 'payments.authorized',
  Declined: 'payments.declined',

  read(e: CloudEvent, patched: boolean): Result<Charge> {
    if (!Orders.isPlaced(e)) return err(`payments cannot read ${showRef(refOf(e))}`);
    const d = e.data;
    const head = all<[string, string, Json]>(() => Read.str(d, 'orderId'), () => Read.str(d, 'customerId'), () => Read.field(d, 'total'));
    if (!head.ok) return head;
    const [orderId, customerId, total] = head.value;
    const [from, amountKey] = typeof total === 'object' && total !== null && !Array.isArray(total) && patched ? [total, 'amount'] : [d, 'total'];
    const rest = all<[Big, string]>(() => Read.decimal(from, amountKey), () => Read.str(from, 'currency'));
    if (!rest.ok) return rest;
    const [amount, currency] = rest.value;
    return ok({ orderId, customerId, amount, currency });
  },

  /** The card issuer's answer, fixed per order so a redrive decides the same way. */
  declines: (orderId: string): boolean => floorMod(scalaStringHash(orderId), 100) < 5,

  handle(emit: Emitter, e: CloudEvent, patched: boolean, now: Date, redrive = false): Result<Out[]> {
    const c = Payments.read(e, patched);
    if (!c.ok) return c;
    const { orderId, customerId, amount, currency } = c.value;
    const base: JsonObject = { orderId, customerId, amount: formatMoney(amount), currency };
    const redriveOf = redrive ? e.id : null;
    if (Payments.declines(orderId)) {
      const data = { ...base, reason: 'card declined', declinedAt: now.toISOString() };
      return ok([{ topic: Payments.Declined, key: orderId, event: emit.emit('PaymentDeclined', 1, now, data, redriveOf) }]);
    }
    const data = { ...base, paymentId: `pay-${stripPrefix(orderId, 'ord-')}`, authorizedAt: now.toISOString() };
    return ok([{ topic: Payments.Authorized, key: orderId, event: emit.emit('PaymentAuthorized', 1, now, data, redriveOf) }]);
  },
};

/** inventory-service: reserves stock for every order placed. It reads only the items, so v2 does not bother it. */
export const Inventory = {
  Group: 'inventory-service',
  Dlq: 'inventory.dlq',
  Reserved: 'inventory.reserved',

  handle(emit: Emitter, e: CloudEvent, now: Date): Result<Out[]> {
    if (!Orders.isPlaced(e)) return err(`inventory cannot read ${showRef(refOf(e))}`);
    const d = e.data;
    const head = all<[string, string, Json]>(() => Read.str(d, 'orderId'), () => Read.str(d, 'customerId'), () => Read.field(d, 'items'));
    if (!head.ok) return head;
    const [orderId, customerId, items] = head.value;
    if (!Array.isArray(items)) return err("'items' is not an array");
    if (!items.length) return err('no items');
    const lines: [string, number, Big][] = [];
    for (const it of items) {
      const line = all<[string, number, Big]>(
        () => Read.str(it, 'sku'),
        () => {
          const q = Read.field(it, 'qty');
          return q.ok && typeof q.value === 'number' && Number.isInteger(q.value) && q.value > 0 ? ok(q.value) : err("'qty' is not a positive whole number");
        },
        () => Read.decimal(it, 'price'),
      );
      if (!line.ok) return line;
      lines.push(line.value);
    }
    const value = lines.reduce((sum, [, q, p]) => sum.plus(p.times(q)), new Big(0));
    const data: JsonObject = {
      reservationId: `res-${stripPrefix(orderId, 'ord-')}`, orderId, customerId,
      items: lines.map(([sku, qty]) => ({ sku, qty })),
      value: formatMoney(value), reservedAt: now.toISOString(),
    };
    return ok([{ topic: Inventory.Reserved, key: orderId, event: emit.emit('StockReserved', 1, now, data) }]);
  },
};

interface Waiting {
  customer: string;
  payment: Big | null;
  stock: boolean;
  since: number;
}

const carriers = ['ups', 'fedex', 'usps', 'dhl'];

/**
 * shipping-service: dispatches an order once it has both its payment and its stock, in either order. An order
 * that never completes, such as a declined one whose stock was reserved, is dropped after `ttlMs`.
 */
export class Shipping {
  static readonly Group = 'shipping-service';
  static readonly Dlq = 'shipping.dlq';
  static readonly Dispatched = 'shipping.dispatched';
  static readonly Reads = [Payments.Authorized, Inventory.Reserved];

  static carrier = (orderId: string): string => carriers[floorMod(javaHash(orderId), carriers.length)]!;

  private readonly waiting = new Map<string, Waiting>();

  constructor(private readonly emit: Emitter, private readonly ttlMs = 30 * 60_000) {}

  get pending(): number {
    return this.waiting.size;
  }

  handle(e: CloudEvent, now: Date): Result<Out[]> {
    this.expire(now);
    const d = e.data;
    const head = all<[string, string]>(() => Read.str(d, 'orderId'), () => Read.str(d, 'customerId'));
    if (!head.ok) return head;
    const [orderId, customer] = head.value;
    let got: (w: Waiting) => Waiting;
    if (e.type === 'PaymentAuthorized') {
      const amount = Read.decimal(d, 'amount');
      if (!amount.ok) return amount;
      got = w => ({ ...w, payment: amount.value });
    } else if (e.type === 'StockReserved') {
      got = w => ({ ...w, stock: true });
    } else {
      return err(`shipping cannot read ${showRef(refOf(e))}`);
    }
    const w = got(this.waiting.get(orderId) ?? { customer, payment: null, stock: false, since: now.getTime() });
    if (w.payment && w.stock) {
      this.waiting.delete(orderId);
      const data: JsonObject = {
        orderId, customerId: w.customer, orderTotal: formatMoney(w.payment), carrier: Shipping.carrier(orderId), dispatchedAt: now.toISOString(),
      };
      return ok([{ topic: Shipping.Dispatched, key: orderId, event: this.emit.emit('OrderDispatched', 1, now, data) }]);
    }
    this.waiting.set(orderId, w);
    return ok([]);
  }

  private expire(now: Date): void {
    const cutoff = now.getTime() - this.ttlMs;
    for (const [id, w] of this.waiting) {
      if (w.since >= cutoff) break;
      this.waiting.delete(id);
    }
  }
}
