import Big from 'big.js';
import { err, ok, type Result } from '../result.js';
import { defaultCatalog, showRef, type Catalog, type EventRef, type Json } from './catalog.js';
import { CloudEvents, refOf, type CloudEvent } from './cloudevents.js';

/** What one event is worth, to whom, and where it sits on the map. */
export interface Priced {
  eventId: string;
  ref: EventRef;
  topic: string;
  district: string;
  value: Big;
  customer: string;
  orderId: string;
  redriveOf: string | null;
}

/** An amount, written as a decimal string or a JSON number. */
export function money(v: Json | undefined): Big | undefined {
  if (typeof v !== 'string' && typeof v !== 'number') return undefined;
  try {
    return new Big(v);
  } catch {
    return undefined;
  }
}

/** Prices events by the paths the catalog declares. Nothing is guessed: an undeclared type or a missing path is an error. */
export class Pricing {
  constructor(readonly catalog: Catalog) {}

  price(event: CloudEvent): Result<Priced> {
    const where = `${showRef(refOf(event))} (${event.id})`;
    const spec = this.catalog.find(refOf(event));
    if (!spec) return err(`${where} is not in the catalog`);
    const value = money(spec.value.read(event.data));
    if (!value) return err(`${where}: no amount at '${spec.value.show}'`);
    const customer = spec.customer.read(event.data);
    if (typeof customer !== 'string') return err(`${where}: no customer at '${spec.customer.show}'`);
    const order = spec.order.read(event.data);
    if (typeof order !== 'string') return err(`${where}: no order at '${spec.order.show}'`);
    return ok({ eventId: event.id, ref: refOf(event), topic: spec.topic, district: spec.district, value, customer, orderId: order, redriveOf: event.redriveOf });
  }

  priceBytes(bytes: Buffer): Result<Priced> {
    const e = CloudEvents.parse(bytes);
    return e.ok ? this.price(e.value) : e;
  }
}

let shipped: Pricing | undefined;

export const defaultPricing = (): Pricing => (shipped ??= new Pricing(defaultCatalog()));
