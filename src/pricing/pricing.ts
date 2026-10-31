import Big from 'big.js';
import { err, ok, type Result } from '../result.js';
import { defaultCatalog, showRef, type Catalog, type EventRef, type EventSpec, type Json } from './catalog.js';
import { CloudEvents, type CloudEvent } from './cloudevents.js';
import { decode, type Decoded } from './records.js';

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
  /** Set when the record's own version is not in the catalog and an older one of its type priced it. */
  pricedAs?: EventRef;
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

/**
 * Prices events by the paths the catalog declares. Nothing is guessed: an undeclared type or a missing path is an
 * error. A record that does not say its type and version is priced as one of the catalog's types for its topic,
 * newest version first: the first whose declared value, customer and order all resolve. A version the catalog does
 * not have yet, the usual way a schema change arrives, is priced the same way from the versions of its type it has.
 */
export class Pricing {
  constructor(readonly catalog: Catalog) {}

  price(event: CloudEvent): Result<Priced> {
    return this.priceDecoded({ id: event.id, type: event.type, version: event.version, data: event.data, redriveOf: event.redriveOf }, [], event.id);
  }

  priceBytes(bytes: Buffer): Result<Priced> {
    const e = CloudEvents.parse(bytes);
    return e.ok ? this.price(e.value) : e;
  }

  /** A record from `topics` (the topic it was read from, or for a dead letter the topics it may have come from). */
  priceRecord(value: Buffer, headers: Map<string, string>, topics: string[], fallbackId: string): Result<Priced> {
    const d = decode(value, headers);
    return d.ok ? this.priceDecoded(d.value, topics, fallbackId) : d;
  }

  priceDecoded(d: Decoded, topics: string[], fallbackId: string): Result<Priced> {
    const id = d.id ?? fallbackId;
    if (d.type !== null && d.version !== null) {
      const ref = { name: d.type, version: d.version };
      const spec = this.catalog.find(ref);
      if (spec) return this.priceAs(spec, d, id);
      const known = this.catalog.events.filter(e => e.ref.name === ref.name).sort((a, b) => b.ref.version - a.ref.version);
      if (!known.length) return err(`${showRef(ref)} (${id}) is not in the catalog`);
      for (const k of known) {
        const p = this.priceAs(k, d, id);
        if (p.ok) return ok({ ...p.value, ref, pricedAs: k.ref });
      }
      return err(`${showRef(ref)} (${id}) is not in the catalog, and no version of ${ref.name} it has can read it`);
    }
    const specs = this.catalog.events
      .filter(e => (d.type !== null ? e.ref.name === d.type : topics.includes(e.topic)))
      .sort((a, b) => b.ref.version - a.ref.version);
    if (!specs.length) {
      return err(d.type !== null ? `${d.type} (${id}) is not in the catalog` : `a record on ${topics.join(', ') || 'no topic'} (${id}) has no event type in the catalog`);
    }
    let first: Result<Priced> | undefined;
    for (const spec of specs) {
      const p = this.priceAs(spec, d, id);
      if (p.ok) return p;
      first ??= p;
    }
    return first!;
  }

  private priceAs(spec: EventSpec, d: Decoded, id: string): Result<Priced> {
    const where = `${showRef(spec.ref)} (${id})`;
    const value = money(spec.value.read(d.data));
    if (!value) return err(`${where}: no amount at '${spec.value.show}'`);
    const customer = spec.customer.read(d.data);
    if (typeof customer !== 'string') return err(`${where}: no customer at '${spec.customer.show}'`);
    const order = spec.order.read(d.data);
    if (typeof order !== 'string') return err(`${where}: no order at '${spec.order.show}'`);
    return ok({ eventId: id, ref: spec.ref, topic: spec.topic, district: spec.district, value, customer, orderId: order, redriveOf: d.redriveOf });
  }
}

let shipped: Pricing | undefined;

export const defaultPricing = (): Pricing => (shipped ??= new Pricing(defaultCatalog()));
