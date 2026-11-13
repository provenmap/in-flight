import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { z } from 'zod';
import { err, ok, type Result } from '../result.js';

import { FieldPath, sameRef, showRef, type EventRef } from './paths.js';
import { parseSemantics, type Semantics } from '../intents/semantics.js';

export { FieldPath, isObject, parseRef, sameRef, showRef } from './paths.js';
export type { EventRef, Json, JsonObject } from './paths.js';

/** A place on the map: a team that produces or reads events. A sink, such as the customers, only reads. */
export interface District {
  id: string;
  name: string;
  sink: boolean;
}

/** An event type, its topic and district, and where its value, customer and order are declared to be. */
export interface EventSpec {
  ref: EventRef;
  topic: string;
  district: string;
  value: FieldPath;
  customer: FieldPath;
  order: FieldPath;
}

/**
 * A reader of topics. A service has a consumer group and a dead-letter queue; a sink has neither. A service that
 * can redrive its dead-letter queue names the consumer group that does it: that group's committed offsets say
 * which dead letters have been dealt with, which is how the observer knows after a restart.
 */
export interface Consumer {
  group?: string;
  district: string;
  reads: string[];
  dlq?: string;
  redrive?: string;
  /** The declared intent this consumer reads by: what it cares about, in the catalog's terms. */
  intent?: string;
  /** The group, or the district for a sink. */
  name: string;
}

/** A service: a consumer with a group. */
export type Service = Consumer & { group: string };

/**
 * The headers a flow's services put on a dead letter, when they are not one of the conventions the observer knows:
 * this app's own, Kafka Connect's and Spring Kafka's.
 */
export interface DeadLetterHeaders {
  error?: string;
  failedAt?: string;
  sourceTopic?: string;
}

// ---- The file's shape. Messages name the field the way the Scala build did, so errors read the same. ----------

const str = (key: string) => z.string({ error: `'${key}' must be a string` });
const path = (key: string) =>
  str(key).transform((text, ctx) => {
    const p = FieldPath.parse(text);
    if (p.ok) return p.value;
    ctx.addIssue({ code: 'custom', message: `${key}: ${p.error}` });
    return z.NEVER;
  });
const array = <T extends z.ZodType>(key: string, item: T) => z.array(item, { error: `'${key}' must be an array` });
const optionalString = z.unknown().optional().transform(v => (typeof v === 'string' ? v : undefined));

const schema = z.object({
  flow: str('flow'),
  currency: str('currency'),
  districts: array('districts', z.object({
    id: str('id'),
    name: str('name'),
    sink: z.unknown().optional().transform(v => v === true),
  })),
  events: array('events', z.object({
    type: str('type'),
    version: z.number({ error: "'version' must be a whole number" }).int({ error: "'version' must be a whole number" }),
    topic: str('topic'),
    district: str('district'),
    value: path('value'),
    customer: path('customer'),
    order: path('order'),
    fields: z.unknown().optional(),
    keys: z.unknown().optional(),
    effects: z.unknown().optional(),
  })),
  consumers: array('consumers', z.object({
    group: optionalString,
    district: str('district'),
    reads: z.unknown().optional().transform(v => (Array.isArray(v) ? v.filter((t): t is string => typeof t === 'string') : [])),
    dlq: optionalString,
    redrive: optionalString,
    intent: optionalString,
  })),
  deadLetterHeaders: z.object({ error: optionalString, failedAt: optionalString, sourceTopic: optionalString }).optional(),
  entities: z.unknown().optional(),
  intents: z.unknown().optional(),
});

/** `events[1]` for a field of the second event, `catalog` for a top-level one; top-level arrays name only themselves. */
function describe(issue: z.core.$ZodIssue): string {
  const owner = issue.path.slice(0, -1);
  if (!owner.length) return issue.code === 'invalid_type' && issue.expected === 'array' ? issue.message : `catalog: ${issue.message}`;
  const where = owner.map((p, i) => (typeof p === 'number' ? `[${p}]` : i ? `.${String(p)}` : String(p))).join('');
  return `${where}: ${issue.message}`;
}

/** The flow: its districts, every event type version, and who reads what. */
export class Catalog {
  constructor(
    readonly flow: string,
    readonly currency: string,
    readonly districts: District[],
    readonly events: EventSpec[],
    readonly consumers: Consumer[],
    readonly deadLetterHeaders: DeadLetterHeaders = {},
    /** What the events mean and the intents declared over them, when the catalog says. */
    readonly semantics?: Semantics,
  ) {}

  /** The consumers that read by this intent. */
  consumersOf(intent: string): Consumer[] {
    return this.consumers.filter(k => k.intent === intent);
  }

  find(ref: EventRef): EventSpec | undefined {
    return this.events.find(e => sameRef(e.ref, ref));
  }

  district(id: string): District | undefined {
    return this.districts.find(d => d.id === id);
  }

  get topics(): string[] {
    return [...new Set(this.events.map(e => e.topic))];
  }

  get dlqs(): string[] {
    return this.consumers.flatMap(k => (k.dlq ? [k.dlq] : []));
  }

  get services(): Service[] {
    return this.consumers.filter((k): k is Service => k.group !== undefined);
  }

  consumer(group: string): Consumer | undefined {
    return this.consumers.find(k => k.group === group);
  }

  readersOf(topic: string): Consumer[] {
    return this.consumers.filter(k => k.reads.includes(topic));
  }

  /** The consumer group that redrives this dead-letter queue, if any. */
  redriveGroupOf(dlq: string): string | undefined {
    return this.ownerOfDlq(dlq)?.redrive;
  }

  /** The service that dead-letters into this queue. */
  ownerOfDlq(dlq: string): Consumer | undefined {
    return this.consumers.find(k => k.dlq === dlq);
  }

  /** The district that produces onto this topic. One topic belongs to one district. */
  districtOf(topic: string): string | undefined {
    return this.events.find(e => e.topic === topic)?.district;
  }

  static load(file: string): Result<Catalog, string[]> {
    return Catalog.parse(readFileSync(file, 'utf8'));
  }

  static parse(text: string): Result<Catalog, string[]> {
    let json: unknown;
    try {
      json = JSON.parse(text);
    } catch (e) {
      return err([`not JSON: ${(e as Error).message}`]);
    }
    return Catalog.fromJson(json);
  }

  static fromJson(json: unknown): Result<Catalog, string[]> {
    const parsed = schema.safeParse(json);
    if (!parsed.success) return err(parsed.error.issues.map(describe));
    const f = parsed.data;
    const events = f.events.map(({ type, version, fields, keys, effects, ...e }) => ({ spec: { ref: { name: type, version }, ...e }, fields, keys, effects }));
    const semantics = parseSemantics({
      entities: f.entities,
      intents: f.intents,
      events: events.map(e => ({ ref: e.spec.ref, topic: e.spec.topic, fields: e.fields, keys: e.keys, effects: e.effects })),
    });
    if (!semantics.ok) return err(semantics.error);
    const byIntent: string[] = [];
    const consumers = f.consumers.map(({ intent, ...k }) => {
      const name = k.group ?? k.district;
      if (intent === undefined) return { ...k, name };
      const table = semantics.value?.table(intent);
      if (!table) {
        byIntent.push(`consumer ${name}: reads by intent '${intent}', which the catalog does not declare`);
        return { ...k, intent, name };
      }
      // Subscribe by intent: with no topics listed, the consumer reads the topics its intent draws on.
      if (!k.reads.length) return { ...k, reads: table.sourceTopics, intent, name };
      for (const t of table.sourceTopics.filter(t => !k.reads.includes(t))) {
        byIntent.push(`consumer ${name}: intent '${intent}' draws on topic '${t}', which it does not read`);
      }
      return { ...k, intent, name };
    });
    const catalog = new Catalog(f.flow, f.currency, f.districts, events.map(e => e.spec), consumers, f.deadLetterHeaders ?? {}, semantics.value);
    const errors = [...byIntent, ...validate(catalog)];
    return errors.length ? err(errors) : ok(catalog);
  }
}

function dupes<A>(as: A[], key: (a: A) => string = String): A[] {
  const seen = new Map<string, { a: A; n: number }>();
  for (const a of as) {
    const k = key(a);
    const s = seen.get(k);
    if (s) s.n++;
    else seen.set(k, { a, n: 1 });
  }
  return [...seen.values()].filter(s => s.n > 1).map(s => s.a);
}

/** Every reference resolves, and no event type version, consumer group or dead-letter queue is declared twice. */
export function validate(c: Catalog): string[] {
  const ids = new Set(c.districts.map(d => d.id));
  const topics = new Set(c.topics);
  const out: string[] = [];

  for (const d of dupes(c.districts.map(d => d.id))) out.push(`district '${d}' is declared twice`);

  for (const e of c.events) {
    if (!ids.has(e.district)) out.push(`${showRef(e.ref)}: unknown district '${e.district}'`);
    if (c.district(e.district)?.sink) out.push(`${showRef(e.ref)}: a sink cannot produce events`);
  }
  for (const r of dupes(c.events.map(e => e.ref), showRef)) out.push(`${showRef(r)} is declared twice`);
  for (const t of c.topics) {
    if (new Set(c.events.filter(e => e.topic === t).map(e => e.district)).size > 1) out.push(`topic '${t}' is produced by more than one district`);
  }

  for (const k of c.consumers) {
    if (!ids.has(k.district)) out.push(`consumer ${k.name}: unknown district '${k.district}'`);
    if (!k.reads.length) out.push(`consumer ${k.name}: reads nothing`);
    for (const t of k.reads.filter(t => !topics.has(t))) out.push(`consumer ${k.name}: reads unknown topic '${t}'`);
    if (k.dlq !== undefined && k.group === undefined) out.push(`consumer ${k.name}: a dead-letter queue needs a consumer group`);
    if (k.dlq !== undefined && topics.has(k.dlq)) out.push(`consumer ${k.name}: dead-letter queue '${k.dlq}' is also an event topic`);
    if (k.redrive !== undefined && k.dlq === undefined) out.push(`consumer ${k.name}: a redrive group needs a dead-letter queue`);
  }
  for (const g of dupes(c.consumers.flatMap(k => [k.group, k.redrive].filter((x): x is string => x !== undefined)))) {
    out.push(`consumer group '${g}' is declared twice`);
  }
  for (const q of dupes(c.dlqs)) out.push(`dead-letter queue '${q}' is declared twice`);

  return out;
}

/** The catalog shipped with the package, beside `src` and `dist`. */
export const catalogFile = process.env.RIF_CATALOG ?? fileURLToPath(new URL('../../catalog/order-to-cash.json', import.meta.url));

let shipped: Catalog | undefined;

export function defaultCatalog(): Catalog {
  if (!shipped) {
    const res = Catalog.load(catalogFile);
    if (!res.ok) throw new Error(`${catalogFile}: ${res.error.join('; ')}`);
    shipped = res.value;
  }
  return shipped;
}
