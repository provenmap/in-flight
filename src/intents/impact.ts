import { sameRef, showRef, type EventRef } from '../pricing/paths.js';
import { compile, type Read, type Route, type RoutingTable } from './compiler.js';
import { showAttr, showEffect, type Model, type SemanticEvent } from './model.js';
import type { Semantics } from './semantics.js';
import { sameType, showType } from './types.js';

/** Someone who reads by an intent, and where what they cannot read ends up. */
export interface IntentConsumer {
  name: string;
  dlq?: string;
}

/**
 * How an event version newly reaching an intent lands on its consumers.
 * - `same-reads`: a new version of a type they already read, and they read the same fields from it.
 * - `new-reads`: a new version whose fields they read have moved or changed type. Their code must change first.
 * - `new-type`: an event type they have never been sent.
 */
export type Arrival = 'same-reads' | 'new-reads' | 'new-type';

export interface RouteArrival {
  route: Route;
  arrival: Arrival;
  /** The version it is compared with, for a new version. */
  previous?: EventRef;
  /** What moved, one line per thing read: `Order.total: from total (decimal) -> from total.amount (decimal)`. */
  differences: string[];
}

/** One intent whose matches a change alters, and who reads by it. */
export interface IntentImpact {
  intent: string;
  predicate: string;
  entity: string;
  arrivals: RouteArrival[];
  removed: Route[];
  consumers: IntentConsumer[];
}

/** A consumer that will meet events it cannot read unless it changes first. */
export interface Breakage {
  consumer: string;
  dlq?: string;
  intent: string;
  event: EventRef;
  arrival: Exclude<Arrival, 'same-reads'>;
  differences: string[];
}

/** Everything a change to the catalog's meaning would do, worked out before a single event is produced. */
export interface Impact {
  changes: string[];
  intents: IntentImpact[];
  unaffected: string[];
  /** Why the change cannot stand at all: a published version edited in place, or an intent that no longer compiles. */
  errors: string[];
  breaks: Breakage[];
}

const showRead = (r: Read): string => `${r.source} (${r.type})`;

function differences(before: Read[], after: Read[]): string[] {
  const out: string[] = [];
  for (const what of new Set([...before, ...after].map(r => r.what))) {
    const b = before.filter(r => r.what === what).map(showRead).join(', ');
    const a = after.filter(r => r.what === what).map(showRead).join(', ');
    if (a !== b) out.push(`${what}: ${b || 'not read'} -> ${a || 'gone'}`);
  }
  return out;
}

function arrival(route: Route, before: RoutingTable | undefined): RouteArrival {
  const earlier = (before?.routes ?? []).filter(r => r.event.name === route.event.name).sort((a, b) => b.event.version - a.event.version);
  const previous = earlier.find(r => r.event.version < route.event.version) ?? earlier[0];
  if (!previous) return { route, arrival: 'new-type', differences: [] };
  const diffs = differences(previous.reads, route.reads);
  return { route, arrival: diffs.length ? 'new-reads' : 'same-reads', previous: previous.event, differences: diffs };
}

function redefinitions(old: SemanticEvent, now: SemanticEvent): string[] {
  const fields = (e: SemanticEvent) => [...e.fields].map(([k, t]) => `${k}: ${showType(t)}`).join(', ');
  const keys = (e: SemanticEvent) => [...e.keys].map(([k, p]) => `${k}: ${p.show}`).join(', ');
  const effects = (e: SemanticEvent) => e.effects.map(showEffect).join('; ');
  return [
    old.topic !== now.topic ? `topic ${old.topic} -> ${now.topic}` : '',
    fields(old) !== fields(now) ? 'fields changed' : '',
    keys(old) !== keys(now) ? 'keys changed' : '',
    effects(old) !== effects(now) ? 'effects changed' : '',
  ].filter(Boolean);
}

/** What changed between two versions of the catalog's meaning, in words. */
export function diff(before: Model, after: Model): { changes: string[]; errors: string[] } {
  const changes: string[] = [];
  const errors: string[] = [];
  for (const e of after.events) {
    const old = before.find(e.ref);
    if (!old) {
      changes.push(before.events.some(x => x.ref.name === e.ref.name) ? `new version ${showRef(e.ref)} on ${e.topic}` : `new event type ${showRef(e.ref)} on ${e.topic}`);
      continue;
    }
    const details = redefinitions(old, e);
    if (details.length) {
      changes.push(`${showRef(e.ref)} redefined in place: ${details.join('; ')}`);
      errors.push(`${showRef(e.ref)} is already published and cannot change (${details.join(', ')}); add version ${e.ref.version + 1} instead`);
    }
  }
  for (const e of before.events) if (!after.find(e.ref)) changes.push(`removed ${showRef(e.ref)}`);
  for (const [entity, attrs] of after.entities) {
    for (const [attribute, t] of attrs) {
      const old = before.attributeType({ entity, attribute });
      if (!old) changes.push(`new attribute ${showAttr({ entity, attribute })}: ${showType(t)}`);
      else if (!sameType(old, t)) changes.push(`${showAttr({ entity, attribute })} retyped ${showType(old)} -> ${showType(t)}`);
    }
  }
  for (const [entity, attrs] of before.entities) {
    for (const attribute of attrs.keys()) if (!after.attributeType({ entity, attribute })) changes.push(`removed attribute ${showAttr({ entity, attribute })}`);
  }
  return { changes, errors };
}

/** Recompiles every intent against both models and says which consumers the difference reaches, and how. */
export function analyse(before: Model, after: Semantics, consumersOf: (intent: string) => IntentConsumer[]): Impact {
  const { changes, errors } = diff(before, after.model);
  const intents: IntentImpact[] = [];
  const unaffected: string[] = [];

  for (const intent of [...after.intents].sort((a, b) => a.name.localeCompare(b.name))) {
    const now = compile(intent, after.model);
    if (!now.ok) {
      errors.push(...now.error.map(e => `would break ${e}`));
      continue;
    }
    const then = compile(intent, before);
    const was = then.ok ? then.value : undefined;
    const arrivals = now.value.routes.filter(r => !was?.routes.some(o => sameRef(o.event, r.event))).map(r => arrival(r, was));
    const removed = (was?.routes ?? []).filter(o => !now.value.routes.some(r => sameRef(r.event, o.event)));
    if (!arrivals.length && !removed.length) unaffected.push(intent.name);
    else intents.push({ intent: intent.name, predicate: now.value.predicate, entity: now.value.entity, arrivals, removed, consumers: consumersOf(intent.name) });
  }

  const breaks = intents.flatMap(i =>
    i.arrivals.flatMap(a => {
      const kind = a.arrival;
      if (kind === 'same-reads') return [];
      return i.consumers.map((c): Breakage => ({ consumer: c.name, ...(c.dlq ? { dlq: c.dlq } : {}), intent: i.intent, event: a.route.event, arrival: kind, differences: a.differences }));
    }),
  );
  return { changes, intents, unaffected, errors, breaks };
}

/** The impact of one event version: the catalog with it against the catalog without it. */
export function impactOf(ref: EventRef, semantics: Semantics, consumersOf: (intent: string) => IntentConsumer[]): Impact | string {
  if (!semantics.model.find(ref)) return `${showRef(ref)} is not in the catalog, or the catalog does not say what it means (fields, keys, effects)`;
  return analyse(semantics.model.without(ref), semantics, consumersOf);
}

/** The impact as a report a person reads top to bottom. */
export function report(impact: Impact): string {
  const lines: string[] = [];
  lines.push('Catalog changes:');
  if (!impact.changes.length) lines.push('  (none)');
  for (const c of impact.changes) lines.push(`  - ${c}`);

  lines.push('Intents affected:');
  if (!impact.intents.length) lines.push('  (none)');
  for (const i of impact.intents) {
    lines.push(`  - ${i.intent}: ${i.predicate}`);
    for (const a of i.arrivals) {
      const why = a.route.reasons.join('; ');
      if (a.arrival === 'new-type') lines.push(`      + ${showRef(a.route.event)} from ${a.route.topic}, a type its consumers have never read: ${why}`);
      else if (a.arrival === 'same-reads') lines.push(`      + ${showRef(a.route.event)} from ${a.route.topic}, read the same way as ${showRef(a.previous!)}`);
      else {
        lines.push(`      ! ${showRef(a.route.event)} from ${a.route.topic}, read differently from ${showRef(a.previous!)}:`);
        for (const d of a.differences) lines.push(`          ${d}`);
      }
    }
    for (const r of i.removed) lines.push(`      - ${showRef(r.event)} no longer matches`);
    lines.push(`      consumers: ${i.consumers.length ? i.consumers.map(c => c.name).join(', ') : 'none declared'}`);
  }
  if (impact.unaffected.length) lines.push(`Intents unaffected: ${impact.unaffected.join(', ')}`);

  if (impact.breaks.length) {
    lines.push('Must change before it ships:');
    for (const b of impact.breaks) {
      const fate = b.dlq ? `; until then every ${showRef(b.event)} it meets goes to ${b.dlq}` : '';
      lines.push(`  ! ${b.consumer} (${b.intent}) ${b.arrival === 'new-type' ? 'has no code for' : 'reads'} ${showRef(b.event)}${b.arrival === 'new-reads' ? ' differently' : ''}${fate}`);
    }
  }
  if (impact.errors.length) {
    lines.push('Blocked:');
    for (const e of impact.errors) lines.push(`  ! ${e}`);
  }
  return lines.join('\n');
}
