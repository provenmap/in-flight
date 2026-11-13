import { err, ok, type Result } from '../result.js';
import { showRef, type EventRef, type FieldPath } from '../pricing/paths.js';
import { namedEntities, parsePredicate, showPredicate, watchedAttributes, type Predicate } from './language.js';
import { affects, sameAttr, showAttr, showEffect, showSource, type Model, type SemanticEvent } from './model.js';
import { showType, typeAt } from './types.js';

/** A declared intent: what a consumer cares about, and which entity its events are keyed by. */
export interface Intent {
  name: string;
  predicate: Predicate;
  by?: string;
}

const Name = /^[a-z0-9][a-z0-9-]{0,99}$/;

export function parseIntent(name: string, predicate: string, by?: string): Result<Intent> {
  if (!Name.test(name)) return err(`intent name '${name}' must be lowercase letters, digits and hyphens`);
  const p = parsePredicate(predicate);
  if (!p.ok) return err(`intent '${name}': ${p.error}`);
  return ok(by ? { name, predicate: p.value, by } : { name, predicate: p.value });
}

/** One thing a consumer of an intent takes from an event: the entity's key, or what feeds a watched attribute. */
export interface Read {
  /** `Order key`, or `Order.total`. */
  what: string;
  /** `from total.amount`, or `= "placed"` for a constant. */
  source: string;
  type: string;
}

/** How one event type version reaches an intent. */
export interface Route {
  event: EventRef;
  topic: string;
  keyPath: FieldPath;
  /** Why it matches: the effects and facts the predicate found. */
  reasons: string[];
  /** What a consumer of the intent reads from it. Two versions with the same reads need the same code. */
  reads: Read[];
}

/** An intent compiled against the catalog: exactly which event versions it means, from which topics. */
export interface RoutingTable {
  intent: string;
  predicate: string;
  entity: string;
  routes: Route[];
  sourceTopics: string[];
}

/** Compiles an intent against the catalog's meaning. It fails on anything it cannot resolve; nothing is guessed. */
export function compile(intent: Intent, model: Model): Result<RoutingTable, string[]> {
  const refErrors = references(intent.predicate, model);
  if (refErrors.length) return err(refErrors.map(e => `intent '${intent.name}': ${e}`));

  const matched = model.events.filter(e => evaluate(intent.predicate, e)).sort((a, b) => a.ref.name.localeCompare(b.ref.name) || a.ref.version - b.ref.version);
  if (!matched.length) return err([`intent '${intent.name}': ${showPredicate(intent.predicate)} matches no event type in the catalog`]);

  const entity = keyEntity(intent, matched, model);
  if (!entity.ok) return err([entity.error]);
  const unkeyed = matched.filter(e => !e.keys.has(entity.value));
  if (unkeyed.length) {
    return err(unkeyed.map(e => `intent '${intent.name}': ${showRef(e.ref)} matches but carries no ${entity.value} key, so it cannot be ordered by ${entity.value}`));
  }

  const routes = matched.map((e): Route => {
    const keyPath = e.keys.get(entity.value)!;
    return { event: e.ref, topic: e.topic, keyPath, reasons: reasons(intent.predicate, e), reads: reads(intent.predicate, e, entity.value, keyPath) };
  });
  return ok({
    intent: intent.name,
    predicate: showPredicate(intent.predicate),
    entity: entity.value,
    routes,
    sourceTopics: [...new Set(routes.map(r => r.topic))].sort(),
  });
}

function references(p: Predicate, m: Model): string[] {
  switch (p.kind) {
    case 'changes': {
      const attrs = m.entities.get(p.attribute.entity);
      if (!attrs) return [`unknown entity '${p.attribute.entity}' in ${showPredicate(p)}`];
      return attrs.has(p.attribute.attribute) ? [] : [`${p.attribute.entity} has no attribute '${p.attribute.attribute}' (has: ${[...attrs.keys()].join(', ')})`];
    }
    case 'emits':
      return m.events.some(e => e.ref.name === p.eventType) ? [] : [`unknown event type '${p.eventType}' in ${showPredicate(p)}`];
    case 'about':
      return m.entities.has(p.entity) ? [] : [`unknown entity '${p.entity}' in ${showPredicate(p)}`];
    case 'and':
    case 'or':
      return [...references(p.left, m), ...references(p.right, m)];
    case 'not':
      return references(p.inner, m);
  }
}

function evaluate(p: Predicate, e: SemanticEvent): boolean {
  switch (p.kind) {
    case 'changes': return affects(e, p.attribute).length > 0;
    case 'emits': return e.ref.name === p.eventType;
    case 'about': return e.keys.has(p.entity);
    case 'and': return evaluate(p.left, e) && evaluate(p.right, e);
    case 'or': return evaluate(p.left, e) || evaluate(p.right, e);
    case 'not': return !evaluate(p.inner, e);
  }
}

function keyEntity(intent: Intent, matched: SemanticEvent[], m: Model): Result<string> {
  if (intent.by !== undefined) return m.entities.has(intent.by) ? ok(intent.by) : err(`intent '${intent.name}': unknown key entity '${intent.by}'`);
  const named = namedEntities(intent.predicate);
  const candidates = named.size
    ? [...named]
    : [...matched.map(e => new Set(e.keys.keys())).reduce((a, b) => new Set([...a].filter(x => b.has(x))))];
  if (candidates.length === 1) return ok(candidates[0]!);
  return candidates.length
    ? err(`intent '${intent.name}': key entity is ambiguous (${candidates.sort().join(', ')}); say which with 'by'`)
    : err(`intent '${intent.name}': no entity keys every match; say which with 'by'`);
}

function reasons(p: Predicate, e: SemanticEvent): string[] {
  switch (p.kind) {
    case 'changes': return affects(e, p.attribute).map(showEffect);
    case 'emits': return e.ref.name === p.eventType ? [`emits ${e.ref.name}`] : [];
    case 'about': return e.keys.has(p.entity) ? [`about ${p.entity}`] : [];
    case 'and':
    case 'or': return [...new Set([...reasons(p.left, e), ...reasons(p.right, e)])];
    case 'not': return [];
  }
}

function reads(p: Predicate, e: SemanticEvent, entity: string, keyPath: FieldPath): Read[] {
  const keyType = typeAt(e.fields, keyPath);
  const out: Read[] = [{ what: `${entity} key`, source: `from ${keyPath.show}`, type: keyType ? showType(keyType) : 'unknown' }];
  const watched = watchedAttributes(p).filter((a, i, all) => all.findIndex(b => sameAttr(a, b)) === i);
  for (const attr of watched) {
    for (const effect of affects(e, attr)) {
      const t = 'field' in effect.source ? typeAt(e.fields, effect.source.field) : undefined;
      out.push({ what: showAttr(attr), source: `${effect.op === 'assign' ? '' : `${effect.op} `}${showSource(effect.source)}`, type: t ? showType(t) : 'constant' });
    }
  }
  return out;
}
