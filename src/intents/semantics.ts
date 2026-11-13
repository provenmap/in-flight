import { err, ok, type Result } from '../result.js';
import { isObject } from '../pricing/paths.js';
import { compile, parseIntent, type Intent, type RoutingTable } from './compiler.js';
import { parseModel, type Model, type RawEvent } from './model.js';

/**
 * What the catalog's events mean, and the intents declared over them, each compiled. This is the part of the
 * catalog that lets a consumer say what it cares about instead of which topics it reads.
 */
export class Semantics {
  constructor(
    readonly model: Model,
    readonly intents: Intent[],
    private readonly tables: Map<string, RoutingTable>,
  ) {}

  /** A declared intent, compiled. */
  table(intent: string): RoutingTable | undefined {
    return this.tables.get(intent);
  }

  get compiled(): RoutingTable[] {
    return this.intents.map(i => this.tables.get(i.name)!);
  }

  /** Compiles a predicate nobody has declared, to see what it would match. */
  explain(predicate: string, by?: string): Result<RoutingTable, string[]> {
    const intent = parseIntent('explain', predicate, by);
    if (!intent.ok) return err([intent.error.replace(/^intent 'explain': /, '')]);
    const table = compile(intent.value, this.model);
    return table.ok ? table : err(table.error.map(e => e.replace(/^intent 'explain': /, '')));
  }
}

export interface RawSemantics {
  entities?: unknown;
  intents?: unknown;
  events: RawEvent[];
}

/** Undefined when the catalog says nothing about meaning: no entities, no intents, and no event with fields, keys or effects. */
export function parseSemantics(raw: RawSemantics): Result<Semantics | undefined, string[]> {
  const typed = raw.events.some(e => e.fields !== undefined || e.keys !== undefined || e.effects !== undefined);
  if (raw.entities === undefined && raw.intents === undefined && !typed) return ok(undefined);

  const { model, errors } = parseModel(raw.entities, raw.events);

  const intents: Intent[] = [];
  if (raw.intents !== undefined && !Array.isArray(raw.intents)) errors.push("'intents' must be an array");
  for (const [i, item] of (Array.isArray(raw.intents) ? raw.intents : []).entries()) {
    if (!isObject(item) || typeof item.name !== 'string' || typeof item.predicate !== 'string') {
      errors.push(`intents[${i}]: needs a 'name' and a 'predicate'`);
      continue;
    }
    const intent = parseIntent(item.name, item.predicate, typeof item.by === 'string' && item.by ? item.by : undefined);
    if (!intent.ok) errors.push(intent.error);
    else if (intents.some(x => x.name === intent.value.name)) errors.push(`intent '${intent.value.name}' is declared twice`);
    else intents.push(intent.value);
  }
  // An intent is only compiled against a model that is itself sound, so its errors are about the intent.
  if (errors.length) return err(errors);

  const tables = new Map<string, RoutingTable>();
  for (const intent of intents) {
    const table = compile(intent, model);
    if (table.ok) tables.set(intent.name, table.value);
    else errors.push(...table.error);
  }
  return errors.length ? err(errors) : ok(new Semantics(model, intents, tables));
}
