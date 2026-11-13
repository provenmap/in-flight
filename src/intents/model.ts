import { FieldPath, isObject, sameRef, showRef, type EventRef, type Json } from '../pricing/paths.js';
import { assignable, isNumeric, parseFields, showType, typeAt, type FieldType, type Fields } from './types.js';

/** `Entity.attribute`, such as `Order.total`. */
export interface AttrRef {
  entity: string;
  attribute: string;
}

export const showAttr = (a: AttrRef): string => `${a.entity}.${a.attribute}`;
export const sameAttr = (a: AttrRef, b: AttrRef): boolean => a.entity === b.entity && a.attribute === b.attribute;

export type EffectOp = 'assign' | 'add' | 'subtract';
export type EffectSource = { field: FieldPath } | { value: Json };

/** What an event does to one attribute of an entity. */
export interface Effect {
  op: EffectOp;
  attribute: AttrRef;
  source: EffectSource;
}

export const showSource = (s: EffectSource): string => ('field' in s ? `from ${s.field.show}` : `= ${JSON.stringify(s.value)}`);
export const showEffect = (e: Effect): string => `${e.op} ${showAttr(e.attribute)} ${showSource(e.source)}`;

/** What one event type version means: its typed fields, the entities it is about, and what it does to them. */
export interface SemanticEvent {
  ref: EventRef;
  topic: string;
  fields: Fields;
  /** Entity to the field that carries its key, such as Order to `orderId`. */
  keys: Map<string, FieldPath>;
  effects: Effect[];
}

export const affects = (e: SemanticEvent, attr: AttrRef): Effect[] => e.effects.filter(x => sameAttr(x.attribute, attr));

/** Entities with typed attributes, and every event type version that says what it means. */
export class Model {
  constructor(
    readonly entities: Map<string, Fields>,
    readonly events: SemanticEvent[],
  ) {}

  find(ref: EventRef): SemanticEvent | undefined {
    return this.events.find(e => sameRef(e.ref, ref));
  }

  versionsOf(name: string): SemanticEvent[] {
    return this.events.filter(e => e.ref.name === name).sort((a, b) => a.ref.version - b.ref.version);
  }

  attributeType(attr: AttrRef): FieldType | undefined {
    return this.entities.get(attr.entity)?.get(attr.attribute);
  }

  /** The same model with one event version taken out: the catalog as it was before that version. */
  without(ref: EventRef): Model {
    return new Model(this.entities, this.events.filter(e => !sameRef(e.ref, ref)));
  }
}

/** One event as the catalog file declares it, before its meaning is checked. */
export interface RawEvent {
  ref: EventRef;
  topic: string;
  fields?: unknown;
  keys?: unknown;
  effects?: unknown;
}

const TypeName = /^[A-Z][A-Za-z0-9]*$/;
const ops: readonly string[] = ['assign', 'add', 'subtract'];

/**
 * Reads and checks the catalog's meaning. An event with no `fields`, `keys` or `effects` stays outside it: it is
 * still priced and watched, but no intent can match it. Every failure names what is wrong; nothing is guessed.
 */
export function parseModel(rawEntities: unknown, rawEvents: RawEvent[]): { model: Model; errors: string[] } {
  const errors: string[] = [];
  const entities = new Map<string, Fields>();

  if (rawEntities !== undefined && !isObject(rawEntities)) errors.push("'entities' must be an object of entity to attributes");
  for (const [entity, attrs] of Object.entries(isObject(rawEntities) ? rawEntities : {})) {
    if (!TypeName.test(entity)) errors.push(`entity '${entity}' must be a capitalised identifier`);
    const fields = parseFields(attrs);
    if (!fields.ok) {
      errors.push(`entity ${entity}: ${fields.error}`);
      continue;
    }
    for (const [attr, t] of fields.value) {
      if (t.kind === 'object') errors.push(`${entity}.${attr} must be a scalar type, was ${showType(t)}`);
    }
    entities.set(entity, fields.value);
  }

  const events: SemanticEvent[] = [];
  for (const raw of rawEvents) {
    if (raw.fields === undefined && raw.keys === undefined && raw.effects === undefined) continue;
    const where = showRef(raw.ref);
    const fields = parseFields(raw.fields ?? {});
    if (!fields.ok) {
      errors.push(`${where}: ${fields.error}`);
      continue;
    }
    const event: SemanticEvent = { ref: raw.ref, topic: raw.topic, fields: fields.value, keys: new Map(), effects: [] };

    if (raw.keys !== undefined && !isObject(raw.keys)) errors.push(`${where}: 'keys' must be an object of entity to field path`);
    for (const [entity, text] of Object.entries(isObject(raw.keys) ? raw.keys : {})) {
      const path = typeof text === 'string' ? FieldPath.parse(text) : undefined;
      if (!path?.ok) {
        errors.push(`${where}: the key for ${entity} must be a dotted field path`);
        continue;
      }
      if (!entities.has(entity)) errors.push(`${where}: key for unknown entity '${entity}'`);
      const t = typeAt(event.fields, path.value);
      if (!t) errors.push(`${where}: key path '${path.value.show}' for ${entity} is not a field`);
      else if (t.optional) errors.push(`${where}: key path '${path.value.show}' for ${entity} is optional`);
      else if (!['string', 'int', 'long'].includes(t.kind)) errors.push(`${where}: key path '${path.value.show}' for ${entity} must be string, int or long, was ${showType(t)}`);
      event.keys.set(entity, path.value);
    }
    if (!event.keys.size) errors.push(`${where}: declares no entity key, so no intent could key it`);

    if (raw.effects !== undefined && !Array.isArray(raw.effects)) errors.push(`${where}: 'effects' must be an array`);
    for (const rawEffect of Array.isArray(raw.effects) ? raw.effects : []) {
      const effect = parseEffect(rawEffect);
      if (typeof effect === 'string') {
        errors.push(`${where}: ${effect}`);
        continue;
      }
      const label = `${where}: effect '${showEffect(effect)}'`;
      const attrType = entities.get(effect.attribute.entity)?.get(effect.attribute.attribute);
      if (!attrType) {
        errors.push(`${label}: unknown attribute ${showAttr(effect.attribute)}`);
        continue;
      }
      if (!event.keys.has(effect.attribute.entity)) {
        errors.push(`${label}: the event carries no ${effect.attribute.entity} key, so it cannot say which ${effect.attribute.entity} it changes`);
      }
      if (effect.op !== 'assign' && !isNumeric(attrType)) {
        errors.push(`${label}: ${effect.op} needs a numeric attribute, ${showAttr(effect.attribute)} is ${showType(attrType)}`);
      }
      if ('field' in effect.source) {
        const t = typeAt(event.fields, effect.source.field);
        if (!t) errors.push(`${label}: '${effect.source.field.show}' is not a field`);
        else if (!assignable(t, attrType)) errors.push(`${label}: '${effect.source.field.show}' is ${showType(t)}, ${showAttr(effect.attribute)} is ${showType(attrType)}`);
      } else {
        if (effect.op !== 'assign') errors.push(`${label}: only assign takes a constant`);
        if (!constFits(effect.source.value, attrType)) errors.push(`${label}: constant ${JSON.stringify(effect.source.value)} does not fit ${showType(attrType)}`);
      }
      event.effects.push(effect);
    }
    events.push(event);
  }

  return { model: new Model(entities, events), errors };
}

function parseEffect(raw: unknown): Effect | string {
  if (!isObject(raw)) return 'an effect must be an object';
  const { op, attribute, from, value } = raw;
  if (typeof op !== 'string' || !ops.includes(op)) return `an effect's 'op' must be one of ${ops.join(', ')}`;
  const [entity, attr, ...rest] = typeof attribute === 'string' ? attribute.split('.') : [];
  if (!entity || !attr || rest.length) return `an effect's 'attribute' must be Entity.attribute, was ${JSON.stringify(attribute ?? null)}`;
  const ref: AttrRef = { entity, attribute: attr };
  if (from !== undefined && value !== undefined) return `effect on ${showAttr(ref)}: give 'from' or 'value', not both`;
  if (typeof from === 'string') {
    const path = FieldPath.parse(from);
    return path.ok ? { op: op as EffectOp, attribute: ref, source: { field: path.value } } : `effect on ${showAttr(ref)}: ${path.error}`;
  }
  if (value !== undefined) return { op: op as EffectOp, attribute: ref, source: { value } };
  return `effect on ${showAttr(ref)}: needs 'from' (a field path) or 'value' (a constant)`;
}

function constFits(value: Json, t: FieldType): boolean {
  switch (t.kind) {
    case 'string':
    case 'timestamp':
      return typeof value === 'string';
    case 'decimal':
      return typeof value === 'string' && value.trim() !== '' && Number.isFinite(Number(value));
    case 'int':
    case 'long':
      return typeof value === 'number' && Number.isInteger(value);
    case 'boolean':
      return typeof value === 'boolean';
    default:
      return false;
  }
}
