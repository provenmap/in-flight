import { err, ok, type Result } from '../result.js';
import { isObject, type FieldPath } from '../pricing/paths.js';

const scalars = ['string', 'int', 'long', 'decimal', 'boolean', 'timestamp'] as const;
export type Scalar = (typeof scalars)[number];

/** The type of an event field or an entity attribute. */
export type FieldType =
  | { kind: Scalar; optional: boolean }
  | { kind: 'object'; fields: Fields; optional: boolean };

export type Fields = Map<string, FieldType>;

export const isNumeric = (t: FieldType): boolean => t.kind === 'int' || t.kind === 'long' || t.kind === 'decimal';

export function showType(t: FieldType): string {
  const base = t.kind === 'object' ? `{ ${[...t.fields].map(([k, v]) => `${k}: ${showType(v)}`).join(', ')} }` : t.kind;
  return t.optional ? `${base}?` : base;
}

export const sameType = (a: FieldType, b: FieldType): boolean => showType(a) === showType(b);

/** `"decimal"`, `"string?"`, or an object of named fields. */
export function parseType(json: unknown): Result<FieldType> {
  if (typeof json === 'string') {
    const optional = json.endsWith('?');
    const base = optional ? json.slice(0, -1) : json;
    return (scalars as readonly string[]).includes(base)
      ? ok({ kind: base as Scalar, optional })
      : err(`unknown field type '${json}' (known: ${[...scalars].sort().join(', ')}, or an object)`);
  }
  if (isObject(json)) {
    const fields = parseFields(json);
    return fields.ok ? ok({ kind: 'object', fields: fields.value, optional: false }) : fields;
  }
  return err('a field type is a name such as "decimal" or an object of fields');
}

export function parseFields(json: unknown): Result<Fields> {
  if (!isObject(json)) return err('fields must be an object of name to type');
  const out: Fields = new Map();
  for (const [name, t] of Object.entries(json)) {
    const parsed = parseType(t);
    if (!parsed.ok) return err(`${name}: ${parsed.error}`);
    out.set(name, parsed.value);
  }
  return ok(out);
}

/** The type at a path, optional when any step on the way is optional. */
export function typeAt(fields: Fields, path: FieldPath): FieldType | undefined {
  let at = fields;
  let optional = false;
  for (const [i, name] of path.segments.entries()) {
    const t = at.get(name);
    if (!t) return undefined;
    optional ||= t.optional;
    if (i === path.segments.length - 1) return { ...t, optional };
    if (t.kind !== 'object') return undefined;
    at = t.fields;
  }
  return undefined;
}

/** A value of type `from` can feed an attribute of type `to`: the same type, or a numeric widening. */
export function assignable(from: FieldType, to: FieldType): boolean {
  if (from.kind === 'object' || to.kind === 'object') return false;
  if (from.kind === to.kind) return true;
  if (from.kind === 'int') return to.kind === 'long' || to.kind === 'decimal';
  return from.kind === 'long' && to.kind === 'decimal';
}
