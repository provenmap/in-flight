import { err, ok, type Result } from '../result.js';

export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export type JsonObject = { [key: string]: Json };

export const isObject = (v: unknown): v is JsonObject => typeof v === 'object' && v !== null && !Array.isArray(v);

/** A dotted path into an event's data, such as `total.amount`. */
export class FieldPath {
  constructor(readonly segments: string[]) {}

  get show(): string {
    return this.segments.join('.');
  }

  /** Reads this path out of a JSON object. Null counts as missing. */
  read(json: Json): Json | undefined {
    let at: Json | undefined = json;
    for (const name of this.segments) {
      at = isObject(at) ? at[name] : undefined;
      if (at === null) return undefined;
    }
    return at;
  }

  static parse(text: string): Result<FieldPath> {
    const segments = text.split('.');
    return text && segments.every(s => s) ? ok(new FieldPath(segments)) : err(`'${text}' is not a dotted field path`);
  }
}

/** One event type at one version. Versions are immutable once published. */
export interface EventRef {
  name: string;
  version: number;
}

export const showRef = (r: EventRef): string => `${r.name}@v${r.version}`;

export const sameRef = (a: EventRef, b: EventRef): boolean => a.name === b.name && a.version === b.version;

/** `OrderPlaced@v2`, the way `showRef` writes it. */
export function parseRef(text: string): Result<EventRef> {
  const m = /^([A-Za-z][A-Za-z0-9]*)@v(\d+)$/.exec(text.trim());
  return m ? ok({ name: m[1]!, version: Number(m[2]) }) : err(`'${text}' is not an event version such as OrderPlaced@v2`);
}
