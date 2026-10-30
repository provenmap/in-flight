import { randomUUID } from 'node:crypto';
import type Big from 'big.js';
import { err, ok, type Result } from '../result.js';
import { isObject, type Json, type JsonObject } from '../pricing/catalog.js';
import { toJson, type CloudEvent } from '../pricing/cloudevents.js';
import { money } from '../pricing/pricing.js';
import type { OutRecord } from '../kafka.js';

/** An event to publish, keyed by its order so one order's events stay in one partition. */
export interface Out {
  topic: string;
  key: string;
  event: CloudEvent;
}

export const record = (o: Out): OutRecord => ({ topic: o.topic, key: o.key, value: toJson(o.event) });

/** Stamps events from one source with a fresh id and that source's own sequence. */
export class Emitter {
  private seq = 0;

  constructor(readonly source: string) {}

  emit(type: string, version: number, at: Date, data: JsonObject, redriveOf: string | null = null): CloudEvent {
    return { id: randomUUID(), source: this.source, type, version, time: at, sequence: ++this.seq, data, redriveOf };
  }
}

/** Reading fields the way a service would: by name, failing with the reason it would log. */
export const Read = {
  str(data: Json, key: string): Result<string> {
    const v = Read.field(data, key);
    if (!v.ok) return v;
    return typeof v.value === 'string' ? ok(v.value) : err(`'${key}' is ${kind(v.value)}, expected a string`);
  },

  /** A decimal string, the way every amount in this flow was written before v2. */
  decimal(data: Json, key: string): Result<Big> {
    const v = Read.field(data, key);
    if (!v.ok) return v;
    if (typeof v.value !== 'string') return err(`'${key}' is ${kind(v.value)}, expected a decimal string`);
    const m = money(v.value);
    return m ? ok(m) : err(`'${key}' is '${v.value}', not a decimal`);
  },

  field(data: Json, key: string): Result<Json> {
    const v = isObject(data) ? data[key] : undefined;
    return v === undefined || v === null ? err(`no '${key}'`) : ok(v);
  },
};

export function kind(v: Json): string {
  if (Array.isArray(v)) return 'an array';
  if (v === null) return 'null';
  switch (typeof v) {
    case 'object': return 'an object';
    case 'string': return 'a string';
    case 'number': return 'a number';
    default: return 'a boolean';
  }
}

/** Amounts on the wire: two decimal places, half up, as a string. */
export const formatMoney = (b: Big): string => b.toFixed(2, 1);

// ---- The JVM's hashes, bit for bit, so the TypeScript simulator decides what the Scala one did. ----------------

const rotl = (x: number, r: number) => (x << r) | (x >>> (32 - r));

function mixLast(h: number, k: number): number {
  k = Math.imul(k, 0xcc9e2d51);
  k = rotl(k, 15);
  k = Math.imul(k, 0x1b873593);
  return h ^ k;
}

function mix(h: number, k: number): number {
  h = rotl(mixLast(h, k), 13);
  return (Math.imul(h, 5) + 0xe6546b64) | 0;
}

function avalanche(h: number): number {
  h ^= h >>> 16;
  h = Math.imul(h, 0x85ebca6b);
  h ^= h >>> 13;
  h = Math.imul(h, 0xc2b2ae35);
  return h ^ (h >>> 16);
}

/** Scala's `MurmurHash3.stringHash`. */
export function scalaStringHash(s: string): number {
  let h = 0xf7ca7fd2 | 0;
  let i = 0;
  for (; i + 1 < s.length; i += 2) h = mix(h, ((s.charCodeAt(i) << 16) + s.charCodeAt(i + 1)) | 0);
  if (i < s.length) h = mixLast(h, s.charCodeAt(i));
  return avalanche(h ^ s.length);
}

/** Java's `String.hashCode`. */
export function javaHash(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (Math.imul(h, 31) + s.charCodeAt(i)) | 0;
  return h;
}

/** A modulus that is never negative, like Java's `Math.floorMod`. */
export const floorMod = (a: number, n: number): number => ((a % n) + n) % n;
