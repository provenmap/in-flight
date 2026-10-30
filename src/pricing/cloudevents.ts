import { randomUUID } from 'node:crypto';
import { err, ok, type Result } from '../result.js';
import { isObject, type JsonObject, type EventRef } from './catalog.js';

/** A structured-mode CloudEvent, with the extensions this flow reads: `schemaversion`, `sequence` and `redriveof`. */
export interface CloudEvent {
  id: string;
  source: string;
  type: string;
  version: number;
  time: Date;
  sequence: number;
  data: JsonObject;
  redriveOf: string | null;
}

export const refOf = (e: CloudEvent): EventRef => ({ name: e.type, version: e.version });

/** The JSON form, as it goes on the wire. */
export function toJson(e: CloudEvent): string {
  return JSON.stringify({
    specversion: '1.0',
    id: e.id,
    source: e.source,
    type: e.type,
    time: e.time.toISOString(),
    datacontenttype: 'application/json',
    schemaversion: e.version,
    sequence: String(e.sequence),
    data: e.data,
    ...(e.redriveOf !== null ? { redriveof: e.redriveOf } : {}),
  });
}

export const CloudEvents = {
  /** A new event with a fresh id. */
  create(source: string, type: string, version: number, time: Date, sequence: number, data: JsonObject, redriveOf: string | null = null): CloudEvent {
    return { id: randomUUID(), source, type, version, time, sequence, data, redriveOf };
  },

  parse(input: string | Buffer): Result<CloudEvent> {
    let json: unknown;
    try {
      json = JSON.parse(typeof input === 'string' ? input : input.toString('utf8'));
    } catch (e) {
      return err(`not JSON: ${(e as Error).message}`);
    }
    if (!isObject(json)) return err('CloudEvent is not an object');
    const str = (key: string) => (typeof json[key] === 'string' ? json[key] : undefined);
    for (const key of ['id', 'source', 'type']) if (str(key) === undefined) return err(`CloudEvent has no '${key}'`);
    const [id, source, type] = [str('id')!, str('source')!, str('type')!];

    const sv = json.schemaversion;
    const version = typeof sv === 'number' ? sv : typeof sv === 'string' && sv.trim() !== '' ? Number(sv) : NaN;
    if (!Number.isInteger(version)) return err(`${type} (${id}) has no whole schemaversion`);
    const data = json.data;
    if (!isObject(data)) return err(`${type} (${id}) has no data object`);

    const t = str('time');
    const time = t !== undefined && !Number.isNaN(Date.parse(t)) ? new Date(t) : new Date(0);
    const seq = str('sequence');
    const sequence = seq !== undefined && /^-?\d+$/.test(seq) ? Number(seq) : 0;
    return ok({ id, source, type, version, time, sequence, data, redriveOf: str('redriveof') ?? null });
  },
};

/** Headers a service sets when it dead-letters a record. */
export const DeadLetter = {
  Error: 'rif-error',
  SourceTopic: 'rif-source-topic',
  Group: 'rif-consumer-group',
  FailedAt: 'rif-failed-at',
} as const;
