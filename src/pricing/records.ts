// A Kafka record as the observer reads it, whatever wrote it: a CloudEvent in JSON (structured mode) or in headers
// (binary mode), or plain JSON. JSON written by a Schema Registry serializer carries a five-byte prefix, dropped here.
import { err, ok, type Result } from '../result.js';
import { isObject, type JsonObject } from './catalog.js';

/** What a record says about itself. What it leaves out, the catalog works out from the topic. */
export interface Decoded {
  id: string | null;
  type: string | null;
  version: number | null;
  data: JsonObject;
  redriveOf: string | null;
}

/** A whole number, written as one or as a string of digits. */
function whole(v: unknown): number | null {
  const n = typeof v === 'number' ? v : typeof v === 'string' && /^-?\d+$/.test(v.trim()) ? Number(v) : NaN;
  return Number.isInteger(n) ? n : null;
}

/** Schema Registry framing: a zero magic byte and a four-byte schema id. JSON never starts with a zero byte. */
const unframed = (value: Buffer): Buffer => (value.length > 5 && value[0] === 0 ? value.subarray(5) : value);

function json(value: Buffer): Result<unknown> {
  try {
    return ok(JSON.parse(unframed(value).toString('utf8')));
  } catch (e) {
    return err(`not JSON: ${(e as Error).message}`);
  }
}

export function decode(value: Buffer, headers: Map<string, string>): Result<Decoded> {
  const parsed = json(value);
  if (!parsed.ok) return parsed;
  const body = parsed.value;

  // Binary mode: the attributes are `ce_` headers and the value is the data.
  const ceType = headers.get('ce_type');
  if (ceType !== undefined) {
    if (!isObject(body)) return err(`${ceType} has no data object`);
    return ok({
      id: headers.get('ce_id') ?? null,
      type: ceType,
      version: whole(headers.get('ce_schemaversion')),
      data: body,
      redriveOf: headers.get('ce_redriveof') ?? null,
    });
  }
  if (!isObject(body)) return err('the record is not a JSON object');

  // Structured mode: the attributes and the data in one JSON object.
  if (typeof body.specversion === 'string') {
    const str = (key: string) => (typeof body[key] === 'string' ? body[key] : null);
    const type = str('type');
    if (type === null) return err("CloudEvent has no 'type'");
    if (!isObject(body.data)) return err(`${type} (${str('id') ?? 'no id'}) has no data object`);
    return ok({ id: str('id'), type, version: whole(body.schemaversion), data: body.data, redriveOf: str('redriveof') });
  }

  return ok({ id: null, type: null, version: null, data: body, redriveOf: null });
}
