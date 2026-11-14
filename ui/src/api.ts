// The setup and recovery API, as the console calls it: the server's answers, typed, and one way to call them.
// Every answer carries `text`, a line or a few for a person, and `ok`; failures come back with a 4xx and the same body.
import type { Measure } from './protocol';

export type Stage = 'connect' | 'discover' | 'watching';

export interface Profile {
  name: string;
  /** The broker adapter's kind; absent means kafka. */
  broker?: string;
  bootstrap: string;
  auth: string;
  secrets?: Record<string, string>;
  settings?: Record<string, string>;
  registry?: { url: string; username?: string; password?: string };
  allowWrites: boolean;
}

export interface Status {
  mode: 'app' | 'try' | 'watch';
  stage: Stage;
  home?: string;
  profile: Profile | null;
  profiles: string[];
  flow: string | null;
  bootstrap?: string;
  flows: string[];
  drafts: string[];
  observing: boolean;
  missingTopics: string[];
  problem?: string;
  canWrite: boolean;
  next: string;
}

export interface Presets {
  presets: { id: string; help: string }[];
  secretForms: string;
}

export interface Answer {
  ok: boolean;
  text: string;
  error?: string;
}

export interface Tested extends Answer {
  profile?: Profile;
  cluster?: { id: string; brokers: number; topics: number };
  registry?: { url: string; ok: boolean; problem?: string };
  problems?: string[];
}

export interface Discovered extends Answer {
  bootstrap: string;
  topics: { name: string; partitions: number; records: number; kind: 'flow' | 'dlq' | 'retry' | 'internal' }[];
  groups: { group: string; state: string; members: number; lag: number; reads: string[] }[];
  dlqs: { topic: string; reasons: string[]; sources: string[]; owner: string | null }[];
  subjects: string[];
  registryProblem?: string;
}

export interface Sampled extends Answer {
  topic: string;
  sampled: number;
  unreadable: number;
  formats: Record<string, number>;
  eventTypes: Record<string, number>;
  fields: { path: string; types: string[]; presence: number; example?: string; numericText?: boolean }[];
  problems: string[];
  headers: string[];
  revealed: boolean;
}

export interface PatchOp {
  op: 'add' | 'remove' | 'replace' | 'move' | 'copy' | 'test';
  path: string;
  value?: unknown;
  from?: string;
}

export interface Guess {
  id: string;
  about: string;
  question: string;
  confidence: number;
  chosen: string | null;
  options: { label: string; patch: PatchOp[] }[];
}

/** A catalog as JSON. Only what the console shows is typed; the rest passes through untouched. */
export interface CatalogJson {
  flow?: string;
  currency?: string;
  districts?: { id: string; name: string; sink?: boolean }[];
  events?: {
    type: string;
    version?: number;
    topic: string;
    district?: string;
    value?: string;
    minorUnits?: boolean;
    currencyField?: string;
    customer?: string;
    order?: string;
  }[];
  consumers?: { group: string; district?: string; reads?: string[]; dlq?: string; redrive?: string }[];
  [key: string]: unknown;
}

export interface Validation extends Answer {
  errors: string[];
  missingTopics: string[];
  warnings: string[];
  currency?: string;
  /** What the catalog counts, when it reads: its currency or unit, and its nouns. */
  measure?: Partial<Measure>;
  events: { event: string; topic: string; sampled: number; priced: number; unknownValue: number; total: number; min?: number; max?: number; problems: Record<string, number> }[];
}

export interface Draft extends Answer {
  flow: string;
  profile: string;
  created: string;
  catalog: CatalogJson;
  guesses: Guess[];
  answered?: { id: string; answer: string }[];
  skipped: string[];
  validation?: Validation;
}

export interface Edited extends Answer {
  catalog: CatalogJson;
  errors?: string[];
  questionsLeft?: number;
}

export interface Activated extends Answer {
  flow: string;
  file: string;
}

export interface Causes {
  currency?: string;
  causes: { cause: string; example: string; events: number; unpriced: number; value: number; first: string; last: string; dlqs: Record<string, number> }[];
}

export interface Redriven extends Answer {
  dryRun?: boolean;
  scanned?: number;
  matched?: number;
  written?: number;
  alreadyRedriven?: number;
  noTarget?: number;
  value?: number;
  unpriced?: number;
  stoppedEarly?: boolean;
  byError?: Record<string, number>;
  byTarget?: Record<string, number>;
  ids?: string[];
  note?: string;
  running?: boolean;
  started?: string;
}

/** A failed call, with the answer's body when there was one: a failed test still says what it found. */
export class ApiError extends Error {
  constructor(message: string, readonly status: number, readonly body: unknown) {
    super(message);
  }
}

export async function api<T>(url: string, method = 'GET', body?: unknown): Promise<T> {
  let res: Response;
  try {
    res = await fetch(url, {
      method,
      headers: body === undefined ? undefined : { 'content-type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  } catch (e) {
    throw new ApiError(`The server does not answer: ${(e as Error).message}`, 0, null);
  }
  const data = (await res.json().catch(() => ({}))) as T & { error?: string; text?: string };
  if (!res.ok) throw new ApiError(data.text ?? data.error ?? `HTTP ${res.status}`, res.status, data);
  return data;
}

/** The body of a failed call when it is an answer, so a failure can be shown like a result. */
export function answerOf<T extends Answer>(e: unknown): T | null {
  if (e instanceof ApiError && e.body && typeof e.body === 'object' && 'text' in e.body) return e.body as T;
  return null;
}

export const errorText = (e: unknown) => (e instanceof Error ? e.message : String(e));

/** The forms a secret reference takes; anything else would be the secret itself, and would be saved. */
export function isReference(ref: string): boolean {
  return /^env:[A-Za-z_][A-Za-z0-9_]*$/.test(ref) || /^file:.+$/.test(ref) || /^keychain:[^/]+(\/.+)?$/.test(ref);
}

export const enc = encodeURIComponent;
