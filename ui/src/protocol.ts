// The observer's WebSocket protocol. Every frame is a JSON array of these; the first frame holds one snapshot.

export interface Flow {
  name: string;
  currency: string;
  districts: { id: string; name: string; sink: boolean }[];
  topics: { topic: string; from: string; to: string[]; events: string[] }[];
  dlqs: { dlq: string; district: string; group: string; redrive: string | null }[];
  groups: { group: string; district: string }[];
  /** What each consumer reads by, in the catalog's terms: from the districts producing its matches to its readers. */
  intents?: FlowIntent[];
}

export interface FlowIntent {
  intent: string;
  predicate: string;
  keyedBy: string;
  /** The name a reader subscribes to, through the proxy. */
  topic: string;
  from: string[];
  sources: string[];
  events: string[];
  to: string[];
  readers: string[];
}

/** `demo` with a simulator and its controls; `watch` for a flow of one's own, with none. */
export type Mode = 'demo' | 'watch';

export interface SimState {
  orderVersion: number;
  patched: boolean;
  rate: number;
  placed: number;
  services: Record<string, { processed: number; deadLettered: number; reads?: string[] }>;
  shippingWaiting: number;
  /** Orders that went out as OrderPlaced v2. */
  placedV2?: number;
  /** Where the orders team writes: straight to Kafka, or through the proxy and its schema gate. */
  ordersVia?: 'kafka' | 'proxy';
  /** Whether there is a proxy to ship through: without one there is no gate. */
  proxy?: boolean;
  /** OrderPlaced v2 sends the schema gate refused. */
  refusedByGate?: number;
  redrive: { running: boolean; redriven: number; remaining: number; error: string | null };
}

export interface Stats {
  type: 'stats';
  /** `estimated`: the part of the value valued from recent averages rather than from events seen (lag on events not read yet). */
  atRisk: { value: number; orders: number; customers: number; unpriced: number; estimated?: number };
  stuck: { value: number; events: number; customers: number; unpriced: number; since: string | null; byDistrict: Record<string, number> };
  lag: { value: number; events: number; estimated?: number; byGroup: Record<string, { events: number; value: number; customers: number; estimated?: number }> };
  recovered: { value: number; events: number };
  lastEpisode: { value: number; events: number; tookMs: number } | null;
  unpriced: number;
  sim: SimState | null;
  /** Lag on each intent's topic, for the intents some service reads by through the router. */
  intents?: Record<string, { topic: string; events: number; value: number; groups: string[] }>;
}

export interface EventMsg {
  type: 'event';
  id: string;
  event: string;
  topic: string;
  district: string;
  to: string[];
  value: number;
  customer: string;
  orderId: string;
  redrive: boolean;
}

export interface DeadMsg {
  type: 'dead';
  id: string;
  dlq: string;
  district: string;
  event: string;
  value: number;
  /** False for a dead letter the catalog cannot price: its value is unknown, not zero. */
  priced: boolean;
  customer: string;
  orderId: string;
  error: string;
  at: string;
  /** The intent the reader that dead-lettered it reads by. */
  intent?: string | null;
  /** Whether the catalog saw it coming: its impact named this reader, it knew the version, or never saw it. */
  prediction?: 'predicted' | 'unpredicted' | 'unseen' | 'unknown';
  predicted?: string;
}

export interface RecoveredMsg {
  type: 'recovered';
  id: string;
  district: string;
  value: number;
  afterMs: number | null;
}

export interface SnapshotMsg {
  type: 'snapshot';
  mode: Mode;
  flow: Flow;
  stuck: DeadMsg[];
  stuckShown: number;
  stats: Stats;
  /** Money at risk over the last hour, kept across restarts: [epoch ms, stuck value, lag value]. */
  history?: [number, number, number][];
}

export type Msg = SnapshotMsg | EventMsg | DeadMsg | RecoveredMsg | Stats;
