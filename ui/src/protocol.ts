// The observer's WebSocket protocol. Every frame is a JSON array of these; the first frame holds one snapshot.

export interface Flow {
  name: string;
  currency: string;
  districts: { id: string; name: string; sink: boolean }[];
  topics: { topic: string; from: string; to: string[]; events: string[] }[];
  dlqs: { dlq: string; district: string; group: string; redrive: string | null }[];
  groups: { group: string; district: string }[];
}

/** `demo` with a simulator and its controls; `watch` for a flow of one's own, with none. */
export type Mode = 'demo' | 'watch';

export interface SimState {
  orderVersion: number;
  patched: boolean;
  rate: number;
  placed: number;
  services: Record<string, { processed: number; deadLettered: number }>;
  shippingWaiting: number;
  redrive: { running: boolean; redriven: number; remaining: number; error: string | null };
}

export interface Stats {
  type: 'stats';
  atRisk: { value: number; orders: number; customers: number; unpriced: number };
  stuck: { value: number; events: number; customers: number; unpriced: number; since: string | null; byDistrict: Record<string, number> };
  lag: { value: number; events: number; byGroup: Record<string, { events: number; value: number; customers: number }> };
  recovered: { value: number; events: number };
  lastEpisode: { value: number; events: number; tookMs: number } | null;
  unpriced: number;
  sim: SimState | null;
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
}

export type Msg = SnapshotMsg | EventMsg | DeadMsg | RecoveredMsg | Stats;
