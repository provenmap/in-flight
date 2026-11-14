// What the console knows, as data. The observer's messages update it; the map and the panels only read it.
import { MONEY, type DeadMsg, type EventMsg, type Flow, type Measure, type Mode, type Msg, type RecoveredMsg, type SnapshotMsg, type Stats } from './protocol';

export const FLIGHT_MS = 1900;
export const MAX_PARTICLES = 4000;
export const MAX_BLOCKS = 3000;
export const GREEN_MS = 1400;
/** Money per minute on each edge is measured over this window. */
export const RATE_WINDOW_MS = 30_000;
/** Money at risk is kept for this long, sampled with every stats message. */
export const HISTORY_MS = 60 * 60_000;
const RECENT = 200;

/**
 * What the flow counts and what it calls things, as its catalog says (the flow message's `measure`): money in dollars,
 * orders and customers unless it says otherwise. One flow is shown at a time, so this is set once per snapshot and
 * read wherever a figure or a noun is shown.
 */
export const shown: Measure = { ...MONEY };

function formatters(m: Measure): { full: (n: number) => string; short: (n: number) => string } {
  if (m.unit) {
    const unit = m.unit;
    const one = unit.length > 2 && unit.endsWith('s') && !unit.endsWith('ss') ? unit.slice(0, -1) : unit;
    // A count is whole, even while the headline's counter tweens towards it.
    const full = new Intl.NumberFormat('en-US', { maximumFractionDigits: m.count ? 0 : 2 });
    const short = new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 1 });
    const word = (n: number) => (Math.round(n * 100) === 100 ? one : unit);
    return { full: n => `${full.format(n)} ${word(n)}`, short: n => `${short.format(n)} ${word(n)}` };
  }
  try {
    const full = new Intl.NumberFormat('en-US', { style: 'currency', currency: m.currency || 'USD' });
    const short = new Intl.NumberFormat('en-US', { style: 'currency', currency: m.currency || 'USD', notation: 'compact', maximumFractionDigits: 1 });
    return { full: n => full.format(n), short: n => short.format(n) };
  } catch {
    // Not a currency code Intl knows: the code, then the amount.
    const n2 = new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    return { full: n => `${m.currency} ${n2.format(n)}`, short: n => `${m.currency} ${n2.format(n)}` };
  }
}
let fmt = formatters(shown);

/** A figure as a flow that is not live yet counts it, such as a draft's: what its validation says it counts. */
export function amountIn(m: Partial<Measure> | undefined, currency?: string | null): (n: number) => string {
  return formatters({ ...MONEY, ...(currency ? { currency } : {}), ...(m ?? {}) }).full;
}

/** Show the flow as its catalog says; nothing (an older server) is money in the flow's currency. */
export function setMeasure(m: Partial<Measure> | undefined, currency?: string | null): void {
  Object.assign(shown, MONEY, currency ? { currency } : {}, m ?? {});
  fmt = formatters(shown);
}

/** A figure as the flow counts it: $1,234.50, EUR 1,234.50, 1,234 parcels. The names are from when it was only money. */
export const money = { format: (n: number) => fmt.full(n) };
export const moneyShort = { format: (n: number) => fmt.short(n) };

/** English plurals, enough for nouns: order, orders; batch, batches; delivery, deliveries. */
export const plural = (noun: string) => /[^aeiou]y$/.test(noun) ? `${noun.slice(0, -1)}ies` : /(s|x|z|ch|sh)$/.test(noun) ? `${noun}es` : `${noun}s`;
export const count = (n: number, noun: string) => `${n.toLocaleString('en-US')} ${n === 1 ? noun : plural(noun)}`;

/** An edge on the map: a district to a district that reads its topic, or to the bin of a topic nobody reads. */
export const edgeKey = (from: string, to: string | null, topic: string) => `${from}>${to ?? `~${topic}`}`;

export interface Particle {
  edge: string;
  born: number;
  value: number;
  kind: 'flow' | 'redrive' | 'declined';
}

/** A dead letter on the pile: what it is and why it failed, and when its recovery plays. */
export interface Block {
  id: string;
  district: string;
  dlq: string;
  event: string;
  orderId: string;
  customer: string;
  value: number;
  priced: boolean;
  error: string;
  /** When it failed, as the dead letter says. */
  at: string;
  born: number;
  greenAt: number | null;
  recoveredAt?: number;
  intent?: string | null;
  prediction?: DeadMsg['prediction'];
  predicted?: string;
}

/** A recent event, for the live feed. */
export interface Recent {
  id: string;
  at: number;
  event: string;
  topic: string;
  district: string;
  to: string[];
  orderId: string;
  customer: string;
  value: number;
  redrive: boolean;
}

export interface Sample {
  t: number;
  stuck: number;
  lag: number;
}

interface Tick {
  t: number;
  value: number;
}

export class Model {
  flow: Flow | null = null;
  mode: Mode = 'demo';
  particles: Particle[] = [];
  pile: Block[] = [];
  byId = new Map<string, Block>();
  stats: Stats | null = null;
  statsAt = 0;
  waveAt = 0;
  /** Money that reached each sink, this session. */
  delivered = new Map<string, { value: number; orders: number }>();
  private sinkOf = new Map<string, string[]>();
  lastError: { event: string; error: string; dlq: string } | null = null;
  /** Kept across reconnects: they are what this page has seen. */
  recent: Recent[] = [];
  history: Sample[] = [];
  private readonly edges = new Map<string, Tick[]>();
  private readonly topics = new Map<string, Tick[]>();
  readonly openedAt = performance.now();
  /** The flow the last snapshot was of, kept through resets to tell a reconnect from a switch. */
  private flowName: string | null = null;

  private forgetFlow(): void {
    this.recent = [];
    this.history = [];
    this.delivered = new Map();
    this.edges.clear();
    this.topics.clear();
  }

  private resetScene(): void {
    this.flow = null;
    this.particles = [];
    this.pile = [];
    this.byId = new Map();
    this.stats = null;
    this.lastError = null;
    this.waveAt = 0;
    this.statsAt = 0;
  }

  /**
   * When the next recovery shows. A redrive lands hundreds of recoveries at once; the console plays them as a wave,
   * 15 ms apart, quicker once the wave is more than five seconds long.
   */
  nextInWave(now: number): number {
    this.waveAt = Math.max(this.waveAt, now);
    const at = this.waveAt;
    this.waveAt += this.waveAt - now > 5000 ? 3 : 15;
    return at;
  }

  /**
   * Blocks recovered whose recovery has not played yet. The counter adds their value back so it drains with the
   * wave, but only for recoveries the latest stats already counted: the others are still in its money at risk.
   */
  pendingRecovery(now: number): { value: number; orders: number } {
    let value = 0, orders = 0;
    for (const b of this.pile) {
      if (!b.greenAt || b.greenAt <= now) continue;
      orders++;
      if ((b.recoveredAt ?? Infinity) <= this.statsAt) value += b.value;
    }
    return { value, orders };
  }

  apply(msg: Msg, now: number): void {
    switch (msg.type) {
      case 'snapshot': return this.snapshot(msg, now);
      case 'event': return this.event(msg, now);
      case 'dead': return this.dead(msg, now);
      case 'recovered': return this.recovered(msg, now);
      case 'stats': return this.onStats(msg, now);
    }
  }

  private snapshot(msg: SnapshotMsg, now: number): void {
    // Another flow (the engine switched): nothing this page saw of the last one belongs to it.
    if (this.flowName !== null && this.flowName !== msg.flow.name) this.forgetFlow();
    this.flowName = msg.flow.name;
    this.resetScene();
    // The server keeps the chart across restarts; it knows more than this page does.
    if (msg.history?.length) {
      this.history = msg.history.map(([t, stuck, lag]) => ({ t, stuck, lag }));
    }
    this.flow = msg.flow;
    setMeasure(msg.flow.measure, msg.flow.currency);
    this.mode = msg.mode;
    const sinks = new Set(msg.flow.districts.filter(d => d.sink).map(d => d.id));
    this.sinkOf = new Map(msg.flow.topics.map(t => [t.topic, t.to.filter(d => sinks.has(d))]));
    for (const d of msg.stuck) this.dead(d, now - 5000);
    this.onStats(msg.stats, now);
  }

  private onStats(s: Stats, now: number): void {
    this.stats = s;
    this.statsAt = now;
    const t = Date.now();
    this.history.push({ t, stuck: s.stuck.value, lag: s.lag.value });
    while (this.history.length && this.history[0]!.t < t - HISTORY_MS) this.history.shift();
  }

  private event(e: EventMsg, now: number): void {
    const kind = e.redrive ? 'redrive' : e.to.length === 0 ? 'declined' : 'flow';
    // A redriven event leaves when its block goes green: its recovery follows it, and takes the same place in the wave.
    const born = e.redrive ? Math.max(this.waveAt, now) : now;
    for (const to of e.to.length ? e.to : [null]) {
      const edge = edgeKey(e.district, to, e.topic);
      this.particles.push({ edge, born, value: e.value, kind });
      this.tickInto(this.edges, edge, now, e.value);
    }
    this.tickInto(this.topics, e.topic, now, e.value);
    for (const sink of this.sinkOf.get(e.topic) ?? []) {
      const d = this.delivered.get(sink) ?? { value: 0, orders: 0 };
      this.delivered.set(sink, { value: d.value + e.value, orders: d.orders + 1 });
    }
    if (this.particles.length > MAX_PARTICLES) this.particles.splice(0, this.particles.length - MAX_PARTICLES);
    this.recent.unshift({ id: e.id, at: Date.now(), event: e.event, topic: e.topic, district: e.district, to: e.to, orderId: e.orderId, customer: e.customer, value: e.value, redrive: e.redrive });
    if (this.recent.length > RECENT) this.recent.length = RECENT;
  }

  private dead(d: DeadMsg, now: number): void {
    if (this.byId.has(d.id)) return;
    // Arrives just after the order that carried it lands, so the block drops in as the particle hits.
    const block: Block = {
      id: d.id, district: d.district, dlq: d.dlq, event: d.event, orderId: d.orderId, customer: d.customer,
      value: d.value, priced: d.priced, error: d.error, at: d.at, born: now, greenAt: null,
      intent: d.intent, prediction: d.prediction, predicted: d.predicted,
    };
    this.byId.set(d.id, block);
    this.pile.push(block);
    this.lastError = { event: d.event, error: d.error, dlq: d.dlq };
    if (this.pile.length > MAX_BLOCKS) this.byId.delete(this.pile.shift()!.id);
  }

  private recovered(r: RecoveredMsg, now: number): void {
    const block = this.byId.get(r.id);
    if (!block || block.greenAt) return;
    block.recoveredAt = now;
    block.greenAt = this.nextInWave(now);
  }

  private tickInto(m: Map<string, Tick[]>, key: string, t: number, value: number): void {
    let ticks = m.get(key);
    if (!ticks) m.set(key, (ticks = []));
    ticks.push({ t, value });
  }

  /** Money a minute and events a second, over the last half minute (or since the page opened). */
  rate(key: string, now: number, kind: 'edge' | 'topic' = 'edge'): { perMinute: number; perSecond: number } {
    const ticks = (kind === 'edge' ? this.edges : this.topics).get(key) ?? [];
    const window = Math.max(1000, Math.min(RATE_WINDOW_MS, now - this.openedAt));
    let value = 0, events = 0;
    for (let i = ticks.length - 1; i >= 0 && ticks[i]!.t >= now - window; i--) {
      value += ticks[i]!.value;
      events++;
    }
    return { perMinute: (value / window) * 60_000, perSecond: (events / window) * 1000 };
  }

  /** Drop finished particles, old rate ticks, and blocks that have gone green and left. */
  tick(now: number): void {
    this.particles = this.particles.filter(p => now - p.born < FLIGHT_MS);
    for (const m of [this.edges, this.topics]) {
      for (const ticks of m.values()) {
        let drop = 0;
        while (drop < ticks.length && ticks[drop]!.t < now - RATE_WINDOW_MS) drop++;
        if (drop) ticks.splice(0, drop);
      }
    }
    const gone = (b: Block) => b.greenAt !== null && now - b.greenAt > GREEN_MS;
    if (this.pile.some(gone)) {
      for (const b of this.pile) if (gone(b)) this.byId.delete(b.id);
      this.pile = this.pile.filter(b => !gone(b));
    }
    if (!this.pile.length) this.lastError = null;
  }

  /** How far a block is through its green exit, 0 before it starts. */
  static greenness(b: Block, now: number): number {
    return b.greenAt && now >= b.greenAt ? Math.min(1, (now - b.greenAt) / GREEN_MS) : 0;
  }
}
