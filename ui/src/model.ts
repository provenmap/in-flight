// The scene as data. The observer's messages update it; the 3D and 2D views only draw it.
import type { DeadMsg, EventMsg, Flow, Msg, RecoveredMsg, SnapshotMsg, Stats } from './protocol';

export interface Place {
  x: number;
  z: number;
}

/** Where each district sits on the ground, x to the right, z towards the viewer. A diamond, customers beyond. */
export const PLACES: Record<string, Place> = {
  orders: { x: -15, z: 0 },
  payments: { x: 0, z: -9 },
  inventory: { x: 0, z: 9 },
  shipping: { x: 15, z: 0 },
  customers: { x: 27, z: 0 },
};

/** The dead-letter pile sits on open ground to the left of its district, growing back and then up. */
export const PILE = { dx: -9.5, dz: 0.5, cols: 10, rows: 7 };

/** Where a declined payment falls: a short drop off the side of payments, going nowhere. */
export const DECLINED = { dx: 5, dz: -4 };

export const FLIGHT_MS = 1900;
export const MAX_PARTICLES = 4000;
export const MAX_BLOCKS = 3000;
const GREEN_MS = 1400;

export type Rgb = [number, number, number];

/** 0 for $10 or less, 1 for $10,000 or more, on a log scale. */
export function worth(value: number): number {
  return Math.min(1, Math.max(0, (Math.log10(Math.max(value, 1)) - 1) / 3));
}

/** Particle radius in scene units. */
export function size(value: number): number {
  return 0.16 + 0.42 * worth(value);
}

/** A colour from cool blue (cheap) through gold to white-hot (dear), as [r, g, b] in 0..1. */
export function heat(value: number): Rgb {
  const stops: Rgb[] = [[0.24, 0.47, 0.78], [1.0, 0.78, 0.33], [1.0, 0.97, 0.88]];
  const t = worth(value) * 2;
  const [a, b] = t < 1 ? [stops[0]!, stops[1]!] : [stops[1]!, stops[2]!];
  const f = t < 1 ? t : t - 1;
  return a.map((x, i) => x + (b[i]! - x) * f) as Rgb;
}

export const money = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' });
export const moneyShort = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', notation: 'compact', maximumFractionDigits: 1 });

export interface Arc {
  topic: string;
  from: string;
  to: string;
}

export interface Particle {
  from: string;
  to: string | null;
  born: number;
  value: number;
  kind: 'flow' | 'redrive' | 'declined';
}

export interface Block {
  id: string;
  district: string;
  value: number;
  born: number;
  error: string;
  greenAt: number | null;
  recoveredAt?: number;
}

export class Model {
  flow: Flow | null = null;
  districts: Flow['districts'] = [];
  arcs: Arc[] = [];
  particles: Particle[] = [];
  pile: Block[] = [];
  byId = new Map<string, Block>();
  stats: Stats | null = null;
  delivered = { value: 0, orders: 0 };
  lastError: { event: string; error: string; dlq: string } | null = null;
  waveAt = 0;
  statsAt = 0;

  reset(): void {
    this.flow = null;
    this.districts = [];
    this.arcs = [];
    this.particles = [];
    this.pile = [];
    this.byId = new Map();
    this.stats = null;
    this.delivered = { value: 0, orders: 0 };
    this.lastError = null;
    this.waveAt = 0;
    this.statsAt = 0;
  }

  /**
   * When the next recovery shows. A redrive lands hundreds of recoveries at once; the view plays them as a wave,
   * 15 ms apart, quicker once the wave is more than five seconds long.
   */
  nextInWave(now: number): number {
    this.waveAt = Math.max(this.waveAt, now);
    const at = this.waveAt;
    this.waveAt += this.waveAt - now > 5000 ? 3 : 15;
    return at;
  }

  /**
   * Blocks recovered whose recovery has not played yet. The HUD adds their value back so it drains with the wave,
   * but only for recoveries the latest stats already counted: the others are still in its money at risk.
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

  snapshot(msg: SnapshotMsg, now: number): void {
    this.reset();
    this.flow = msg.flow;
    this.districts = msg.flow.districts;
    this.arcs = msg.flow.topics.flatMap(t => t.to.map(to => ({ topic: t.topic, from: t.from, to })));
    for (const d of msg.stuck) this.dead(d, now - 5000);
    this.stats = msg.stats;
    this.statsAt = now;
  }

  apply(msg: Msg, now: number): void {
    switch (msg.type) {
      case 'event': return this.event(msg, now);
      case 'dead': return this.dead(msg, now);
      case 'recovered': return this.recovered(msg, now);
      case 'stats': this.stats = msg; this.statsAt = now; return;
      case 'snapshot': return this.snapshot(msg, now);
    }
  }

  event(e: EventMsg, now: number): void {
    const kind = e.redrive ? 'redrive' : e.to.length === 0 ? 'declined' : 'flow';
    const targets: (string | null)[] = e.to.length ? e.to : [null];
    // A redriven event leaves when its block goes green: its recovery follows it, and takes the same place in the wave.
    const born = e.redrive ? Math.max(this.waveAt, now) : now;
    for (const to of targets) this.particles.push({ from: e.district, to, born, value: e.value, kind });
    if (e.topic === 'shipping.dispatched') {
      this.delivered.value += e.value;
      this.delivered.orders += 1;
    }
    if (this.particles.length > MAX_PARTICLES) this.particles.splice(0, this.particles.length - MAX_PARTICLES);
  }

  dead(d: DeadMsg, now: number): void {
    if (this.byId.has(d.id)) return;
    // Arrives just after the order that carried it lands, so the block drops in as the particle hits.
    const block: Block = { id: d.id, district: d.district, value: d.value, born: now, error: d.error, greenAt: null };
    this.byId.set(d.id, block);
    this.pile.push(block);
    this.lastError = { event: d.event, error: d.error, dlq: d.dlq };
    if (this.pile.length > MAX_BLOCKS) this.byId.delete(this.pile.shift()!.id);
  }

  recovered(r: RecoveredMsg, now: number): void {
    const block = this.byId.get(r.id);
    if (!block || block.greenAt) return;
    block.recoveredAt = now;
    block.greenAt = this.nextInWave(now);
  }

  /** Drop finished particles and blocks that have gone green and left. */
  tick(now: number): void {
    this.particles = this.particles.filter(p => now - p.born < FLIGHT_MS);
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

export interface Point {
  x: number;
  y: number;
  z: number;
  t?: number;
}

/** A point on the arc between two places: a parabola lifted by the distance it spans. */
export function arcPoint(from: Place, to: Place, t: number, out: Point = { x: 0, y: 0, z: 0 }): Point {
  const dx = to.x - from.x, dz = to.z - from.z;
  const lift = 2 + Math.hypot(dx, dz) * 0.18;
  out.x = from.x + dx * t;
  out.z = from.z + dz * t;
  out.y = 0.8 + 4 * lift * t * (1 - t);
  return out;
}

/** Where a particle is: on its arc, or for a declined payment, falling off to the side and fading. */
export function particlePoint(p: Particle, now: number, out: Point = { x: 0, y: 0, z: 0 }): (Point & { t: number }) | null {
  if (now < p.born) return null;
  const t = Math.min(1, (now - p.born) / FLIGHT_MS);
  const from = PLACES[p.from];
  if (!from) return null;
  if (p.kind === 'declined') {
    out.x = from.x + DECLINED.dx * t;
    out.z = from.z + DECLINED.dz * t;
    out.y = 0.8 + 2.5 * t - 4.5 * t * t;
    out.t = t;
    return out as Point & { t: number };
  }
  const to = p.to ? PLACES[p.to] : undefined;
  if (!to) return null;
  arcPoint(from, to, easeInOut(t), out);
  out.t = t;
  return out as Point & { t: number };
}

function easeInOut(t: number): number {
  return t < 0.5 ? 2 * t * t : 1 - Math.pow(-2 * t + 2, 2) / 2;
}

/** A block's resting place in its district's pile: rows away from the district, then layers upward. */
export function pileSlot(district: string, index: number, out: Point = { x: 0, y: 0, z: 0 }): Point {
  const home = PLACES[district] ?? PLACES.payments!;
  const perLayer = PILE.cols * PILE.rows;
  const layer = Math.floor(index / perLayer);
  const inLayer = index % perLayer;
  const col = inLayer % PILE.cols;
  const row = Math.floor(inLayer / PILE.cols);
  out.x = home.x + PILE.dx + (col - (PILE.cols - 1) / 2) * 0.52;
  out.z = home.z + PILE.dz - row * 0.52;
  out.y = 0.22 + layer * 0.46;
  return out;
}
