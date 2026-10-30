import Big from 'big.js';
import { showRef, type Catalog } from '../pricing/catalog.js';
import type { Priced } from '../pricing/pricing.js';
import { tpKey } from '../kafka.js';

/** A record's place in a topic. */
export interface Pos {
  topic: string;
  partition: number;
  offset: number;
}

/** A dead-lettered event: where it sits, what it is worth, and why it could not be read. */
export interface Stuck {
  id: string;
  dlq: string;
  district: string;
  pos: Pos;
  priced: Priced;
  error: string;
  at: Date;
}

/** One message for the view. `afterMs` is null for a dead letter found already redriven when the observer started. */
export type Msg =
  | { kind: 'event'; priced: Priced; to: string[] }
  | { kind: 'dead'; stuck: Stuck }
  | { kind: 'recovered'; stuck: Stuck; afterMs: number | null };

const num = (b: Big): number => Number(b.toString());

export function stuckJson(s: Stuck) {
  return {
    type: 'dead', id: s.id, dlq: s.dlq, district: s.district, event: showRef(s.priced.ref),
    value: num(s.priced.value), customer: s.priced.customer, orderId: s.priced.orderId,
    error: s.error, at: s.at.toISOString(),
  };
}

export function msgJson(m: Msg) {
  switch (m.kind) {
    case 'event': {
      const p = m.priced;
      return {
        type: 'event', id: p.eventId, event: showRef(p.ref), topic: p.topic, district: p.district,
        to: m.to, value: num(p.value), customer: p.customer, orderId: p.orderId, redrive: p.redriveOf !== null,
      };
    }
    case 'dead':
      return stuckJson(m.stuck);
    case 'recovered':
      return { type: 'recovered', id: m.stuck.id, district: m.stuck.district, value: num(m.stuck.priced.value), afterMs: m.afterMs };
  }
}

/** Consumer lag of one group: how many events it has not committed, and what the ones the observer saw are worth. */
export interface GroupLag {
  events: number;
  value: Big;
  customers: number;
}

/** A stuck spell: from the first event stuck while none were, to the last recovery that emptied the pile. */
export interface Episode {
  value: Big;
  events: number;
  tookMs: number;
}

export interface Stats {
  stuckValue: Big;
  stuckEvents: number;
  stuckCustomers: number;
  stuckSince: Date | null;
  stuckByDistrict: Map<string, Big>;
  lag: Map<string, GroupLag>;
  lagValue: Big;
  lagEvents: number;
  recoveredValue: Big;
  recoveredEvents: number;
  atRiskValue: Big;
  atRiskOrders: number;
  atRiskCustomers: number;
  lastEpisode: Episode | null;
  unpriced: number;
}

const sorted = <V, W>(m: Map<string, V>, f: (v: V) => W): Record<string, W> =>
  Object.fromEntries([...m].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)).map(([k, v]) => [k, f(v)]));

export function statsJson(s: Stats) {
  return {
    type: 'stats',
    atRisk: { value: num(s.atRiskValue), orders: s.atRiskOrders, customers: s.atRiskCustomers },
    stuck: {
      value: num(s.stuckValue), events: s.stuckEvents, customers: s.stuckCustomers,
      since: s.stuckSince?.toISOString() ?? null,
      byDistrict: sorted(s.stuckByDistrict, num),
    },
    lag: {
      value: num(s.lagValue), events: s.lagEvents,
      byGroup: sorted(s.lag, l => ({ events: l.events, value: num(l.value), customers: l.customers })),
    },
    recovered: { value: num(s.recoveredValue), events: s.recoveredEvents },
    lastEpisode: s.lastEpisode && { value: num(s.lastEpisode.value), events: s.lastEpisode.events, tookMs: s.lastEpisode.tookMs },
    unpriced: s.unpriced,
  };
}

/** A queue that drops from the front in constant time. */
class Deque<T> {
  private items: T[] = [];
  private head = 0;

  get size(): number {
    return this.items.length - this.head;
  }

  push(t: T): void {
    this.items.push(t);
  }

  peek(): T | undefined {
    return this.items[this.head];
  }

  shift(): void {
    this.head++;
    if (this.head > 1024 && this.head * 2 > this.items.length) {
      this.items = this.items.slice(this.head);
      this.head = 0;
    }
  }

  *[Symbol.iterator](): Iterator<T> {
    for (let i = this.head; i < this.items.length; i++) yield this.items[i]!;
  }
}

const topicOf = (key: string): string => key.slice(0, key.lastIndexOf(':'));
const sum = (bs: Big[]): Big => bs.reduce((a, b) => a.plus(b), new Big(0));

/**
 * Money at risk, from what the observer has read. One caller feeds it and asks it for stats.
 *
 *  - Stuck: every dead letter not yet recovered.
 *  - Lag: for each service's group, the events it has not committed that the observer saw arrive.
 *  - Recovered: a flow event whose `redriveof` names a stuck one, or a dead letter below its redrive group's
 *    committed offset when the observer started.
 *
 * The headline counts each order once, at the most it is worth anywhere it is held up, and each customer once.
 */
export class Ledger {
  private readonly stuck = new Map<string, Stuck>();
  private readonly recoveredIds = new Set<string>();
  private readonly redrivenBelow = new Map<string, number>();
  private readonly recent = new Map<string, Deque<[number, Priced]>>();
  private readonly committed = new Map<string, Map<string, number>>();
  private readonly ends = new Map<string, number>();
  private recoveredValue = new Big(0);
  private recoveredCount = 0;
  private unpriced = 0;
  private episodeStart: Date | null = null;
  private episodeValue = new Big(0);
  private episodeCount = 0;
  private lastEpisode: Episode | null = null;

  /** Topics a service group reads: only those need their recent events remembered for lag. */
  private readonly tracked: Set<string>;

  constructor(private readonly catalog: Catalog, private readonly window = 50_000) {
    this.tracked = new Set(catalog.services.flatMap(s => s.reads));
  }

  /** On start: the redrive group of `dlq` has dealt with everything on `partition` below `offset`. */
  redriven(dlq: string, partition: number, offset: number): void {
    this.redrivenBelow.set(tpKey(dlq, partition), offset);
  }

  unreadable(): void {
    this.unpriced++;
  }

  onDead(dlq: string, pos: Pos, priced: Priced, error: string, failedAt: Date): Msg {
    const district = this.catalog.ownerOfDlq(dlq)?.district ?? priced.district;
    const s: Stuck = { id: priced.eventId, dlq, district, pos, priced, error, at: failedAt };
    if (pos.offset < (this.redrivenBelow.get(tpKey(dlq, pos.partition)) ?? 0) || this.recoveredIds.has(s.id)) {
      return this.recover(s, null);
    }
    if (!this.stuck.size) {
      this.episodeStart = failedAt;
      this.episodeValue = new Big(0);
      this.episodeCount = 0;
    }
    this.stuck.set(s.id, s);
    return { kind: 'dead', stuck: s };
  }

  onFlow(pos: Pos, priced: Priced, at: Date): Msg[] {
    if (this.tracked.has(pos.topic)) {
      const k = tpKey(pos.topic, pos.partition);
      let q = this.recent.get(k);
      if (!q) this.recent.set(k, (q = new Deque()));
      q.push([pos.offset, priced]);
      while (q.size > this.window) q.shift();
    }
    const out: Msg[] = [{ kind: 'event', priced, to: this.catalog.readersOf(pos.topic).map(k => k.district) }];
    const orig = priced.redriveOf;
    if (orig !== null) {
      const s = this.stuck.get(orig);
      if (s) {
        this.stuck.delete(orig);
        out.push(this.recover(s, at.getTime() - s.at.getTime()));
        this.episodeValue = this.episodeValue.plus(s.priced.value);
        this.episodeCount++;
        if (!this.stuck.size) {
          if (this.episodeStart) {
            this.lastEpisode = { value: this.episodeValue, events: this.episodeCount, tookMs: at.getTime() - this.episodeStart.getTime() };
          }
          this.episodeStart = null;
        }
      } else {
        // The redriven event overtook its dead letter: remember it, so the dead letter arrives recovered.
        this.recoveredIds.add(orig);
      }
    }
    return out;
  }

  private recover(s: Stuck, afterMs: number | null): Msg {
    this.recoveredValue = this.recoveredValue.plus(s.priced.value);
    this.recoveredCount++;
    this.recoveredIds.delete(s.id);
    return { kind: 'recovered', stuck: s, afterMs };
  }

  /** The latest committed offsets of a service group, and the end of each partition, keyed by `topic:partition`. */
  onOffsets(group: string, offsets: Map<string, number>, logEnds: Map<string, number>): void {
    this.committed.set(group, offsets);
    for (const [k, v] of logEnds) this.ends.set(k, v);
    // Forget events every group reading their topic has committed.
    for (const [k, q] of this.recent) {
      const floors = this.catalog.services.filter(s => s.reads.includes(topicOf(k))).map(s => this.committed.get(s.group)?.get(k));
      if (!floors.length || floors.some(f => f === undefined)) continue;
      const floor = Math.min(...(floors as number[]));
      while (q.size && q.peek()![0] < floor) q.shift();
    }
  }

  /**
   * Events a group has not committed, with the ones the observer saw, between its committed offset and the end
   * polled with it. Events newer than that end are in flight, not yet lag, so the count and the value agree.
   */
  private lagOf(group: string): [number, Priced[]] {
    const reads = new Set(this.catalog.consumer(group)?.reads ?? []);
    const offs = this.committed.get(group) ?? new Map<string, number>();
    let count = 0;
    const held: Priced[] = [];
    for (const [k, end] of this.ends) {
      if (!reads.has(topicOf(k))) continue;
      count += Math.max(0, end - (offs.get(k) ?? end));
      const from = offs.get(k) ?? Number.MAX_SAFE_INTEGER;
      for (const [o, p] of this.recent.get(k) ?? []) if (o >= from && o < end) held.push(p);
    }
    return [count, held];
  }

  stuckNow(): Stuck[] {
    return [...this.stuck.values()];
  }

  stats(): Stats {
    const held = [...this.stuck.values()];
    const groups = this.catalog.services.map(s => [s.group, this.lagOf(s.group)] as const);
    const byOrder = new Map<string, Priced>();
    for (const p of [...held.map(s => s.priced), ...groups.flatMap(([, [, ps]]) => ps)]) {
      const had = byOrder.get(p.orderId);
      if (!had || p.value.gt(had.value)) byOrder.set(p.orderId, p);
    }
    const byDistrict = new Map<string, Big>();
    for (const s of held) byDistrict.set(s.district, (byDistrict.get(s.district) ?? new Big(0)).plus(s.priced.value));
    const lag = new Map(groups.map(([g, [events, ps]]) => [g, { events, value: sum(ps.map(p => p.value)), customers: new Set(ps.map(p => p.customer)).size }]));
    return {
      stuckValue: sum(held.map(s => s.priced.value)),
      stuckEvents: held.length,
      stuckCustomers: new Set(held.map(s => s.priced.customer)).size,
      stuckSince: held.length ? new Date(held.reduce((min, s) => Math.min(min, s.at.getTime()), Infinity)) : null,
      stuckByDistrict: byDistrict,
      lag,
      lagValue: sum([...lag.values()].map(l => l.value)),
      lagEvents: [...lag.values()].reduce((n, l) => n + l.events, 0),
      recoveredValue: this.recoveredValue,
      recoveredEvents: this.recoveredCount,
      atRiskValue: sum([...byOrder.values()].map(p => p.value)),
      atRiskOrders: byOrder.size,
      atRiskCustomers: new Set([...byOrder.values()].map(p => p.customer)).size,
      lastEpisode: this.lastEpisode,
      unpriced: this.unpriced,
    };
  }
}
