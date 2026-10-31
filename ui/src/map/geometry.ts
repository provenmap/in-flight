// Where things sit on the flow map, worked out from the flow: districts in columns left to right in the order money
// moves (each one column past the furthest district it reads from, sinks last), each column's districts stacked
// about the middle, a dead-letter tank above each district with a queue, and a bin off to the right for each topic
// nobody reads. Order-to-cash comes out as Orders, then Payments over Inventory, then Shipping, then Customers.
import { edgeKey } from '../model';
import type { Flow } from '../protocol';

export interface Point {
  x: number;
  y: number;
}

/** A box by its centre. */
export interface Box extends Point {
  w: number;
  h: number;
}

export interface Curve {
  p0: Point;
  c1: Point;
  c2: Point;
  p3: Point;
}

export interface Layout {
  w: number;
  h: number;
  nodes: Record<string, Box>;
  /** Each district's dead-letter tank, above it. */
  tanks: Record<string, Box>;
  /** Where each topic nobody reads ends, with its label. */
  bins: Record<string, Point & { label: string }>;
}

const MARGIN = 12;
const NODE = { w: 184, h: 92 };
const SINK_W = 152;
const COLUMN = 318;
const ROW = 176;
/** Room above the districts for the tanks. */
const TANKS = 170;

/** `payments.declined` is labelled Declined. */
const binLabel = (topic: string) => {
  const last = topic.split('.').at(-1) ?? topic;
  return last.charAt(0).toUpperCase() + last.slice(1);
};

export function layoutOf(flow: Flow): Layout {
  const ids = flow.districts.map(d => d.id);
  const sink = new Set(flow.districts.filter(d => d.sink).map(d => d.id));
  const links = flow.topics.flatMap(t => t.to.filter(to => to !== t.from).map(to => [t.from, to] as const));

  // Longest path from a source, capped so a cycle cannot run away.
  const col = new Map(ids.map(id => [id, 0]));
  for (let round = 0; round < ids.length; round++) {
    for (const [a, b] of links) {
      const next = Math.min((col.get(a) ?? 0) + 1, ids.length - 1);
      if (!sink.has(b) && next > (col.get(b) ?? 0)) col.set(b, next);
    }
  }
  const last = Math.max(0, ...ids.filter(id => !sink.has(id)).map(id => col.get(id)!));
  for (const id of sink) col.set(id, last + 1);

  const columns: string[][] = [];
  for (const id of ids) (columns[col.get(id)!] ??= []).push(id);
  const used = columns.filter(Boolean);
  const tall = Math.max(1, ...used.map(c => c.length));
  const top = flow.dlqs.length ? TANKS : 40;
  const band = tall * ROW - (ROW - NODE.h);
  const nodes: Record<string, Box> = {};
  used.forEach((ids, i) => {
    ids.forEach((id, j) => {
      nodes[id] = {
        x: MARGIN + NODE.w / 2 + i * COLUMN,
        y: top + band / 2 + (j - (ids.length - 1) / 2) * ROW,
        w: sink.has(id) ? SINK_W : NODE.w,
        h: NODE.h,
      };
    });
  });
  const lastColumn = used.at(-1) ?? [];
  const w = Math.max(...lastColumn.map(id => nodes[id]!.x + nodes[id]!.w / 2), NODE.w) + MARGIN;
  const h = top + band + 32;

  const tanks: Record<string, Box> = {};
  for (const q of flow.dlqs) {
    const n = nodes[q.district];
    if (n) tanks[q.district] = { x: n.x, y: 70, w: n.w + 24, h: 112 };
  }

  const bins: Layout['bins'] = {};
  const perDistrict = new Map<string, number>();
  for (const t of flow.topics.filter(t => !t.to.length)) {
    const n = nodes[t.from];
    if (!n) continue;
    const k = perDistrict.get(t.from) ?? 0;
    perDistrict.set(t.from, k + 1);
    bins[t.topic] = { x: Math.min(n.x + COLUMN * 0.75, w - 80), y: n.y - 88 + k * 30, label: binLabel(t.topic) };
  }
  return { w, h, nodes, tanks, bins };
}

function curve(p0: Point, p3: Point): Curve {
  const dx = (p3.x - p0.x) * 0.5;
  return { p0, c1: { x: p0.x + dx, y: p0.y }, c2: { x: p3.x - dx, y: p3.y }, p3 };
}

/** The curve of an edge: right side of one district to the left side of the next, or up to a topic's bin. */
export function edgeCurve(layout: Layout, e: { from: string; to: string | null; topic: string }): Curve | null {
  const a = layout.nodes[e.from];
  if (!a) return null;
  if (e.to === null) {
    const bin = layout.bins[e.topic];
    return bin ? curve({ x: a.x + a.w / 2, y: a.y - a.h / 4 }, bin) : null;
  }
  const b = layout.nodes[e.to];
  if (!b) return null;
  return curve({ x: a.x + a.w / 2, y: a.y }, { x: b.x - b.w / 2, y: b.y });
}

export function pointAt(c: Curve, t: number, out: Point = { x: 0, y: 0 }): Point {
  const u = 1 - t;
  const a = u * u * u, b = 3 * u * u * t, d = 3 * u * t * t, e = t * t * t;
  out.x = a * c.p0.x + b * c.c1.x + d * c.c2.x + e * c.p3.x;
  out.y = a * c.p0.y + b * c.c1.y + d * c.c2.y + e * c.p3.y;
  return out;
}

export const pathOf = (c: Curve) => `M${c.p0.x},${c.p0.y} C${c.c1.x},${c.c1.y} ${c.c2.x},${c.c2.y} ${c.p3.x},${c.p3.y}`;

/** Every edge the flow has: one per reader of each topic, and one to its bin for a topic nobody reads. */
export function edgesOf(topics: { topic: string; from: string; to: string[] }[]): { key: string; topic: string; from: string; to: string | null }[] {
  return topics.flatMap(t => (t.to.length ? t.to : [null]).map(to => ({ key: edgeKey(t.from, to, t.topic), topic: t.topic, from: t.from, to })));
}

export const ease = (t: number) => (t < 0.5 ? 2 * t * t : 1 - Math.pow(-2 * t + 2, 2) / 2);

/** Particles move at a near-even pace, so they do not bunch up at either end of an edge. */
export const glide = (t: number) => 0.85 * t + 0.15 * ease(t);
