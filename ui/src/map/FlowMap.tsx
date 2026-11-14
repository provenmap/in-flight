// The flow map: districts left to right as money moves, each edge as thick as the money crossing it a minute, events
// as particles sized by their value, and stuck orders as blocks in a dead-letter tank: the one above the district
// holding the most, or with nothing stuck, above the first district whose queue has a redrive.
import { scaleSqrt } from 'd3-scale';
import { useEffect, useMemo, useRef } from 'react';
import { cn } from '@/lib/utils';
import { FLIGHT_MS, Model, count, money, moneyShort, shown, type Block } from '../model';
import type { Flow, Stats } from '../protocol';
import { factsFor, type Tone } from './facts';
import { edgeCurve, edgesOf, ease, glide, layoutOf, pathOf, pointAt, type Curve, type Layout, type Point } from './geometry';

const toneText: Record<Tone, string> = {
  flow: 'text-flow', recovered: 'text-recovered', lag: 'text-lag', stuck: 'text-stuck', muted: 'text-muted-foreground',
};
const toneDot: Record<Tone, string> = {
  flow: 'bg-flow', recovered: 'bg-recovered', lag: 'bg-lag', stuck: 'bg-stuck', muted: 'bg-muted-foreground',
};

/** Particle radius by value: $10 a speck, $2,000 a bead. */
const radius = scaleSqrt().domain([10, 2000]).range([2, 6.5]).clamp(true);

/** What a predicate matches, lit up on the map: the topics its events travel on. */
export interface Highlight {
  topics: string[];
  label: string;
}

interface Props {
  model: Model;
  flow: Flow | null;
  stats: Stats | null;
  theme: string;
  onSelect: (district: string) => void;
  /** Label each edge by the topic it carries, or by the intent its reader reads by. */
  edgeLabels?: 'topics' | 'intents';
  highlight?: Highlight | null;
}

/**
 * The intents an edge stands for: its reader reads by them, and its producer produces their matches. The same events
 * travel either way; only what the edge is called changes.
 */
export function intentsOn(flow: Flow | null, from: string, to: string | null, topic: string) {
  if (!flow?.intents || to === null) return [];
  return flow.intents.filter(i => i.to.includes(to) && i.from.includes(from) && i.sources.includes(topic));
}

function colors(): Record<'flow' | 'recovered' | 'stuck' | 'declined' | 'card', string> {
  const css = getComputedStyle(document.documentElement);
  const v = (name: string) => css.getPropertyValue(name).trim();
  return { flow: v('--flow'), recovered: v('--recovered'), stuck: v('--stuck'), declined: v('--declined'), card: v('--card') };
}

/** Lays a pile of `n` blocks into the tank: the biggest cell that fits them all, filled from the bottom. */
function tankGrid(n: number, tank: { x: number; y: number; w: number; h: number }) {
  const left = tank.x - tank.w / 2 + 10, right = tank.x + tank.w / 2 - 10;
  const bottom = tank.y + tank.h / 2 - 8, top = tank.y - tank.h / 2 + 42;
  const aw = right - left, ah = bottom - top;
  let cell = Math.min(14, Math.max(2, Math.floor(Math.sqrt((aw * ah) / Math.max(n, 1)))));
  while (cell > 2 && Math.ceil(n / Math.floor(aw / cell)) * cell > ah) cell--;
  const cols = Math.max(1, Math.floor(aw / cell));
  const x0 = left + (aw - cols * cell) / 2;
  return (i: number, out: Point) => {
    out.x = x0 + (i % cols) * cell + cell / 2;
    out.y = bottom - Math.floor(i / cols) * cell - cell / 2;
    return cell;
  };
}

/** The district whose tank is drawn: the one holding the most dead letters, else the first whose queue has a redrive. */
function featuredTank(flow: Flow | null, model: Model): string | null {
  if (!flow?.dlqs.length) return null;
  const held = new Map<string, number>();
  for (const b of model.pile) held.set(b.district, (held.get(b.district) ?? 0) + 1);
  const most = [...held].sort((a, b) => b[1] - a[1])[0]?.[0];
  if (most && flow.dlqs.some(q => q.district === most)) return most;
  return (flow.dlqs.find(q => q.redrive) ?? flow.dlqs[0]!).district;
}

export function FlowMap({ model, flow, stats, theme, onSelect, edgeLabels = 'topics', highlight = null }: Props) {
  const canvas = useRef<HTMLCanvasElement>(null);
  const box = useRef<HTMLDivElement>(null);
  const layout: Layout = useMemo(() => (flow ? layoutOf(flow) : { w: 1146, h: 470, nodes: {}, tanks: {}, bins: {} }), [flow]);
  const { w: W, h: H, nodes } = layout;
  const edges = useMemo(() => (flow ? edgesOf(flow.topics) : []), [flow]);
  const curves = useMemo(() => new Map(edges.map(e => [e.key, edgeCurve(layout, e)] as const).filter((x): x is [string, Curve] => !!x[1])), [edges, layout]);
  const tankDistrict = featuredTank(flow, model);
  const tank = tankDistrict ? layout.tanks[tankDistrict] ?? null : null;

  // The moving parts, every frame, on a canvas over the drawing.
  useEffect(() => {
    const el = canvas.current!;
    const ctx = el.getContext('2d')!;
    let scale = 1;
    const resize = () => {
      const dpr = Math.min(window.devicePixelRatio, 2);
      const w = box.current!.clientWidth;
      scale = w / W;
      el.width = Math.round(w * dpr);
      el.height = Math.round(((w * H) / W) * dpr);
      ctx.setTransform(dpr * scale, 0, 0, dpr * scale, 0, 0);
    };
    const observer = new ResizeObserver(resize);
    observer.observe(box.current!);
    resize();

    let c = colors();
    const recolor = setInterval(() => { c = colors(); }, 1000);
    const pt: Point = { x: 0, y: 0 };
    const slot: Point = { x: 0, y: 0 };
    const positions = new Map<string, Point>();
    let raf = 0;

    const draw = (now: number) => {
      ctx.clearRect(0, 0, W, H);

      // Particles in flight.
      for (const p of model.particles) {
        const curve = curves.get(p.edge);
        if (!curve || now < p.born) continue;
        const t = Math.min(1, (now - p.born) / FLIGHT_MS);
        pointAt(curve, glide(t), pt);
        let r = radius(p.value);
        // Grow out of the port and shrink into the next one, so particles never pile up on a district's edge.
        r *= Math.min(1, t / 0.06, (1 - t) / 0.08);
        ctx.globalAlpha = p.kind === 'declined' ? 1 - t : 0.95;
        ctx.fillStyle = p.kind === 'redrive' ? c.recovered : p.kind === 'declined' ? c.declined : c.flow;
        ctx.beginPath();
        ctx.arc(pt.x, pt.y, Math.max(r, 0.5), 0, Math.PI * 2);
        ctx.fill();
        if (p.value >= 1000 && p.kind === 'flow') {
          ctx.globalAlpha = 0.12;
          ctx.beginPath();
          ctx.arc(pt.x, pt.y, r * 1.8, 0, Math.PI * 2);
          ctx.fill();
        }
      }
      ctx.globalAlpha = 1;

      // The tank: blocks rise into their slot from the district below, and go green and drop back when recovered.
      const home = tankDistrict ? nodes[tankDistrict] : undefined;
      const pile = tank && home ? model.pile.filter(b => b.district === tankDistrict) : [];
      const entry = home ? { x: home.x, y: home.y - home.h / 2 } : { x: 0, y: 0 };
      const place = tank ? tankGrid(pile.length, tank) : () => 0;
      const seen = new Set<string>();
      pile.forEach((b: Block, i: number) => {
        const cell = place(i, slot);
        let cur = positions.get(b.id);
        if (!cur) positions.set(b.id, (cur = { x: entry.x, y: entry.y }));
        const age = now - b.born;
        if (age < 600) {
          const k = ease(age / 600);
          cur.x = entry.x + (slot.x - entry.x) * k;
          cur.y = entry.y + (slot.y - entry.y) * k;
        } else {
          cur.x += (slot.x - cur.x) * 0.15;
          cur.y += (slot.y - cur.y) * 0.15;
        }
        seen.add(b.id);
        let x = cur.x, y = cur.y, size = cell - (cell > 4 ? 1.5 : 0.5);
        const g = Model.greenness(b, now);
        if (b.greenAt !== null && now >= b.greenAt) {
          const k = g * g;
          x += (entry.x - x) * k;
          y += (entry.y - y) * k;
          size *= 1 - 0.7 * k;
          ctx.globalAlpha = 1 - 0.8 * k;
          ctx.fillStyle = c.recovered;
        } else {
          ctx.globalAlpha = age < 900 ? 0.55 + 0.45 * (age / 900) : 1;
          ctx.fillStyle = c.stuck;
        }
        ctx.beginPath();
        ctx.roundRect(x - size / 2, y - size / 2, size, size, Math.min(2, size / 4));
        ctx.fill();
      });
      ctx.globalAlpha = 1;
      for (const id of positions.keys()) if (!seen.has(id)) positions.delete(id);

      raf = requestAnimationFrame(draw);
    };
    raf = requestAnimationFrame(draw);
    return () => {
      cancelAnimationFrame(raf);
      clearInterval(recolor);
      observer.disconnect();
    };
  }, [model, curves, tank, tankDistrict, nodes, W, H, theme]);

  // Edge widths follow money a minute, on a scale that keeps the busiest edge at most 14 units wide.
  const now = performance.now();
  const rates = new Map(edges.map(e => [e.key, model.rate(e.key, now)]));
  const busiest = Math.max(2000, ...[...rates.values()].map(r => r.perMinute));
  const width = scaleSqrt().domain([0, busiest]).range([1.5, 14]);
  const stuckValue = tankDistrict ? stats?.stuck.byDistrict[tankDistrict] ?? 0 : 0;
  const inTank = model.pile.filter(b => b.district === tankDistrict);
  const stuckEvents = inTank.filter(b => b.greenAt === null).length;
  const recoveringEvents = inTank.length - stuckEvents;
  const dlqName = flow?.dlqs.find(q => q.district === tankDistrict)?.dlq ?? 'dead letters';

  return (
    <div ref={box} className="relative w-full" style={{ aspectRatio: `${W} / ${H}` }}>
      <svg viewBox={`0 0 ${W} ${H}`} className="absolute inset-0 size-full" role="img" aria-label={`${shown.name} moving through the ${flow?.name ?? ''} flow`}>
        <defs>
          <pattern id="dots" width="20" height="20" patternUnits="userSpaceOnUse">
            <circle cx="1" cy="1" r="1" className="fill-grid" />
          </pattern>
        </defs>
        <rect width={W} height={H} fill="url(#dots)" />

        {edges.map(e => {
          const curve = curves.get(e.key);
          if (!curve) return null;
          const d = pathOf(curve);
          const rate = rates.get(e.key)!;
          const declined = e.to === null;
          const lit = !!highlight && highlight.topics.includes(e.topic);
          return (
            <g key={e.key} data-edge={e.key} data-lit={lit || undefined}>
              <path d={d} fill="none" className="stroke-edge" strokeWidth={1.25} strokeDasharray={declined ? '4 4' : undefined} />
              <path d={d} fill="none" className={declined ? 'stroke-declined' : 'stroke-flow'} strokeOpacity={highlight ? (lit ? 0.4 : 0.06) : 0.16} strokeWidth={width(rate.perMinute)} strokeLinecap="round" />
              {lit && <path d={d} fill="none" className="stroke-primary" strokeWidth={2} strokeDasharray="6 4" />}
            </g>
          );
        })}
        {Object.entries(layout.bins).map(([topic, b]) => (
          <text key={topic} x={b.x + 8} y={b.y + 4} className="fill-muted-foreground text-[11px]">{b.label}</text>
        ))}

        {/* The tank, joined to the district that fills it. */}
        {tank && tankDistrict && <>
        <line x1={tank.x} x2={tank.x} y1={tank.y + tank.h / 2} y2={nodes[tankDistrict]!.y - nodes[tankDistrict]!.h / 2} className="stroke-edge" strokeWidth={1.25} strokeDasharray="3 3" />
        <rect
          x={tank.x - tank.w / 2} y={tank.y - tank.h / 2} width={tank.w} height={tank.h} rx={8}
          className={cn('fill-card', stuckEvents ? 'stroke-stuck' : 'stroke-border')} strokeWidth={stuckEvents ? 1.5 : 1}
        />
        {/* Two lines, so a long queue name (fulfilment-service.DLT) never pushes its figures out of the tank. */}
        <foreignObject x={tank.x - tank.w / 2} y={tank.y - tank.h / 2} width={tank.w} height={40}>
          <div className="flex flex-col px-2.5 pt-1.5 text-[11px] leading-tight">
            <span className="truncate font-medium text-foreground" title={dlqName}>{dlqName}</span>
            <span className={cn('figures', stuckEvents ? 'text-stuck' : recoveringEvents ? 'text-recovered' : 'text-muted-foreground')}>
              {stuckEvents ? `${count(stuckEvents, shown.case)}, ${money.format(stuckValue)}` : recoveringEvents ? `Recovering ${count(recoveringEvents, shown.case)}` : 'Empty'}
            </span>
          </div>
        </foreignObject>
        </>}

        {flow?.districts.filter(d => nodes[d.id]).map(d => {
          const n = nodes[d.id]!;
          const f = factsFor(d.id, stats, model);
          return (
            <foreignObject key={d.id} x={n.x - n.w / 2} y={n.y - n.h / 2} width={n.w} height={n.h}>
              <button
                type="button"
                data-district={d.id}
                onClick={() => onSelect(d.id)}
                className={cn(
                  'flex size-full cursor-pointer flex-col gap-1 rounded-lg border bg-card px-3 py-2.5 text-left shadow-xs transition-colors hover:border-ring/60 focus-visible:ring-[3px] focus-visible:ring-ring/50 focus-visible:outline-none',
                  f.tone === 'stuck' && 'border-stuck/70',
                  d.sink && 'rounded-[2rem] px-4',
                )}
              >
                <span className="flex items-center justify-between gap-2">
                  <span className="text-[13px] font-semibold text-foreground">{d.name}</span>
                  <span className={cn('flex items-center gap-1 text-[10.5px] font-medium', toneText[f.tone])}>
                    <span className={cn('size-1.5 rounded-full', toneDot[f.tone])} />
                    {f.status}
                  </span>
                </span>
                {f.lines.map((l, i) => (
                  <span key={i} className={cn('truncate text-[11.5px] leading-tight', l.tone ? toneText[l.tone] : 'text-muted-foreground')}>{l.text}</span>
                ))}
              </button>
            </foreignObject>
          );
        })}
      </svg>
      <canvas ref={canvas} className="pointer-events-none absolute inset-0 size-full" />
      {/* Topic labels sit above the particles, like signs over a road. A short edge carries its label above it. */}
      <svg viewBox={`0 0 ${W} ${H}`} className="pointer-events-none absolute inset-0 size-full" aria-hidden>
        {edges.filter(e => e.to !== null).map(e => {
          const curve = curves.get(e.key);
          if (!curve) return null;
          const mid = pointAt(curve, 0.5);
          const short = Math.abs(curve.p3.x - curve.p0.x) < 110;
          const by = edgeLabels === 'intents' ? intentsOn(flow, e.from, e.to, e.topic) : [];
          const lit = !!highlight && highlight.topics.includes(e.topic);
          if (edgeLabels === 'intents') {
            return (
              <foreignObject key={e.key} x={mid.x - 90} y={mid.y - (short ? 66 : 25)} width={180} height={52}>
                <div className={cn('mx-auto flex w-fit max-w-[176px] flex-col items-center rounded-md border bg-card px-1.5 py-0.5 leading-tight shadow-xs', lit && 'border-primary')}>
                  {by.length ? by.map(i => (
                    <span key={i.intent} className="flex flex-col items-center">
                      <span className="text-[10px] font-medium text-foreground">intent.{i.intent}</span>
                      <span className="max-w-[168px] truncate font-mono text-[9.5px] text-muted-foreground" title={i.predicate}>{i.predicate}</span>
                    </span>
                  )) : <span className="text-[10px] text-muted-foreground">{e.topic}, by topic</span>}
                </div>
              </foreignObject>
            );
          }
          return (
            <foreignObject key={e.key} x={mid.x - 60} y={mid.y - (short ? 58 : 19)} width={120} height={38}>
              <div className={cn('mx-auto flex w-fit flex-col items-center rounded-md border bg-card px-1.5 py-0.5 leading-tight shadow-xs', lit && 'border-primary')}>
                <span className="text-[10px] text-muted-foreground">{e.topic}</span>
                <span className="figures text-[11.5px] font-medium">{moneyShort.format(rates.get(e.key)!.perMinute)}/min</span>
              </div>
            </foreignObject>
          );
        })}
      </svg>
    </div>
  );
}
