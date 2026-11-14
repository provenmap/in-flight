// Money at risk over time, stuck under lag, from the stats this page has received.
import { max } from 'd3-array';
import { scaleLinear, scaleTime } from 'd3-scale';
import { area, curveMonotoneX } from 'd3-shape';
import { useEffect, useRef, useState } from 'react';
import { ToggleGroup, ToggleGroupItem } from '@/components/ui/toggle-group';
import { moneyShort, shown, type Sample } from '../model';

const RANGES = { '1m': 60_000, '5m': 300_000, '15m': 900_000, '1h': 3_600_000 } as const;
type Range = keyof typeof RANGES;

function useWidth<T extends HTMLElement>() {
  const ref = useRef<T>(null);
  const [width, setWidth] = useState(600);
  useEffect(() => {
    const o = new ResizeObserver(([e]) => setWidth(e!.contentRect.width));
    o.observe(ref.current!);
    return () => o.disconnect();
  }, []);
  return [ref, width] as const;
}

export function RiskChart({ history, height = 150 }: { history: Sample[]; height?: number }) {
  const [range, setRange] = useState<Range>('5m');
  const [ref, width] = useWidth<HTMLDivElement>();
  const now = history.at(-1)?.t ?? Date.now();
  const from = now - RANGES[range];
  const data = history.filter(s => s.t >= from);
  const pad = { top: 8, right: 28, bottom: 20, left: 44 };
  const x = scaleTime().domain([from, now]).range([pad.left, width - pad.right]);
  const top = Math.max(100, max(data, d => d.stuck + d.lag) ?? 0);
  const y = scaleLinear().domain([0, top]).nice(4).range([height - pad.bottom, pad.top]);
  const stuck = area<Sample>().x(d => x(d.t)).y0(y(0)).y1(d => y(d.stuck)).curve(curveMonotoneX);
  const lag = area<Sample>().x(d => x(d.t)).y0(d => y(d.stuck)).y1(d => y(d.stuck + d.lag)).curve(curveMonotoneX);
  const ticks = x.ticks(width < 420 ? 3 : 5);
  const time = new Intl.DateTimeFormat('en-US', { hour: 'numeric', minute: '2-digit', second: range === '1m' ? '2-digit' : undefined });

  return (
    <div className="flex min-w-0 flex-col gap-2">
      <div className="flex items-center justify-between gap-3">
        <div className="flex items-center gap-4 text-xs text-muted-foreground">
          <span className="flex items-center gap-1.5"><span className="size-2 rounded-sm bg-stuck" />Stuck</span>
          <span className="flex items-center gap-1.5"><span className="size-2 rounded-sm bg-lag" />In lag</span>
        </div>
        <ToggleGroup type="single" value={range} onValueChange={v => v && setRange(v as Range)} aria-label="Time range">
          {(Object.keys(RANGES) as Range[]).map(r => <ToggleGroupItem key={r} value={r}>{r}</ToggleGroupItem>)}
        </ToggleGroup>
      </div>
      <div ref={ref} className="w-full">
        <svg width={width} height={height} role="img" aria-label={`${shown.name} at risk over time`}>
          {y.ticks(4).map(t => (
            <g key={t}>
              <line x1={pad.left} x2={width - pad.right} y1={y(t)} y2={y(t)} className="stroke-border" strokeDasharray={t ? '2 4' : undefined} />
              <text x={pad.left - 6} y={y(t) + 3.5} textAnchor="end" className="figures fill-muted-foreground text-[10.5px]">{moneyShort.format(t)}</text>
            </g>
          ))}
          {ticks.map(t => (
            <text key={+t} x={x(t)} y={height - 4} textAnchor="middle" className="fill-muted-foreground text-[10.5px]">{time.format(t)}</text>
          ))}
          {data.length > 1 ? (
            <>
              <path d={lag(data) ?? ''} className="fill-lag" fillOpacity={0.35} />
              <path d={stuck(data) ?? ''} className="fill-stuck" fillOpacity={0.45} />
            </>
          ) : (
            <text x={(pad.left + width) / 2} y={height / 2} textAnchor="middle" className="fill-muted-foreground text-xs">Collecting the first samples</text>
          )}
        </svg>
      </div>
    </div>
  );
}
