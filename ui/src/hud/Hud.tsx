// The counter above the scene: money at risk, what it is made of, and the last recovery.
import type { RefObject } from 'react';
import { cn } from '../components/cn';
import { money } from '../model';
import type { Stats } from '../protocol';

export const count = (n: number, noun: string) => `${n.toLocaleString()} ${noun}${n === 1 ? '' : 's'}`;

const seconds = (iso: string) => Math.max(0, (Date.now() - Date.parse(iso)) / 1000);

/** `value` is written by the frame loop, so the counter can ease toward its target every frame without re-rendering. */
export function Hud({ stats, recovering, value }: { stats: Stats | null; recovering: number; value: RefObject<HTMLDivElement | null> }) {
  const stuck = !!stats && stats.stuck.events > 0;
  const lagging = !!stats && stats.lag.events > 0;
  const ep = stats?.lastEpisode;
  const sub = recovering
    ? `recovering ${count(recovering, 'order')}`
    : stats?.atRisk.orders
      ? `${count(stats.atRisk.orders, 'order')} · ${count(stats.atRisk.customers, 'customer')} waiting`
      : 'nothing held up';

  return (
    <section
      data-testid="hud"
      aria-live="polite"
      className={cn(
        'group pointer-events-none fixed top-[66px] left-1/2 w-[calc(100vw-32px)] -translate-x-1/2 rounded-[14px] border border-panel-edge bg-panel px-3.5 py-2.5 text-center backdrop-blur-sm',
        'wide:top-3.5 wide:w-auto wide:max-w-[calc(100vw-32px)] wide:min-w-[300px] wide:px-[22px] wide:pt-3 wide:pb-3.5',
      )}
      data-alarm={!!stats && stats.atRisk.value > 0 && stuck}
    >
      <div className="text-[11px] tracking-[0.14em] text-muted uppercase">Money at risk</div>
      <div
        ref={value}
        data-testid="at-risk"
        className="mt-0.5 font-mono text-[30px] leading-[1.1] font-semibold tabular-nums transition-colors duration-400 wide:text-[40px] group-data-[alarm=true]:text-red group-data-[alarm=true]:[text-shadow:0_0_24px_rgb(255_77_94/0.45)]"
      />
      <div data-testid="at-risk-sub" className="text-[13px] text-muted">{sub}</div>
      <div className="mt-2 flex flex-wrap justify-center gap-2">
        <span data-testid="stuck" className={cn('rounded-full bg-white/5 px-2 py-0.5 font-mono text-xs text-muted', stuck && 'bg-red-soft text-red')}>
          {stuck ? `stuck ${money.format(stats.stuck.value)} · ${seconds(stats.stuck.since ?? new Date().toISOString()).toFixed(0)} s` : 'stuck $0'}
        </span>
        <span data-testid="lag" className={cn('rounded-full bg-white/5 px-2 py-0.5 font-mono text-xs text-muted', lagging && 'bg-amber-soft text-amber')}>
          {lagging ? `in lag ${money.format(stats.lag.value)} · ${count(stats.lag.events, 'event')}` : 'in lag $0'}
        </span>
      </div>
      <div
        data-testid="recovered"
        hidden={!(ep && !stuck)}
        className="mt-2.5 rounded-lg bg-green-soft px-3 py-1.5 font-mono text-[13px] font-semibold text-green"
      >
        {ep && `Recovered ${money.format(ep.value)} · ${count(ep.events, 'order')} · stuck ${(ep.tookMs / 1000).toFixed(1)} s`}
      </div>
    </section>
  );
}
