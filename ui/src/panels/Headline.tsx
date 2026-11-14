// Money at risk: what is held up now, what it is made of, and how it has moved.
import type { RefObject } from 'react';
import { Card, CardContent } from '@/components/ui/card';
import { cn } from '@/lib/utils';
import { count, money, shown, type Sample } from '../model';
import type { Stats } from '../protocol';
import { RiskChart } from './RiskChart';

const seconds = (iso: string) => Math.max(0, (Date.now() - Date.parse(iso)) / 1000);

function duration(s: number): string {
  if (s < 90) return `${s.toFixed(0)} s`;
  return `${Math.floor(s / 60)} min ${Math.round(s % 60)} s`;
}

/** `counter` is written by the frame loop, so it can ease toward its target every frame without re-rendering. */
export function Headline({ stats, recovering, counter, history }: { stats: Stats | null; recovering: number; counter: RefObject<HTMLDivElement | null>; history: Sample[] }) {
  const stuck = !!stats && stats.stuck.events > 0;
  const lagging = !!stats && stats.lag.events > 0;
  const total = (stats?.stuck.value ?? 0) + (stats?.lag.value ?? 0);
  const ep = stats?.lastEpisode;
  const unpriced = stats?.atRisk.unpriced ?? 0;
  const unknown = unpriced ? `${count(unpriced, 'dead letter')} of unknown value` : '';
  const sub = recovering
    ? `Recovering ${count(recovering, shown.case)}`
    : stats?.atRisk.orders
      ? `${count(stats.atRisk.orders, shown.case)} and ${count(stats.atRisk.customers, shown.party)} waiting${unknown && `, and ${unknown}`}`
      : unknown ? `Nothing priced held up, but ${unknown}` : 'Nothing held up';
  const stuckValue = stats && stats.stuck.events > stats.stuck.unpriced ? money.format(stats.stuck.value) : null;
  const estimated = stats?.atRisk.estimated ?? 0;
  const lagEstimated = stats?.lag.estimated ?? 0;

  return (
    <Card className="gap-0 py-0">
      <CardContent className="grid gap-6 p-5 lg:grid-cols-[minmax(260px,340px)_1fr] lg:gap-8">
        <div className="flex flex-col gap-3" aria-live="polite">
          <span className="text-sm text-muted-foreground">{shown.name} at risk</span>
          <div
            ref={counter}
            data-testid="at-risk"
            className={cn('figures text-5xl leading-none font-semibold tracking-tight transition-colors duration-500 sm:text-[56px]', stuck && 'text-stuck')}
          />
          <span data-testid="at-risk-sub" className="text-sm text-muted-foreground">{sub}</span>
          {estimated > 0 && (
            <span data-testid="at-risk-estimated" className="-mt-2 text-xs text-muted-foreground" title="Lag on events not read yet is valued from each topic's recent average.">
              ≈ incl. {money.format(estimated)} estimated from recent averages
            </span>
          )}

          <div className="mt-1 flex h-2 w-full overflow-hidden rounded-full bg-muted" aria-hidden>
            <span className="h-full bg-stuck transition-[width] duration-500" style={{ width: total ? `${(100 * (stats?.stuck.value ?? 0)) / total}%` : 0 }} />
            <span className="h-full bg-lag transition-[width] duration-500" style={{ width: total ? `${(100 * (stats?.lag.value ?? 0)) / total}%` : 0 }} />
          </div>
          <dl className="grid grid-cols-2 gap-3 text-sm">
            <div>
              <dt className="flex items-center gap-1.5 text-muted-foreground"><span className="size-2 rounded-sm bg-stuck" />Stuck</dt>
              <dd data-testid="stuck" className={cn('figures mt-0.5 text-[15px] font-medium', stuck && 'text-stuck')}>
                {stuck
                  ? `${[stuckValue, unpriced && `${unpriced} unpriced`].filter(Boolean).join(' + ')} for ${duration(seconds(stats.stuck.since ?? new Date().toISOString()))}`
                  : 'Nothing stuck'}
              </dd>
            </div>
            <div>
              <dt className="flex items-center gap-1.5 text-muted-foreground"><span className="size-2 rounded-sm bg-lag" />In lag</dt>
              <dd data-testid="lag" className={cn('figures mt-0.5 text-[15px] font-medium', lagging && 'text-lag')}>
                {lagging ? `${lagEstimated > 0 ? '≈ ' : ''}${money.format(stats.lag.value)} in ${count(stats.lag.events, 'event')}` : 'No lag'}
              </dd>
              {lagging && lagEstimated > 0 && <dd className="text-xs text-muted-foreground">incl. {money.format(lagEstimated)} estimated</dd>}
            </div>
          </dl>
          <div
            data-testid="recovered"
            hidden={!(ep && !stuck)}
            className="rounded-md border border-recovered/30 bg-recovered/10 px-3 py-2 text-sm text-recovered"
          >
            {ep && (
              <>
                <span className="font-medium">Recovered {money.format(ep.value)}</span> across {count(ep.events, shown.case)}, stuck for {duration(ep.tookMs / 1000)}.
              </>
            )}
          </div>
        </div>
        <RiskChart history={history} />
      </CardContent>
    </Card>
  );
}
