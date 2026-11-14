// What is stuck, grouped by the error that stopped it: one row per cause, with how many events, what they are worth,
// which queues hold them, and when it started and last happened. An incident usually has one cause; this says which.
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useEffect } from 'react';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { api, errorText, type Causes as CausesAnswer } from '../api';
import { count, money } from '../model';

const time = new Intl.DateTimeFormat('en-US', { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit', second: '2-digit' });
const when = (iso: string) => {
  const t = Date.parse(iso);
  return Number.isNaN(t) ? iso : time.format(t);
};

export function Causes({ stuck }: { stuck: number }) {
  // Every few seconds while anything is stuck, now and then otherwise.
  const causes = useQuery({
    queryKey: ['causes'],
    queryFn: () => api<CausesAnswer>('/api/stuck/causes'),
    refetchInterval: stuck ? 3000 : 15_000,
    retry: false,
  });
  // The moment nothing is stuck any more, ask again: otherwise the last causes stay up until the slow poll comes round.
  const client = useQueryClient();
  useEffect(() => {
    if (!stuck) void client.invalidateQueries({ queryKey: ['causes'] });
  }, [stuck, client]);
  const rows = causes.data?.causes ?? [];
  return (
    <Card data-testid="causes-view">
      <CardHeader>
        <CardTitle>Errors by cause</CardTitle>
        <CardDescription>
          {causes.isError ? errorText(causes.error) : rows.length ? 'Everything stuck, grouped by the error that stopped it.' : causes.isPending ? 'Loading…' : 'Nothing is stuck.'}
        </CardDescription>
      </CardHeader>
      {rows.length > 0 && (
        <CardContent>
          <ul className="flex flex-col divide-y rounded-md border">
            {rows.map(c => (
              <li key={c.cause} data-testid="cause" className="flex flex-col gap-1 px-3 py-2.5">
                <div className="flex items-start justify-between gap-3">
                  <span className="min-w-0 text-sm font-medium break-words" title={c.example}>{c.cause}</span>
                  <span className="figures shrink-0 text-sm text-stuck">{money.format(c.value)}</span>
                </div>
                <p className="text-xs text-muted-foreground">
                  {count(c.events, 'event')}{c.unpriced ? `, ${c.unpriced} of unknown value` : ''}. From {when(c.first)} to {when(c.last)}.
                </p>
                {c.example && c.example !== c.cause && <p className="truncate font-mono text-[11px] text-muted-foreground" title={c.example}>{c.example}</p>}
                <div className="flex flex-wrap gap-1">
                  {Object.entries(c.dlqs).map(([q, n]) => <Badge key={q} variant="outline">{q} × {n}</Badge>)}
                </div>
              </li>
            ))}
          </ul>
        </CardContent>
      )}
    </Card>
  );
}
