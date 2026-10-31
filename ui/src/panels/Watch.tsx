// Watching a flow of one's own, in place of the runbook: no controls, but where the dead letters are, what they are
// worth, and what will count as their recovery.
import { Badge, type BadgeVariant } from '@/components/ui/badge';
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { cn } from '@/lib/utils';
import { count, money, type Model } from '../model';
import type { Flow, Stats } from '../protocol';

const age = (iso: string) => {
  const s = Math.max(0, (Date.now() - Date.parse(iso)) / 1000);
  return s < 90 ? `${s.toFixed(0)} s` : s < 5400 ? `${Math.round(s / 60)} min` : `${(s / 3600).toFixed(1)} h`;
};

function status(stats: Stats | null, connected: boolean): { label: string; variant: BadgeVariant; detail: string } {
  if (!connected || !stats) return { label: 'Offline', variant: 'secondary', detail: 'Waiting for the observer.' };
  if (stats.stuck.events) return { label: 'Dead letters', variant: 'stuck', detail: 'Events are stuck in a dead-letter queue.' };
  if (stats.lag.events) return { label: 'Lagging', variant: 'lag', detail: 'A consumer group is behind.' };
  return { label: 'Healthy', variant: 'recovered', detail: 'Every event is getting through.' };
}

export function Watch({ flow, stats, model, connected }: { flow: Flow | null; stats: Stats | null; model: Model; connected: boolean }) {
  const s = status(stats, connected);
  const name = (id: string) => flow?.districts.find(d => d.id === id)?.name ?? id;
  const queues = (flow?.dlqs ?? []).map(q => {
    const held = model.pile.filter(b => b.dlq === q.dlq && b.greenAt === null);
    return {
      ...q,
      events: held.length,
      unpriced: held.filter(b => !b.priced).length,
      value: held.reduce((v, b) => v + b.value, 0),
      oldest: held.reduce<string | null>((o, b) => (!o || b.at < o ? b.at : o), null),
    };
  });
  const stuck = queues.filter(q => q.events);
  const hint = stuck.length
    ? `Fix what ${stuck.map(q => name(q.district)).join(' and ')} cannot read, then redrive with your own tooling. ` +
      'An order counts as recovered when its district produces for it again' +
      (stuck.some(q => q.redrive) ? ', or when the redrive group commits past its dead letter.' : '.')
    : connected && stats ? `Watching ${count(queues.length, 'dead-letter queue')}. A dead letter shows here the moment it lands.` : '';

  return (
    <Card>
      <CardHeader>
        <CardTitle>Watching</CardTitle>
        <CardDescription>{s.detail}</CardDescription>
        <CardAction><Badge variant={s.variant}>{s.label}</Badge></CardAction>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        <p data-testid="hint" className="min-h-5 rounded-md bg-muted px-3 py-2 text-sm empty:hidden">{hint}</p>
        <ul data-testid="queues" className="flex flex-col divide-y rounded-md border">
          {queues.map(q => (
            <li key={q.dlq} className="flex flex-col gap-0.5 px-3 py-2.5 text-sm">
              <span className="flex items-center justify-between gap-2">
                <span className="font-medium">{q.dlq}</span>
                <span className={cn('figures', q.events ? 'text-stuck' : 'text-muted-foreground')}>
                  {q.events ? `${count(q.events, 'event')}, ${money.format(q.value)}` : 'Empty'}
                </span>
              </span>
              <span className="text-xs text-muted-foreground">
                {name(q.district)}, {q.group}
                {q.redrive && `, redriven by ${q.redrive}`}
                {q.oldest && `. Oldest ${age(q.oldest)}`}
                {q.unpriced > 0 && `. ${count(q.unpriced, 'event')} of unknown value`}
              </span>
            </li>
          ))}
        </ul>
        <p className="text-xs text-muted-foreground">No simulator is attached, so there are no controls: this console only reads.</p>
      </CardContent>
    </Card>
  );
}
