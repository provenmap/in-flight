// The incident runbook: where the story is, the three steps that make and fix the jam, and order traffic.
import { CheckIcon, Loader2Icon } from 'lucide-react';
import { useState } from 'react';
import { Badge, type BadgeVariant } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Separator } from '@/components/ui/separator';
import { Slider } from '@/components/ui/slider';
import { cn } from '@/lib/utils';
import type { SimState, Stats } from '../protocol';

export type Action = 'break' | 'restore' | 'patch' | 'unpatch' | 'redrive' | 'rate';

interface Props {
  stats: Stats | null;
  connected: boolean;
  pending: Action | null;
  act: (action: Action, query?: string) => void;
}

type Status = { label: string; variant: BadgeVariant; detail: string };

function status(stats: Stats | null, sim: SimState | null, connected: boolean): Status {
  const stuck = !!stats && stats.stuck.events > 0;
  if (!connected) return { label: 'Offline', variant: 'secondary', detail: 'Waiting for the observer.' };
  if (!sim) return { label: 'Offline', variant: 'secondary', detail: 'The simulator is not running.' };
  if (sim.redrive.running) return { label: 'Recovering', variant: 'flow', detail: 'Dead letters are replaying through payments.' };
  if (stuck && !sim.patched) return { label: 'Incident', variant: 'stuck', detail: 'Payments cannot read OrderPlaced v2.' };
  if (stuck) return { label: 'Mitigated', variant: 'lag', detail: 'New orders flow; the dead letters wait for a redrive.' };
  if (stats?.lastEpisode) return { label: 'Resolved', variant: 'recovered', detail: 'Every stuck order was recovered.' };
  return { label: 'Healthy', variant: 'recovered', detail: 'Every order reaches shipping.' };
}

/** What to say next, and which step to suggest. */
function hint(stats: Stats | null, sim: SimState | null, connected: boolean): { text: string; next: Action | null } {
  const stuck = !!stats && stats.stuck.events > 0;
  if (!connected) return { text: 'Waiting for the observer…', next: null };
  if (!sim) return { text: 'The simulator is not running. Start it and orders will flow.', next: null };
  if (sim.redrive.running) return { text: `Redriving payments.dlq: ${sim.redrive.redriven} done, ${sim.redrive.remaining} to go.`, next: null };
  if (stuck && !sim.patched) return { text: 'Payments cannot read OrderPlaced v2, so every new order is dead-lettered. Patch payments.', next: 'patch' };
  if (stuck) return { text: 'New orders flow again, but the dead letters are still stuck. Redrive the DLQ.', next: 'redrive' };
  if (sim.orderVersion === 1 && !sim.patched) return { text: 'Orders are flowing. The orders team is about to ship OrderPlaced v2.', next: 'break' };
  if (stats?.lastEpisode) return { text: 'Recovered. Go back to v1 and unpatch to run it again.', next: null };
  return { text: '', next: null };
}

interface StepProps {
  n: number;
  title: string;
  detail: string;
  done: boolean;
  current: boolean;
  children: React.ReactNode;
}

function Step({ n, title, detail, done, current, children }: StepProps) {
  return (
    <li className="group relative flex gap-3 pb-5 last:pb-0">
      <span className="absolute top-7 bottom-0 left-[13px] w-px bg-border group-last:hidden" aria-hidden />
      <span
        className={cn(
          'relative z-10 flex size-[27px] shrink-0 items-center justify-center rounded-full border text-xs font-semibold',
          done ? 'border-recovered bg-recovered text-white' : current ? 'border-primary bg-primary text-primary-foreground' : 'bg-card text-muted-foreground',
        )}
      >
        {done ? <CheckIcon className="size-3.5" /> : n}
      </span>
      <div className="flex min-w-0 flex-1 flex-col gap-2 pt-0.5">
        <div>
          <p className={cn('text-sm font-medium', !current && !done && 'text-muted-foreground')}>{title}</p>
          <p className="text-xs text-muted-foreground">{detail}</p>
        </div>
        <div className="flex flex-wrap gap-2">{children}</div>
      </div>
    </li>
  );
}

export function Runbook({ stats, connected, pending, act }: Props) {
  const sim = stats?.sim ?? null;
  const up = !!sim && connected;
  const s = status(stats, sim, connected);
  const h = hint(stats, sim, connected);
  const [draft, setDraft] = useState<number | null>(null);
  const rate = draft ?? sim?.rate ?? 8;
  const stuck = !!stats && stats.stuck.events > 0;
  const off = (a: Action, enabled: boolean) => !up || !enabled || pending === a;
  const busy = (a: Action) => pending === a && <Loader2Icon className="animate-spin" />;
  const v2 = sim?.orderVersion === 2;

  return (
    <Card>
      <CardHeader>
        <CardTitle>Incident runbook</CardTitle>
        <CardDescription>{s.detail}</CardDescription>
        <CardAction><Badge variant={s.variant}>{s.label}</Badge></CardAction>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        <p data-testid="hint" className="min-h-5 rounded-md bg-muted px-3 py-2 text-sm empty:hidden">{h.text}</p>
        <ol className="flex flex-col">
          <Step n={1} title="The orders team ships OrderPlaced v2" detail="total becomes an object with amount and currency." done={v2 || stuck} current={h.next === 'break'}>
            <Button data-action="break" data-testid="break" size="sm" variant={h.next === 'break' ? 'default' : 'outline'} disabled={off('break', sim?.orderVersion === 1)} onClick={() => act('break')}>
              {busy('break')}Ship OrderPlaced v2
            </Button>
            <Button data-action="restore" size="sm" variant="ghost" disabled={off('restore', sim?.orderVersion !== 1)} onClick={() => act('restore')}>
              {busy('restore')}Back to v1
            </Button>
          </Step>
          <Step n={2} title="Patch payments" detail="Teach the payments decoder both shapes of total." done={!!sim?.patched} current={h.next === 'patch'}>
            <Button data-action="patch" data-testid="patch" size="sm" variant={h.next === 'patch' ? 'default' : 'outline'} disabled={off('patch', !sim?.patched)} onClick={() => act('patch')}>
              {busy('patch')}Patch payments
            </Button>
            <Button data-action="unpatch" size="sm" variant="ghost" disabled={off('unpatch', !!sim?.patched)} onClick={() => act('unpatch')}>
              {busy('unpatch')}Unpatch
            </Button>
          </Step>
          <Step n={3} title="Redrive the dead letters" detail="Replay payments.dlq through the current payments code." done={!!stats?.lastEpisode && !stuck} current={h.next === 'redrive'}>
            <Button data-action="redrive" data-testid="redrive" size="sm" variant={h.next === 'redrive' ? 'default' : 'outline'} disabled={off('redrive', !sim?.redrive.running)} onClick={() => act('redrive')}>
              {(busy('redrive') || sim?.redrive.running) && <Loader2Icon className="animate-spin" />}Redrive DLQ
            </Button>
          </Step>
        </ol>
        <Separator />
        <div className="flex flex-col gap-3">
          <div className="flex items-center justify-between text-sm">
            <label id="rate-label" className="font-medium">Order traffic</label>
            <output data-testid="rate" className="figures text-muted-foreground">{rate}/s</output>
          </div>
          <Slider
            aria-labelledby="rate-label"
            min={0}
            max={60}
            step={1}
            value={[rate]}
            disabled={!up}
            onValueChange={([v]) => setDraft(v!)}
            onValueCommit={([v]) => { act('rate', `?perSecond=${v}`); setDraft(null); }}
          />
        </div>
        <p data-testid="sim-state" className="text-xs text-muted-foreground">
          {sim
            ? `OrderPlaced v${sim.orderVersion}, payments ${sim.patched ? 'patched' : 'strict'}, ${sim.placed.toLocaleString('en-US')} orders placed` +
              (sim.redrive.redriven || sim.redrive.error ? `. Last redrive: ${sim.redrive.redriven} replayed${sim.redrive.error ? `, stopped at ${sim.redrive.error}` : ''}.` : '.')
            : 'No simulator.'}
        </p>
      </CardContent>
    </Card>
  );
}
