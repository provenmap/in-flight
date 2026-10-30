// The controls below the scene: the orders team's schema, the payments team's patch and redrive, and traffic.
import { useEffect, useRef, useState } from 'react';
import { Button } from '../components/button';
import type { SimState, Stats } from '../protocol';

export type Action = 'break' | 'restore' | 'patch' | 'unpatch' | 'redrive' | 'rate';

interface Props {
  stats: Stats | null;
  connected: boolean;
  pending: Action | null;
  act: (action: Action, query?: string) => void;
}

/** What to say, and which control to suggest, for where the story is. */
function next(stats: Stats | null, sim: SimState | null, connected: boolean): { hint: string; suggest: Action | null } {
  const stuck = !!stats && stats.stuck.events > 0;
  if (!connected) return { hint: 'Waiting for the observer…', suggest: null };
  if (!sim) return { hint: 'The simulator is not running. Start it and orders will flow.', suggest: null };
  if (sim.redrive.running) return { hint: `Redriving payments.dlq… ${sim.redrive.redriven} done, ${sim.redrive.remaining} to go`, suggest: null };
  if (stuck && !sim.patched) return { hint: 'Payments cannot read OrderPlaced v2, so every new order is dead-lettered. Patch payments.', suggest: 'patch' };
  if (stuck && sim.patched) return { hint: 'New orders flow again, but the dead letters are still stuck. Redrive the DLQ.', suggest: 'redrive' };
  if (sim.orderVersion === 1 && !sim.patched) return { hint: 'Orders are flowing. The orders team is about to ship OrderPlaced v2.', suggest: 'break' };
  if (stats?.lastEpisode) return { hint: 'Recovered. Go back to v1 and unpatch to run it again.', suggest: null };
  return { hint: '', suggest: null };
}

export function Controls({ stats, connected, pending, act }: Props) {
  const sim = stats?.sim ?? null;
  const up = !!sim && connected;
  const [draft, setDraft] = useState<number | null>(null);
  // The native change event fires once the slider is let go (or a key moves it), not on every step of a drag.
  const slider = useRef<HTMLInputElement>(null);
  const latest = useRef(act);
  useEffect(() => {
    latest.current = act;
  });
  useEffect(() => {
    const el = slider.current!;
    const commit = () => {
      latest.current('rate', `?perSecond=${el.value}`);
      setDraft(null);
    };
    el.addEventListener('change', commit);
    return () => el.removeEventListener('change', commit);
  }, []);
  const { hint, suggest } = next(stats, sim, connected);
  const enabled: Record<Exclude<Action, 'rate'>, boolean> = {
    break: sim?.orderVersion === 1,
    restore: sim?.orderVersion !== 1,
    patch: !sim?.patched,
    unpatch: !!sim?.patched,
    redrive: !sim?.redrive.running,
  };
  const control = (action: Exclude<Action, 'rate'>, label: string, quiet = false, testid = true) => {
    const disabled = !up || !enabled[action] || pending === action;
    return (
      <Button
        data-action={action}
        data-testid={testid ? action : undefined}
        variant={quiet ? 'quiet' : 'default'}
        suggested={suggest === action && !disabled}
        disabled={disabled}
        onClick={() => act(action)}
      >
        {label}
      </Button>
    );
  };
  const rate = draft ?? sim?.rate ?? 8;
  const groupLabel = 'mr-0.5 hidden text-[11.5px] tracking-[0.08em] text-muted uppercase wide:inline';

  return (
    <footer className="fixed bottom-2 left-1/2 w-[min(980px,calc(100vw-32px))] -translate-x-1/2 rounded-[14px] border border-panel-edge bg-panel px-2.5 py-2 backdrop-blur-sm wide:bottom-3.5 wide:px-3.5 wide:py-2.5">
      <p data-testid="hint" className="mb-2 min-h-[18px] text-center text-[13px] text-gold">{hint}</p>
      <div className="flex flex-wrap items-center justify-center gap-x-[22px] gap-y-2.5">
        <div className="flex flex-wrap items-center gap-1.5">
          <span className={groupLabel}>Orders team</span>
          {control('break', 'Ship OrderPlaced v2')}
          {control('restore', 'Back to v1', true, false)}
        </div>
        <div className="flex flex-wrap items-center gap-1.5">
          <span className={groupLabel}>Payments team</span>
          {control('patch', 'Patch payments')}
          {control('redrive', 'Redrive DLQ')}
          {control('unpatch', 'Unpatch', true, false)}
        </div>
        <div className="flex flex-wrap items-center gap-1.5">
          <label className={groupLabel} htmlFor="rate">Traffic</label>
          <input
            ref={slider}
            id="rate"
            type="range"
            min={0}
            max={60}
            step={1}
            value={rate}
            disabled={!up}
            aria-label="Orders per second"
            className="w-[140px] accent-accent"
            onChange={e => setDraft(Number(e.target.value))}
          />
          <output htmlFor="rate" data-testid="rate" className="min-w-9 font-mono text-xs text-ink">{rate}/s</output>
        </div>
      </div>
      <p data-testid="sim-state" className="mt-2 hidden text-center font-mono text-[11.5px] text-faint wide:block">
        {sim
          ? `OrderPlaced v${sim.orderVersion} · payments ${sim.patched ? 'patched' : 'strict'} · ${sim.placed.toLocaleString()} orders placed` +
            (sim.redrive.redriven || sim.redrive.error
              ? ` · last redrive: ${sim.redrive.redriven} redriven${sim.redrive.error ? `, stopped at ${sim.redrive.error}` : ''}`
              : '')
          : ''}
      </p>
    </footer>
  );
}
