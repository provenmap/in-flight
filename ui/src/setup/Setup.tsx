// Setup, before there is a flow to watch, or when switching to another: Connect, then Discover, then Confirm.
import { CheckIcon } from 'lucide-react';
import { useState } from 'react';
import { cn } from '@/lib/utils';
import type { Status } from '../api';
import { Confirm } from './Confirm';
import { Connect } from './Connect';
import { Discover } from './Discover';

export type Step = 'connect' | 'discover' | 'confirm';

const STEPS: { id: Step; title: string; detail: string }[] = [
  { id: 'connect', title: 'Connect', detail: 'a cluster, tested' },
  { id: 'discover', title: 'Discover', detail: 'its topics and queues' },
  { id: 'confirm', title: 'Confirm', detail: 'what the money is' },
];

function Steps({ step, reachable, go }: { step: Step; reachable: (s: Step) => boolean; go: (s: Step) => void }) {
  const at = STEPS.findIndex(s => s.id === step);
  return (
    <ol data-testid="setup-steps" className="flex flex-wrap items-center gap-x-2 gap-y-2">
      {STEPS.map((s, i) => {
        const done = i < at;
        const current = i === at;
        return (
          <li key={s.id} className="flex items-center gap-2">
            {i > 0 && <span className="hidden h-px w-8 bg-border sm:block" aria-hidden />}
            <button
              type="button"
              data-testid={`step-${s.id}`}
              data-state={current ? 'current' : done ? 'done' : 'todo'}
              aria-current={current ? 'step' : undefined}
              disabled={!reachable(s.id)}
              onClick={() => go(s.id)}
              className="flex items-center gap-2 rounded-md px-1.5 py-1 text-left disabled:cursor-not-allowed enabled:hover:bg-accent"
            >
              <span
                className={cn(
                  'flex size-[27px] shrink-0 items-center justify-center rounded-full border text-xs font-semibold',
                  done ? 'border-recovered bg-recovered text-white' : current ? 'border-primary bg-primary text-primary-foreground' : 'bg-card text-muted-foreground',
                )}
              >
                {done ? <CheckIcon className="size-3.5" /> : i + 1}
              </span>
              <span className="flex flex-col leading-tight">
                <span className={cn('text-sm font-medium', !current && !done && 'text-muted-foreground')}>{s.title}</span>
                <span className="hidden text-xs text-muted-foreground sm:block">{s.detail}</span>
              </span>
            </button>
          </li>
        );
      })}
    </ol>
  );
}

export function Setup({ status, initial, onActivated }: { status: Status; initial?: Step; onActivated: () => void }) {
  const [chosen, setChosen] = useState<Step>(initial ?? (status.stage === 'connect' ? 'connect' : 'discover'));
  const [flow, setFlow] = useState<string | null>(null);
  // Without a profile in use there is nothing to discover: back to Connect, whatever was chosen.
  const step: Step = !status.profile ? 'connect' : chosen === 'confirm' && !flow ? 'discover' : chosen;
  const reachable = (s: Step) => s === 'connect' || (s === 'discover' && !!status.profile) || (s === 'confirm' && !!status.profile && !!flow);
  const toConfirm = (f: string) => { setFlow(f); setChosen('confirm'); };

  return (
    <main data-testid="setup" className="mx-auto flex max-w-[1600px] flex-col gap-5 p-4 sm:p-6">
      <div className="flex flex-col gap-3">
        <Steps step={step} reachable={reachable} go={setChosen} />
        <p data-testid="setup-next" className="text-sm text-muted-foreground">
          {status.problem ? <span className="text-stuck">{status.problem} </span> : null}
          {step === 'connect' ? 'Point the console at your Kafka. Nothing is written to the cluster unless you allow it.'
            : step === 'discover' ? <>Connected to <span className="font-medium text-foreground">{status.profile?.name}</span> ({status.profile?.bootstrap}). Pick the topics of one flow and draft its catalog.</>
              : 'Answer what the draft is unsure of, check the dry run, then activate.'}
        </p>
      </div>
      {step === 'connect' && <Connect status={status} onConnected={() => setChosen('discover')} />}
      {step === 'discover' && <Discover key={status.profile?.name} status={status} onDrafted={toConfirm} onActivated={onActivated} />}
      {step === 'confirm' && flow && <Confirm key={flow} flow={flow} onBack={() => setChosen('discover')} onActivated={onActivated} />}
    </main>
  );
}
