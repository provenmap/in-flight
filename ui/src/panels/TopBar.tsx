import { ArrowLeftIcon, MoonIcon, Settings2Icon, SunIcon } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';
import type { Status } from '../api';
import { shown } from '../model';
import type { Flow } from '../protocol';
import type { Connection } from '../useStream';

/** The mark: a coin in flight, its trail behind it. */
function Mark() {
  return (
    <svg viewBox="0 0 32 32" className="size-7 shrink-0" aria-hidden>
      <rect width="32" height="32" rx="7" className="fill-primary" />
      <path d="M5 21 C11 21, 13 13, 20 13" fill="none" className="stroke-primary-foreground" strokeOpacity="0.45" strokeWidth="2" strokeLinecap="round" />
      <circle cx="21.5" cy="13" r="4.5" className="fill-primary-foreground" />
    </svg>
  );
}

interface Props {
  flow: Flow | null;
  /** The stream's state, in the live console; none while setting up. */
  connection?: Connection;
  theme: string;
  toggleTheme: () => void;
  /** Sample data from the simulator, under `in-flight try`. */
  demo?: boolean;
  status?: Status;
  /** Back to setup, from the live console: another flow, or another cluster. */
  onSetup?: () => void;
  /** Back to the live console, from setup, when there is a flow to go back to. */
  onBack?: () => void;
}

export function TopBar({ flow, connection, theme, toggleTheme, demo, status, onSetup, onBack }: Props) {
  const label = connection === 'live' ? 'Live from Kafka' : connection === 'down' ? 'Reconnecting to the observer' : 'Connecting';
  const short = connection === 'live' ? 'Live' : connection === 'down' ? 'Reconnecting' : 'Connecting';
  const subtitle = connection
    ? `${shown.name} moving through the ${flow?.name ?? status?.flow ?? 'order-to-cash'} pipeline`
    : 'Set up: connect a cluster, find its flow, confirm what its events are worth';
  return (
    <header className="sticky top-0 z-40 border-b bg-background/85 backdrop-blur supports-[backdrop-filter]:bg-background/70">
      <div className="mx-auto flex h-14 max-w-[1600px] items-center gap-3 px-4 sm:px-6">
        <Mark />
        <div className="flex min-w-0 flex-col leading-tight">
          <span className="truncate text-[15px] font-semibold">In Flight</span>
          <span className="hidden truncate text-xs text-muted-foreground sm:block">{subtitle}</span>
        </div>
        {demo && (
          <Badge data-testid="sample-badge" variant="lag" className="shrink-0" title="in-flight try: a simulated business, not your cluster">Sample data</Badge>
        )}
        {flow && <Badge variant="outline" className="hidden sm:inline-flex">{shown.unit ?? flow.currency}</Badge>}
        <div className="ml-auto flex shrink-0 items-center gap-2">
          {onBack && (
            <Button variant="outline" size="sm" data-testid="setup-close" onClick={onBack}>
              <ArrowLeftIcon /><span className="hidden sm:inline">Back to {status?.flow ?? 'the live view'}</span>
            </Button>
          )}
          {onSetup && (
            <Button variant="outline" size="sm" data-testid="setup-open" onClick={onSetup} title="Switch flow, or connect another cluster" aria-label="Switch flow, or connect another cluster">
              <Settings2Icon />
              <span className="hidden max-w-[240px] truncate md:inline">
                {status?.profile?.name ?? 'Setup'}{status?.flow ? ` · ${status.flow}` : ''}
              </span>
            </Button>
          )}
          {connection && (
            <span data-testid="connection" data-state={connection} title={label} className="flex items-center gap-2 rounded-md border px-2.5 py-1 text-xs font-medium">
              <span className="relative flex size-2">
                {connection === 'live' && <span className="absolute inline-flex size-full animate-ping rounded-full bg-recovered opacity-60 motion-reduce:hidden" />}
                <span className={cn('relative inline-flex size-2 rounded-full', connection === 'live' ? 'bg-recovered' : connection === 'down' ? 'bg-stuck' : 'bg-muted-foreground')} />
              </span>
              <span className="sm:hidden">{short}</span>
              <span className="hidden sm:inline">{label}</span>
            </span>
          )}
          <Button variant="ghost" size="icon" onClick={toggleTheme} aria-label={theme === 'dark' ? 'Use the light theme' : 'Use the dark theme'}>
            {theme === 'dark' ? <SunIcon /> : <MoonIcon />}
          </Button>
        </div>
      </div>
    </header>
  );
}
