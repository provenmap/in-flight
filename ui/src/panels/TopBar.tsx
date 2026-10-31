import { MoonIcon, SunIcon } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';
import type { Flow } from '../protocol';
import type { Connection } from '../useStream';

/** The mark: a coin in flight, its trail behind it. */
function Mark() {
  return (
    <svg viewBox="0 0 32 32" className="size-7" aria-hidden>
      <rect width="32" height="32" rx="7" className="fill-primary" />
      <path d="M5 21 C11 21, 13 13, 20 13" fill="none" className="stroke-primary-foreground" strokeOpacity="0.45" strokeWidth="2" strokeLinecap="round" />
      <circle cx="21.5" cy="13" r="4.5" className="fill-primary-foreground" />
    </svg>
  );
}

export function TopBar({ flow, connection, theme, toggleTheme }: { flow: Flow | null; connection: Connection; theme: string; toggleTheme: () => void }) {
  const label = connection === 'live' ? 'Live from Kafka' : connection === 'down' ? 'Reconnecting to the observer' : 'Connecting';
  return (
    <header className="sticky top-0 z-40 border-b bg-background/85 backdrop-blur supports-[backdrop-filter]:bg-background/70">
      <div className="mx-auto flex h-14 max-w-[1600px] items-center gap-3 px-4 sm:px-6">
        <Mark />
        <div className="flex min-w-0 flex-col leading-tight">
          <span className="text-[15px] font-semibold">Revenue in Flight</span>
          <span className="hidden text-xs text-muted-foreground sm:block">Money moving through the {flow?.name ?? 'order-to-cash'} pipeline</span>
        </div>
        {flow && <Badge variant="outline" className="hidden sm:inline-flex">{flow.currency}</Badge>}
        <div className="ml-auto flex items-center gap-2">
          <span data-testid="connection" data-state={connection} className="flex items-center gap-2 rounded-md border px-2.5 py-1 text-xs font-medium">
            <span className="relative flex size-2">
              {connection === 'live' && <span className="absolute inline-flex size-full animate-ping rounded-full bg-recovered opacity-60 motion-reduce:hidden" />}
              <span className={cn('relative inline-flex size-2 rounded-full', connection === 'live' ? 'bg-recovered' : connection === 'down' ? 'bg-stuck' : 'bg-muted-foreground')} />
            </span>
            {label}
          </span>
          <Button variant="ghost" size="icon" onClick={toggleTheme} aria-label={theme === 'dark' ? 'Use the light theme' : 'Use the dark theme'}>
            {theme === 'dark' ? <SunIcon /> : <MoonIcon />}
          </Button>
        </div>
      </div>
    </header>
  );
}
