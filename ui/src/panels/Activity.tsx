import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { cn } from '@/lib/utils';
import type { Entry, EntryTone } from '../timeline';

const dot: Record<EntryTone, string> = { info: 'bg-flow', stuck: 'bg-stuck', lag: 'bg-lag', recovered: 'bg-recovered' };
const time = new Intl.DateTimeFormat('en-US', { hour: 'numeric', minute: '2-digit', second: '2-digit' });

export function Activity({ entries, demo }: { entries: Entry[]; demo: boolean }) {
  return (
    <Card className="min-h-0">
      <CardHeader>
        <CardTitle>Activity</CardTitle>
        <CardDescription>What changed since this page opened</CardDescription>
      </CardHeader>
      <CardContent className="max-h-[380px] overflow-y-auto">
        {entries.length ? (
          <ol data-testid="activity" className="flex flex-col">
            {entries.map(e => (
              <li key={e.id} className="relative flex gap-3 pb-4 last:pb-0">
                <span className="absolute top-3 bottom-0 left-[3.5px] w-px bg-border" aria-hidden />
                <span className={cn('relative mt-1.5 size-2 shrink-0 rounded-full', dot[e.tone])} />
                <div className="min-w-0 flex-1">
                  <div className="flex items-baseline justify-between gap-2">
                    <p className="text-sm font-medium">{e.title}</p>
                    <time className="figures shrink-0 text-xs text-muted-foreground">{time.format(e.at)}</time>
                  </div>
                  {e.detail && <p className="text-xs break-words text-muted-foreground">{e.detail}</p>}
                </div>
              </li>
            ))}
          </ol>
        ) : (
          <p className="text-sm text-muted-foreground">{demo ? 'Nothing has changed yet. Ship OrderPlaced v2 to start an incident.' : 'Nothing has changed yet. Dead letters and recoveries will show here.'}</p>
        )}
      </CardContent>
    </Card>
  );
}
