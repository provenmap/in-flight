// Recovery for one real dead-letter queue: a dry run first (what would be written back, by cause and by target), then
// a redrive that writes each dead letter back to the topic it came from, only when the profile allows writes, and only
// once confirmed. A real run goes on in the background; its status is polled until it ends.
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Loader2Icon, PlayIcon, SearchCheckIcon, SquareIcon } from 'lucide-react';
import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { cn } from '@/lib/utils';
import { answerOf, api, enc, errorText, type Redriven } from '../api';

function Report({ r, testid }: { r: Redriven; testid: string }) {
  const rows = (m: Record<string, number> | undefined) => Object.entries(m ?? {}).sort((a, b) => b[1] - a[1]);
  const byError = rows(r.byError);
  const byTarget = rows(r.byTarget);
  return (
    <div data-testid={testid} className={cn('flex flex-col gap-1.5 rounded-md border px-2.5 py-2 text-xs', r.ok ? 'bg-muted/40' : 'border-stuck/30 bg-stuck/8 text-stuck')}>
      <p className="whitespace-pre-line">{r.text}</p>
      {byError.length > 0 && (
        <div>
          <p className="font-medium text-muted-foreground">By cause</p>
          <ul className="flex flex-col">
            {byError.slice(0, 6).map(([e, n]) => (
              <li key={e} className="flex justify-between gap-2"><span className="truncate" title={e}>{e}</span><span className="figures shrink-0">{n}</span></li>
            ))}
          </ul>
        </div>
      )}
      {byTarget.length > 0 && (
        <div>
          <p className="font-medium text-muted-foreground">Back to</p>
          <ul className="flex flex-col">
            {byTarget.map(([t, n]) => <li key={t} className="flex justify-between gap-2"><span className="truncate">{t}</span><span className="figures shrink-0">{n}</span></li>)}
          </ul>
        </div>
      )}
    </div>
  );
}

const failed = (e: unknown): Redriven => answerOf<Redriven>(e) ?? { ok: false, text: errorText(e) };

export function QueueActions({ dlq, canWrite, bootstrap }: { dlq: string; canWrite: boolean; bootstrap?: string }) {
  const client = useQueryClient();
  const [error, setError] = useState('');
  const [dry, setDry] = useState<Redriven | null>(null);
  const [confirming, setConfirming] = useState(false);
  const key = ['redrive', dlq];
  const last = useQuery({
    queryKey: key,
    queryFn: () => api<Redriven>(`/api/redrive/${enc(dlq)}`),
    refetchInterval: q => (q.state.data?.running ? 1500 : false),
    retry: false,
  });
  const running = !!last.data?.running;
  const filter = error.trim() ? { error: error.trim() } : {};

  const dryRun = useMutation({
    mutationFn: () => api<Redriven>('/api/redrive', 'POST', { dlq, dryRun: true, ...filter }),
    onSuccess: setDry,
    onError: e => setDry(failed(e)),
  });
  const run = useMutation({
    mutationFn: () => api<Redriven>('/api/redrive', 'POST', { dlq, dryRun: false, ...filter }),
    onSuccess: r => { setConfirming(false); setDry(null); client.setQueryData(key, r); },
    onError: e => { setConfirming(false); setDry(failed(e)); },
  });
  const stop = useMutation({
    mutationFn: () => api<Redriven>(`/api/redrive/${enc(dlq)}/stop`, 'POST'),
    onSuccess: r => client.setQueryData(key, r),
    onError: () => void client.invalidateQueries({ queryKey: key }),
  });
  // Only a redrive that ran says what it scanned; before any, the status is just a line.
  const lastReport = last.data && (last.data.running || last.data.scanned !== undefined) ? last.data : null;

  return (
    <div className="flex flex-col gap-2 pt-1.5">
      <div className="flex flex-wrap items-center gap-1.5">
        <Input
          value={error} onChange={e => { setError(e.target.value); setDry(null); }} placeholder="Only errors containing…" aria-label={`Filter ${dlq} by error`}
          className="h-8 min-w-0 flex-1 basis-40 text-xs md:text-xs" data-testid={`redrive-filter-${dlq}`}
        />
        <Button size="sm" variant="outline" data-testid={`redrive-dry-${dlq}`} disabled={dryRun.isPending || running} onClick={() => dryRun.mutate()}>
          {dryRun.isPending ? <Loader2Icon className="animate-spin" /> : <SearchCheckIcon />}Dry run
        </Button>
        <Button
          size="sm" variant={dry?.ok && (dry.matched ?? 0) > 0 ? 'default' : 'outline'} data-testid={`redrive-run-${dlq}`}
          disabled={!canWrite || running || run.isPending} onClick={() => setConfirming(true)}
          title={canWrite ? undefined : 'The connection profile does not allow writes'}
        >
          {running || run.isPending ? <Loader2Icon className="animate-spin" /> : <PlayIcon />}Redrive
        </Button>
        {running && (
          <Button size="sm" variant="ghost" data-testid={`redrive-stop-${dlq}`} disabled={stop.isPending} onClick={() => stop.mutate()}><SquareIcon />Stop</Button>
        )}
      </div>
      {!canWrite && (
        <p className="text-xs text-muted-foreground">
          Dry run only: the connection profile does not allow writes. To redrive from here, edit the connection in Connect and allow redrive to write to this cluster.
        </p>
      )}
      {confirming && (
        <div data-testid={`redrive-confirm-${dlq}`} className="flex flex-col gap-2 rounded-md border border-lag/40 bg-lag/10 px-2.5 py-2 text-xs">
          <p>
            Write the dead letters of <span className="font-medium">{dlq}</span>{error.trim() ? <> whose error contains &ldquo;{error.trim()}&rdquo;</> : null} back to the
            topics they came from{bootstrap ? <>, on {bootstrap}</> : null}? Their consumer reads them again. Each carries a redriveof header, so none is written twice.
          </p>
          <p className="text-muted-foreground">{dry?.ok ? `The dry run said: ${dry.text}` : 'No dry run yet: a dry run first shows what would be written.'}</p>
          <div className="flex gap-1.5">
            <Button size="sm" variant="destructive" data-testid={`redrive-confirm-yes-${dlq}`} disabled={run.isPending} onClick={() => run.mutate()}>
              {run.isPending && <Loader2Icon className="animate-spin" />}Yes, redrive
            </Button>
            <Button size="sm" variant="ghost" onClick={() => setConfirming(false)}>Cancel</Button>
          </div>
        </div>
      )}
      {dry && <Report r={dry} testid={`redrive-report-${dlq}`} />}
      {lastReport && <Report r={lastReport} testid={`redrive-status-${dlq}`} />}
    </div>
  );
}
