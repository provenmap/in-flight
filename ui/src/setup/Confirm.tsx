// Confirm: one screen of the draft's questions, least sure first, the catalog they make, and a dry run of it on
// records sampled now. Answer, validate again, activate. Editing the JSON stays possible, as an escape hatch.
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { ArrowLeftIcon, CheckCircle2Icon, CodeIcon, Loader2Icon, PlayIcon, RefreshCwIcon } from 'lucide-react';
import { useState } from 'react';
import { Badge, type BadgeVariant } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Textarea } from '@/components/ui/field';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { cn } from '@/lib/utils';
import { amountIn } from '../model';
import { answerOf, api, enc, errorText, type Activated, type CatalogJson, type Draft, type Edited, type Guess, type Validation } from '../api';

function sureness(c: number): { label: string; variant: BadgeVariant } {
  const pct = `${Math.round(c * 100)}% sure`;
  return { label: pct, variant: c < 0.5 ? 'stuck' : c < 0.8 ? 'lag' : 'recovered' };
}

/** Whether an option is the drafter's own guess: its label is the guessed value, or says yes to it. */
const isChosen = (g: Guess, label: string) => !!g.chosen && (label === g.chosen || label === `Yes, ${g.chosen}`);

function Question({ g, pending, busyLabel, onAnswer }: { g: Guess; pending: boolean; busyLabel: string | null; onAnswer: (label: string) => void }) {
  const s = sureness(g.confidence);
  return (
    <li data-testid={`guess-${g.id}`} className="flex flex-col gap-2 px-4 py-3">
      <div className="flex items-start justify-between gap-3">
        <p className="text-sm">{g.question}</p>
        <Badge variant={s.variant} className="figures">{s.label}</Badge>
      </div>
      <div className="flex flex-wrap items-center gap-2">
        {g.options.map(o => (
          <Button
            key={o.label} size="sm" variant={isChosen(g, o.label) ? 'default' : 'outline'} disabled={pending}
            data-testid={`guess-${g.id}-option`} data-option={o.label} onClick={() => onAnswer(o.label)}
          >
            {busyLabel === o.label && <Loader2Icon className="animate-spin" />}{o.label}
          </Button>
        ))}
        <span className="font-mono text-[11px] text-muted-foreground">{g.about}</span>
      </div>
    </li>
  );
}

function CatalogSummary({ catalog }: { catalog: CatalogJson }) {
  const events = catalog.events ?? [];
  const consumers = catalog.consumers ?? [];
  return (
    <Card data-testid="draft-catalog">
      <CardHeader>
        <CardTitle>The catalog</CardTitle>
        <CardDescription>
          {events.length} event{events.length === 1 ? '' : 's'}, {consumers.length} consumer group{consumers.length === 1 ? '' : 's'}
          {catalog.currency ? `, in ${catalog.currency}` : ''}.
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Event</TableHead>
              <TableHead>Topic</TableHead>
              <TableHead>Value</TableHead>
              <TableHead>Order</TableHead>
              <TableHead>Customer</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {events.map((e, i) => (
              <TableRow key={i}>
                <TableCell className="font-medium">{e.type}@v{e.version ?? 1}</TableCell>
                <TableCell className="text-muted-foreground">{e.topic}</TableCell>
                <TableCell>
                  {e.value ? <span className="font-mono text-xs">{e.value}</span> : <span className="text-stuck">none</span>}
                  {e.minorUnits && <Badge variant="secondary" className="ml-1.5">minor units</Badge>}
                  {e.currencyField && <span className="ml-1.5 text-xs text-muted-foreground">currency in <span className="font-mono">{e.currencyField}</span></span>}
                </TableCell>
                <TableCell className="font-mono text-xs">{e.order ?? '—'}</TableCell>
                <TableCell className="font-mono text-xs">{e.customer ?? '—'}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
        {consumers.length > 0 && (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Consumer group</TableHead>
                <TableHead>Reads</TableHead>
                <TableHead>Dead-letter queue</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {consumers.map(c => (
                <TableRow key={c.group}>
                  <TableCell className="font-medium">{c.group}</TableCell>
                  <TableCell className="text-muted-foreground">{(c.reads ?? []).join(', ')}</TableCell>
                  <TableCell>{c.dlq ?? <span className="text-muted-foreground">none</span>}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </CardContent>
    </Card>
  );
}

function DryRun({ v, stale, busy, onValidate }: { v: Validation | undefined; stale: boolean; busy: boolean; onValidate: () => void }) {
  const fmt = amountIn(v?.measure, v?.currency);
  const top = (p: Record<string, number>) => Object.entries(p).sort((a, b) => b[1] - a[1])[0];
  return (
    <Card data-testid="validation">
      <CardHeader>
        <CardTitle>Dry run</CardTitle>
        <CardDescription>
          {stale ? 'The catalog changed since this ran. Validate again to see what it prices now.' : 'What the catalog makes of records sampled from the cluster.'}
        </CardDescription>
        <CardAction>
          <Button size="sm" variant="outline" data-testid="validate-button" disabled={busy} onClick={onValidate}>
            <RefreshCwIcon className={cn(busy && 'animate-spin')} />Validate again
          </Button>
        </CardAction>
      </CardHeader>
      <CardContent className={cn('flex flex-col gap-3', stale && 'opacity-60')}>
        {!v ? (
          <p className="text-sm text-muted-foreground">Not validated yet.</p>
        ) : (
          <>
            <p className={cn('flex items-start gap-2 text-sm whitespace-pre-line', v.ok ? 'text-recovered' : 'text-stuck')}>
              {v.ok && <CheckCircle2Icon className="mt-0.5 size-4 shrink-0" />}
              {v.text ? v.text.split('\n')[0] : v.ok ? 'The catalog checks out on the sampled records.' : 'The catalog does not check out.'}
            </p>
            {(v.errors ?? []).length > 0 && <ul className="list-disc pl-5 text-sm text-stuck">{v.errors.map(e => <li key={e}>{e}</li>)}</ul>}
            {(v.missingTopics ?? []).length > 0 && <p className="text-sm text-lag">Not on the cluster yet: {v.missingTopics.join(', ')}. They are waited for.</p>}
            {(v.events ?? []).length > 0 && (
              <ul data-testid="validation-events" className="flex flex-col divide-y rounded-md border">
                {v.events.map(e => {
                  const p = top(e.problems ?? {});
                  const short = e.sampled > 0 && e.priced < e.sampled;
                  return (
                    <li key={`${e.event}-${e.topic}`} className="flex flex-col gap-0.5 px-3 py-2 text-sm">
                      <span className="flex items-baseline justify-between gap-2">
                        <span className="min-w-0 truncate"><span className="font-medium">{e.event}</span> <span className="text-xs text-muted-foreground">{e.topic}</span></span>
                        <span className={cn('figures shrink-0', short && 'text-lag', e.sampled > 0 && !e.priced && 'text-stuck', !e.sampled && 'text-muted-foreground')}>
                          {e.sampled ? `priced ${e.priced} of ${e.sampled}` : 'none sampled'}
                        </span>
                      </span>
                      {e.sampled > 0 && (
                        <span className="figures text-xs text-muted-foreground">
                          {e.min !== undefined && e.max !== undefined ? `${fmt(e.min)} to ${fmt(e.max)}, ` : ''}{fmt(e.total)} in all
                          {e.unknownValue > 0 ? `, ${e.unknownValue} of unknown value` : ''}
                        </span>
                      )}
                      {p && <span className="truncate text-xs text-stuck" title={p[0]}>{p[0]} ({p[1]})</span>}
                    </li>
                  );
                })}
              </ul>
            )}
            {(v.warnings ?? []).length > 0 && <ul className="list-disc pl-5 text-xs text-muted-foreground">{v.warnings.map(w => <li key={w}>{w}</li>)}</ul>}
          </>
        )}
      </CardContent>
    </Card>
  );
}

function EditJson({ flow, catalog, onSaved }: { flow: string; catalog: CatalogJson; onSaved: (r: Edited) => void }) {
  const [text, setText] = useState(() => JSON.stringify(catalog, null, 2));
  const [problem, setProblem] = useState<string | null>(null);
  const save = useMutation({
    mutationFn: (value: unknown) => api<Edited>(`/api/flows/${enc(flow)}`, 'PATCH', { patch: [{ op: 'replace', path: '', value }], target: 'draft' }),
    onSuccess: r => {
      setProblem(r.ok ? null : [r.text, ...(r.errors ?? [])].join('\n'));
      onSaved(r);
    },
    onError: e => setProblem(answerOf(e)?.text ?? errorText(e)),
  });
  const submit = () => {
    let value: unknown;
    try {
      value = JSON.parse(text);
    } catch (e) {
      setProblem(`Not JSON: ${(e as Error).message}`);
      return;
    }
    save.mutate(value);
  };
  return (
    <div className="flex flex-col gap-2">
      <Textarea data-testid="catalog-json" aria-label="The draft catalog as JSON" value={text} onChange={e => setText(e.target.value)} spellCheck={false} className="min-h-80 font-mono text-xs leading-relaxed md:text-xs" />
      <div className="flex flex-wrap items-center gap-2">
        <Button size="sm" data-testid="catalog-json-save" disabled={save.isPending} onClick={submit}>{save.isPending && <Loader2Icon className="animate-spin" />}Save to the draft</Button>
        <span className="text-xs text-muted-foreground">Replaces the whole draft catalog. It is checked before it can be activated.</span>
      </div>
      {problem && <p className="text-sm whitespace-pre-line text-stuck">{problem}</p>}
    </div>
  );
}

export function Confirm({ flow, onBack, onActivated }: { flow: string; onBack: () => void; onActivated: () => void }) {
  const client = useQueryClient();
  const draft = useQuery({ queryKey: ['draft', flow], queryFn: () => api<Draft>(`/api/flows/${enc(flow)}/draft`), retry: false });
  const [validation, setValidation] = useState<Validation | null>(null);
  const [stale, setStale] = useState(false);
  const [note, setNote] = useState<{ ok: boolean; text: string } | null>(null);
  const [editing, setEditing] = useState(false);
  const changed = (ok: boolean, text: string) => {
    setNote({ ok, text });
    setStale(true);
    void client.invalidateQueries({ queryKey: ['draft', flow] });
  };

  const answer = useMutation({
    mutationFn: ({ guess, option }: { guess: string; option: string }) => api<Edited>(`/api/flows/${enc(flow)}`, 'PATCH', { guess, option }),
    onSuccess: r => changed(r.ok, [r.text, ...(r.errors ?? [])].join('\n')),
    onError: e => setNote({ ok: false, text: answerOf(e)?.text ?? errorText(e) }),
  });
  const validate = useMutation({
    mutationFn: () => api<Validation>(`/api/flows/${enc(flow)}/validate`, 'POST', {}),
    onSuccess: r => { setValidation(r); setStale(false); void client.invalidateQueries({ queryKey: ['draft', flow] }); },
    onError: e => { setValidation(answerOf<Validation>(e) ?? { ok: false, text: errorText(e), errors: [], warnings: [], missingTopics: [], events: [] }); setStale(false); },
  });
  const activate = useMutation({
    mutationFn: () => api<Activated>(`/api/flows/${enc(flow)}/activate`, 'POST', {}),
    onSuccess: () => {
      void client.invalidateQueries({ queryKey: ['status'] });
      onActivated();
    },
  });

  const d = draft.data;
  if (!d) {
    return (
      <Card data-testid="confirm-view">
        <CardHeader>
          <CardTitle>{flow}</CardTitle>
          <CardDescription>{draft.isError ? answerOf(draft.error)?.text ?? errorText(draft.error) : 'Loading the draft…'}</CardDescription>
        </CardHeader>
        <CardContent><Button variant="outline" onClick={onBack}><ArrowLeftIcon />Back to discover</Button></CardContent>
      </Card>
    );
  }
  const v = validation ?? d.validation;
  const left = d.guesses.length;

  return (
    <div data-testid="confirm-view" className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_minmax(0,420px)]">
      <div className="flex min-w-0 flex-col gap-4">
        <Card>
          <CardHeader>
            <CardTitle>Confirm the draft of {flow}</CardTitle>
            <CardDescription>
              {left ? `${left} question${left === 1 ? '' : 's'}, least sure first. Each answer edits the draft; whatever you leave keeps the guess shown dark.` : 'No questions left.'}
              {d.answered?.length ? ` ${d.answered.length} answered.` : ''}
            </CardDescription>
            <CardAction><Button size="sm" variant="ghost" onClick={onBack}><ArrowLeftIcon />Topics</Button></CardAction>
          </CardHeader>
          <CardContent className="flex flex-col gap-3">
            {left > 0 && (
              <ul data-testid="guesses" className="flex flex-col divide-y rounded-md border">
                {d.guesses.map(g => (
                  <Question key={g.id} g={g} pending={answer.isPending} busyLabel={answer.isPending && answer.variables.guess === g.id ? answer.variables.option : null} onAnswer={option => answer.mutate({ guess: g.id, option })} />
                ))}
              </ul>
            )}
            {note && <p data-testid="confirm-note" className={cn('text-sm whitespace-pre-line', note.ok ? 'text-muted-foreground' : 'text-stuck')}>{note.text}</p>}
            {d.skipped.length > 0 && <p className="text-xs text-muted-foreground">Left out: {d.skipped.join('; ')}.</p>}
          </CardContent>
        </Card>
        <CatalogSummary catalog={d.catalog} />
        <Card>
          <CardHeader>
            <CardTitle>The JSON</CardTitle>
            <CardDescription>The catalog is a file you can keep in a repository. Edit it here when a question does not cover what you need.</CardDescription>
            <CardAction>
              <Button size="sm" variant="outline" data-testid="edit-json" onClick={() => setEditing(e => !e)}><CodeIcon />{editing ? 'Close' : 'Edit JSON'}</Button>
            </CardAction>
          </CardHeader>
          {editing && (
            <CardContent>
              <EditJson key={JSON.stringify(d.catalog)} flow={flow} catalog={d.catalog} onSaved={r => changed(r.ok, [r.text, ...(r.errors ?? [])].join('\n'))} />
            </CardContent>
          )}
        </Card>
      </div>
      <div className="flex min-w-0 flex-col gap-4 xl:sticky xl:top-20 xl:self-start">
        <DryRun v={v} stale={stale} busy={validate.isPending} onValidate={() => validate.mutate()} />
        <Card>
          <CardHeader>
            <CardTitle>Activate</CardTitle>
            <CardDescription>
              Saves the catalog as the flow {flow} and starts watching it, without a restart.
              {left > 0 && ` ${left} question${left === 1 ? ' is' : 's are'} still open: their guesses are used as they are.`}
            </CardDescription>
          </CardHeader>
          <CardContent className="flex flex-col gap-3">
            <Button data-testid="activate-button" disabled={activate.isPending} onClick={() => activate.mutate()}>
              {activate.isPending ? <Loader2Icon className="animate-spin" /> : <PlayIcon />}Activate and watch
            </Button>
            {v && !v.ok && !stale && <p className="text-xs text-lag">The last dry run found problems. You can still activate; what it cannot price shows as unknown value.</p>}
            {activate.error && <p className="text-sm whitespace-pre-line text-stuck">{answerOf(activate.error)?.text ?? errorText(activate.error)}</p>}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
