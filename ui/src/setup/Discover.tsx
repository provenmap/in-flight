// Discover: what is on the cluster. Flow-looking topics to pick from, the consumer groups and what they read, the
// dead-letter queues and why each looks like one, and a sample of any topic's records. Then a name, and a draft.
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { EyeIcon, EyeOffIcon, Loader2Icon, RefreshCwIcon, TriangleAlertIcon } from 'lucide-react';
import { useState } from 'react';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Checkbox, Field } from '@/components/ui/field';
import { Input } from '@/components/ui/input';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { cn } from '@/lib/utils';
import { answerOf, api, enc, errorText, type Activated, type Discovered, type Draft, type Sampled, type Status } from '../api';

const FLOW = /^[a-z0-9][a-z0-9-]{0,62}$/;
const num = (n: number) => n.toLocaleString('en-US');

function Sample({ topic }: { topic: string }) {
  const [reveal, setReveal] = useState(false);
  const sample = useQuery({
    queryKey: ['sample', topic, reveal],
    queryFn: () => api<Sampled>(`/api/topics/${enc(topic)}/sample?n=50&reveal=${reveal}`),
    staleTime: 60_000,
    retry: false,
  });
  const s = sample.data;
  return (
    <Card data-testid="sample-view" className="gap-3">
      <CardHeader>
        <CardTitle className="truncate">{topic}</CardTitle>
        <CardDescription>{s ? s.text : sample.isError ? errorText(sample.error) : 'Sampling its latest records…'}</CardDescription>
        <CardAction>
          <Button
            size="sm" variant={reveal ? 'default' : 'outline'} data-testid="sample-reveal"
            onClick={() => (reveal || window.confirm('Show example values as they are? They may hold customers\' personal data, and will be on this screen.')) && setReveal(r => !r)}
          >
            {reveal ? <EyeOffIcon /> : <EyeIcon />}{reveal ? 'Mask values' : 'Reveal values'}
          </Button>
        </CardAction>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        {reveal && (
          <p className="flex items-start gap-2 rounded-md border border-lag/30 bg-lag/10 px-3 py-2 text-xs text-lag">
            <TriangleAlertIcon className="mt-px size-3.5 shrink-0" />Example values are shown as they are on the topic.
          </p>
        )}
        {sample.isFetching && !s && <Loader2Icon className="size-4 animate-spin text-muted-foreground" />}
        {s && (
          <>
            <div className="flex flex-wrap gap-1.5">
              {Object.entries(s.formats).map(([f, n]) => <Badge key={f} variant="secondary">{f} × {n}</Badge>)}
              {Object.entries(s.eventTypes).map(([t, n]) => <Badge key={t} variant="flow">{t} × {n}</Badge>)}
              {s.unreadable > 0 && <Badge variant="stuck">{s.unreadable} unreadable</Badge>}
            </div>
            {s.fields.length > 0 && (
              <Table data-testid="sample-fields">
                <TableHeader>
                  <TableRow>
                    <TableHead>Field</TableHead>
                    <TableHead>Types</TableHead>
                    <TableHead className="text-right">Present</TableHead>
                    <TableHead>Example</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {s.fields.map(f => (
                    <TableRow key={f.path}>
                      <TableCell className="font-mono text-xs">{f.path}</TableCell>
                      <TableCell className="text-xs text-muted-foreground">{f.types.join(', ')}{f.numericText ? ' (number as text)' : ''}</TableCell>
                      <TableCell className="figures text-right">{Math.round(f.presence * 100)}%</TableCell>
                      <TableCell className="max-w-[180px] truncate font-mono text-xs text-muted-foreground" title={f.example}>{f.example ?? '—'}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
            {s.headers.length > 0 && <p className="text-xs text-muted-foreground">Headers: <span className="font-mono">{s.headers.join(', ')}</span></p>}
            {s.problems.length > 0 && (
              <ul className="list-disc pl-5 text-xs text-stuck">{s.problems.map(p => <li key={p}>{p}</li>)}</ul>
            )}
          </>
        )}
      </CardContent>
    </Card>
  );
}

/** Drafts and flows already in the workspace: carry on with one, or watch one. */
function Existing({ status, onDraft, onActivated }: { status: Status; onDraft: (flow: string) => void; onActivated: () => void }) {
  const watch = useMutation({
    mutationFn: (flow: string) => api<Activated>(`/api/flows/${enc(flow)}/activate`, 'POST', {}),
    onSuccess: onActivated,
  });
  const flows = status.flows;
  if (!status.drafts.length && !flows.length) return null;
  return (
    <Card data-testid="existing-flows">
      <CardHeader>
        <CardTitle>Already in the workspace</CardTitle>
        <CardDescription>Carry on with a draft, or switch the live view to a saved flow.</CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        <ul className="flex flex-col divide-y rounded-md border">
          {status.drafts.map(d => (
            <li key={`draft-${d}`} className="flex items-center gap-2 px-3 py-2 text-sm">
              <span className="font-medium">{d}</span><Badge variant="lag">Draft</Badge>
              <Button size="sm" variant="outline" className="ml-auto" data-testid={`continue-${d}`} onClick={() => onDraft(d)}>Continue</Button>
            </li>
          ))}
          {flows.map(f => (
            <li key={`flow-${f}`} className="flex items-center gap-2 px-3 py-2 text-sm">
              <span className="font-medium">{f}</span>
              {f === status.flow ? <Badge variant="recovered">Watching</Badge> : <Badge variant="secondary">Saved</Badge>}
              <Button size="sm" variant="outline" className="ml-auto" data-testid={`watch-${f}`} disabled={watch.isPending || f === status.flow} onClick={() => watch.mutate(f)}>
                {watch.isPending && watch.variables === f && <Loader2Icon className="animate-spin" />}Watch
              </Button>
            </li>
          ))}
        </ul>
        {watch.error && <p className="text-sm text-stuck">{answerOf(watch.error)?.text ?? errorText(watch.error)}</p>}
      </CardContent>
    </Card>
  );
}

export function Discover({ status, onDrafted, onActivated }: { status: Status; onDrafted: (flow: string) => void; onActivated: () => void }) {
  const client = useQueryClient();
  const found = useQuery({ queryKey: ['discover', status.profile?.name], queryFn: () => api<Discovered>('/api/discover'), staleTime: Infinity, retry: false });
  const [picked, setPicked] = useState<Set<string> | null>(null);
  const [looking, setLooking] = useState<string | null>(null);
  const [name, setName] = useState(status.flow ?? (status.flows.length ? '' : 'checkout'));
  const d = found.data;
  const flowTopics = d?.topics.filter(t => t.kind === 'flow') ?? [];
  const others = d?.topics.filter(t => t.kind === 'internal' || t.kind === 'retry') ?? [];
  const chosen = picked ?? new Set(flowTopics.filter(t => t.records > 0).map(t => t.name));
  const toggle = (t: string) => setPicked(() => {
    const s = new Set(chosen);
    if (s.has(t)) s.delete(t); else s.add(t);
    return s;
  });
  const nameProblem = name && !FLOW.test(name) ? 'Lowercase letters, digits and hyphens, such as checkout.' : null;

  const draft = useMutation({
    mutationFn: () => api<Draft>(`/api/flows/${enc(name)}/draft`, 'POST', { topics: [...chosen] }),
    onSuccess: r => {
      client.setQueryData(['draft', name], r);
      void client.invalidateQueries({ queryKey: ['status'] });
      onDrafted(name);
    },
  });

  return (
    <div data-testid="discover-view" className="flex flex-col gap-4">
      <Existing status={status} onDraft={onDrafted} onActivated={onActivated} />
      <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_minmax(0,460px)]">
        <div className="flex min-w-0 flex-col gap-4">
          <Card>
            <CardHeader>
              <CardTitle>Topics</CardTitle>
              <CardDescription>
                {d ? d.text : found.isError ? <span className="text-stuck">{answerOf(found.error)?.text ?? errorText(found.error)}</span> : `Reading ${status.profile?.bootstrap ?? 'the cluster'}…`}
              </CardDescription>
              <CardAction>
                <Button size="sm" variant="outline" disabled={found.isFetching} onClick={() => { setPicked(null); void found.refetch(); }}>
                  <RefreshCwIcon className={cn(found.isFetching && 'animate-spin')} />Rescan
                </Button>
              </CardAction>
            </CardHeader>
            <CardContent className="flex flex-col gap-3">
              {d && (
                <>
                  <p className="text-xs text-muted-foreground">Tick the topics that carry this flow&apos;s events. Select one to see the shape of its records.</p>
                  {flowTopics.length ? (
                    <Table data-testid="discover-topics">
                      <TableHeader>
                        <TableRow>
                          <TableHead className="w-8"><span className="sr-only">In the flow</span></TableHead>
                          <TableHead>Topic</TableHead>
                          <TableHead className="text-right">Partitions</TableHead>
                          <TableHead className="text-right">Records</TableHead>
                          <TableHead>Read by</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {flowTopics.map(t => (
                          <TableRow key={t.name} data-state={looking === t.name ? 'selected' : undefined}>
                            <TableCell>
                              <Checkbox aria-label={`${t.name} is in the flow`} data-testid={`topic-pick-${t.name}`} checked={chosen.has(t.name)} onChange={() => toggle(t.name)} />
                            </TableCell>
                            <TableCell>
                              <button type="button" className="font-medium underline-offset-4 hover:underline" data-testid={`topic-${t.name}`} onClick={() => setLooking(t.name)}>{t.name}</button>
                            </TableCell>
                            <TableCell className="figures text-right">{t.partitions}</TableCell>
                            <TableCell className={cn('figures text-right', !t.records && 'text-muted-foreground')}>{num(t.records)}</TableCell>
                            <TableCell className="max-w-[220px] truncate text-xs text-muted-foreground">
                              {d.groups.filter(g => g.reads.includes(t.name)).map(g => g.group).join(', ') || '—'}
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  ) : (
                    <p className="py-6 text-center text-sm text-muted-foreground">No topics that look like a flow&apos;s.</p>
                  )}
                  {others.length > 0 && (
                    <details className="text-sm">
                      <summary className="cursor-pointer text-muted-foreground">{others.length} internal and retry topics</summary>
                      <ul className="mt-2 flex flex-wrap gap-1.5">
                        {others.map(t => (
                          <li key={t.name}>
                            <button type="button" onClick={() => setLooking(t.name)}><Badge variant="secondary">{t.name}{t.kind === 'retry' ? ' (retry)' : ''}</Badge></button>
                          </li>
                        ))}
                      </ul>
                    </details>
                  )}
                </>
              )}
            </CardContent>
          </Card>

          {d && (
            <Card>
              <CardHeader>
                <CardTitle>Dead-letter queues</CardTitle>
                <CardDescription>{d.dlqs.length ? 'Why each looks like one, and who writes to it.' : 'None found by name, headers or writer.'}</CardDescription>
              </CardHeader>
              {d.dlqs.length > 0 && (
                <CardContent>
                  <ul data-testid="discover-dlqs" className="flex flex-col divide-y rounded-md border">
                    {d.dlqs.map(q => (
                      <li key={q.topic} className="flex flex-col gap-0.5 px-3 py-2.5 text-sm">
                        <span className="flex items-center justify-between gap-2">
                          <button type="button" className="font-medium underline-offset-4 hover:underline" onClick={() => setLooking(q.topic)}>{q.topic}</button>
                          {q.owner ? <Badge variant="outline">{q.owner}</Badge> : <Badge variant="lag">Owner unknown</Badge>}
                        </span>
                        <span className="text-xs text-muted-foreground">
                          {q.reasons.join('; ')}
                          {q.sources.length > 0 && `. From ${q.sources.join(', ')}`}
                        </span>
                      </li>
                    ))}
                  </ul>
                </CardContent>
              )}
            </Card>
          )}

          {d && (
            <Card>
              <CardHeader>
                <CardTitle>Consumer groups</CardTitle>
                <CardDescription>What each reads, and how far behind it is.</CardDescription>
              </CardHeader>
              <CardContent>
                {d.groups.length ? (
                  <Table data-testid="discover-groups">
                    <TableHeader>
                      <TableRow>
                        <TableHead>Group</TableHead>
                        <TableHead>State</TableHead>
                        <TableHead className="text-right">Members</TableHead>
                        <TableHead className="text-right">Lag</TableHead>
                        <TableHead>Reads</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {d.groups.map(g => (
                        <TableRow key={g.group}>
                          <TableCell className="font-medium">{g.group}</TableCell>
                          <TableCell className="text-muted-foreground">{g.state}</TableCell>
                          <TableCell className="figures text-right">{g.members}</TableCell>
                          <TableCell className={cn('figures text-right', g.lag > 0 && 'text-lag')}>{num(g.lag)}</TableCell>
                          <TableCell className="max-w-[280px] truncate text-xs text-muted-foreground" title={g.reads.join(', ')}>{g.reads.join(', ')}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                ) : (
                  <p className="py-4 text-center text-sm text-muted-foreground">No consumer groups.</p>
                )}
                {(d.subjects.length > 0 || d.registryProblem) && (
                  <p className={cn('mt-3 text-xs', d.registryProblem ? 'text-stuck' : 'text-muted-foreground')}>
                    {d.registryProblem ? `Schema registry: ${d.registryProblem}` : `Schema registry: ${d.subjects.length} subjects.`}
                  </p>
                )}
              </CardContent>
            </Card>
          )}
        </div>

        <div className="flex min-w-0 flex-col gap-4 xl:sticky xl:top-20 xl:self-start">
          <Card>
            <CardHeader>
              <CardTitle>Draft the catalog</CardTitle>
              <CardDescription>
                It samples the ticked topics and guesses which field is the money, the order and the customer, asking about whatever it is unsure of.
              </CardDescription>
            </CardHeader>
            <CardContent className="flex flex-col gap-3">
              <Field
                label="Flow name" htmlFor="flow-name" problem={nameProblem}
                help={status.flows.includes(name) ? `A flow named ${name} exists: drafting replaces its draft; the flow itself changes only when you activate.` : 'Lowercase and hyphens, such as checkout.'}
              >
                <Input id="flow-name" data-testid="flow-name" value={name} onChange={e => setName(e.target.value.trim())} placeholder="checkout" spellCheck={false} aria-invalid={!!nameProblem} />
              </Field>
              <p className="text-xs text-muted-foreground">
                {chosen.size ? `${chosen.size} topic${chosen.size === 1 ? '' : 's'}: ${[...chosen].join(', ')}` : 'No topics ticked.'}
              </p>
              <Button data-testid="draft-button" disabled={!d || !chosen.size || !name || !!nameProblem || draft.isPending} onClick={() => draft.mutate()}>
                {draft.isPending && <Loader2Icon className="animate-spin" />}{draft.isPending ? 'Sampling and drafting…' : 'Draft the catalog'}
              </Button>
              {draft.error && <p className="text-sm whitespace-pre-line text-stuck">{answerOf(draft.error)?.text ?? errorText(draft.error)}</p>}
            </CardContent>
          </Card>
          {looking ? <Sample key={looking} topic={looking} /> : d && <p className="px-1 text-sm text-muted-foreground">Select a topic to see the fields of its latest records.</p>}
        </div>
      </div>
    </div>
  );
}
