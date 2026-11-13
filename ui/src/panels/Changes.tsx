// Changes: what a schema change will do, who must act first, and whether it is safe to ship. A proposal is worked out
// from the catalog before anything is produced; each consumer that must change gets its binding regenerated as a
// migration; the schema gate refuses the new version until every migration has merged and the change is approved.
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { GitMergeIcon, Loader2Icon, ShieldCheckIcon, ShieldXIcon } from 'lucide-react';
import { useEffect, useRef, useState } from 'react';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Separator } from '@/components/ui/separator';
import { cn } from '@/lib/utils';
import { count, money } from '../model';
import type { Stats } from '../protocol';

interface Migration {
  consumer: string;
  intent: string;
  bindingPath: string;
  branch: string;
  commit: string;
  pullRequest: string | null;
  merged: boolean;
  diff: string;
}

interface Change {
  id: string;
  revision: number;
  status: 'pending' | 'approved';
  headline: string;
  report: string;
  migrations: Migration[];
  unbound: string[];
  gate: { event: string; gate: 'refused' | 'accepted' | 'unknown' }[];
  allMerged: boolean;
}

async function call<T>(url: string, method = 'GET'): Promise<T> {
  const res = await fetch(url, { method });
  const body = (await res.json().catch(() => ({}))) as T & { error?: string };
  if (!res.ok) throw new Error(body.error ?? `HTTP ${res.status}`);
  return body;
}

/** A unified diff, its additions and removals coloured. */
function Diff({ text }: { text: string }) {
  const lines = text.split('\n').filter(l => !l.startsWith('diff --git') && !l.startsWith('index '));
  return (
    <pre className="max-h-64 overflow-auto rounded-md border bg-muted/40 p-2 font-mono text-[10.5px] leading-snug" data-testid="binding-diff">
      {lines.map((l, i) => (
        <div key={i} className={cn(l.startsWith('+') && !l.startsWith('+++') && 'text-recovered', l.startsWith('-') && !l.startsWith('---') && 'text-stuck', l.startsWith('@@') && 'text-muted-foreground')}>
          {l || ' '}
        </div>
      ))}
    </pre>
  );
}

/** What the two runs of the demo cost: the first, around the catalog, and the second, through it. */
export interface Runs {
  around: { value: number; events: number; tookMs: number } | null;
  through: { refused: number; shipped: number; stuck: number } | null;
}

/**
 * Remembers each run of the demo as it happens: an episode recovered while the orders team wrote straight to Kafka is
 * the first run; v2 shipped through the gate is the second.
 */
export function useRuns(stats: Stats | null): Runs {
  const [runs, setRuns] = useState<Runs>({ around: null, through: null });
  const base = useRef<{ refused: number; v2: number; stuck: number } | null>(null);
  const sim = stats?.sim;
  const via = sim?.ordersVia;
  const episode = stats?.lastEpisode;
  useEffect(() => {
    if (!stats || !sim) return;
    if (via !== 'proxy') {
      base.current = null;
      if (episode && (!runs.around || runs.around.tookMs !== episode.tookMs || runs.around.events !== episode.events)) setRuns(r => ({ ...r, around: episode }));
      return;
    }
    base.current ??= { refused: sim.refusedByGate ?? 0, v2: sim.placedV2 ?? 0, stuck: stats.stuck.events };
    const through = { refused: (sim.refusedByGate ?? 0) - base.current.refused, shipped: (sim.placedV2 ?? 0) - base.current.v2, stuck: Math.max(0, stats.stuck.events - base.current.stuck) };
    if (JSON.stringify(through) !== JSON.stringify(runs.through)) setRuns(r => ({ ...r, through }));
  }, [stats, sim, via, episode, runs]);
  return runs;
}

function TwoWays({ runs }: { runs: Runs }) {
  if (!runs.around && !runs.through) return null;
  return (
    <div className="grid gap-2 sm:grid-cols-2" data-testid="two-ways">
      <div className="rounded-md border px-3 py-2">
        <p className="text-xs font-medium text-muted-foreground">Around the catalog</p>
        {runs.around
          ? <p className="text-sm"><span className="figures font-semibold text-stuck">{money.format(runs.around.value)}</span> stuck, {count(runs.around.events, 'order')}, {(runs.around.tookMs / 1000).toFixed(0)} s to recover</p>
          : <p className="text-sm text-muted-foreground">Not run yet</p>}
      </div>
      <div className="rounded-md border px-3 py-2">
        <p className="text-xs font-medium text-muted-foreground">Through the catalog</p>
        {runs.through
          ? <p className="text-sm"><span className="figures font-semibold text-recovered">{money.format(0)}</span> stuck{runs.through.stuck ? `, but ${count(runs.through.stuck, 'order')} held up` : ''}; {count(runs.through.refused, 'v2 order')} held at the gate, {count(runs.through.shipped, 'v2 order')} shipped once approved</p>
          : <p className="text-sm text-muted-foreground">Not run yet</p>}
      </div>
    </div>
  );
}

interface Props {
  stats: Stats | null;
  watching: boolean;
  toast: (text: string) => void;
}

export function Changes({ stats, watching, toast }: Props) {
  const client = useQueryClient();
  const changes = useQuery({ queryKey: ['changes'], queryFn: () => call<{ changes: Change[] }>('/api/changes'), refetchInterval: 3000, retry: false });
  const [busy, setBusy] = useState<string | null>(null);
  const runs = useRuns(stats);
  const sim = stats?.sim;

  const act = async (key: string, url: string, method = 'POST', done?: (body: { text?: string; refused?: boolean; approved?: boolean }) => void) => {
    setBusy(key);
    try {
      const body = await call<{ text?: string; refused?: boolean; approved?: boolean }>(url, method);
      done?.(body);
      await client.invalidateQueries({ queryKey: ['changes'] });
    } catch (e) {
      toast((e as Error).message);
    } finally {
      setBusy(null);
    }
  };

  const list = changes.data?.changes ?? [];
  const pending = list.filter(c => c.status === 'pending');
  return (
    <Card data-testid="changes-view">
      <CardHeader>
        <CardTitle>Changes</CardTitle>
        <CardDescription>What a schema change will do, who must change first, and whether it can ship.</CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        <TwoWays runs={runs} />
        {changes.isError && <p className="text-sm text-muted-foreground">{(changes.error as Error).message}.</p>}
        {sim && !sim.proxy && <p className="text-sm text-muted-foreground">There is no proxy, so no gate: proposals need the router and the registry. Run the demo for the whole loop.</p>}
        {!watching && (
          <div className="flex flex-wrap items-center gap-2">
            <Button
              size="sm" data-testid="propose" disabled={!!busy || pending.length > 0}
              onClick={() => act('propose', '/api/changes', 'POST', b => toast(b.refused ? 'The proposal is refused: see why below.' : 'Proposed. The gate refuses the new version until it is approved.'))}
            >
              {busy === 'propose' && <Loader2Icon className="animate-spin" />}Propose the catalog's next revision
            </Button>
            {sim?.proxy && (
              <Button
                size="sm" variant="outline" data-testid="gate" disabled={!!busy}
                onClick={() => act('gate', `/api/sim/gate?on=${sim.ordersVia !== 'proxy'}`)}
              >
                {sim.ordersVia === 'proxy' ? <ShieldCheckIcon /> : <ShieldXIcon />}
                {sim.ordersVia === 'proxy' ? 'Orders ship through the gate' : 'Orders ship straight to Kafka'}
              </Button>
            )}
          </div>
        )}
        {!list.length && !changes.isError && <p className="text-sm text-muted-foreground">Nothing proposed. A proposal works out who the change reaches before a single event is produced.</p>}
        {list.slice().reverse().map(c => (
          <div key={c.id} className="flex flex-col gap-3" data-change={c.id}>
            <Separator />
            <div className="flex items-center justify-between gap-2">
              <span className="text-sm font-semibold">{c.id}: catalog revision {c.revision}</span>
              <Badge variant={c.status === 'approved' ? 'recovered' : 'lag'}>{c.status}</Badge>
            </div>
            <p className="text-sm">{c.headline}</p>
            <ul className="flex flex-col gap-1 text-xs">
              {c.gate.map(g => (
                <li key={g.event} className="flex items-center justify-between">
                  <span>{g.event}</span>
                  <Badge variant={g.gate === 'refused' ? 'stuck' : g.gate === 'accepted' ? 'recovered' : 'secondary'}>
                    {g.gate === 'refused' ? 'the gate refuses it' : g.gate === 'accepted' ? 'the gate lets it through' : 'gate unknown'}
                  </Badge>
                </li>
              ))}
            </ul>
            {c.migrations.map(m => (
              <div key={m.consumer} className="flex flex-col gap-1.5 rounded-md border p-2.5">
                <div className="flex items-center justify-between gap-2">
                  <span className="text-sm font-medium">{m.consumer} must change</span>
                  <Badge variant={m.merged ? 'recovered' : 'lag'}>{m.merged ? 'merged' : 'open'}</Badge>
                </div>
                <p className="text-xs text-muted-foreground">
                  Reads by {m.intent}. Its regenerated binding, {m.pullRequest ? <a className="underline" href={m.pullRequest}>pull request</a> : <>branch <code>{m.branch}</code></>}:
                </p>
                <Diff text={m.diff} />
              </div>
            ))}
            {c.unbound.length > 0 && <p className="text-xs text-muted-foreground">Also reached: {c.unbound.join(', ')}, with no binding to regenerate.</p>}
            {c.status === 'pending' && !watching && (
              <div className="flex flex-wrap gap-2">
                <Button
                  size="sm" variant="outline" data-testid="merge" disabled={!!busy || c.allMerged}
                  onClick={() => act(`merge-${c.id}`, `/api/changes/${c.id}/merge`, 'POST', () => toast('Merged and deployed: payments now reads through its regenerated binding.'))}
                >
                  {busy === `merge-${c.id}` ? <Loader2Icon className="animate-spin" /> : <GitMergeIcon />}Merge the migrations (demo)
                </Button>
                <Button
                  size="sm" data-testid="approve" disabled={!!busy || !c.allMerged}
                  onClick={() => act(`approve-${c.id}`, `/api/changes/${c.id}/approve`, 'POST', b => toast(b.approved ? 'Approved: the gate lets the new version through.' : (b.text ?? 'Not approved.')))}
                >
                  {busy === `approve-${c.id}` && <Loader2Icon className="animate-spin" />}Approve
                </Button>
              </div>
            )}
            <details className="text-xs">
              <summary className="cursor-pointer text-muted-foreground">The impact report</summary>
              <pre className="mt-2 overflow-auto rounded-md bg-muted/40 p-2 font-mono text-[10.5px] leading-snug">{c.report}</pre>
            </details>
          </div>
        ))}
      </CardContent>
    </Card>
  );
}
