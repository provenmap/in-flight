// Intents: who cares about what, in the catalog's terms, and what a predicate would match. Typing a predicate compiles
// it as you type: its matches light up on the map, and an error names what the catalog does not have. This is the way
// in for someone who knows their business and not their topics.
import { useQuery } from '@tanstack/react-query';
import { Loader2Icon } from 'lucide-react';
import { useEffect, useState } from 'react';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Separator } from '@/components/ui/separator';
import { cn } from '@/lib/utils';
import type { Highlight } from '../map/FlowMap';
import { count, money } from '../model';
import type { Stats } from '../protocol';

interface Read {
  what: string;
  source: string;
  type: string;
}

interface Table {
  intent: string;
  predicate: string;
  keyedBy: string;
  topics: string[];
  matches: { event: string; topic: string; key: string; why: string[]; reads: Read[] }[];
  consumers?: string[];
  readerDistricts?: string[];
}

interface IntentsAnswer {
  intents: Table[];
  meaning: boolean;
  router: { intents?: Record<string, string>; [k: string]: unknown } | null;
}

interface Tailed {
  entity: string;
  event: string;
  at: string;
  from: string;
  late: boolean;
}

async function json<T>(res: Response): Promise<T> {
  const body = (await res.json().catch(() => ({}))) as T & { error?: string };
  if (!res.ok) throw new Error(body.error ?? `HTTP ${res.status}`);
  return body;
}

const time = new Intl.DateTimeFormat('en-US', { hour: 'numeric', minute: '2-digit', second: '2-digit' });

function Matches({ table }: { table: Table }) {
  return (
    <ul className="flex flex-col gap-2">
      {table.matches.map(m => (
        <li key={m.event} className="rounded-md border px-2.5 py-2 text-xs">
          <div className="flex items-center justify-between gap-2">
            <span className="font-medium text-foreground">{m.event}</span>
            <span className="text-muted-foreground">{m.topic}</span>
          </div>
          <p className="text-muted-foreground">{m.why.length ? m.why.join('; ') : 'matched'}</p>
          <p className="mt-1 text-muted-foreground">
            Reads {m.reads.map(r => `${r.what} ${r.source} (${r.type})`).join(', ')}
          </p>
        </li>
      ))}
    </ul>
  );
}

function Tail({ intent }: { intent: string }) {
  const tail = useQuery({
    queryKey: ['tail', intent],
    queryFn: async () => json<{ events: Tailed[] }>(await fetch(`/api/intents/${intent}/tail?n=8`)),
    refetchInterval: 3000,
  });
  const events = tail.data?.events ?? [];
  if (!events.length) return <p className="text-xs text-muted-foreground">Nothing on intent.{intent} yet: the router writes it.</p>;
  return (
    <ul className="flex flex-col gap-1 text-xs" data-testid="intent-tail">
      {events.slice().reverse().map((e, i) => (
        <li key={i} className="flex items-center justify-between gap-2">
          <span className="truncate"><span className="font-medium">{e.entity}</span> {e.event}</span>
          <span className="figures shrink-0 text-muted-foreground">{e.late ? 'late, ' : ''}{time.format(Date.parse(e.at))}</span>
        </li>
      ))}
    </ul>
  );
}

interface Props {
  stats: Stats | null;
  watching: boolean;
  onHighlight: (h: Highlight | null) => void;
  toast: (text: string) => void;
}

export function Intents({ stats, watching, onHighlight, toast }: Props) {
  const intents = useQuery({ queryKey: ['intents'], queryFn: async () => json<IntentsAnswer>(await fetch('/api/intents')), refetchInterval: 5000 });
  const [predicate, setPredicate] = useState('');
  const [name, setName] = useState('');
  const [explained, setExplained] = useState<{ table: Table | null; errors: string[] }>({ table: null, errors: [] });
  const [open, setOpen] = useState<string | null>(null);
  const [declaring, setDeclaring] = useState(false);

  // Compile as you type, a moment after the last keystroke.
  useEffect(() => {
    if (!predicate.trim()) {
      setExplained({ table: null, errors: [] });
      onHighlight(null);
      return;
    }
    const ctl = new AbortController();
    const t = setTimeout(async () => {
      try {
        const res = await fetch('/api/intents/explain', { method: 'POST', body: JSON.stringify({ predicate }), signal: ctl.signal, headers: { 'content-type': 'application/json' } });
        const body = (await res.json()) as Table & { errors?: string[]; error?: string };
        if (res.ok && !body.errors) {
          setExplained({ table: body, errors: [] });
          onHighlight({ topics: body.topics, label: predicate });
        } else {
          setExplained({ table: null, errors: body.errors ?? [body.error ?? `HTTP ${res.status}`] });
          onHighlight(null);
        }
      } catch {
        // superseded by the next keystroke
      }
    }, 250);
    return () => {
      clearTimeout(t);
      ctl.abort();
    };
  }, [predicate, onHighlight]);

  const declare = async () => {
    setDeclaring(true);
    try {
      await json(await fetch(`/api/intents/${encodeURIComponent(name)}`, { method: 'PUT', body: JSON.stringify({ predicate }), headers: { 'content-type': 'application/json' } }));
      toast(`Declared intent.${name}: the router is writing it now, from the events that already exist.`);
      setName('');
      void intents.refetch();
    } catch (e) {
      toast((e as Error).message);
    } finally {
      setDeclaring(false);
    }
  };

  const live = intents.data?.router?.intents ?? {};
  const declared = intents.data?.intents ?? [];
  return (
    <Card data-testid="intents-view">
      <CardHeader>
        <CardTitle>Intents</CardTitle>
        <CardDescription>Subscribe to what you care about, in your business's terms, not to topics.</CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        <div className="flex flex-col gap-2">
          <label htmlFor="predicate" className="text-sm font-medium">What would this match?</label>
          <Input
            id="predicate"
            data-testid="predicate"
            value={predicate}
            onChange={e => setPredicate(e.target.value)}
            placeholder="changes(Order.total) or emits(PaymentAuthorized)"
            className="font-mono text-xs"
            spellCheck={false}
          />
          <p className="text-xs text-muted-foreground">changes(Entity.attribute), emits(EventType), about(Entity), with and, or, not and parentheses.</p>
          {explained.errors.length > 0 && (
            <p data-testid="predicate-error" className="rounded-md bg-stuck/10 px-2.5 py-2 text-xs text-stuck">{explained.errors.join('; ')}</p>
          )}
          {explained.table && (
            <div data-testid="predicate-matches" className="flex flex-col gap-2">
              <p className="text-xs text-muted-foreground">
                {count(explained.table.matches.length, 'event version')}, keyed by {explained.table.keyedBy}, from {explained.table.topics.join(', ')}. Lit up on the map.
              </p>
              <Matches table={explained.table} />
              {!watching && (
                <div className="flex gap-2">
                  <Input value={name} onChange={e => setName(e.target.value)} placeholder="name, such as payment-decisions" className="text-xs" aria-label="Intent name" />
                  <Button size="sm" disabled={!/^[a-z0-9][a-z0-9-]*$/.test(name) || declaring} onClick={declare}>
                    {declaring && <Loader2Icon className="animate-spin" />}Declare
                  </Button>
                </div>
              )}
            </div>
          )}
        </div>
        <Separator />
        {!intents.data?.meaning && <p className="text-sm text-muted-foreground">This catalog does not say what its events mean, so it declares no intents.</p>}
        <ul className="flex flex-col gap-3">
          {declared.map(t => {
            const lag = stats?.intents?.[t.intent];
            const state = live[t.intent];
            return (
              <li key={t.intent} className="flex flex-col gap-1.5" data-intent={t.intent}>
                <button type="button" className="flex items-center justify-between gap-2 text-left" onClick={() => setOpen(open === t.intent ? null : t.intent)}>
                  <span className="min-w-0">
                    <span className="block text-sm font-medium">intent.{t.intent}</span>
                    <span className="block truncate font-mono text-xs text-muted-foreground">{t.predicate}</span>
                  </span>
                  <Badge variant={state === 'RUNNING' ? 'recovered' : state ? 'lag' : 'secondary'}>{state ? state.toLowerCase() : 'not routed'}</Badge>
                </button>
                <p className="text-xs text-muted-foreground">
                  Read by {t.consumers?.length ? t.consumers.join(', ') : 'nobody yet'}; keyed by {t.keyedBy}
                  {lag ? <>; <span className={cn(lag.events && 'text-lag')}>{count(lag.events, 'event')} behind, {money.format(lag.value)}</span></> : null}
                </p>
                {open === t.intent && <div className="flex flex-col gap-2"><Matches table={t} /><Tail intent={t.intent} /></div>}
              </li>
            );
          })}
        </ul>
      </CardContent>
    </Card>
  );
}
