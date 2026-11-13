// The details behind the counter: every stuck order, every consumer group, the live event stream, and throughput.
import { DownloadIcon, PauseIcon, PlayIcon, SearchIcon } from 'lucide-react';
import { useMemo, useState } from 'react';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { ToggleGroup, ToggleGroupItem } from '@/components/ui/toggle-group';
import { cn } from '@/lib/utils';
import { money, moneyShort, type Model, type Recent } from '../model';
import type { Flow, Stats } from '../protocol';

const time = new Intl.DateTimeFormat('en-US', { hour: 'numeric', minute: '2-digit', second: '2-digit' });
const num = new Intl.NumberFormat('en-US', { maximumFractionDigits: 1 });

function Empty({ children }: { children: React.ReactNode }) {
  return <p className="px-2 py-10 text-center text-sm text-muted-foreground">{children}</p>;
}

function csv(rows: string[][]): string {
  return rows.map(r => r.map(c => (/[",\n]/.test(c) ? `"${c.replace(/"/g, '""')}"` : c)).join(',')).join('\n');
}

function download(name: string, text: string) {
  const url = URL.createObjectURL(new Blob([text], { type: 'text/csv' }));
  const a = Object.assign(document.createElement('a'), { href: url, download: name });
  a.click();
  URL.revokeObjectURL(url);
}

function DeadLetters({ model, stats }: { model: Model; stats: Stats | null }) {
  const [query, setQuery] = useState('');
  const [order, setOrder] = useState<'newest' | 'largest'>('newest');
  const [shown, setShown] = useState(50);
  const rows = useMemo(() => {
    const q = query.trim().toLowerCase();
    const found = model.pile.filter(b => !q || [b.orderId, b.customer, b.error, b.event].some(s => s.toLowerCase().includes(q)));
    return order === 'largest' ? found.sort((a, b) => b.value - a.value) : found.reverse();
    // The pile changes in place; stats arrive with every change worth showing.
  }, [model.pile, query, order, stats]); // eslint-disable-line react-hooks/exhaustive-deps
  const total = stats?.stuck.events ?? 0;

  if (!model.pile.length) {
    return <Empty>{model.mode === 'demo' ? 'No dead letters. Ship OrderPlaced v2 in the runbook to see what a schema change does to payments.' : 'No dead letters. Every event is getting through.'}</Empty>;
  }
  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-wrap items-center gap-2">
        <div className="relative w-full sm:w-72">
          <SearchIcon className="pointer-events-none absolute top-2.5 left-2.5 size-4 text-muted-foreground" />
          <Input value={query} onChange={e => setQuery(e.target.value)} placeholder="Search order, customer or error" className="pl-8" aria-label="Search dead letters" />
        </div>
        <ToggleGroup type="single" value={order} onValueChange={v => v && setOrder(v as typeof order)} aria-label="Sort">
          <ToggleGroupItem value="newest">Newest</ToggleGroupItem>
          <ToggleGroupItem value="largest">Largest</ToggleGroupItem>
        </ToggleGroup>
        <Button
          variant="outline" size="sm" className="ml-auto"
          onClick={() => download('dead-letters.csv', csv([
            ['failed_at', 'dlq', 'order_id', 'customer', 'event', 'value', 'error'],
            ...rows.map(b => [b.at, b.dlq, b.orderId, b.customer, b.event, b.priced ? b.value.toFixed(2) : '', b.error]),
          ]))}
        >
          <DownloadIcon />Export CSV
        </Button>
      </div>
      <Table data-testid="dead-letters">
        <TableHeader>
          <TableRow>
            <TableHead>Failed</TableHead>
            <TableHead>Order</TableHead>
            <TableHead>Customer</TableHead>
            <TableHead>Event</TableHead>
            <TableHead>Intent</TableHead>
            <TableHead className="text-right">Value</TableHead>
            <TableHead>Error</TableHead>
            <TableHead>Status</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.slice(0, shown).map(b => (
            <TableRow key={b.id}>
              <TableCell className="figures text-muted-foreground">{time.format(Date.parse(b.at))}</TableCell>
              <TableCell className="font-medium">{b.orderId || '—'}</TableCell>
              <TableCell>{b.customer || '—'}</TableCell>
              <TableCell title={b.predicted}>{b.event}</TableCell>
              <TableCell className="text-muted-foreground">{b.intent ?? '—'}</TableCell>
              <TableCell className="figures text-right">{b.priced ? money.format(b.value) : <span className="text-muted-foreground">Unknown</span>}</TableCell>
              <TableCell className="max-w-[320px] truncate text-muted-foreground" title={b.error}>{b.error}</TableCell>
              <TableCell>{b.greenAt !== null ? <Badge variant="recovered">Recovered</Badge> : <Badge variant="stuck">Stuck</Badge>}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
      <div className="flex items-center justify-between text-xs text-muted-foreground">
        <span>
          Showing {Math.min(shown, rows.length).toLocaleString('en-US')} of {rows.length.toLocaleString('en-US')}
          {total > model.pile.length ? `, the newest of ${total.toLocaleString('en-US')} stuck` : ''}
        </span>
        {rows.length > shown && <Button variant="ghost" size="sm" onClick={() => setShown(s => s + 100)}>Show 100 more</Button>}
      </div>
    </div>
  );
}

const groupStatus = (events: number, dead: number, stuck: number) =>
  stuck ? <Badge variant="stuck">Dead-lettering</Badge> : events ? <Badge variant="lag">Lagging</Badge> : dead ? <Badge variant="secondary">Recovered</Badge> : <Badge variant="recovered">Healthy</Badge>;

function Groups({ flow, stats }: { flow: Flow | null; stats: Stats | null }) {
  if (!flow || !stats) return <Empty>Waiting for the observer.</Empty>;
  return (
    <Table data-testid="groups">
      <TableHeader>
        <TableRow>
          <TableHead>Consumer group</TableHead>
          <TableHead>Reads</TableHead>
          <TableHead className="text-right">Processed</TableHead>
          <TableHead className="text-right">Dead-lettered</TableHead>
          <TableHead className="text-right">Behind</TableHead>
          <TableHead className="text-right">Value behind</TableHead>
          <TableHead>Status</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {flow.dlqs.map(q => {
          const lag = stats.lag.byGroup[q.group] ?? { events: 0, value: 0, customers: 0 };
          const svc = stats.sim?.services[q.district];
          const reads = flow.topics.filter(t => t.to.includes(q.district)).map(t => t.topic);
          return (
            <TableRow key={q.group}>
              <TableCell className="font-medium">{q.group}</TableCell>
              <TableCell className="text-muted-foreground">{reads.join(', ')}</TableCell>
              <TableCell className="figures text-right">{svc ? svc.processed.toLocaleString('en-US') : '–'}</TableCell>
              <TableCell className={cn('figures text-right', svc?.deadLettered && 'text-stuck')}>{svc ? svc.deadLettered.toLocaleString('en-US') : '–'}</TableCell>
              <TableCell className={cn('figures text-right', lag.events && 'text-lag')}>{lag.events.toLocaleString('en-US')}</TableCell>
              <TableCell className="figures text-right">{money.format(lag.value)}</TableCell>
              <TableCell>{groupStatus(lag.events, svc?.deadLettered ?? 0, stats.stuck.byDistrict[q.district] ?? 0)}</TableCell>
            </TableRow>
          );
        })}
      </TableBody>
    </Table>
  );
}

function LiveEvents({ model, stats }: { model: Model; stats: Stats | null }) {
  const [paused, setPaused] = useState<Recent[] | null>(null);
  const rows = (paused ?? model.recent).slice(0, 60);
  void stats; // re-rendered with each stats message
  return (
    <div className="flex flex-col gap-3">
      <div className="flex items-center justify-between gap-2">
        <p className="text-xs text-muted-foreground">{paused ? 'Paused. New events are still counted.' : 'The newest 60 events, as the observer reads them.'}</p>
        <Button variant="outline" size="sm" onClick={() => setPaused(p => (p ? null : [...model.recent]))}>
          {paused ? <><PlayIcon />Resume</> : <><PauseIcon />Pause</>}
        </Button>
      </div>
      {rows.length ? (
        <Table data-testid="live-events">
          <TableHeader>
            <TableRow>
              <TableHead>Time</TableHead>
              <TableHead>Event</TableHead>
              <TableHead>Topic</TableHead>
              <TableHead>Order</TableHead>
              <TableHead>Customer</TableHead>
              <TableHead className="text-right">Value</TableHead>
              <TableHead />
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map(e => (
              <TableRow key={`${e.id}-${e.topic}`}>
                <TableCell className="figures text-muted-foreground">{time.format(e.at)}</TableCell>
                <TableCell className="font-medium">{e.event}</TableCell>
                <TableCell className="text-muted-foreground">{e.topic}</TableCell>
                <TableCell>{e.orderId}</TableCell>
                <TableCell>{e.customer}</TableCell>
                <TableCell className="figures text-right">{money.format(e.value)}</TableCell>
                <TableCell>
                  {e.redrive ? <Badge variant="recovered">Redriven</Badge> : e.to.length === 0 ? <Badge variant="secondary">Declined</Badge> : null}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      ) : (
        <Empty>No events yet. Start the simulator and orders will flow.</Empty>
      )}
    </div>
  );
}

function Throughput({ model, flow, stats }: { model: Model; flow: Flow | null; stats: Stats | null }) {
  if (!flow) return <Empty>Waiting for the observer.</Empty>;
  void stats;
  const now = performance.now();
  const names = Object.fromEntries(flow.districts.map(d => [d.id, d.name]));
  return (
    <Table data-testid="throughput">
      <TableHeader>
        <TableRow>
          <TableHead>Topic</TableHead>
          <TableHead>From</TableHead>
          <TableHead>Read by</TableHead>
          <TableHead className="text-right">Events a second</TableHead>
          <TableHead className="text-right">Money a minute</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {flow.topics.map(t => {
          const r = model.rate(t.topic, now, 'topic');
          return (
            <TableRow key={t.topic}>
              <TableCell className="font-medium">{t.topic}</TableCell>
              <TableCell>{names[t.from] ?? t.from}</TableCell>
              <TableCell className="text-muted-foreground">{t.to.length ? t.to.map(d => names[d] ?? d).join(', ') : 'Nobody'}</TableCell>
              <TableCell className="figures text-right">{num.format(r.perSecond)}</TableCell>
              <TableCell className="figures text-right">{moneyShort.format(r.perMinute)}</TableCell>
            </TableRow>
          );
        })}
      </TableBody>
    </Table>
  );
}

export function DataTabs({ model, flow, stats }: { model: Model; flow: Flow | null; stats: Stats | null }) {
  const stuck = stats?.stuck.events ?? 0;
  return (
    <Card className="py-4">
      <CardContent className="px-4">
        <Tabs defaultValue="dead-letters">
          <div className="-mx-1 overflow-x-auto px-1">
            <TabsList>
              <TabsTrigger value="dead-letters">
                Dead letters
                {stuck > 0 && <Badge variant="stuck" className="figures px-1.5 py-0">{stuck.toLocaleString('en-US')}</Badge>}
              </TabsTrigger>
              <TabsTrigger value="groups">Consumer groups</TabsTrigger>
              <TabsTrigger value="events">Live events</TabsTrigger>
              <TabsTrigger value="throughput">Throughput</TabsTrigger>
            </TabsList>
          </div>
          <TabsContent value="dead-letters"><DeadLetters model={model} stats={stats} /></TabsContent>
          <TabsContent value="groups"><Groups flow={flow} stats={stats} /></TabsContent>
          <TabsContent value="events"><LiveEvents model={model} stats={stats} /></TabsContent>
          <TabsContent value="throughput"><Throughput model={model} flow={flow} stats={stats} /></TabsContent>
        </Tabs>
      </CardContent>
    </Card>
  );
}
