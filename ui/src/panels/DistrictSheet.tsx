// One district in detail: what it produces and reads, how its consumer group is doing, and what it is holding up.
import { Badge } from '@/components/ui/badge';
import { Separator } from '@/components/ui/separator';
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from '@/components/ui/sheet';
import { factsFor } from '../map/facts';
import { count, money, shown, type Model } from '../model';
import type { Flow, Stats } from '../protocol';

function Row({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="grid grid-cols-[140px_1fr] gap-3 py-2 text-sm">
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="min-w-0 break-words">{children}</dd>
    </div>
  );
}

const variant = { flow: 'flow', recovered: 'recovered', lag: 'lag', stuck: 'stuck', muted: 'secondary' } as const;

export function DistrictSheet({ id, onClose, flow, stats, model }: { id: string | null; onClose: () => void; flow: Flow | null; stats: Stats | null; model: Model }) {
  const d = flow?.districts.find(x => x.id === id);
  const dlq = flow?.dlqs.find(q => q.district === id);
  const produces = flow?.topics.filter(t => t.from === id) ?? [];
  const reads = flow?.topics.filter(t => t.to.includes(id ?? '')) ?? [];
  const group = flow?.groups.find(g => g.district === id)?.group;
  const lag = group && stats ? stats.lag.byGroup[group] : undefined;
  const received = id ? model.delivered.get(id) : undefined;
  const svc = id ? stats?.sim?.services[id] : undefined;
  const stuckBlocks = model.pile.filter(b => b.district === id && b.greenAt === null);
  const f = id ? factsFor(id, stats, model) : null;

  return (
    <Sheet open={!!d} onOpenChange={open => !open && onClose()}>
      <SheetContent>
        {d && f && (
          <>
            <SheetHeader>
              <div className="flex items-center gap-2">
                <SheetTitle className="text-lg">{d.name}</SheetTitle>
                <Badge variant={variant[f.tone]}>{f.status}</Badge>
              </div>
              <SheetDescription>{d.sink ? `Where the ${flow?.name} flow's money ends up.` : `A district of the ${flow?.name} flow.`}</SheetDescription>
            </SheetHeader>
            <div className="overflow-y-auto px-5 pb-6">
              <dl className="divide-y">
                {produces.length > 0 && <Row label="Produces">{produces.map(t => <div key={t.topic}>{t.topic} <span className="text-muted-foreground">({t.events.join(', ')})</span></div>)}</Row>}
                {reads.length > 0 && <Row label="Reads">{reads.map(t => t.topic).join(', ')}</Row>}
                {group && <Row label="Consumer group">{group}</Row>}
                {dlq && <Row label="Dead-letter queue">{dlq.dlq}</Row>}
                {dlq?.redrive && <Row label="Redrive group">{dlq.redrive}</Row>}
                {svc && <Row label="Processed"><span className="figures">{svc.processed.toLocaleString('en-US')}</span></Row>}
                {svc && <Row label="Dead-lettered"><span className="figures">{svc.deadLettered.toLocaleString('en-US')}</span></Row>}
                {lag && (
                  <Row label="Behind">
                    <span className="figures">{count(lag.events, 'event')}, {lag.estimated ? '≈ ' : ''}{money.format(lag.value)}</span>
                    {!!lag.estimated && <span className="block text-xs text-muted-foreground">incl. {money.format(lag.estimated)} estimated from recent averages</span>}
                  </Row>
                )}
                {dlq && (
                  <Row label="Stuck now">
                    <span className={stuckBlocks.length ? 'figures text-stuck' : 'figures'}>
                      {count(stuckBlocks.length, shown.case)}, {money.format(stats?.stuck.byDistrict[d.id] ?? 0)}
                    </span>
                  </Row>
                )}
                {d.sink && <Row label="Received"><span className="figures">{count(received?.orders ?? 0, shown.case)}, {money.format(received?.value ?? 0)}</span></Row>}
              </dl>
              {stuckBlocks.length > 0 && (
                <>
                  <Separator className="my-4" />
                  <p className="mb-2 text-sm font-medium">Why they are stuck</p>
                  <p className="rounded-md border border-stuck/30 bg-stuck/8 p-3 text-sm text-stuck">{stuckBlocks.at(-1)!.event}: {stuckBlocks.at(-1)!.error}</p>
                </>
              )}
            </div>
          </>
        )}
      </SheetContent>
    </Sheet>
  );
}
