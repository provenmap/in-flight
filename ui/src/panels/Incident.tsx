// What the catalog says about what is stuck: for each kind of dead letter, the intent its reader was reading by, and
// whether the catalog had predicted it. "Predicted by the impact of OrderPlaced@v2" reads very differently from "a
// version the catalog has never seen".
import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { count, money, type Model } from '../model';
import type { Stats } from '../protocol';

const verdict: Record<string, { label: string; variant: 'lag' | 'stuck' | 'secondary' }> = {
  predicted: { label: 'Predicted', variant: 'lag' },
  unpredicted: { label: 'Not predicted', variant: 'stuck' },
  unseen: { label: 'Never seen', variant: 'stuck' },
  unknown: { label: 'Unknown', variant: 'secondary' },
};

export function Incident({ model, stats }: { model: Model; stats: Stats | null }) {
  const groups = new Map<string, { event: string; intent: string | null; prediction: string; predicted: string; n: number; value: number; district: string }>();
  for (const b of model.pile) {
    if (b.greenAt !== null) continue;
    const key = `${b.district}|${b.event}`;
    const g = groups.get(key) ?? { event: b.event, intent: b.intent ?? null, prediction: b.prediction ?? 'unknown', predicted: b.predicted ?? '', n: 0, value: 0, district: b.district };
    g.n++;
    g.value += b.priced ? b.value : 0;
    groups.set(key, g);
  }
  const rows = [...groups.values()].sort((a, b) => b.n - a.n);
  return (
    <Card data-testid="incident-facts">
      <CardHeader>
        <CardTitle>What the catalog says</CardTitle>
        <CardDescription>{stats?.stuck.events ? 'Why each kind of stuck event is stuck, in the catalog\'s terms.' : 'Nothing is stuck.'}</CardDescription>
      </CardHeader>
      {rows.length > 0 && (
        <CardContent className="flex flex-col gap-3">
          {rows.map(r => (
            <div key={`${r.district}|${r.event}`} className="flex flex-col gap-1">
              <div className="flex items-center justify-between gap-2">
                <span className="text-sm font-medium">{r.event} at {r.district}</span>
                <Badge variant={verdict[r.prediction]?.variant ?? 'secondary'}>{verdict[r.prediction]?.label ?? r.prediction}</Badge>
              </div>
              <p className="text-xs text-muted-foreground">
                {count(r.n, 'event')}, {money.format(r.value)}. {r.intent ? `Its reader reads by intent ${r.intent}. ` : ''}{r.predicted}
              </p>
            </div>
          ))}
        </CardContent>
      )}
    </Card>
  );
}
