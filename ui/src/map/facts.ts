// What each district's card on the map says, from the latest stats, and in the demo the simulator's state too.
import { count, moneyShort, shown, type Model } from '../model';
import type { SimState, Stats } from '../protocol';

export type Tone = 'flow' | 'recovered' | 'lag' | 'stuck' | 'muted';

export interface Facts {
  status: string;
  tone: Tone;
  lines: { text: string; tone?: Tone }[];
}

type Line = Facts['lines'][number];

/** Money a minute on the topics a district produces, or on the ones it reads. */
function perMinute(model: Model, id: string, way: 'out' | 'in'): number {
  const now = performance.now();
  const topics = model.flow?.topics.filter(t => (way === 'out' ? t.from === id : t.to.includes(id))) ?? [];
  return topics.reduce((sum, t) => sum + model.rate(t.topic, now, 'topic').perMinute, 0);
}

export function factsFor(id: string, s: Stats | null, model: Model): Facts {
  const groups = model.flow?.groups.filter(g => g.district === id).map(g => s?.lag.byGroup[g.group]).filter(l => !!l) ?? [];
  const behind = groups.reduce((n, l) => n + l.events, 0);
  const behindValue = groups.reduce((n, l) => n + l.value, 0);
  const stuck = s?.stuck.byDistrict[id] ?? 0;
  const stuckEvents = model.pile.filter(b => b.district === id && b.greenAt === null).length;
  const lagLine: Line[] = behind ? [{ text: `${count(behind, shown.case)} behind, ${moneyShort.format(behindValue)}`, tone: 'lag' }] : [];
  const state = (healthy: string): Pick<Facts, 'status' | 'tone'> =>
    stuck || stuckEvents ? { status: 'Dead-lettering', tone: 'stuck' } : behind ? { status: 'Lagging', tone: 'lag' } : { status: healthy, tone: 'recovered' };
  const received = model.delivered.get(id)?.value ?? 0;

  const sim = model.mode === 'demo' ? (s?.sim ?? null) : null;
  if (model.mode === 'demo') {
    const demo = demoFacts(id, sim, stuck, lagLine, state, received);
    if (demo) return demo;
  }

  const district = model.flow?.districts.find(d => d.id === id);
  if (district?.sink) return { status: 'Receiving', tone: 'flow', lines: [{ text: `${moneyShort.format(received)} received` }, { text: 'this session' }] };
  const reads = !!model.flow?.topics.some(t => t.to.includes(id));
  const out = perMinute(model, id, 'out');
  if (!reads) {
    return out
      ? { status: 'Producing', tone: 'recovered', lines: [{ text: `${moneyShort.format(out)}/min out` }] }
      : { status: 'Quiet', tone: 'muted', lines: [{ text: 'Nothing produced lately' }] };
  }
  const stuckLine: Line[] = stuckEvents
    ? [{ text: `${count(stuckEvents, shown.case)} stuck, ${moneyShort.format(stuck)}`, tone: 'stuck' }]
    : lagLine;
  return { ...state(out ? 'Healthy' : 'Reading'), lines: [{ text: `${moneyShort.format(perMinute(model, id, 'in'))}/min in` }, ...stuckLine] };
}

/** The order-to-cash demo's own words for its five districts. */
function demoFacts(id: string, sim: SimState | null, stuck: number, lagLine: Line[], state: (healthy: string) => Pick<Facts, 'status' | 'tone'>, received: number): Facts | null {
  switch (id) {
    case 'orders':
      if (!sim) return { status: 'Not running', tone: 'muted', lines: [{ text: 'Start the simulator to place orders' }] };
      return {
        status: sim.orderVersion > 1 ? 'New schema' : 'Placing',
        tone: sim.orderVersion > 1 ? 'lag' : 'recovered',
        lines: [{ text: `Ships OrderPlaced v${sim.orderVersion}`, tone: sim.orderVersion > 1 ? 'lag' : undefined }, { text: `${sim.rate} orders a second` }],
      };
    case 'payments':
      return {
        ...state('Charging'),
        lines: [
          ...(sim ? [{ text: sim.patched ? 'Reads v1 and v2' : 'Reads v1 only', tone: (sim.patched ? 'recovered' : undefined) as Tone | undefined }] : []),
          ...(stuck ? [{ text: `${moneyShort.format(stuck)} stuck in its DLQ`, tone: 'stuck' as Tone }] : lagLine),
        ],
      };
    case 'inventory':
      return { ...state('Reserving'), lines: [{ text: 'Reads the items only' }, ...lagLine] };
    case 'shipping':
      return {
        ...state('Dispatching'),
        lines: [...(sim ? [{ text: `${sim.shippingWaiting.toLocaleString('en-US')} awaiting payment`, tone: (sim.shippingWaiting > 40 ? 'lag' : undefined) as Tone | undefined }] : []), ...lagLine],
      };
    case 'customers':
      return { status: 'Receiving', tone: 'flow', lines: [{ text: `${moneyShort.format(received)} dispatched` }, { text: 'this session' }] };
    default:
      return null;
  }
}
