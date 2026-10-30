// Label content, shared by both views: one label per district and one over the pile.
import { moneyShort, money, type Model } from './model';
import type { SimState, Stats } from './protocol';

export type Tone = 'bad' | 'warn' | 'good' | '';

export interface Label {
  name: string;
  lines: { text: string; tone?: Tone }[];
  error?: string | null;
  alarm?: boolean;
}

export interface Labels {
  districts: Record<string, Label>;
  pile: (Label & { district: string; count: number }) | null;
}

export const noLabels: Labels = { districts: {}, pile: null };

const isStuck = (s: Stats) => s.stuck.events > 0;

function lagOf(s: Stats, group: string): Label['lines'] {
  const l = s.lag.byGroup[group];
  return l && l.events ? [{ text: `lag ${l.events} · ${moneyShort.format(l.value)}`, tone: 'warn' }] : [];
}

export function labelsFor(s: Stats, sim: SimState | null, model: Model): Labels {
  const stuckAt = s.stuck.byDistrict;
  const districts: Record<string, Label> = {
    orders: {
      name: 'Orders',
      lines: sim
        ? [{ text: `OrderPlaced v${sim.orderVersion}`, tone: sim.orderVersion > 1 ? 'warn' : '' }, { text: `${sim.rate} orders/s` }]
        : [{ text: 'simulator not running', tone: 'bad' }],
    },
    payments: {
      name: 'Payments',
      alarm: !!stuckAt.payments,
      lines: [
        ...(sim ? [{ text: sim.patched ? 'reads v1 and v2' : 'reads v1 only', tone: (sim.patched ? 'good' : '') as Tone }] : []),
        ...(stuckAt.payments ? [{ text: `${moneyShort.format(stuckAt.payments)} stuck`, tone: 'bad' as Tone }] : []),
        ...lagOf(s, 'payments-service'),
      ],
    },
    inventory: {
      name: 'Inventory',
      alarm: !!stuckAt.inventory,
      lines: [{ text: 'reads items only' }, ...lagOf(s, 'inventory-service')],
    },
    shipping: {
      name: 'Shipping',
      alarm: !!stuckAt.shipping,
      lines: [
        ...(sim ? [{ text: `${sim.shippingWaiting} waiting for payment`, tone: (sim.shippingWaiting > 40 ? 'warn' : '') as Tone }] : []),
        ...lagOf(s, 'shipping-service'),
      ],
    },
    customers: {
      name: 'Customers',
      lines: [{ text: `${moneyShort.format(model.delivered.value)} dispatched`, tone: 'good' }, { text: 'since you opened this' }],
    },
  };
  let pile: Labels['pile'] = null;
  if (model.pile.length) {
    const district = model.pile[0]!.district;
    pile = {
      district,
      count: model.pile.length,
      name: isStuck(s)
        ? `${model.flow?.dlqs.find(q => q.district === district)?.dlq ?? 'dead letters'} · ${s.stuck.events.toLocaleString()} · ${money.format(s.stuck.value)}`
        : 'redriving',
      lines: [],
      error: isStuck(s) && model.lastError ? `${model.lastError.event}: ${model.lastError.error}` : null,
    };
  }
  return { districts, pile };
}
