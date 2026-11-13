import { describe, expect, test } from 'vitest';
import { showRef } from '../pricing/paths.js';
import { defaultCatalog } from '../pricing/catalog.js';
import { raw } from './fixture.test-data.js';
import { consumersOf, impactLine } from './describe.js';
import { analyse, impactOf, report } from './impact.js';
import { parseSemantics, type Semantics } from './semantics.js';

function semantics(edit: (r: ReturnType<typeof raw>) => void = () => {}): Semantics {
  const r = raw();
  edit(r);
  const res = parseSemantics(r);
  if (!res.ok || !res.value) throw new Error(res.ok ? 'no semantics' : res.error.join('; '));
  return res.value;
}

const readers = (intent: string) => (intent === 'order-total-changes' ? [{ name: 'revenue', dlq: 'revenue.dlq' }] : []);

describe('the impact of a change', () => {
  test('the demo: OrderPlaced v2 breaks payments and nobody else, said before an event is produced', () => {
    const catalog = defaultCatalog();
    const impact = impactOf({ name: 'OrderPlaced', version: 2 }, catalog.semantics!, consumersOf(catalog));
    if (typeof impact === 'string') throw new Error(impact);

    expect(impact.breaks).toEqual([{
      consumer: 'payments-service', dlq: 'payments.dlq', intent: 'order-total-changes', event: { name: 'OrderPlaced', version: 2 },
      arrival: 'new-reads', differences: ['Order.total: from total (decimal) -> from total.amount (decimal)'],
    }]);
    // Inventory is sent v2 as well, and reads it the way it reads v1.
    expect(impact.intents.find(i => i.intent === 'orders-placed')!.arrivals).toMatchObject([{ arrival: 'same-reads', previous: { name: 'OrderPlaced', version: 1 } }]);
    expect(impact.unaffected).toEqual(['order-dispatched', 'ready-to-ship']);
    expect(impactLine(impact)).toBe('payments-service must change before this ships, or its events pile up in payments.dlq.');
    expect(report(impact)).toContain('! payments-service (order-total-changes) reads OrderPlaced@v2 differently; until then every OrderPlaced@v2 it meets goes to payments.dlq');
  });

  test('a new event type that reaches an intent is one its consumers have no code for', () => {
    const after = semantics(r => {
      r.entities.Invoice = { balance: 'decimal' };
      r.events.push({
        ref: { name: 'InvoiceAdjusted', version: 1 }, topic: 'billing.invoices',
        fields: { invoiceId: 'string', orderId: 'string', delta: 'decimal' }, keys: { Order: 'orderId', Invoice: 'invoiceId' },
        effects: [{ op: 'add', attribute: 'Order.total', from: 'delta' }, { op: 'add', attribute: 'Invoice.balance', from: 'delta' }],
      });
    });
    const impact = analyse(semantics().model, after, readers);
    expect(impact.changes).toEqual(['new event type InvoiceAdjusted@v1 on billing.invoices', 'new attribute Invoice.balance: decimal']);
    expect(impact.breaks).toMatchObject([{ consumer: 'revenue', arrival: 'new-type', event: { name: 'InvoiceAdjusted', version: 1 } }]);
    expect(report(impact)).toContain('+ InvoiceAdjusted@v1 from billing.invoices, a type its consumers have never read: add Order.total from delta');
  });

  test('a new version read the same way is safe', () => {
    const after = semantics(r => {
      r.events.push({ ...r.events[1]!, ref: { name: 'OrderAmended', version: 2 }, fields: { orderId: 'string', newTotal: 'decimal', reason: 'string?' } });
    });
    const impact = analyse(semantics().model, after, readers);
    expect(impact.breaks).toEqual([]);
    expect(impact.intents[0]!.arrivals.map(a => [showRef(a.route.event), a.arrival])).toEqual([['OrderAmended@v2', 'same-reads']]);
    expect(impactLine(impact)).toBe('Safe to ship: it reaches revenue, and each reads it the way it reads what it gets today.');
  });

  test('an event that stops matching is reported', () => {
    const impact = analyse(semantics().model, semantics(r => { r.events.splice(2, 1); }), readers);
    expect(impact.changes).toEqual(['removed RefundIssued@v1']);
    expect(impact.intents[0]!.removed.map(r => showRef(r.event))).toEqual(['RefundIssued@v1']);
    expect(impact.breaks).toEqual([]);
  });

  test('a published version edited in place is refused', () => {
    const after = semantics(r => {
      r.events[1] = { ...r.events[1]!, fields: { orderId: 'string', newTotal: 'decimal', by: 'string' } };
    });
    const impact = analyse(semantics().model, after, readers);
    expect(impact.errors).toEqual(['OrderAmended@v1 is already published and cannot change (fields changed); add version 2 instead']);
    expect(impactLine(impact)).toMatch(/^Blocked: OrderAmended@v1 is already published/);
  });

  test('an event the catalog gives no meaning to has no impact to work out', () => {
    expect(impactOf({ name: 'Heartbeat', version: 1 }, semantics(), readers)).toMatch(/^Heartbeat@v1 is not in the catalog, or the catalog does not say what it means/);
  });
});
