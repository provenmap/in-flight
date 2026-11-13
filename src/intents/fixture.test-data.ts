import type { RawSemantics } from './semantics.js';

/** Orders whose total is set by one event, amended by another and reduced by a refund; shipments keyed by two entities. */
export const raw = (): RawSemantics & { entities: Record<string, Record<string, string>>; intents: { name: string; predicate: string; by?: string }[] } => ({
  entities: { Order: { total: 'decimal', status: 'string' }, Shipment: { carrier: 'string' } },
  intents: [{ name: 'order-total-changes', predicate: 'changes(Order.total)' }],
  events: [
    {
      ref: { name: 'OrderPlaced', version: 1 }, topic: 'orders.placed',
      fields: { orderId: 'string', total: 'decimal', note: 'string?' }, keys: { Order: 'orderId' },
      effects: [{ op: 'assign', attribute: 'Order.total', from: 'total' }, { op: 'assign', attribute: 'Order.status', value: 'placed' }],
    },
    {
      ref: { name: 'OrderAmended', version: 1 }, topic: 'orders.amended',
      fields: { orderId: 'string', newTotal: 'decimal' }, keys: { Order: 'orderId' },
      effects: [{ op: 'assign', attribute: 'Order.total', from: 'newTotal' }],
    },
    {
      ref: { name: 'RefundIssued', version: 1 }, topic: 'billing.refunds',
      fields: { refund: { orderId: 'string', amount: 'decimal' } }, keys: { Order: 'refund.orderId' },
      effects: [{ op: 'subtract', attribute: 'Order.total', from: 'refund.amount' }],
    },
    {
      ref: { name: 'OrderShipped', version: 1 }, topic: 'orders.shipped',
      fields: { orderId: 'string', shipmentId: 'string', carrier: 'string' }, keys: { Order: 'orderId', Shipment: 'shipmentId' },
      effects: [{ op: 'assign', attribute: 'Order.status', value: 'shipped' }, { op: 'assign', attribute: 'Shipment.carrier', from: 'carrier' }],
    },
    { ref: { name: 'Heartbeat', version: 1 }, topic: 'ops.heartbeat' },
  ],
});
