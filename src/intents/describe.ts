import { showRef } from '../pricing/paths.js';
import type { Catalog } from '../pricing/catalog.js';
import type { RoutingTable } from './compiler.js';
import type { Impact, IntentConsumer } from './impact.js';

/** The consumers that read by an intent, with the queue their failures land in. */
export const consumersOf = (catalog: Catalog) => (intent: string): IntentConsumer[] =>
  catalog.consumersOf(intent).map(k => (k.dlq ? { name: k.name, dlq: k.dlq } : { name: k.name }));

export const noSemantics =
  'This catalog does not say what its events mean. Give it "entities", and each event "fields", "keys" and "effects", to declare intents over it.';

/** One compiled intent, in lines: what it matches, why, and what its consumers read from each match. */
export function describeTable(table: RoutingTable, consumers: IntentConsumer[] | null): string {
  const lines = [`${table.intent}: ${table.predicate}`, `  keyed by ${table.entity}, from ${table.sourceTopics.join(', ')}`];
  for (const r of table.routes) {
    lines.push(`  ${showRef(r.event)} on ${r.topic}${r.reasons.length ? `: ${r.reasons.join('; ')}` : ''}`);
    for (const read of r.reads) lines.push(`      reads ${read.what} ${read.source} (${read.type})`);
  }
  if (consumers) lines.push(`  consumers: ${consumers.length ? consumers.map(c => c.name).join(', ') : 'none declared'}`);
  return lines.join('\n');
}

export const tableJson = (table: RoutingTable, consumers: IntentConsumer[] | null) => ({
  intent: table.intent,
  predicate: table.predicate,
  keyedBy: table.entity,
  topics: table.sourceTopics,
  matches: table.routes.map(r => ({ event: showRef(r.event), topic: r.topic, key: r.keyPath.show, why: r.reasons, reads: r.reads })),
  ...(consumers ? { consumers: consumers.map(c => c.name) } : {}),
});

/** The headline of an impact: who must change first, or that nobody must. */
export function impactLine(impact: Impact): string {
  if (impact.errors.length) return `Blocked: ${impact.errors[0]}${impact.errors.length > 1 ? ` (and ${impact.errors.length - 1} more)` : ''}`;
  if (!impact.breaks.length) {
    const reached = impact.intents.flatMap(i => i.consumers.map(c => c.name));
    return reached.length
      ? `Safe to ship: it reaches ${[...new Set(reached)].join(', ')}, and each reads it the way it reads what it gets today.`
      : 'Safe to ship: no declared consumer is reached by it.';
  }
  const who = [...new Set(impact.breaks.map(b => b.consumer))];
  const queues = [...new Set(impact.breaks.flatMap(b => (b.dlq ? [b.dlq] : [])))];
  return `${who.join(', ')} must change before this ships${queues.length ? `, or its events pile up in ${queues.join(', ')}` : ''}.`;
}

export const impactJson = (impact: Impact) => ({
  changes: impact.changes,
  mustChangeFirst: impact.breaks.map(b => ({ consumer: b.consumer, intent: b.intent, event: showRef(b.event), because: b.arrival, differences: b.differences, deadLettersTo: b.dlq ?? null })),
  intents: impact.intents.map(i => ({
    intent: i.intent,
    predicate: i.predicate,
    consumers: i.consumers.map(c => c.name),
    arrivals: i.arrivals.map(a => ({ event: showRef(a.route.event), topic: a.route.topic, arrival: a.arrival, comparedWith: a.previous ? showRef(a.previous) : null, differences: a.differences })),
    removed: i.removed.map(r => showRef(r.event)),
  })),
  unaffected: impact.unaffected,
  blocked: impact.errors,
});
