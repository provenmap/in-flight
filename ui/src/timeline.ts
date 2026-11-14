// The activity timeline: what changed, worked out from one stats message to the next, since this page opened.
import { useEffect, useRef, useState } from 'react';
import { count, money, shown, type Model } from './model';
import type { Stats } from './protocol';

export type EntryTone = 'info' | 'stuck' | 'lag' | 'recovered';

export interface Entry {
  id: number;
  at: number;
  tone: EntryTone;
  title: string;
  detail?: string;
}

let next = 0;

export function changes(prev: Stats | null, s: Stats, model: Model): Omit<Entry, 'id' | 'at'>[] {
  const out: Omit<Entry, 'id' | 'at'>[] = [];
  const a = prev?.sim ?? null, b = s.sim;
  if (prev && !a && b) out.push({ tone: 'info', title: 'Simulator connected' });
  if (a && !b) out.push({ tone: 'stuck', title: 'Simulator not reachable', detail: 'Controls are off until it is back.' });
  if (a && b) {
    if (a.orderVersion !== b.orderVersion) {
      out.push(b.orderVersion > a.orderVersion
        ? { tone: 'lag', title: `Orders team shipped OrderPlaced v${b.orderVersion}`, detail: 'total is now an object with amount and currency.' }
        : { tone: 'info', title: `Orders team went back to OrderPlaced v${b.orderVersion}` });
    }
    if (a.patched !== b.patched) {
      out.push(b.patched
        ? { tone: 'info', title: 'Payments patched', detail: 'It reads OrderPlaced v1 and v2.' }
        : { tone: 'info', title: 'Payments unpatched', detail: 'It reads OrderPlaced v1 only.' });
    }
    if (!a.redrive.running && b.redrive.running) out.push({ tone: 'info', title: 'Redrive started on payments.dlq' });
    if (a.redrive.running && !b.redrive.running) {
      out.push(b.redrive.error
        ? { tone: 'stuck', title: `Redrive stopped after ${count(b.redrive.redriven, shown.case)}`, detail: b.redrive.error }
        : { tone: 'recovered', title: `Redrive finished: ${count(b.redrive.redriven, shown.case)} replayed` });
    }
    if (a.rate !== b.rate) out.push({ tone: 'info', title: `Order traffic set to ${b.rate} a second` });
  }
  if (prev && prev.stuck.events === 0 && s.stuck.events > 0) {
    const q = model.lastError && model.flow?.dlqs.find(d => d.dlq === model.lastError!.dlq);
    const who = (q && model.flow?.districts.find(d => d.id === q.district)?.name) ?? 'A service';
    out.push({ tone: 'stuck', title: `${who} started dead-lettering orders`, detail: model.lastError ? `${model.lastError.event}: ${model.lastError.error}` : undefined });
  }
  const ep = s.lastEpisode, was = prev?.lastEpisode;
  if (prev && ep && (!was || was.events !== ep.events || was.value !== ep.value || was.tookMs !== ep.tookMs)) {
    out.push({ tone: 'recovered', title: `Recovered ${money.format(ep.value)}`, detail: `${count(ep.events, shown.case)}, stuck for ${(ep.tookMs / 1000).toFixed(1)} s.` });
  }
  return out;
}

export function useTimeline(stats: Stats | null, model: Model): Entry[] {
  const [entries, setEntries] = useState<Entry[]>([]);
  const prev = useRef<Stats | null>(null);
  useEffect(() => {
    if (!stats) return;
    const found = changes(prev.current, stats, model);
    prev.current = stats;
    if (found.length) {
      const at = Date.now();
      setEntries(e => [...found.map(f => ({ ...f, id: next++, at })).reverse(), ...e].slice(0, 100));
    }
  }, [stats, model]);
  return entries;
}
