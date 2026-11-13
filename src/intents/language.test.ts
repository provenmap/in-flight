import { describe, expect, test } from 'vitest';
import { parsePredicate, showPredicate } from './language.js';

const round = (text: string) => {
  const p = parsePredicate(text);
  return p.ok ? showPredicate(p.value) : `error: ${p.error}`;
};

describe('the intent language', () => {
  test('the three predicates', () => {
    expect(round('changes(Order.total)')).toBe('changes(Order.total)');
    expect(round('  emits( RefundIssued )')).toBe('emits(RefundIssued)');
    expect(round('about(Order)')).toBe('about(Order)');
  });

  test('not binds tightest, then and, then or', () => {
    expect(round('changes(Order.total) or about(Order) and not emits(OrderPlaced)')).toBe('changes(Order.total) or about(Order) and not emits(OrderPlaced)');
    expect(round('(changes(Order.total) or about(Order)) and emits(OrderPlaced)')).toBe('(changes(Order.total) or about(Order)) and emits(OrderPlaced)');
    expect(round('not (emits(A) or emits(B))')).toBe('not (emits(A) or emits(B))');
    expect(parsePredicate('emits(A) or emits(B) and emits(C)')).toMatchObject({ ok: true, value: { kind: 'or', right: { kind: 'and' } } });
  });

  test('errors say where', () => {
    expect(round('')).toBe('error: the predicate is empty');
    expect(round('changes(Order)')).toBe("error: expected '.' between entity and attribute at column 14");
    expect(round('changes(Order.total) emits(A)')).toBe('error: unexpected input at column 22');
    expect(round('touches(Order)')).toBe("error: unknown function 'touches', expected changes, emits or about, at column 1");
    expect(round('emits(A) and')).toBe("error: expected changes(...), emits(...), about(...), 'not' or '(' at the end");
    expect(round('emits(A) & emits(B)')).toBe("error: unexpected '&' at column 10");
  });
});
