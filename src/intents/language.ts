import { err, ok, type Result } from '../result.js';
import { showAttr, type AttrRef } from './model.js';

/** A predicate over the event catalog. It denotes a set of event type versions. */
export type Predicate =
  | { kind: 'changes'; attribute: AttrRef }
  | { kind: 'emits'; eventType: string }
  | { kind: 'about'; entity: string }
  | { kind: 'and' | 'or'; left: Predicate; right: Predicate }
  | { kind: 'not'; inner: Predicate };

export function showPredicate(p: Predicate): string {
  const operand = (child: Predicate, parent: Predicate['kind']): string => {
    const wrap = (parent === 'and' && child.kind === 'or') || (parent === 'not' && (child.kind === 'and' || child.kind === 'or'));
    return wrap ? `(${showPredicate(child)})` : showPredicate(child);
  };
  switch (p.kind) {
    case 'changes': return `changes(${showAttr(p.attribute)})`;
    case 'emits': return `emits(${p.eventType})`;
    case 'about': return `about(${p.entity})`;
    case 'and': return `${operand(p.left, 'and')} and ${operand(p.right, 'and')}`;
    case 'or': return `${operand(p.left, 'or')} or ${operand(p.right, 'or')}`;
    case 'not': return `not ${operand(p.inner, 'not')}`;
  }
}

/** Entities this predicate names through `changes` and `about`, outside any `not`. */
export function namedEntities(p: Predicate): Set<string> {
  switch (p.kind) {
    case 'changes': return new Set([p.attribute.entity]);
    case 'about': return new Set([p.entity]);
    case 'and':
    case 'or': return new Set([...namedEntities(p.left), ...namedEntities(p.right)]);
    default: return new Set();
  }
}

/** Every attribute the predicate asks about with `changes`, outside any `not`. */
export function watchedAttributes(p: Predicate): AttrRef[] {
  switch (p.kind) {
    case 'changes': return [p.attribute];
    case 'and':
    case 'or': return [...watchedAttributes(p.left), ...watchedAttributes(p.right)];
    default: return [];
  }
}

type Tok = { t: 'ident'; text: string; at: number } | { t: '(' | ')' | '.'; at: number };

function tokenize(text: string): Result<Tok[]> {
  const out: Tok[] = [];
  let i = 0;
  while (i < text.length) {
    const c = text[i]!;
    if (/\s/.test(c)) i++;
    else if (c === '(' || c === ')' || c === '.') out.push({ t: c, at: i++ });
    else if (/[A-Za-z_]/.test(c)) {
      const start = i;
      while (i < text.length && /[A-Za-z0-9_]/.test(text[i]!)) i++;
      out.push({ t: 'ident', text: text.slice(start, i), at: start });
    } else return err(`unexpected '${c}' at column ${i + 1}`);
  }
  return ok(out);
}

class Failure extends Error {}

/** The intent language: `changes(Order.total) and not emits(OrderPlaced)`. `not` binds tightest, then `and`, then `or`. */
export function parsePredicate(text: string): Result<Predicate> {
  const toks = tokenize(text);
  if (!toks.ok) return toks;
  const ts = toks.value;
  if (!ts.length) return err('the predicate is empty');
  let pos = 0;

  const where = () => (pos < ts.length ? `at column ${ts[pos]!.at + 1}` : 'at the end');
  const fail = (msg: string): never => {
    throw new Failure(`${msg} ${where()}`);
  };
  const keyword = (k: string): boolean => {
    const t = ts[pos];
    if (t?.t === 'ident' && t.text === k) {
      pos++;
      return true;
    }
    return false;
  };
  const expect = (t: '(' | ')' | '.', what: string): void => {
    if (ts[pos]?.t === t) pos++;
    else fail(`expected ${what}`);
  };
  const ident = (what: string): string => {
    const t = ts[pos];
    if (t?.t === 'ident' && !['and', 'or', 'not'].includes(t.text)) {
      pos++;
      return t.text;
    }
    return fail(`expected ${what}`);
  };

  const expr = (): Predicate => {
    let acc = term();
    while (keyword('or')) acc = { kind: 'or', left: acc, right: term() };
    return acc;
  };
  const term = (): Predicate => {
    let acc = factor();
    while (keyword('and')) acc = { kind: 'and', left: acc, right: factor() };
    return acc;
  };
  const factor = (): Predicate => {
    if (keyword('not')) return { kind: 'not', inner: factor() };
    if (ts[pos]?.t === '(') {
      pos++;
      const e = expr();
      expect(')', "')'");
      return e;
    }
    return call();
  };
  const call = (): Predicate => {
    const name = ident("changes(...), emits(...), about(...), 'not' or '('");
    switch (name) {
      case 'changes': {
        expect('(', "'(' after changes");
        const entity = ident('an entity');
        expect('.', "'.' between entity and attribute");
        const attribute = ident('an attribute');
        expect(')', "')'");
        return { kind: 'changes', attribute: { entity, attribute } };
      }
      case 'emits': {
        expect('(', "'(' after emits");
        const eventType = ident('an event type');
        expect(')', "')'");
        return { kind: 'emits', eventType };
      }
      case 'about': {
        expect('(', "'(' after about");
        const entity = ident('an entity');
        expect(')', "')'");
        return { kind: 'about', entity };
      }
      default:
        pos--;
        return fail(`unknown function '${name}', expected changes, emits or about,`);
    }
  };

  try {
    const p = expr();
    if (pos !== ts.length) fail('unexpected input');
    return ok(p);
  } catch (e) {
    if (e instanceof Failure) return err(e.message);
    throw e;
  }
}
