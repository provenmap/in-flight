package inflight.catalog;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Parser for the intent language: {@code changes(Order.total) and not emits(OrderPlaced)}. {@code not} binds tightest,
 * then {@code and}, then {@code or}. Errors say where, by column.
 */
final class IntentLanguage {

    private IntentLanguage() {}

    private record Tok(char kind, String text, int at) {} // kind: 'i' identifier, '(', ')' or '.'

    private static final class Failure extends RuntimeException {
        Failure(String msg) {
            super(msg, null, false, false);
        }
    }

    private static final Set<String> KEYWORDS = Set.of("and", "or", "not");

    static Result<Predicate> parse(String text) {
        List<Tok> toks = new ArrayList<>();
        int i = 0;
        while (i < text.length()) {
            char c = text.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
            } else if (c == '(' || c == ')' || c == '.') {
                toks.add(new Tok(c, null, i++));
            } else if (isStart(c)) {
                int start = i;
                while (i < text.length() && isPart(text.charAt(i))) {
                    i++;
                }
                toks.add(new Tok('i', text.substring(start, i), start));
            } else {
                return Result.err("unexpected '" + c + "' at column " + (i + 1));
            }
        }
        if (toks.isEmpty()) {
            return Result.err("the predicate is empty");
        }
        Parser p = new Parser(toks);
        try {
            Predicate out = p.expr();
            if (p.pos != toks.size()) {
                p.fail("unexpected input");
            }
            return Result.ok(out);
        } catch (Failure f) {
            return Result.err(f.getMessage());
        }
    }

    private static boolean isStart(char c) {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_';
    }

    private static boolean isPart(char c) {
        return isStart(c) || (c >= '0' && c <= '9');
    }

    private static final class Parser {
        final List<Tok> ts;
        int pos;

        Parser(List<Tok> ts) {
            this.ts = ts;
        }

        String where() {
            return pos < ts.size() ? "at column " + (ts.get(pos).at() + 1) : "at the end";
        }

        <T> T fail(String msg) {
            throw new Failure(msg + " " + where());
        }

        boolean keyword(String k) {
            if (pos < ts.size() && ts.get(pos).kind() == 'i' && ts.get(pos).text().equals(k)) {
                pos++;
                return true;
            }
            return false;
        }

        void expect(char t, String what) {
            if (pos < ts.size() && ts.get(pos).kind() == t) {
                pos++;
            } else {
                fail("expected " + what);
            }
        }

        String ident(String what) {
            if (pos < ts.size() && ts.get(pos).kind() == 'i' && !KEYWORDS.contains(ts.get(pos).text())) {
                return ts.get(pos++).text();
            }
            return fail("expected " + what);
        }

        Predicate expr() {
            Predicate acc = term();
            while (keyword("or")) {
                acc = new Predicate.Or(acc, term());
            }
            return acc;
        }

        Predicate term() {
            Predicate acc = factor();
            while (keyword("and")) {
                acc = new Predicate.And(acc, factor());
            }
            return acc;
        }

        Predicate factor() {
            if (keyword("not")) {
                return new Predicate.Not(factor());
            }
            if (pos < ts.size() && ts.get(pos).kind() == '(') {
                pos++;
                Predicate e = expr();
                expect(')', "')'");
                return e;
            }
            return call();
        }

        Predicate call() {
            String name = ident("changes(...), emits(...), about(...), 'not' or '('");
            switch (name) {
                case "changes" -> {
                    expect('(', "'(' after changes");
                    String entity = ident("an entity");
                    expect('.', "'.' between entity and attribute");
                    String attribute = ident("an attribute");
                    expect(')', "')'");
                    return new Predicate.Changes(new AttrRef(entity, attribute));
                }
                case "emits" -> {
                    expect('(', "'(' after emits");
                    String type = ident("an event type");
                    expect(')', "')'");
                    return new Predicate.Emits(type);
                }
                case "about" -> {
                    expect('(', "'(' after about");
                    String entity = ident("an entity");
                    expect(')', "')'");
                    return new Predicate.About(entity);
                }
                default -> {
                    pos--;
                    return fail("unknown function '" + name + "', expected changes, emits or about,");
                }
            }
        }
    }
}
