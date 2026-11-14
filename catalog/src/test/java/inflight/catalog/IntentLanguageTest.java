package inflight.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.List;

import org.junit.jupiter.api.Test;

import inflight.catalog.Predicate.About;
import inflight.catalog.Predicate.And;
import inflight.catalog.Predicate.Changes;
import inflight.catalog.Predicate.Emits;
import inflight.catalog.Predicate.Not;
import inflight.catalog.Predicate.Or;

/** Ported from src/intents/language.test.ts, then intent-bus's IntentLanguageSuite. */
class IntentLanguageTest {

    static String round(String text) {
        Result<Predicate> p = Predicate.parse(text);
        return p.isOk() ? p.value().show() : "error: " + p.error();
    }

    @Test
    void theThreePredicates() {
        assertEquals("changes(Order.total)", round("changes(Order.total)"));
        assertEquals("emits(RefundIssued)", round("  emits( RefundIssued )"));
        assertEquals("about(Order)", round("about(Order)"));
    }

    @Test
    void notBindsTightestThenAndThenOr() {
        assertEquals("changes(Order.total) or about(Order) and not emits(OrderPlaced)", round("changes(Order.total) or about(Order) and not emits(OrderPlaced)"));
        assertEquals("(changes(Order.total) or about(Order)) and emits(OrderPlaced)", round("(changes(Order.total) or about(Order)) and emits(OrderPlaced)"));
        assertEquals("not (emits(A) or emits(B))", round("not (emits(A) or emits(B))"));
        assertEquals(new Or(new Emits("A"), new And(new Emits("B"), new Emits("C"))), Predicate.parse("emits(A) or emits(B) and emits(C)").value());
    }

    @Test
    void errorsSayWhere() {
        assertEquals("error: the predicate is empty", round(""));
        assertEquals("error: expected '.' between entity and attribute at column 14", round("changes(Order)"));
        assertEquals("error: unexpected input at column 22", round("changes(Order.total) emits(A)"));
        assertEquals("error: unknown function 'touches', expected changes, emits or about, at column 1", round("touches(Order)"));
        assertEquals("error: expected changes(...), emits(...), about(...), 'not' or '(' at the end", round("emits(A) and"));
        assertEquals("error: unexpected '&' at column 10", round("emits(A) & emits(B)"));
    }

    // ---- intent-bus ----

    @Test
    void parsesASingleFunction() {
        assertEquals(new Changes(new AttrRef("Order", "total")), Predicate.parse("changes(Order.total)").value());
    }

    @Test
    void andBindsTighterThanOrNotTighterThanAnd() {
        assertEquals(new Or(new Emits("A"), new And(new Not(new Emits("B")), new About("Order"))),
                Predicate.parse("emits(A) or not emits(B) and about(Order)").value());
    }

    @Test
    void parenthesesGroup() {
        assertEquals(new And(new Or(new Emits("A"), new Emits("B")), new About("Order")), Predicate.parse("(emits(A) or emits(B)) and about(Order)").value());
    }

    @Test
    void showRoundTripsThroughTheParser() {
        for (String text : List.of("changes(Order.total)", "(emits(A) or emits(B)) and not about(Order)", "not (emits(A) and emits(B))",
                "emits(A) or emits(B) and emits(C)")) {
            Predicate p = Predicate.parse(text).value();
            assertEquals(p, Predicate.parse(p.show()).value(), p.show());
        }
    }

    @Test
    void errorsNameTheColumn() {
        assertEquals(List.of("expected '.' between entity and attribute at column 14"), Predicate.parse("changes(Order)").errors());
        assertEquals(List.of("unexpected input at column 10"), Predicate.parse("emits(A) xor emits(B)").errors());
        assertEquals(List.of("unknown function 'anything', expected changes, emits or about, at column 1"), Predicate.parse("anything(Order)").errors());
        assertFalse(Predicate.parse("").isOk());
        assertFalse(Predicate.parse("changes(Order.total) $").isOk());
    }

    @Test
    void intentNamesAreTopicSafe() {
        assertFalse(Intent.parse("Order Totals", "emits(A)").isOk());
        assertEquals("__intent.order-totals", Intent.parse("order-totals", "emits(A)").value().topic());
        assertEquals("intent.order-totals", Intent.aliasFor("order-totals"));
    }
}
