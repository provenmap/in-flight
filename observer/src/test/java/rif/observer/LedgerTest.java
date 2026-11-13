package rif.observer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;

import rif.catalog.Catalog;
import rif.catalog.EventRef;
import rif.catalog.Pricing.Priced;
import rif.observer.Ledger.GroupLag;
import rif.observer.Ledger.Msg;
import rif.observer.Ledger.Pos;
import rif.observer.Ledger.Stats;

/** Ported from src/observer/ledger.test.ts and reader.test.ts. */
class LedgerTest {

    static final Catalog CATALOG = Catalog.defaultCatalog();
    static final Instant T0 = Instant.parse("2026-10-30T12:00:00Z");

    static Instant plus(long s) {
        return T0.plusSeconds(s);
    }

    static Pos pos(String topic, int partition, long offset) {
        return new Pos(topic, partition, offset);
    }

    static Priced placed(String id, String order, String customer, String value, int version) {
        return new Priced(id, new EventRef("OrderPlaced", version), "orders.placed", "orders", new BigDecimal(value), customer, order, null, null);
    }

    static Priced placed(String id, String order, String customer, String value) {
        return placed(id, order, customer, value, 2);
    }

    static Priced authorized(String id, String order, String customer, String value, String redriveOf) {
        return new Priced(id, new EventRef("PaymentAuthorized", 1), "payments.authorized", "payments", new BigDecimal(value), customer, order, redriveOf, null);
    }

    static Msg dead(Ledger l, long offset, Priced p, Instant at) {
        return l.onDead("payments.dlq", pos("payments.dlq", 0, offset), p, "'total' is an object", at);
    }

    static Msg dead(Ledger l, long offset, Priced p) {
        return dead(l, offset, p, T0);
    }

    static ObjectNode json(Msg m) {
        return Messages.msg(m);
    }

    static String money(BigDecimal b) {
        return b.stripTrailingZeros().toPlainString();
    }

    static List<Object> lag(GroupLag g) {
        return List.of(g.events(), money(g.value()), g.customers());
    }

    @Test
    void aDeadLetterIsStuckMoneyAtPayments() {
        Ledger l = new Ledger(CATALOG);
        Msg m = dead(l, 0, placed("e1", "o1", "c1", "100.00"));
        assertTrue(m instanceof Ledger.Dead);
        assertEquals("payments", json(m).get("district").asText());
        dead(l, 1, placed("e2", "o2", "c1", "50.50"), plus(1));
        Stats s = l.stats();
        assertEquals(List.of("150.5", 2L, 1), List.of(money(s.stuckValue()), s.stuckEvents(), s.stuckCustomers()));
        assertEquals(T0, s.stuckSince());
        assertEquals(Map.of("payments", "150.5"), Map.of("payments", money(s.stuckByDistrict().get("payments"))));
        assertEquals(1, s.stuckByDistrict().size());
        assertEquals(List.of("150.5", 2, 1), List.of(money(s.atRiskValue()), s.atRiskOrders(), s.atRiskCustomers()));
    }

    @Test
    void aRedrivenSuccessRecoversItsDeadLetterWithTheTimeItWasStuck() {
        Ledger l = new Ledger(CATALOG);
        dead(l, 0, placed("e1", "o1", "c1", "100.00"));
        dead(l, 1, placed("e2", "o2", "c2", "20.00"), plus(5));
        List<Msg> msgs = l.onFlow(pos("payments.authorized", 0, 7), authorized("p1", "o1", "c1", "100.00", "e1"), plus(30));
        assertEquals(List.of("event", "recovered"), msgs.stream().map(m -> json(m).get("type").asText()).toList());
        assertEquals(30000, json(msgs.get(1)).get("afterMs").asLong());
        assertEquals("[\"shipping\"]", json(msgs.get(0)).get("to").toString());
        assertTrue(json(msgs.get(0)).get("redrive").asBoolean());
        Stats s = l.stats();
        assertEquals(List.of("20", "100", 1L), List.of(money(s.stuckValue()), money(s.recoveredValue()), s.recoveredEvents()));
        assertNull(s.lastEpisode());

        l.onFlow(pos("payments.authorized", 0, 8), authorized("p2", "o2", "c2", "20.00", "e2"), plus(45));
        s = l.stats();
        assertEquals(List.of("120", 2L, 45000L), List.of(money(s.lastEpisode().value()), s.lastEpisode().events(), s.lastEpisode().tookMs()));
        assertEquals("0", money(s.atRiskValue()));
    }

    @Test
    void afterARestartDeadLettersTheRedriveAlreadyPassedCountAsRecoveredNotStuck() {
        Ledger l = new Ledger(CATALOG);
        l.redriven("payments.dlq", 0, 2);
        List<Msg> msgs = List.of(0, 1, 2).stream().map(o -> dead(l, o, placed("e" + o, "o" + o, "c", "10.00"))).toList();
        assertEquals(List.of("recovered", "recovered", "dead"), msgs.stream().map(m -> json(m).get("type").asText()).toList());
        assertTrue(json(msgs.get(0)).get("afterMs").isNull());
        assertEquals(List.of(1L, 2L), List.of(l.stats().stuckEvents(), l.stats().recoveredEvents()));
    }

    @Test
    void aRedriveOffsetOnOnePartitionSaysNothingAboutAnother() {
        Ledger l = new Ledger(CATALOG);
        l.redriven("payments.dlq", 1, 100);
        assertEquals("dead", json(dead(l, 0, placed("e", "o", "c", "1.00"))).get("type").asText());
    }

    @Test
    void aRedrivenEventThatOvertakesItsDeadLetterStillRecoversIt() {
        Ledger l = new Ledger(CATALOG);
        assertEquals(1, l.onFlow(pos("payments.authorized", 0, 0), authorized("p1", "o1", "c1", "9.00", "e1"), T0).size());
        assertEquals("recovered", json(dead(l, 0, placed("e1", "o1", "c1", "9.00"))).get("type").asText());
        assertEquals(0, l.stats().stuckEvents());
    }

    @Test
    void lagIsWhatAGroupHasNotCommittedValuedFromTheEventsTheObserverSaw() {
        Ledger l = new Ledger(CATALOG);
        for (int o = 0; o <= 4; o++) {
            l.onFlow(pos("orders.placed", 0, o), placed("e" + o, "o" + o, "c" + (o % 2), "10.00", 1), T0);
        }
        l.onOffsets("payments-service", Map.of("orders.placed:0", 2L), Map.of("orders.placed:0", 5L));
        l.onOffsets("inventory-service", Map.of("orders.placed:0", 5L), Map.of());
        Stats s = l.stats();
        assertEquals(List.of(3L, "30", 2), lag(s.lag().get("payments-service")));
        assertEquals(List.of(0L, "0", 0), lag(s.lag().get("inventory-service")));
        assertEquals(0, s.lag().get("shipping-service").events());
        assertEquals(List.of("30", 3, 2), List.of(money(s.atRiskValue()), s.atRiskOrders(), s.atRiskCustomers()));
    }

    @Test
    void eventsPastThePolledEndAreInFlightNotLag() {
        Ledger l = new Ledger(CATALOG);
        for (int o = 0; o <= 4; o++) {
            l.onFlow(pos("orders.placed", 0, o), placed("e" + o, "o" + o, "c", "10.00", 1), T0);
        }
        l.onOffsets("payments-service", Map.of("orders.placed:0", 2L), Map.of("orders.placed:0", 3L));
        assertEquals(List.of(1L, "10", 1), lag(l.stats().lag().get("payments-service")));
    }

    @Test
    void lagCountsEventsTheObserverDidNotSeeButCannotValueThem() {
        Ledger l = new Ledger(CATALOG);
        l.onOffsets("payments-service", Map.of("orders.placed:1", 10L), Map.of("orders.placed:1", 25L));
        assertEquals(List.of(15L, "0", 0), lag(l.stats().lag().get("payments-service")));
    }

    @Test
    void anOrderHeldUpInTwoPlacesIsAtRiskOnceAtItsLargestValue() {
        Ledger l = new Ledger(CATALOG);
        dead(l, 0, placed("e1", "o1", "c1", "100.00"));
        l.onFlow(pos("orders.placed", 0, 0), placed("e1", "o1", "c1", "100.00"), T0);
        l.onFlow(pos("inventory.reserved", 0, 0),
                new Priced("s1", new EventRef("StockReserved", 1), "inventory.reserved", "inventory", new BigDecimal("95.00"), "c1", "o1", null, null), T0);
        l.onOffsets("inventory-service", Map.of("orders.placed:0", 0L), Map.of("orders.placed:0", 1L));
        l.onOffsets("shipping-service", Map.of("inventory.reserved:0", 0L), Map.of("inventory.reserved:0", 1L));
        Stats s = l.stats();
        assertEquals("295", money(s.stuckValue().add(s.lagValue())));
        assertEquals(List.of("100", 1, 1), List.of(money(s.atRiskValue()), s.atRiskOrders(), s.atRiskCustomers()));
    }

    @Test
    void eventsEveryReaderHasCommittedAreForgotten() {
        Ledger l = new Ledger(CATALOG);
        for (int o = 0; o <= 9; o++) {
            l.onFlow(pos("orders.placed", 0, o), placed("e" + o, "o" + o, "c", "1.00", 1), T0);
        }
        l.onOffsets("payments-service", Map.of("orders.placed:0", 10L), Map.of("orders.placed:0", 10L));
        l.onOffsets("inventory-service", Map.of("orders.placed:0", 6L), Map.of());
        assertEquals(List.of(4L, "4", 1), lag(l.stats().lag().get("inventory-service")));
        // Payments rewinds (a reset, say): only what inventory still holds can be valued.
        l.onOffsets("payments-service", Map.of("orders.placed:0", 0L), Map.of());
        assertEquals(List.of(10L, "4", 1), lag(l.stats().lag().get("payments-service")));
    }

    @Test
    void flowTopicsNobodyReadsAreNotRememberedAndGoNowhere() {
        Ledger l = new Ledger(CATALOG);
        Priced declined = new Priced("d1", new EventRef("PaymentDeclined", 1), "payments.declined", "payments", new BigDecimal("5.00"), "c", "o", null, null);
        assertEquals("[]", json(l.onFlow(pos("payments.declined", 0, 0), declined, T0).get(0)).get("to").toString());
    }

    @Test
    void theRememberedWindowIsBoundedOldestFirst() {
        Ledger l = new Ledger(CATALOG, 3);
        for (int o = 0; o <= 9; o++) {
            l.onFlow(pos("orders.placed", 0, o), placed("e" + o, "o" + o, "c", "1.00", 1), T0);
        }
        l.onOffsets("payments-service", Map.of("orders.placed:0", 0L), Map.of("orders.placed:0", 10L));
        assertEquals(List.of(10L, "3", 1), lag(l.stats().lag().get("payments-service")));
    }

    @Test
    void withNoRedriveMarkerTheDistrictProducingForTheStuckOrderRecoversIt() {
        Ledger l = new Ledger(CATALOG);
        dead(l, 0, placed("e1", "o1", "c1", "100.00"));
        // Inventory reserving the same order is not payments getting through.
        Priced stock = new Priced("s1", new EventRef("StockReserved", 1), "inventory.reserved", "inventory", new BigDecimal("100.00"), "c1", "o1", null, null);
        assertEquals(List.of("event"), l.onFlow(pos("inventory.reserved", 0, 1), stock, plus(2)).stream().map(m -> json(m).get("type").asText()).toList());
        List<Msg> msgs = l.onFlow(pos("payments.authorized", 0, 3), authorized("p1", "o1", "c1", "100.00", null), plus(9));
        assertEquals(List.of("event", "recovered"), msgs.stream().map(m -> json(m).get("type").asText()).toList());
        assertEquals(9000, json(msgs.get(1)).get("afterMs").asLong());
        assertEquals(1, l.stats().lastEpisode().events());
    }

    @Test
    void aDeadLetterTheCatalogCannotPriceIsStuckWithItsValueUnknownUntilItsRedriveGroupPassesIt() {
        Ledger l = new Ledger(CATALOG);
        Msg m = l.onDeadUnpriced("payments.dlq", pos("payments.dlq", 1, 4), "OrderPlaced@v3", "no version reads it", "boom", T0);
        ObjectNode j = json(m);
        assertEquals(List.of("dead", "OrderPlaced@v3", "false", "payments", "0.0"),
                List.of(j.get("type").asText(), j.get("event").asText(), j.get("priced").asText(), j.get("district").asText(), j.get("value").asText()));
        dead(l, 0, placed("e1", "o1", "c1", "10.00"));
        Stats s = l.stats();
        assertEquals(List.of(2L, 1L, 1, 1, "10"), List.of(s.stuckEvents(), s.stuckUnpriced(), s.stuckCustomers(), s.atRiskOrders(), money(s.atRiskValue())));

        assertEquals(List.of(), l.onRedriveOffsets("payments.dlq", Map.of("payments.dlq:1", 4L), plus(5)));
        List<Msg> passed = l.onRedriveOffsets("payments.dlq", Map.of("payments.dlq:1", 5L), plus(6));
        assertEquals(List.of("recovered payments.dlq:1:4"), passed.stream().map(x -> json(x).get("type").asText() + " " + json(x).get("id").asText()).toList());
        s = l.stats();
        assertEquals(List.of(1L, 0L), List.of(s.stuckEvents(), s.stuckUnpriced()));
    }

    @Test
    void anEventWhoseWorthIsUnknownIsStuckAsUnpricedButKeepsItsOrder() {
        Ledger l = new Ledger(CATALOG);
        Priced unknown = new Priced("e9", new EventRef("OrderPlaced", 2), "orders.placed", "orders", null, "c9", "o9", null, null);
        ObjectNode j = json(l.onDead("payments.dlq", pos("payments.dlq", 0, 0), unknown, "boom", T0));
        assertEquals(List.of("false", "o9"), List.of(j.get("priced").asText(), j.get("orderId").asText()));
        // The order getting through recovers it, as for any other dead letter.
        List<Msg> msgs = l.onFlow(pos("payments.authorized", 0, 1), authorized("p9", "o9", "c9", "10.00", null), plus(3));
        assertEquals(List.of("event", "recovered"), msgs.stream().map(x -> json(x).get("type").asText()).toList());
    }

    @Test
    void aGroupReadingByIntentLagsOnTheIntentsTopicAndTheEventsThereMakeNoMessage() {
        Ledger l = new Ledger(CATALOG);
        String topic = "__intent.order-total-changes";
        for (int o = 0; o <= 3; o++) {
            l.onFlow(pos("orders.placed", 0, o), placed("e" + o, "o" + o, "c" + o, "10.00", 1), T0);
            l.onIntent(pos(topic, 0, o), placed("e" + o, "o" + o, "c" + o, "10.00", 1));
        }
        // Payments reads by intent through the router: its commits are on the intent's topic, none on orders.placed.
        l.onOffsets("payments-service", Map.of(topic + ":0", 1L), Map.of(topic + ":0", 4L, "orders.placed:0", 4L));
        l.onOffsets("inventory-service", Map.of("orders.placed:0", 4L), Map.of());
        Stats s = l.stats();
        assertEquals(List.of(3L, "30", 3), lag(s.lag().get("payments-service")));
        Ledger.IntentLag il = s.byIntent().get("order-total-changes");
        assertEquals(List.of(topic, 3L, "30"), List.of(il.topic(), il.events(), money(il.value())));
        assertEquals(List.of("payments-service"), il.groups());
        assertEquals(List.of("30", 3), List.of(money(s.atRiskValue()), s.atRiskOrders()));
    }

    @Test
    void theCatalogsHeaderNamesComeFirstThenThisAppsKafkaConnectsAndSpringKafkas() {
        assertEquals(List.of("x-error", "rif-error", "__connect.errors.exception.message", "kafka_dlt-exception-message"),
                Reader.deadLetterHeaders(new Catalog.DeadLetterHeaders("x-error", null, null)).error());
        assertEquals(List.of("rif-source-topic", "__connect.errors.topic", "kafka_dlt-original-topic"),
                Reader.deadLetterHeaders(Catalog.DeadLetterHeaders.NONE).sourceTopic());
    }
}
