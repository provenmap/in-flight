package inflight.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static inflight.catalog.Fixtures.event;
import static inflight.catalog.Fixtures.load;
import static inflight.catalog.Fixtures.sound;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Ported from src/intents/compiler.test.ts, then intent-bus's IntentCompilerSuite against its demo catalogs. */
class IntentCompilerTest {

    static List<String> names(String predicate, String by) {
        Result<RoutingTable> t = sound().explain(predicate, by);
        return t.isOk() ? t.value().routes().stream().map(r -> r.event().name()).toList() : t.errors();
    }

    static List<String> explain(String predicate, String by) {
        Result<RoutingTable> t = sound().explain(predicate, by);
        return t.isOk() ? List.of() : t.errors();
    }

    @Test
    void changesMeansExactlyTheEventTypesWithAnEffectOnTheAttributeFromWhicheverTopics() {
        RoutingTable t = sound().table("order-total-changes");
        assertEquals("Order", t.entity());
        assertEquals(List.of("OrderAmended@v1", "OrderPlaced@v1", "RefundIssued@v1"), t.routes().stream().map(r -> r.event().show()).toList());
        assertEquals(List.of("billing.refunds", "orders.amended", "orders.placed"), t.sourceTopics());
        RoutingTable.Route refund = t.routes().get(2);
        assertEquals(List.of("subtract Order.total from refund.amount"), refund.reasons());
        assertEquals(List.of(new RoutingTable.Read("Order key", "from refund.orderId", "string"),
                new RoutingTable.Read("Order.total", "subtract from refund.amount", "decimal")), refund.reads());
    }

    @Test
    void emitsAboutAndOrNot() {
        assertEquals(List.of("OrderShipped"), names("emits(OrderShipped)", "Order"));
        assertEquals(List.of("OrderShipped"), names("about(Shipment)", null));
        assertEquals(List.of("OrderShipped"), names("about(Order) and not changes(Order.total)", null));
        assertEquals(List.of("OrderPlaced", "OrderShipped", "RefundIssued"), names("changes(Order.status) or emits(RefundIssued)", null));
        // An event the catalog gives no meaning to is outside every intent, even a negated one.
        assertFalse(names("not emits(OrderPlaced)", "Order").contains("Heartbeat"));
    }

    @Test
    void itFailsOnWhatItCannotResolveAndGuessesNothing() {
        assertEquals(List.of("unknown entity 'Invoice' in changes(Invoice.total)"), explain("changes(Invoice.total)", null));
        assertEquals(List.of("Order has no attribute 'totl' (has: total, status)"), explain("changes(Order.totl)", null));
        assertEquals(List.of("unknown event type 'OrderLost' in emits(OrderLost)"), explain("emits(OrderLost)", null));
        assertEquals(List.of("changes(Order.total) and emits(OrderShipped) matches no event type in the catalog"),
                explain("changes(Order.total) and emits(OrderShipped)", null));
        assertEquals(List.of("key entity is ambiguous (Order, Shipment); say which with 'by'"), explain("emits(OrderShipped)", null));
        assertTrue(explain("about(Shipment) or about(Order)", "Shipment").get(0).matches(".*OrderAmended@v1 matches but carries no Shipment key.*"));
    }

    @Test
    void aDeclaredIntentThatDoesNotCompileFailsTheCatalog() {
        assertEquals(List.of("intent 'bad': Order has no attribute 'colour' (has: total, status)"),
                load(r -> ((ArrayNode) r.get("intents")).addObject().put("name", "bad").put("predicate", "changes(Order.colour)")).errors());
        assertEquals(List.of("intent name 'Bad Name' must be lowercase letters, digits and hyphens"),
                load(r -> ((ArrayNode) r.get("intents")).addObject().put("name", "Bad Name").put("predicate", "about(Order)")).errors());
        assertEquals(List.of("intent 'order-total-changes' is declared twice"),
                load(r -> ((ArrayNode) r.get("intents")).addObject().put("name", "order-total-changes").put("predicate", "about(Order)")).errors());
    }

    @Test
    void theMeaningIsTypedAndChecked() {
        assertEquals(List.of("OrderPlaced@v1: key path 'note' for Order is optional"),
                load(r -> event(r, 0).putObject("keys").put("Order", "note")).errors());
        assertTrue(load(r -> event(r, 0).putObject("keys").put("Parcel", "orderId")).errors().contains("OrderPlaced@v1: key for unknown entity 'Parcel'"));
        assertEquals(List.of("OrderAmended@v1: effect 'assign Order.total from orderId': 'orderId' is string, Order.total is decimal"),
                load(r -> event(r, 1).putArray("effects").addObject().put("op", "assign").put("attribute", "Order.total").put("from", "orderId")).errors());
        assertTrue(load(r -> event(r, 1).putArray("effects").addObject().put("op", "add").put("attribute", "Order.status").put("value", "x")).errors()
                .contains("OrderAmended@v1: effect 'add Order.status = \"x\"': add needs a numeric attribute, Order.status is string"));
        assertEquals(List.of("OrderAmended@v1: effect 'assign Shipment.carrier from orderId': the event carries no Shipment key, so it cannot say which Shipment it changes"),
                load(r -> event(r, 1).putArray("effects").addObject().put("op", "assign").put("attribute", "Shipment.carrier").put("from", "orderId")).errors());
        assertTrue(load(r -> event(r, 1).remove("keys")).errors().contains("OrderAmended@v1: declares no entity key, so no intent could key it"));
        assertTrue(load(r -> ((ObjectNode) r.get("entities").get("Order")).put("total", "money")).errors().get(0)
                .startsWith("entity Order: total: unknown field type 'money'"));
    }

    @Test
    void aCatalogThatSaysNothingAboutMeaningHasNone() {
        ObjectNode r = Json.object();
        r.putArray("events").addObject().put("type", "A").put("version", 1).put("topic", "t");
        Result<Semantics> s = Fixtures.parseSemantics(r);
        assertTrue(s.isOk());
        assertNull(s.value());
    }

    // ---- intent-bus, against its demo catalogs (reference errors now name the intent, as the TypeScript build did) ----

    static Intent intent(String predicate, String by) {
        return Intent.parse("t", predicate, by).value();
    }

    static Model model(int rev) {
        return Fixtures.intentBus(rev).semantics().model();
    }

    @Test
    void changesOrderTotalMatchesExactlyTheTypesWithAnEffectOnIt() {
        RoutingTable table = IntentCompiler.compile(intent("changes(Order.total)", null), model(1)).value();
        assertEquals(List.of("OrderAmended@v1", "OrderPlaced@v1", "RefundIssued@v1"), table.routes().stream().map(r -> r.event().show()).toList());
        assertEquals("Order", table.entity());
        assertEquals(List.of("billing.refunds", "orders.amended", "orders.placed"), table.sourceTopics());
        assertEquals(Optional.of(List.of("subtract Order.total from amount")), table.lookup("RefundIssued", 1).map(RoutingTable.Route::reasons));
        assertEquals(Optional.empty(), table.lookup("OrderShipped", 1));
    }

    @Test
    void setOperations() {
        RoutingTable t = IntentCompiler.compile(intent("about(Order) and not changes(Order.total)", null), model(1)).value();
        assertEquals(List.of("OrderShipped"), t.routes().stream().map(r -> r.event().name()).toList());
    }

    @Test
    void aNewEventTypeWithAnEffectJoinsTheIntentOnTheNextRevision() {
        RoutingTable t = IntentCompiler.compile(intent("changes(Order.total)", null), model(2), 2).value();
        assertEquals(Optional.of(List.of("add Order.total from adjustment.amount")), t.lookup("InvoiceAdjusted", 1).map(RoutingTable.Route::reasons));
        assertEquals(2, t.revision());
    }

    @Test
    void unknownReferencesAreCompileErrors() {
        assertEquals(List.of("intent 't': Order has no attribute 'totl' (has: total, currency, status)"),
                IntentCompiler.compile(intent("changes(Order.totl)", null), model(1)).errors());
        assertEquals(List.of("intent 't': unknown event type 'InvoiceAdjusted' in emits(InvoiceAdjusted)"),
                IntentCompiler.compile(intent("emits(InvoiceAdjusted)", null), model(1)).errors());
    }

    @Test
    void aMatchWithoutAKeyForTheStreamsEntityIsACompileError() {
        List<String> errs = IntentCompiler.compile(intent("changes(Order.total)", "Invoice"), model(2)).errors();
        assertTrue(errs.stream().anyMatch(e -> e.contains("OrderPlaced@v1 matches but carries no Invoice key")), errs.toString());
    }

    @Test
    void anAmbiguousKeyEntityMustBeStated() {
        assertTrue(IntentCompiler.compile(intent("changes(Order.total) or about(Invoice)", null), model(2)).errors().get(0).contains("ambiguous"));
    }

    @Test
    void emitsOnlyIntentsInferTheKeyFromTheMatchedTypes() {
        assertTrue(IntentCompiler.compile(intent("emits(InvoiceAdjusted)", null), model(2)).errors().get(0).startsWith("intent 't': key entity is ambiguous (Inv"));
        assertEquals("Order", IntentCompiler.compile(intent("emits(RefundIssued)", null), model(2)).value().entity());
    }

    @Test
    void anEmptyMatchIsAnError() {
        assertTrue(IntentCompiler.compile(intent("emits(OrderPlaced) and emits(RefundIssued)", null), model(1)).errors().get(0).contains("matches no event type"));
    }
}
