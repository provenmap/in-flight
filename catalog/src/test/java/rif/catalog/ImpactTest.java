package rif.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static rif.catalog.Fixtures.event;
import static rif.catalog.Fixtures.sound;

import java.util.List;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Ported from src/intents/impact.test.ts, then intent-bus's CatalogSuite: the diff between revisions. */
class ImpactTest {

    static final Function<String, List<Impact.Reader>> READERS =
            intent -> intent.equals("order-total-changes") ? List.of(new Impact.Reader("revenue", "revenue.dlq")) : List.of();

    @Test
    void theDemoOrderPlacedV2BreaksPaymentsAndNobodyElseSaidBeforeAnEventIsProduced() {
        Catalog catalog = Catalog.defaultCatalog();
        Impact impact = Impact.of(new EventRef("OrderPlaced", 2), catalog.semantics(), catalog.readers()).value();

        assertEquals(List.of(new Impact.Breakage("payments-service", "payments.dlq", "order-total-changes", new EventRef("OrderPlaced", 2),
                Impact.Arrival.NEW_READS, List.of("Order.total: from total (decimal) -> from total.amount (decimal)"))), impact.breaks());
        // Inventory is sent v2 as well, and reads it the way it reads v1.
        Impact.IntentImpact placed = impact.intents().stream().filter(i -> i.intent().equals("orders-placed")).findFirst().orElseThrow();
        assertEquals(1, placed.arrivals().size());
        assertEquals(Impact.Arrival.SAME_READS, placed.arrivals().get(0).arrival());
        assertEquals(new EventRef("OrderPlaced", 1), placed.arrivals().get(0).previous());
        assertEquals(List.of("order-dispatched", "ready-to-ship"), impact.unaffected());
        assertEquals("payments-service must change before this ships, or its events pile up in payments.dlq.", impact.headline());
        assertTrue(impact.report().contains(
                "! payments-service (order-total-changes) reads OrderPlaced@v2 differently; until then every OrderPlaced@v2 it meets goes to payments.dlq"));
    }

    @Test
    void aNewEventTypeThatReachesAnIntentIsOneItsConsumersHaveNoCodeFor() {
        Semantics after = sound(r -> {
            ((ObjectNode) r.get("entities")).putObject("Invoice").put("balance", "decimal");
            ((ArrayNode) r.get("events")).add(Json.parse("""
                    {"type": "InvoiceAdjusted", "version": 1, "topic": "billing.invoices",
                     "fields": {"invoiceId": "string", "orderId": "string", "delta": "decimal"}, "keys": {"Order": "orderId", "Invoice": "invoiceId"},
                     "effects": [{"op": "add", "attribute": "Order.total", "from": "delta"}, {"op": "add", "attribute": "Invoice.balance", "from": "delta"}]}
                    """).value());
        });
        Impact impact = Impact.analyse(sound().model(), after, READERS);
        assertEquals(List.of("new event type InvoiceAdjusted@v1 on billing.invoices", "new attribute Invoice.balance: decimal"), impact.changes());
        assertEquals(1, impact.breaks().size());
        assertEquals("revenue", impact.breaks().get(0).consumer());
        assertEquals(Impact.Arrival.NEW_TYPE, impact.breaks().get(0).arrival());
        assertEquals(new EventRef("InvoiceAdjusted", 1), impact.breaks().get(0).event());
        assertTrue(impact.report().contains("+ InvoiceAdjusted@v1 from billing.invoices, a type its consumers have never read: add Order.total from delta"));
    }

    @Test
    void aNewVersionReadTheSameWayIsSafe() {
        Semantics after = sound(r -> {
            ObjectNode v2 = event(r, 1).deepCopy();
            v2.put("version", 2);
            v2.putObject("fields").put("orderId", "string").put("newTotal", "decimal").put("reason", "string?");
            ((ArrayNode) r.get("events")).add(v2);
        });
        Impact impact = Impact.analyse(sound().model(), after, READERS);
        assertEquals(List.of(), impact.breaks());
        assertEquals(List.of("OrderAmended@v2 same-reads"),
                impact.intents().get(0).arrivals().stream().map(a -> a.route().event().show() + " " + a.arrival().wire).toList());
        assertEquals("Safe to ship: it reaches revenue, and each reads it the way it reads what it gets today.", impact.headline());
    }

    @Test
    void anEventThatStopsMatchingIsReported() {
        Impact impact = Impact.analyse(sound().model(), sound(r -> ((ArrayNode) r.get("events")).remove(2)), READERS);
        assertEquals(List.of("removed RefundIssued@v1"), impact.changes());
        assertEquals(List.of("RefundIssued@v1"), impact.intents().get(0).removed().stream().map(x -> x.event().show()).toList());
        assertEquals(List.of(), impact.breaks());
    }

    @Test
    void aPublishedVersionEditedInPlaceIsRefused() {
        Semantics after = sound(r -> event(r, 1).putObject("fields").put("orderId", "string").put("newTotal", "decimal").put("by", "string"));
        Impact impact = Impact.analyse(sound().model(), after, READERS);
        assertEquals(List.of("OrderAmended@v1 is already published and cannot change (fields changed); add version 2 instead"), impact.errors());
        assertTrue(impact.headline().startsWith("Blocked: OrderAmended@v1 is already published"));
    }

    @Test
    void anEventTheCatalogGivesNoMeaningToHasNoImpactToWorkOut() {
        assertTrue(Impact.of(new EventRef("Heartbeat", 1), sound(), READERS).error()
                .startsWith("Heartbeat@v1 is not in the catalog, or the catalog does not say what it means"));
    }

    // ---- intent-bus's CatalogSuite: revisions 1 and 2 of its demo catalog ----

    @Test
    void demoCatalogsLoadAndValidate() {
        Catalog rev1 = Fixtures.intentBus(1);
        Catalog rev2 = Fixtures.intentBus(2);
        assertEquals(4, rev1.events().size());
        assertEquals(5, rev2.events().size());
        Model.Event invoice = rev2.semantics().model().find(new EventRef("InvoiceAdjusted", 1)).orElseThrow();
        assertEquals("decimal", FieldType.at(invoice.fields(), FieldPath.of("adjustment.amount")).show());
    }

    @Test
    void anEffectFromAFieldOfTheWrongTypeIsRejected() {
        Result<Catalog> res = Fixtures.editJson(Fixtures.intentBus(1),
                j -> ((ObjectNode) j.get("events").get(1).get("effects").get(0)).put("from", "reason"));
        assertEquals(List.of("OrderAmended@v1: effect 'assign Order.total from reason': 'reason' is string?, Order.total is decimal"), res.errors());
    }

    @Test
    void addOnANonNumericAttributeIsRejected() {
        Result<Catalog> res = Fixtures.editJson(Fixtures.intentBus(1), j -> ((ObjectNode) j.get("events").get(2).get("effects").get(0)).put("op", "add"));
        assertTrue(res.errors().stream().anyMatch(e -> e.contains("add needs a numeric attribute")), res.errors().toString());
    }

    @Test
    void anEffectOnAnEntityTheEventCarriesNoKeyForIsRejected() {
        Result<Catalog> res = Fixtures.editJson(Fixtures.intentBus(2), j -> ((ArrayNode) j.get("events").get(1).get("effects")).addObject()
                .put("op", "add").put("attribute", "Invoice.balance").put("from", "newTotal"));
        assertTrue(res.errors().stream().anyMatch(e -> e.contains("carries no Invoice key")), res.errors().toString());
    }

    @Test
    void diffFindsTheNewEventTypeAndTheNewEntity() {
        assertEquals(List.of("new event type InvoiceAdjusted@v1 on billing.invoices from invoice-service", "new attribute Invoice.balance: decimal"),
                Impact.diff(Fixtures.intentBus(1).semantics().model(), Fixtures.intentBus(2).semantics().model()).changes());
    }

    @Test
    void diffFlagsAPublishedVersionRedefinedInPlace() {
        Catalog rev2 = Fixtures.intentBus(2);
        Catalog edited = Fixtures.editJson(rev2, j -> ((ObjectNode) j.get("events").get(1).get("fields")).put("newTotal", "long")).value();
        assertEquals(List.of("OrderAmended@v1 redefined in place: fields changed"), Impact.diff(rev2.semantics().model(), edited.semantics().model()).changes());
    }

    @Test
    void routingDiffForAnIntent() {
        Semantics rev2 = Fixtures.intentBus(2).semantics().declare(Intent.parse("order-total-changes", "changes(Order.total)").value()).value();
        Impact impact = Impact.analyse(Fixtures.intentBus(1).semantics().model(), rev2, i -> List.of());
        Impact.IntentImpact i = impact.intents().get(0);
        assertEquals(List.of("InvoiceAdjusted@v1"), i.arrivals().stream().map(a -> a.route().event().show()).toList());
        assertTrue(i.removed().isEmpty() && i.rekeyed().isEmpty());
    }
}
