package rif.router;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Test;

import rif.catalog.Catalog;
import rif.catalog.Intent;
import rif.catalog.Json;
import rif.catalog.Result;
import rif.catalog.RoutingTable;

/**
 * The service's checks and its per-intent applications, with the in-memory registry and streams not running. Ported
 * from RouterServiceSuite, then the per-intent applications that fix intent-bus's shared-application bug.
 */
class RouterServiceTest {

    static Catalog catalog(int rev) {
        return Catalog.load(Path.of("catalog/src/test/resources/intent-bus/rev" + rev + ".json")).value();
    }

    /** The shipped flow as the router's first revision; the tests then activate revisions after it. */
    static Catalog shipped() {
        return Catalog.load(Path.of("catalog/order-to-cash.json")).value().withRevision(1);
    }

    static final StreamsSettings SETTINGS = new StreamsSettings("dummy:9092", "rif-router", Path.of("build-state"), "at_least_once", 1);

    static RouterService service(Registry registry) {
        return new RouterService(registry, null, SETTINGS, new IntentTopology.Ordering(Duration.ofSeconds(1), Duration.ofMillis(100)), false);
    }

    static RouterService service() {
        return service(new InMemoryRegistry());
    }

    static final Intent TOTALS = Intent.parse("order-total-changes", "changes(Order.total)").value();

    static Result<Catalog> fromJson(JsonNode json) {
        return Catalog.fromJson(json);
    }

    static RoutingTable only(RouterService s) {
        assertEquals(1, s.routingTables().size());
        return s.routingTables().get(0);
    }

    // ---- ported from RouterServiceSuite ------------------------------------------------------------------------------

    @Test
    void anIntentNeedsAnActiveCatalog() {
        assertFalse(service().declareIntent(TOTALS).isOk());
    }

    @Test
    void aNewRevisionRecompilesIntentsTheNewTypeJoins() {
        RouterService s = service();
        s.activateCatalog(catalog(1)).value();
        s.declareIntent(TOTALS).value();
        assertEquals(3, only(s).routes().size());
        assertTrue(s.activateCatalog(catalog(2)).isOk());
        assertEquals(List.of("InvoiceAdjusted", "OrderAmended", "OrderPlaced", "RefundIssued"), only(s).routes().stream().map(r -> r.event().name()).toList());
        assertEquals(2, only(s).revision());
    }

    @Test
    void aRevisionThatBreaksADeclaredIntentIsRefusedAndTheOldOneStaysActive() {
        RouterService s = service();
        s.activateCatalog(catalog(1)).value();
        s.declareIntent(Intent.parse("shipments", "emits(OrderShipped)").value()).value();
        ObjectNode withoutShipped = (ObjectNode) catalog(2).toJson();
        ArrayNode events = (ArrayNode) withoutShipped.get("events");
        for (int i = events.size() - 1; i >= 0; i--) {
            if (events.get(i).get("type").asText().equals("OrderShipped")) {
                events.remove(i);
            }
        }
        Result<Catalog> res = fromJson(withoutShipped).flatMap(s::activateCatalog);
        assertEquals(List.of("would break intent 'shipments': unknown event type 'OrderShipped' in emits(OrderShipped)"), res.errors());
        assertEquals(Optional.of(1), s.catalog().map(Catalog::revision));
    }

    @Test
    void removingAnAttributeThatPublishedVersionsDependOnIsARedefinition() {
        RouterService s = service();
        s.activateCatalog(catalog(1)).value();
        ObjectNode broken = (ObjectNode) catalog(2).toJson();
        ((ObjectNode) broken.get("entities").get("Order")).remove("total");
        for (JsonNode e : broken.get("events")) {
            ArrayNode effects = (ArrayNode) e.get("effects");
            for (int i = effects.size() - 1; i >= 0; i--) {
                if (effects.get(i).get("attribute").asText().endsWith("Order.total")) {
                    effects.remove(i);
                }
            }
        }
        Result<Catalog> res = fromJson(broken).flatMap(s::activateCatalog);
        assertTrue(res.errors().stream().anyMatch(e -> e.startsWith("OrderPlaced@v1 is already published")), res.errors().toString());
    }

    @Test
    void aPublishedVersionCannotBeRedefinedInPlace() {
        RouterService s = service();
        s.activateCatalog(catalog(1)).value();
        ObjectNode edited = (ObjectNode) catalog(2).toJson();
        ((ObjectNode) edited.get("events").get(1).get("fields")).put("newTotal", "long");
        Result<Catalog> res = fromJson(edited).flatMap(s::activateCatalog);
        assertEquals("OrderAmended@v1 is already published and cannot change (fields changed); add version 2 instead", res.errors().get(0));
    }

    @Test
    void revisionsOnlyMoveForward() {
        RouterService s = service();
        s.activateCatalog(catalog(2)).value();
        assertTrue(s.activateCatalog(catalog(1)).errors().get(0).contains("not newer"));
    }

    // ---- the catalog's own intents -----------------------------------------------------------------------------------

    @Test
    void theShippedCatalogSeedsItsOwnIntents() {
        RouterService s = service();
        s.activateCatalog(shipped()).value();
        assertEquals(List.of("order-dispatched", "order-total-changes", "orders-placed", "ready-to-ship"), List.copyOf(s.applications().keySet()));
        assertTrue(s.declaredByCatalog("ready-to-ship"));
        assertEquals(List.of("inventory.reserved", "payments.authorized"), s.routingTable("ready-to-ship").orElseThrow().sourceTopics());
        assertEquals(Map.of(), s.runtimeIntents());
    }

    @Test
    void anIntentTheCatalogDeclaresCannotBeRedeclaredDifferentlyAtRuntime() {
        RouterService s = service();
        s.activateCatalog(shipped()).value();
        Result<RoutingTable> same = s.declareIntent(Intent.parse("orders-placed", "emits(OrderPlaced)", "Order").value());
        assertTrue(same.isOk(), same.errors().toString());
        Result<RoutingTable> other = s.declareIntent(Intent.parse("orders-placed", "emits(OrderDispatched)", "Order").value());
        assertTrue(other.errors().get(0).contains("is declared by catalog revision 1"), other.errors().toString());
        assertEquals(1, s.applications().get("orders-placed").generation());
    }

    @Test
    void aCatalogThatComesToDeclareARuntimeIntentTakesItOver() {
        InMemoryRegistry registry = new InMemoryRegistry();
        RouterService s = service(registry);
        s.activateCatalog(catalog(1)).value();
        s.declareIntent(TOTALS).value();
        assertEquals(List.of("order-total-changes"), List.copyOf(registry.load().intents().keySet()));

        ObjectNode rev2 = (ObjectNode) catalog(2).toJson();
        rev2.putArray("intents").addObject().put("name", "order-total-changes").put("predicate", "changes(Order.total)");
        s.activateCatalog(fromJson(rev2).value()).value();
        assertTrue(s.declaredByCatalog("order-total-changes"));
        assertEquals(Map.of(), registry.load().intents());
    }

    @Test
    void aCatalogWithoutMeaningCannotCarryRuntimeIntents() {
        RouterService s = service();
        s.activateCatalog(catalog(1)).value();
        s.declareIntent(TOTALS).value();
        ObjectNode bare = (ObjectNode) catalog(1).toJson();
        bare.put("revision", 3);
        bare.remove("entities");
        for (JsonNode e : bare.get("events")) {
            ((ObjectNode) e).remove(List.of("fields", "keys", "effects"));
        }
        Result<Catalog> res = fromJson(bare).flatMap(s::activateCatalog);
        assertFalse(res.isOk());
        assertTrue(res.errors().stream().anyMatch(e -> e.contains("meaning removed")), res.errors().toString());
    }

    // ---- one Streams application per intent --------------------------------------------------------------------------

    @Test
    void eachIntentHasItsOwnApplicationIdAndStateDirectory() {
        RouterService s = service();
        s.activateCatalog(shipped()).value();
        IntentApplication totals = s.applications().get("order-total-changes");
        assertEquals("rif-router-order-total-changes", totals.applicationId());
        assertEquals(Path.of("build-state", "order-total-changes"), totals.stateDir());
        assertEquals("rif-router-order-total-changes", SETTINGS.propertiesFor("order-total-changes").get("application.id"));
        assertEquals("earliest", SETTINGS.propertiesFor("order-total-changes").get("consumer.auto.offset.reset"));
        assertEquals(4, s.applications().values().stream().map(IntentApplication::applicationId).distinct().count());
        assertEquals(4, s.applications().values().stream().map(IntentApplication::topology).distinct().count());
        assertTrue(totals.topology().describe().toString().contains("orders.placed"));
        assertFalse(totals.topology().describe().toString().contains("payments.authorized"), "an application reads only its own intent's topics");
    }

    @Test
    void declaringASecondIntentLeavesTheFirstApplicationAlone() {
        RouterService s = service();
        s.activateCatalog(catalog(1)).value();
        s.declareIntent(TOTALS).value();
        IntentApplication first = s.applications().get("order-total-changes");

        s.declareIntent(Intent.parse("shipments", "emits(OrderShipped)").value()).value();
        assertSame(first, s.applications().get("order-total-changes"), "the first intent is not restarted");
        assertEquals(1, first.generation());
        IntentApplication second = s.applications().get("shipments");
        assertEquals("rif-router-shipments", second.applicationId());
        assertNotEquals(first.applicationId(), second.applicationId());
        assertEquals(List.of("orders.shipped"), second.table().sourceTopics());
    }

    @Test
    void redeclaringAnIntentRestartsOnlyThatIntentUnderTheSameApplicationId() {
        RouterService s = service();
        s.activateCatalog(catalog(1)).value();
        s.declareIntent(TOTALS).value();
        s.declareIntent(Intent.parse("shipments", "emits(OrderShipped)").value()).value();
        IntentApplication totals = s.applications().get("order-total-changes");
        IntentApplication before = s.applications().get("shipments");

        s.declareIntent(Intent.parse("shipments", "emits(OrderShipped) or emits(OrderPlaced)").value()).value();
        IntentApplication after = s.applications().get("shipments");
        assertNotSame(before, after);
        assertEquals(2, after.generation());
        assertEquals(before.applicationId(), after.applicationId());
        assertEquals(List.of("orders.placed", "orders.shipped"), after.table().sourceTopics());
        assertSame(totals, s.applications().get("order-total-changes"));

        s.declareIntent(Intent.parse("shipments", "emits(OrderShipped) or emits(OrderPlaced)").value()).value();
        assertSame(after, s.applications().get("shipments"), "the same declaration again changes nothing");
    }

    @Test
    void aNewCatalogRestartsOnlyTheIntentsWhoseRoutesChange() {
        RouterService s = service();
        s.activateCatalog(shipped()).value();
        Map<String, IntentApplication> before = s.applications();

        ObjectNode rev2 = (ObjectNode) shipped().toJson();
        rev2.put("revision", 2);
        ArrayNode events = (ArrayNode) rev2.get("events");
        JsonNode dispatched = null;
        for (JsonNode e : events) {
            if (e.get("type").asText().equals("OrderDispatched")) {
                dispatched = e;
            }
        }
        events.add(((ObjectNode) dispatched.deepCopy()).put("version", 2));
        s.activateCatalog(fromJson(rev2).value()).value();

        Map<String, IntentApplication> after = s.applications();
        assertNotSame(before.get("order-dispatched"), after.get("order-dispatched"));
        assertEquals(2, after.get("order-dispatched").generation());
        assertEquals(2, after.get("order-dispatched").table().routes().size());
        for (String unchanged : List.of("order-total-changes", "orders-placed", "ready-to-ship")) {
            assertSame(before.get(unchanged), after.get(unchanged), unchanged + " is not restarted");
            assertEquals(2, after.get(unchanged).table().revision(), unchanged + " routes under the new revision");
            assertTrue(after.get(unchanged).routing().known().contains(new rif.catalog.EventRef("OrderDispatched", 2)));
        }
    }

    @Test
    void anIntentTheCatalogDropsIsStopped() {
        RouterService s = service();
        s.activateCatalog(shipped()).value();
        ObjectNode rev2 = (ObjectNode) shipped().toJson();
        rev2.put("revision", 2);
        ArrayNode intents = (ArrayNode) rev2.get("intents");
        for (int i = intents.size() - 1; i >= 0; i--) {
            if (intents.get(i).get("name").asText().equals("orders-placed")) {
                intents.remove(i);
            }
        }
        for (JsonNode k : rev2.get("consumers")) {
            if ("orders-placed".equals(Json.text(k, "intent"))) {
                ((ObjectNode) k).remove("intent");
            }
        }
        Result<Catalog> res = fromJson(rev2).flatMap(s::activateCatalog);
        assertTrue(res.isOk(), res.errors().toString());
        assertFalse(s.applications().containsKey("orders-placed"));
        assertEquals(3, s.applications().size());
    }

    @Test
    void aRestartedRouterRunsWhatTheRegistryHeld() throws Exception {
        InMemoryRegistry registry = new InMemoryRegistry();
        RouterService first = service(registry);
        first.activateCatalog(shipped()).value();
        first.declareIntent(Intent.parse("payments", "emits(PaymentAuthorized) or emits(PaymentDeclined)", "Order").value()).value();

        RouterService second = service(registry);
        assertTrue(second.applications().isEmpty());
        second.start();
        assertEquals(List.of("order-dispatched", "order-total-changes", "orders-placed", "payments", "ready-to-ship"), List.copyOf(second.applications().keySet()));
        assertFalse(second.declaredByCatalog("payments"));
        assertEquals("NOT_RUNNING", second.streamsState());
    }

    @Test
    void anIntentThatCannotStartIsReportedAndLeavesTheOthersAlone() {
        StreamsSettings broken = new StreamsSettings("dummy:9092", "rif-router", Path.of("build-state"), "no_such_guarantee", 1);
        RouterService s = new RouterService(new InMemoryRegistry(), null, broken, new IntentTopology.Ordering(Duration.ofSeconds(1), Duration.ofMillis(100)), true);
        s.activateCatalog(shipped()).value();
        assertEquals(4, s.applications().size());
        assertEquals("ERROR", s.streamsState());
        assertTrue(s.applications().get("orders-placed").failure().contains("processing.guarantee"), s.applications().get("orders-placed").failure());
        s.close();
    }
}
