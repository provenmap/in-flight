package rif.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static rif.catalog.Fixtures.edit;
import static rif.catalog.Fixtures.errors;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Ported from src/pricing/catalog.test.ts; what the merge adds is at the end. */
class CatalogTest {

    final Catalog c = Catalog.load(Fixtures.SHIPPED).value();

    private static ObjectNode at(ObjectNode j, String array, int i) {
        return (ObjectNode) j.get(array).get(i);
    }

    @Test
    void theShippedCatalogLoadsAndTheDefaultIsThatFile() {
        assertEquals(c, Catalog.defaultCatalog());
    }

    @Test
    void districtsTopicsAndDeadLetterQueues() {
        assertEquals(List.of("orders", "payments", "inventory", "shipping", "customers"), c.districts().stream().map(Catalog.District::id).toList());
        assertEquals(List.of("customers"), c.districts().stream().filter(Catalog.District::sink).map(Catalog.District::id).toList());
        assertEquals(List.of("orders.placed", "payments.authorized", "payments.declined", "inventory.reserved", "shipping.dispatched"), c.topics());
        assertEquals(List.of("payments.dlq", "inventory.dlq", "shipping.dlq"), c.dlqs());
        assertEquals(List.of("payments-service", "inventory-service", "shipping-service"), c.services().stream().map(Catalog.Consumer::group).toList());
    }

    @Test
    void whoReadsWhat() {
        assertEquals(List.of("payments", "inventory"), c.readersOf("orders.placed").stream().map(Catalog.Consumer::district).toList());
        assertEquals(List.of("shipping"), c.readersOf("payments.authorized").stream().map(Catalog.Consumer::district).toList());
        assertEquals(List.of(), c.readersOf("payments.declined"));
        assertEquals(List.of("customers"), c.readersOf("shipping.dispatched").stream().map(Catalog.Consumer::name).toList());
        assertEquals("payments-service", c.ownerOfDlq("payments.dlq").orElseThrow().group());
        assertEquals("inventory", c.districtOf("inventory.reserved"));
        assertEquals("payments-service-redrive", c.redriveGroupOf("payments.dlq"));
        assertNull(c.redriveGroupOf("inventory.dlq"));
    }

    @Test
    void aRedriveGroupNeedsADeadLetterQueueAndCannotReuseAServicesGroup() {
        assertEquals(List.of("consumer customers: a redrive group needs a dead-letter queue"),
                errors(edit(j -> at(j, "consumers", 3).put("redrive", "customers-redrive"))));
        assertEquals(List.of("consumer group 'inventory-service' is declared twice"),
                errors(edit(j -> at(j, "consumers", 0).put("redrive", "inventory-service"))));
    }

    @Test
    void bothOrderPlacedVersionsShareATopicButDeclareTheirValueInDifferentPlaces() {
        Catalog.EventSpec v1 = c.find(new EventRef("OrderPlaced", 1)).orElseThrow();
        Catalog.EventSpec v2 = c.find(new EventRef("OrderPlaced", 2)).orElseThrow();
        assertEquals(v1.topic(), v2.topic());
        assertEquals("total", c.measureEffects(v1).get(0).source() instanceof Effect.FromField f ? f.field().show() : null);
        assertEquals("total.amount", c.measureEffects(v2).get(0).source() instanceof Effect.FromField f ? f.field().show() : null);
    }

    @Test
    void anUnknownDistrictIsRejected() {
        assertEquals(List.of("PaymentAuthorized@v1: unknown district 'billing'"), errors(edit(j -> at(j, "events", 2).put("district", "billing"))));
    }

    @Test
    void aConsumerOfAnUnknownTopicIsRejected() {
        assertEquals(List.of("consumer shipping-service: reads unknown topic 'billing.invoices'"),
                errors(edit(j -> ((ArrayNode) at(j, "consumers", 2).get("reads")).add("billing.invoices"))));
    }

    @Test
    void aVersionDeclaredTwiceIsRejected() {
        List<String> errs = errors(edit(j -> ((ArrayNode) j.get("events")).add(at(j, "events", 0).deepCopy())));
        assertTrue(errs.contains("OrderPlaced@v1 is declared twice"), errs.toString());
    }

    @Test
    void aDeadLetterQueueWithoutAConsumerGroupIsRejected() {
        assertEquals(List.of("consumer customers: a dead-letter queue needs a consumer group"),
                errors(edit(j -> at(j, "consumers", 3).put("dlq", "customers.dlq"))));
    }

    @Test
    void aSinkCannotProduce() {
        assertEquals(List.of("OrderDispatched@v1: a sink cannot produce events"), errors(edit(j -> at(j, "events", 5).put("district", "customers"))));
    }

    @Test
    void aBadValuePathAndAMissingVersionAreBothReported() {
        Result<Catalog> res = edit(j -> {
            at(j, "events", 0).put("value", "total..amount");
            at(j, "events", 1).remove("version");
        });
        assertEquals(Set.of("events[0]: value: 'total..amount' is not a dotted field path", "events[1]: 'version' must be a whole number"),
                Set.copyOf(errors(res)));
    }

    @Test
    void aMissingStringAndAMissingArrayAreNamed() {
        assertEquals(List.of("districts[1]: 'name' must be a string"), errors(edit(j -> at(j, "districts", 1).remove("name"))));
        // The flow's name is optional since the merge (intent-bus's catalogs have none), but it must be text.
        assertEquals(List.of("catalog: 'flow' must be a string"), errors(edit(j -> j.put("flow", 7))));
        assertEquals(List.of("'consumers' must be an array"), errors(edit(j -> j.putObject("consumers"))));
        assertEquals(List.of("'events' must be an array"), errors(edit(j -> j.remove("events"))));
    }

    @Test
    void consumersReadByIntentAndTheIntentSuppliesTheTopicsWhenNoneAreListed() {
        assertEquals(List.of("payments-service"), c.consumersOf("order-total-changes").stream().map(Catalog.Consumer::name).toList());
        assertEquals(List.of("inventory.reserved", "payments.authorized"), c.semantics().table("ready-to-ship").sourceTopics());

        Result<Catalog> byIntent = edit(j -> at(j, "consumers", 2).remove("reads"));
        assertEquals(List.of("inventory.reserved", "payments.authorized"), byIntent.value().consumer("shipping-service").orElseThrow().reads());

        assertEquals(List.of("consumer payments-service: reads by intent 'refunds', which the catalog does not declare"),
                errors(edit(j -> at(j, "consumers", 0).put("intent", "refunds"))));
        assertEquals(List.of("consumer shipping-service: intent 'ready-to-ship' draws on topic 'inventory.reserved', which it does not read"),
                errors(edit(j -> at(j, "consumers", 2).putArray("reads").add("payments.authorized"))));
        assertEquals(List.of("intent 'order-total-changes': Order has no attribute 'colour' (has: total, status, payment, stock)"),
                errors(edit(j -> at(j, "intents", 0).put("predicate", "changes(Order.colour)"))));
    }

    @Test
    void aCatalogWithNoMeaningDeclaredStillLoadsWithNoIntents() {
        Result<Catalog> plain = edit(j -> {
            j.remove("entities");
            j.remove("intents");
            j.remove("roles");
            j.get("events").forEach(e -> ((ObjectNode) e).remove(List.of("fields", "keys", "effects")));
            j.get("consumers").forEach(k -> ((ObjectNode) k).remove("intent"));
        });
        assertNull(plain.value().semantics());
        assertEquals(c.topics(), plain.value().topics());
    }

    // ---- the merge ---------------------------------------------------------------------------------------------------

    @Test
    void intentBusCatalogsLoadTheirProducerIsTheDistrict() {
        Catalog rev1 = Fixtures.intentBus(1);
        Catalog rev2 = Fixtures.intentBus(2);
        assertEquals(1, rev1.revision());
        assertEquals(2, rev2.revision());
        assertEquals(List.of("order-service", "billing-service"), rev1.districts().stream().map(Catalog.District::id).toList());
        assertEquals("invoice-service", rev2.find(new EventRef("InvoiceAdjusted", 1)).orElseThrow().district());
        assertNull(rev1.currency());
        assertFalse(rev1.prices());
    }

    @Test
    void theRevisionIsAWholeNumberOfAtLeastOne() {
        assertEquals(2, c.revision());
        assertEquals(List.of("catalog: 'revision' must be at least 1, was 0"), errors(edit(j -> j.put("revision", 0))));
        assertEquals(List.of("catalog: 'revision' must be a whole number"), errors(edit(j -> j.put("revision", "two"))));
    }

    @Test
    void rolesNameTheCaseThePartyAndTheMeasure() {
        assertEquals(new Catalog.Roles("Order", "Customer", new AttrRef("Order", "total")), c.roles());
        Catalog.EventSpec authorized = c.find(new EventRef("PaymentAuthorized", 1)).orElseThrow();
        assertEquals("orderId", c.orderPath(authorized).show());
        assertEquals("customerId", c.customerPath(authorized).show());

        assertEquals(List.of("roles: the measure Order.status must be numeric, is string"),
                errors(edit(j -> ((ObjectNode) j.get("roles")).put("measure", "Order.status"))));
        assertEquals(List.of("roles: the party 'Shopper' is not an entity"), errors(edit(j -> ((ObjectNode) j.get("roles")).put("party", "Shopper"))));
        assertEquals(List.of("PaymentDeclined@v1: no customer: declare 'customer', or a key for Customer"), errors(edit(j -> {
            ObjectNode declined = at(j, "events", 3);
            declined.remove("customer");
            ((ObjectNode) declined.get("keys")).remove("Customer");
        })));
    }

    @Test
    void reservedAndInvalidTopicNamesAreRejected() {
        List<String> errs = errors(edit(j -> {
            at(j, "events", 2).put("topic", "intent.payments");
            at(j, "consumers", 2).putArray("reads").add("intent.payments").add("inventory.reserved");
        }));
        assertTrue(errs.contains("PaymentAuthorized@v1: 'intent.payments' cannot be a source topic (reserved or invalid name)"), errs.toString());
    }

    @Test
    void itWritesBackOutAsItReadsIn() {
        assertEquals(c, Catalog.fromJson(c.toJson()).value());
        Catalog rev2 = Fixtures.intentBus(2);
        assertEquals(rev2, Catalog.fromJson(rev2.toJson()).value());
    }
}
