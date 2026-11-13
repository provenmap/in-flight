package rif.discovery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import rif.catalog.Catalog;
import rif.catalog.Json;
import rif.catalog.JsonPatch;
import rif.catalog.Records;
import rif.catalog.Result;

/** A draft from a cluster that knows nothing of this tool: plain JSON, cents, a Spring dead-letter topic. */
class DrafterTest {

    static Sampler.Sampled rec(long offset, String json, Map<String, String> headers) {
        ObjectNode data = (ObjectNode) Json.parse(json).value();
        return new Sampler.Sampled(0, offset, 0, headers, "json", Result.ok(new Records.Decoded(null, null, null, data, null)));
    }

    static Discovery.Found found() {
        return new Discovery.Found("k:9092",
                List.of(new Discovery.Topic("checkout.orders", 3, 120, "flow"), new Discovery.Topic("checkout.payments", 3, 100, "flow"),
                        new Discovery.Topic("payments-service.DLT", 1, 4, "dlq"), new Discovery.Topic("__consumer_offsets", 50, 0, "internal")),
                List.of(new Discovery.Group("payments-service", "stable", 1, List.of("checkout.orders"), 0),
                        new Discovery.Group("ledger-app", "stable", 1, List.of("checkout.payments"), 3)),
                List.of(new Discovery.DeadLetterQueue("payments-service.DLT", List.of("its name marks it as a dead-letter queue"), List.of("checkout.orders"), "payments-service")),
                List.of(), null);
    }

    static Map<String, List<Sampler.Sampled>> samples() {
        Map<String, List<Sampler.Sampled>> m = new LinkedHashMap<>();
        List<Sampler.Sampled> orders = new ArrayList<>();
        List<Sampler.Sampled> payments = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            orders.add(rec(i, "{\"order_id\":\"ord-" + i + "\",\"customer_id\":\"cus-" + i + "\",\"total_cents\":" + (1000 + i) + ",\"currency\":\"EUR\",\"items\":" + i + "}", Map.of()));
            payments.add(rec(i, "{\"orderId\":\"ord-" + i + "\",\"userId\":\"cus-" + i + "\",\"amount\":\"" + (10 + i) + ".50\",\"fee\":0.3}", Map.of()));
        }
        m.put("checkout.orders", orders);
        m.put("checkout.payments", payments);
        return m;
    }

    @Test
    void aDraftNamesTheMoneyTheOrderAndTheCustomerAndAsksAboutTheRest() {
        Drafter.Draft d = Drafter.draft("checkout", found(), samples(), null);
        JsonNode c = d.catalog();
        assertEquals("EUR", c.get("currency").asText(), "the records' currency");
        JsonNode orders = c.get("events").get(0);
        assertEquals("CheckoutOrders", orders.get("type").asText());
        assertEquals("total_cents", orders.get("value").asText());
        assertTrue(orders.get("minorUnits").asBoolean());
        assertEquals("order_id", orders.get("order").asText());
        assertEquals("customer_id", orders.get("customer").asText());
        JsonNode payments = c.get("events").get(1);
        assertEquals("amount", payments.get("value").asText(), "a number written as text, not the fee");
        assertEquals("orderId", payments.get("order").asText());
        assertEquals("userId", payments.get("customer").asText());
        JsonNode pay = c.get("consumers").get(0);
        assertEquals("payments", pay.get("district").asText());
        assertEquals("payments-service.DLT", pay.get("dlq").asText());
        assertEquals("payments-service-redrive", pay.get("redrive").asText());
        assertEquals("ledger", c.get("consumers").get(1).get("district").asText());
        // The draft is a usable catalog as it is.
        Result<Catalog> parsed = Catalog.fromJson(c);
        assertTrue(parsed.isOk(), parsed.error());
        // Every guess is asked about, least sure first, each answer a patch that applies.
        assertFalse(d.guesses().isEmpty());
        for (int i = 1; i < d.guesses().size(); i++) {
            assertTrue(d.guesses().get(i - 1).confidence() <= d.guesses().get(i).confidence());
        }
        for (Drafter.Guess g : d.guesses()) {
            for (Drafter.Option o : g.options()) {
                Result<JsonNode> applied = JsonPatch.apply(c, o.patch());
                assertTrue(applied.isOk(), g.id() + " / " + o.label() + ": " + applied.error());
            }
        }
        Drafter.Guess type = d.guesses().stream().filter(g -> g.id().equals("events.0.type")).findFirst().orElseThrow();
        assertTrue(type.question().contains("checkout.orders"));
    }

    @Test
    void theDryRunPricesTheDraftOnItsOwnSamples() {
        Drafter.Draft d = Drafter.draft("checkout", found(), samples(), null);
        Validator.Report r = Validator.validate(d.catalog(), found(), samples());
        assertTrue(r.ok(), Validator.text(r));
        Validator.EventRun orders = r.events().get(0);
        assertEquals(20, orders.priced());
        assertEquals("10.00", orders.min().toPlainString(), "cents made euros");
        assertTrue(Validator.text(r).contains("20 of 20 priced"));
    }

    @Test
    void topicsWithNothingReadableAreLeftOutAndSaidSo() {
        Map<String, List<Sampler.Sampled>> s = samples();
        s.put("checkout.payments", List.of(new Sampler.Sampled(0, 0, 0, Map.of(), "unknown", Result.err("the record is not JSON"))));
        Drafter.Draft d = Drafter.draft("checkout", found(), s, null);
        assertEquals(1, d.catalog().get("events").size());
        assertTrue(d.skipped().get(0).startsWith("checkout.payments: none of its records could be read"));
    }

    @Test
    void namesAndShapes() {
        assertEquals("OrderPlaced", Drafter.typeFromTopic("orders.placed"));
        assertEquals("PaymentAuthorized", Drafter.typeFromTopic("payments-authorized-v2"));
        assertEquals("payments", Drafter.districtOfGroup("payments-service"));
        assertEquals("dlq", Discovery.kindOf("orders.dlq"));
        assertEquals("dlq", Discovery.kindOf("dlq-orders"));
        assertEquals("dlq", Discovery.kindOf("payments-service.DLT"));
        assertEquals("internal", Discovery.kindOf("__consumer_offsets"));
        assertEquals("retry", Discovery.kindOf("orders-retry-2"));
        assertEquals("flow", Discovery.kindOf("dltrades"));
        assertEquals("payments", Discovery.stem("payments.dlq"));
        assertEquals("payments-service", Discovery.owner("payments.dlq", List.of(), found().groups()));
        assertNull(Discovery.owner("shipping.dlq", List.of(), found().groups()));
        assertEquals("xxx-99999", Sampler.mask(Json.tree("ord-81723")));
        assertEquals("999.99", Sampler.mask(Json.tree("129.95")));
        assertEquals("Xxx", Sampler.mask(Json.tree("Bob")));
        Sampler.Shape shape = Sampler.shape("checkout.orders", samples().get("checkout.orders"), false);
        Sampler.Field total = shape.fields().stream().filter(f -> f.path().equals("total_cents")).findFirst().orElseThrow();
        assertEquals(1.0, total.presence());
        assertTrue(total.wholeNumbers());
        assertEquals("9999", total.example());
    }
}
