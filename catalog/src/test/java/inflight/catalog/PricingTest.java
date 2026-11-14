package inflight.catalog;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;

import inflight.catalog.Pricing.Priced;

/** Ported from src/pricing/pricing.test.ts; pricing by meaning (the merge) is at the end. */
class PricingTest {

    final Pricing pricing = new Pricing(Catalog.defaultCatalog());
    static final Instant AT = Instant.parse("2026-10-30T12:00:00Z");

    static ObjectNode data(String json) {
        return (ObjectNode) Json.parse(json).value();
    }

    static CloudEvent event(String type, int version, ObjectNode data) {
        return new CloudEvent("e-1", "/test", type, version, AT, 1, data, null);
    }

    static CloudEvent placedV1() {
        return event("OrderPlaced", 1, data("{\"orderId\":\"o-1\",\"customerId\":\"c-42\",\"total\":\"129.90\",\"currency\":\"USD\"}"));
    }

    static CloudEvent placedV2() {
        return event("OrderPlaced", 2, data("{\"orderId\":\"o-2\",\"customerId\":\"c-7\",\"total\":{\"amount\":\"1049.00\",\"currency\":\"USD\"}}"));
    }

    static String plain(BigDecimal b) {
        return b.stripTrailingZeros().toPlainString();
    }

    @Test
    void orderPlacedV1IsWorthItsTotal() {
        Priced want = new Priced("e-1", new EventRef("OrderPlaced", 1), "orders.placed", "orders", new BigDecimal("129.90"), "c-42", "o-1", null, null);
        assertEquals(want, pricing.price(placedV1()).value());
    }

    @Test
    void orderPlacedV2IsWorthTotalAmount() {
        Priced p = pricing.price(placedV2()).value();
        assertEquals(List.of("orders", "1049.00", "c-7", "o-2"), List.of(p.district(), p.value().setScale(2).toPlainString(), p.customer(), p.orderId()));
    }

    @Test
    void everyDownstreamEventIsPricedInItsOwnDistrict() {
        String[][] cases = {{"PaymentAuthorized", "amount", "payments"}, {"PaymentDeclined", "amount", "payments"},
                {"StockReserved", "value", "inventory"}, {"OrderDispatched", "orderTotal", "shipping"}};
        for (String[] c : cases) {
            Priced p = pricing.price(event(c[0], 1, data("{\"orderId\":\"o-3\",\"customerId\":\"c-1\",\"" + c[1] + "\":\"15.25\"}"))).value();
            assertEquals(List.of(c[2], "15.25"), List.of(p.district(), p.value().toPlainString()), c[0]);
        }
    }

    @Test
    void aJsonNumberIsAnAmountToo() {
        Priced p = pricing.price(event("PaymentAuthorized", 1, data("{\"orderId\":\"o\",\"customerId\":\"c\",\"amount\":12.5}"))).value();
        assertEquals("12.5", plain(p.value()));
    }

    @Test
    void v2DataReadWithTheV1PathIsNotGuessedAt() {
        assertEquals(List.of("OrderPlaced@v1 (e-1): no amount at 'total'"), pricing.price(placedV2().withVersion(1)).errors());
    }

    @Test
    void anUndeclaredTypeIsAnError() {
        assertEquals(List.of("OrderAmended@v1 (e-1) is not in the catalog"), pricing.price(placedV1().withType("OrderAmended")).errors());
    }

    @Test
    void anUndeclaredVersionIsPricedByTheNewestVersionOfItsTypeThatReadsItAndSaysSo() {
        Priced p = pricing.price(placedV1().withVersion(3)).value();
        assertEquals(new EventRef("OrderPlaced", 3), p.ref());
        assertEquals(new EventRef("OrderPlaced", 1), p.pricedAs());
        assertEquals("129.9", plain(p.value()));
        // A shape no declared version reads stays unpriced: the value is never guessed.
        CloudEvent v3 = event("OrderPlaced", 3, data("{\"orderId\":\"o-3\",\"customerId\":\"c\",\"total\":{\"cents\":12990}}"));
        assertEquals(List.of("OrderPlaced@v3 (e-1) is not in the catalog, and no version of OrderPlaced it has can read it"), pricing.price(v3).errors());
    }

    @Test
    void aMissingCustomerOrOrderIsAnError() {
        CloudEvent noCustomer = event("StockReserved", 1, data("{\"orderId\":\"o\",\"value\":\"1.00\"}"));
        assertEquals(List.of("StockReserved@v1 (e-1): no customer at 'customerId'"), pricing.price(noCustomer).errors());
    }

    @Test
    void aRedrivenEventKeepsTheIdOfTheOneItRecoversThroughTheWireFormat() {
        CloudEvent redriven = event("PaymentAuthorized", 1, data("{\"orderId\":\"o\",\"customerId\":\"c\",\"amount\":\"9.99\"}")).withId("e-2").withRedriveOf("e-1");
        assertEquals(redriven, CloudEvent.parse(redriven.toJson()).value());
        assertEquals("e-1", pricing.priceBytes(redriven.toJson().getBytes(UTF_8)).value().redriveOf());
    }

    @Test
    void aFreshEventRoundTripsAndHasNoRedriveMarker() {
        CloudEvent e = CloudEvent.create("/orders", "OrderPlaced", 1, AT, 7, data("{\"orderId\":\"o\",\"customerId\":\"c\",\"total\":\"1.00\"}"), null);
        assertEquals(e, CloudEvent.parse(e.toJson()).value());
        assertFalse(e.toJson().contains("redriveof"));
        assertTrue(e.toJson().contains("\"time\":\"2026-10-30T12:00:00.000Z\""), e.toJson());
    }

    @Test
    void anEnvelopeWithoutASchemaversionOrDataIsRejected() {
        assertEquals(List.of("T (x) has no whole schemaversion"), CloudEvent.parse("{\"id\":\"x\",\"source\":\"s\",\"type\":\"T\",\"data\":{}}").errors());
        assertEquals(List.of("T (x) has no data object"), CloudEvent.parse("{\"id\":\"x\",\"source\":\"s\",\"type\":\"T\",\"schemaversion\":1}").errors());
        assertFalse(CloudEvent.parse("not json").isOk());
    }

    // ---- records from other producers ----

    static final Map<String, String> NONE = Map.of();
    static final String DATA = "{\"orderId\":\"o-9\",\"customerId\":\"c-9\",\"amount\":\"42.00\"}";

    @Test
    void plainJsonIsPricedByItsTopic() {
        Priced p = pricing.priceRecord(DATA.getBytes(UTF_8), NONE, List.of("payments.authorized"), "payments.authorized:0:12").value();
        assertEquals(List.of("PaymentAuthorized@v1", "payments.authorized:0:12", "42"), List.of(p.ref().show(), p.eventId(), plain(p.value())));
    }

    @Test
    void plainJsonOnATopicWithSeveralVersionsTakesTheNewestOneThatReadsIt() {
        Priced v1 = pricing.priceRecord("{\"orderId\":\"o\",\"customerId\":\"c\",\"total\":\"10.00\"}".getBytes(UTF_8), NONE, List.of("orders.placed"), "x").value();
        Priced v2 = pricing.priceRecord("{\"orderId\":\"o\",\"customerId\":\"c\",\"total\":{\"amount\":\"11.00\"}}".getBytes(UTF_8), NONE, List.of("orders.placed"), "x").value();
        assertEquals(List.of("OrderPlaced@v1", "OrderPlaced@v2"), List.of(v1.ref().show(), v2.ref().show()));
    }

    @Test
    void aBinaryModeCloudEventHasItsAttributesInCeHeaders() {
        Map<String, String> headers = Map.of("ce_id", "b-1", "ce_type", "PaymentAuthorized", "ce_schemaversion", "1", "ce_redriveof", "e-0");
        Priced p = pricing.priceRecord(DATA.getBytes(UTF_8), headers, List.of("payments.authorized"), "x").value();
        assertEquals(List.of("b-1", "e-0"), List.of(p.eventId(), p.redriveOf()));
    }

    @Test
    void schemaRegistryFramingIsDropped() {
        byte[] body = DATA.getBytes(UTF_8);
        byte[] framed = new byte[body.length + 5];
        framed[4] = 7;
        System.arraycopy(body, 0, framed, 5, body.length);
        assertTrue(pricing.priceRecord(framed, NONE, List.of("payments.authorized"), "x").isOk());
    }

    @Test
    void aStructuredCloudEventWithoutASchemaversionIsPricedByItsType() {
        String e = "{\"specversion\":\"1.0\",\"id\":\"s-1\",\"source\":\"/s\",\"type\":\"StockReserved\",\"data\":{\"orderId\":\"o\",\"customerId\":\"c\",\"value\":\"3.00\"}}";
        assertEquals("StockReserved@v1", pricing.priceRecord(e.getBytes(UTF_8), NONE, List.of(), "x").value().ref().show());
    }

    @Test
    void decodingErrorsSayWhatWasWrong() {
        assertFalse(Records.decode("nope".getBytes(UTF_8), NONE).isOk());
        assertEquals(List.of("the record is not a JSON object"), Records.decode("[1]".getBytes(UTF_8), NONE).errors());
        assertEquals(List.of("a record on nowhere (x) has no event type in the catalog"),
                pricing.priceRecord("{\"a\":1}".getBytes(UTF_8), NONE, List.of("nowhere"), "x").errors());
    }

    // ---- pricing by meaning: roles instead of paths ----

    /** The shipped catalog with every explicit path taken out: case, party and worth come from keys and effects. */
    static Pricing byMeaning() {
        return new Pricing(Fixtures.edit(j -> j.get("events").forEach(e -> ((ObjectNode) e).remove(List.of("value", "customer", "order")))).value());
    }

    @Test
    void withRolesAnOrderIsWorthItsEffectOnTheMeasureAndItsCaseAndPartyComeFromItsKeys() {
        Pricing p = byMeaning();
        Priced placed = p.price(placedV2()).value();
        assertEquals(List.of("1049.00", "c-7", "o-2"), List.of(placed.value().toPlainString(), placed.customer(), placed.orderId()));
        assertEquals(List.of("OrderPlaced@v1 (e-1): no amount at 'total'"), p.price(placedV2().withVersion(1)).errors());
    }

    @Test
    void withRolesALaterEventIsWorthWhatItsCaseWasSeenToBeWorthAndUnknownBeforeThat() {
        Pricing p = byMeaning();
        CloudEvent stock = event("StockReserved", 1, data("{\"orderId\":\"o-1\",\"customerId\":\"c-42\",\"value\":\"999.00\"}"));
        Priced unseen = p.price(stock).value();
        assertNull(unseen.value(), "a worth that cannot be established is unknown, never guessed");
        assertFalse(unseen.known());

        p.price(placedV1());
        assertEquals("129.90", p.price(stock).value().value().toPlainString());
    }
}
