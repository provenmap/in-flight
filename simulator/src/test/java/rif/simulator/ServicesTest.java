package rif.simulator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import rif.catalog.Catalog;
import rif.catalog.CloudEvent;
import rif.catalog.Json;
import rif.catalog.Pricing;
import rif.catalog.Result;
import rif.simulator.Events.Emitter;
import rif.simulator.Events.Out;
import rif.simulator.Services.Inventory;
import rif.simulator.Services.Payments;
import rif.simulator.Services.Shipping;

/** Ported from src/simulator/services.test.ts. */
class ServicesTest {

    static final Instant AT = Instant.parse("2026-10-30T12:00:00Z");
    static final Emitter EMIT = new Emitter("/test");
    final Pricing pricing = new Pricing(Catalog.defaultCatalog());

    static Instant plus(long s) {
        return AT.plusSeconds(s);
    }

    static CloudEvent order(int version, int seed) {
        return Orders.placed(EMIT, Orders.seeded(seed), version, AT).event();
    }

    static CloudEvent order(int version) {
        return order(version, 1);
    }

    static String first(Predicate<String> pred) {
        for (int i = 1; ; i++) {
            if (pred.test("ord-" + i)) {
                return "ord-" + i;
            }
        }
    }

    /** An order id the issuer authorises, and one it declines. */
    static final String APPROVED = first(id -> !Payments.declines(id));
    static final String DECLINED = first(Payments::declines);

    static CloudEvent withOrderId(CloudEvent e, String id) {
        ObjectNode d = e.data().deepCopy();
        d.put("orderId", id);
        return e.withData(d);
    }

    static <T> T value(Result<T> r) {
        return r.value();
    }

    String priced(CloudEvent e) {
        Result<Pricing.Priced> p = pricing.price(e);
        return p.isOk() ? p.value().value().stripTrailingZeros().toPlainString() : p.error();
    }

    @Test
    void theHashesMatchTheJvmSoDecisionsMatchTheScalaBuild() {
        Object[][] jvm = {{"ord-1", -1725563276, 106004549}, {"ord-2", 1990441921, 106004550}, {"ord-17", -1068601923, -1008826222},
                {"ord-00a1b2c3d4", 1402889169, 339870956}, {"x", 659534019, 120}, {"", 377927480, 0}};
        for (Object[] c : jvm) {
            assertEquals(c[1], Events.scalaStringHash((String) c[0]), (String) c[0]);
            assertEquals(c[2], Events.javaHash((String) c[0]), (String) c[0]);
        }
    }

    @Test
    void ordersV1HasADecimalTotalV2AnAmountObjectAndBothAgreeWithTheItems() {
        for (int seed = 1; seed <= 50; seed++) {
            CloudEvent v1 = order(1, seed);
            CloudEvent v2 = order(2, seed);
            assertTrue(v1.data().get("total").isTextual());
            assertEquals("USD", v2.data().get("total").get("currency").asText());
            BigDecimal items = BigDecimal.ZERO;
            for (JsonNode i : v1.data().get("items")) {
                items = items.add(new BigDecimal(i.get("price").asText()).multiply(BigDecimal.valueOf(i.get("qty").asInt())));
            }
            assertEquals(0, new BigDecimal(v1.data().get("total").asText()).compareTo(items));
            assertEquals(priced(v1), priced(v2));
        }
    }

    @Test
    void ordersAbout2000CustomersAndBelievableTotals() {
        Orders.Rng rng = Orders.seeded(7);
        List<CloudEvent> orders = IntStream.range(0, 5000).mapToObj(i -> Orders.placed(EMIT, rng, 1, AT).event()).toList();
        double[] totals = orders.stream().mapToDouble(e -> Double.parseDouble(e.data().get("total").asText())).toArray();
        double mean = java.util.Arrays.stream(totals).average().orElse(0);
        Set<String> customers = new HashSet<>();
        orders.forEach(e -> customers.add(e.data().get("customerId").asText()));
        assertTrue(customers.size() > 1800);
        assertTrue(mean > 40, "mean order " + mean);
        assertTrue(mean < 300, "mean order " + mean);
        assertTrue(java.util.Arrays.stream(totals).max().orElse(0) > 1000, "some orders are big ones");
    }

    @Test
    void paymentsAuthorisesAV1OrderForItsTotal() {
        CloudEvent e = withOrderId(order(1), APPROVED);
        List<Out> out = value(Payments.handle(EMIT, e, false, AT));
        assertEquals(List.of(List.of(Payments.AUTHORIZED, APPROVED, "PaymentAuthorized")), out.stream().map(o -> List.of(o.topic(), o.key(), o.event().type())).toList());
        assertEquals(e.data().get("total").asText(), out.get(0).event().data().get("amount").asText());
        assertNull(out.get(0).event().redriveOf());
    }

    @Test
    void paymentsDeclinesAboutOneOrderInTwentyTheSameWayEveryTime() {
        long declined = IntStream.rangeClosed(1, 10000).mapToObj(i -> "ord-" + i).filter(Payments::declines).count();
        double rate = declined / 10000.0;
        assertTrue(rate > 0.04 && rate < 0.06, "rate " + rate);
        List<Out> out = value(Payments.handle(EMIT, withOrderId(order(1), DECLINED), false, AT));
        assertEquals(List.of(Payments.DECLINED), out.stream().map(Out::topic).toList());
        assertEquals("card declined", out.get(0).event().data().get("reason").asText());
    }

    @Test
    void unpatchedPaymentsCannotReadV2AndSaysWhy() {
        assertEquals(List.of("'total' is an object, expected a decimal string"), Payments.handle(EMIT, order(2), false, AT).errors());
    }

    @Test
    void patchedPaymentsReadsBothShapes() {
        for (int v : new int[] {1, 2}) {
            CloudEvent e = withOrderId(order(v), APPROVED);
            String amount = value(Payments.handle(EMIT, e, true, AT)).get(0).event().data().get("amount").asText();
            assertEquals(Events.formatMoney(value(pricing.price(e)).value()), amount, "v" + v);
        }
    }

    @Test
    void aRedrivenPaymentCarriesTheIdOfTheOrderEventItRecovers() {
        CloudEvent e = withOrderId(order(2), APPROVED);
        assertEquals(e.id(), value(Payments.handle(EMIT, e, true, AT, true)).get(0).event().redriveOf());
    }

    @Test
    void paymentsRefusesEventsThatAreNotOrders() {
        CloudEvent stock = value(Inventory.handle(EMIT, order(1), AT)).get(0).event();
        assertEquals(List.of("payments cannot read StockReserved@v1"), Payments.handle(EMIT, stock, true, AT).errors());
    }

    @Test
    void inventoryReservesStockForBothVersionsValuedFromTheItems() {
        for (int v : new int[] {1, 2}) {
            CloudEvent e = order(v, 3);
            List<Out> out = value(Inventory.handle(EMIT, e, AT));
            assertEquals(List.of(Inventory.RESERVED), out.stream().map(Out::topic).toList());
            assertEquals(priced(e), priced(out.get(0).event()), "v" + v);
        }
    }

    @Test
    void inventoryDeadLettersAnOrderWithoutItems() {
        CloudEvent e = order(1).withData((ObjectNode) Json.parse("{\"orderId\":\"o\",\"customerId\":\"c\",\"items\":[]}").value());
        assertEquals(List.of("no items"), Inventory.handle(EMIT, e, AT).errors());
        CloudEvent bad = order(1).withData((ObjectNode) Json.parse("{\"orderId\":\"o\",\"customerId\":\"c\",\"items\":[{\"sku\":\"x\",\"qty\":0,\"price\":\"1.00\"}]}").value());
        assertEquals(List.of("'qty' is not a positive whole number"), Inventory.handle(EMIT, bad, AT).errors());
    }

    static CloudEvent[] paymentAndStock(String id) {
        CloudEvent e = withOrderId(order(1), id);
        return new CloudEvent[] {value(Payments.handle(EMIT, e, false, AT)).get(0).event(), value(Inventory.handle(EMIT, e, AT)).get(0).event()};
    }

    @Test
    void shippingDispatchesOnceItHasPaymentAndStockInEitherOrder() {
        CloudEvent[] ps = paymentAndStock(APPROVED);
        for (CloudEvent[] pair : new CloudEvent[][] {{ps[0], ps[1]}, {ps[1], ps[0]}}) {
            Shipping s = new Shipping(EMIT);
            assertEquals(List.of(), value(s.handle(pair[0], AT)));
            assertEquals(1, s.pending());
            List<Out> out = value(s.handle(pair[1], AT));
            assertEquals(List.of(List.of(Shipping.DISPATCHED, APPROVED)), out.stream().map(o -> List.of(o.topic(), o.key())).toList());
            assertEquals(ps[0].data().get("amount").asText(), out.get(0).event().data().get("orderTotal").asText());
            assertEquals(0, s.pending());
        }
    }

    @Test
    void shippingWaitsForAPaymentHeldUpInADeadLetterQueueThenDispatchesTheRedrivenOne() {
        CloudEvent e = withOrderId(order(2), APPROVED);
        CloudEvent stock = value(Inventory.handle(EMIT, e, AT)).get(0).event();
        Shipping s = new Shipping(EMIT);
        assertEquals(List.of(), value(s.handle(stock, AT)));
        CloudEvent redriven = value(Payments.handle(EMIT, e, true, plus(120), true)).get(0).event();
        assertEquals(List.of(Shipping.DISPATCHED), value(s.handle(redriven, plus(120))).stream().map(Out::topic).toList());
    }

    @Test
    void shippingForgetsOrdersThatNeverComplete() {
        CloudEvent stock = paymentAndStock(DECLINED)[1];
        Shipping s = new Shipping(EMIT, 5 * 60_000L);
        s.handle(stock, AT);
        s.handle(paymentAndStock(APPROVED)[1], plus(301));
        assertEquals(1, s.pending());
    }

    @Test
    void shippingDeadLettersWhatItDoesNotRead() {
        assertEquals(List.of("shipping cannot read OrderPlaced@v1"), new Shipping(EMIT).handle(order(1), AT).errors());
    }
}
