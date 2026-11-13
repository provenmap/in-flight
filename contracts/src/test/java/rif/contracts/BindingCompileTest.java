package rif.contracts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.node.ObjectNode;

import rif.catalog.Catalog;
import rif.catalog.CloudEvent;
import rif.catalog.Json;

/**
 * The generated Java compiles with the JDK's compiler against Jackson alone, with every lint warning an error, and
 * decodes CloudEvents of each type it names into the entity they are about and their typed effects.
 */
class BindingCompileTest {

    @TempDir
    Path work;

    static String ce(String type, int version, String data) {
        ObjectNode d = (ObjectNode) Json.parse(data).value();
        return new CloudEvent("e-1", "test", type, version, Instant.parse("2026-11-13T10:00:00Z"), 7, d, null).toJson();
    }

    @Test
    void theShippedCatalogsBindingForPaymentsCompilesAndDecodesBothVersionsOfOrderPlaced() throws Exception {
        Compiled b = Compiled.compile(work, Fixtures.SIMULATOR, Fixtures.binding(Fixtures.shipped(), Fixtures.SIMULATOR));

        assertEquals("order-total-changes", b.constant("INTENT"));
        assertEquals("intent.order-total-changes", b.constant("TOPIC"));
        assertEquals(2, b.constant("CATALOG_REVISION"));
        assertEquals("Order", b.constant("ENTITY"));
        assertEquals("[EventVersion[type=OrderPlaced, version=1], EventVersion[type=OrderPlaced, version=2]]", b.constant("EVENTS").toString());

        Object v1 = b.event(ce("OrderPlaced", 1,
                "{\"orderId\": \"o-1\", \"customerId\": \"c-1\", \"total\": \"120.00\", \"currency\": \"USD\", \"placedAt\": \"2026-11-13T09:59:00.000Z\"}"));
        assertEquals("OrderPlacedV1", v1.getClass().getSimpleName());
        assertEquals("o-1", Compiled.entityId(v1));
        assertEquals(List.of("AssignOrderTotal(120.00)", "AssignOrderStatus(placed)"), Compiled.effects(v1));
        assertEquals("Meta(e-1, test, 2026-11-13T10:00:00Z, Optional[7])", Compiled.show(Compiled.call(v1, "meta")));

        Object v2 = b.event(ce("OrderPlaced", 2,
                "{\"orderId\": \"o-2\", \"customerId\": \"c-1\", \"total\": {\"amount\": 99.5, \"currency\": \"USD\"}, \"placedAt\": \"2026-11-13T09:59:00Z\"}"));
        assertEquals("o-2", Compiled.entityId(v2));
        assertEquals(List.of("AssignOrderTotal(99.5)", "AssignOrderStatus(placed)"), Compiled.effects(v2));
        assertEquals("Total(99.5, USD)", Compiled.show(Compiled.call(v2, "total")));

        assertEquals("UnknownEvent(PaymentAuthorized, 1)", Compiled.show(b.decode(ce("PaymentAuthorized", 1, "{\"orderId\": \"o-1\"}"))));
        assertEquals("UnknownEvent(OrderPlaced, 3)", Compiled.show(b.decode(ce("OrderPlaced", 3, "{}"))));
        // v1's shape on v2, which is what payments' dead letters are made of in the incident demo.
        assertEquals("Malformed(OrderPlaced, 2, data.total: expected an object, got \"120.00\")", Compiled.show(b.decode(ce("OrderPlaced", 2,
                "{\"orderId\": \"o-1\", \"customerId\": \"c-1\", \"total\": \"120.00\", \"currency\": \"USD\", \"placedAt\": \"2026-11-13T09:59:00Z\"}"))));
        assertEquals("Malformed(OrderPlaced, 1, data.orderId is missing)", Compiled.show(b.decode(ce("OrderPlaced", 1,
                "{\"customerId\": \"c-1\", \"total\": \"1\", \"currency\": \"USD\", \"placedAt\": \"2026-11-13T09:59:00Z\"}"))));
        assertEquals("Malformed(OrderPlaced, 1, data.total: expected a decimal, got \"lots\")", Compiled.show(b.decode(ce("OrderPlaced", 1,
                "{\"orderId\": \"o-1\", \"customerId\": \"c-1\", \"total\": \"lots\", \"currency\": \"USD\", \"placedAt\": \"2026-11-13T09:59:00Z\"}"))));
        assertTrue(Compiled.show(b.decode("not json")).startsWith("Malformed(?, 0, not JSON"));
        assertEquals("Malformed(?, 0, not JSON: no content)", Compiled.show(b.decode("")));
    }

    @Test
    void intentBusRevisionOneAndTwoCompileAndFoldTheSameTotals() throws Exception {
        String placed = ce("OrderPlaced", 1,
                "{\"orderId\": \"o-1\", \"customerId\": \"c-1\", \"total\": \"120.00\", \"currency\": \"EUR\", \"placedAt\": \"2026-11-13T09:00:00Z\"}");
        String amended = ce("OrderAmended", 1, "{\"orderId\": \"o-1\", \"newTotal\": \"110.00\", \"amendedAt\": \"2026-11-13T09:01:00Z\"}");
        String refund = ce("RefundIssued", 1,
                "{\"refundId\": \"r-1\", \"orderId\": \"o-1\", \"amount\": \"30.00\", \"reason\": \"damaged\", \"issuedAt\": \"2026-11-13T09:02:00Z\"}");
        String invoice = ce("InvoiceAdjusted", 1, "{\"invoiceId\": \"i-1\", \"orderId\": \"o-1\", "
                + "\"adjustment\": {\"amount\": \"7.50\", \"currency\": \"EUR\", \"reason\": \"shipping\"}, \"adjustedAt\": \"2026-11-13T09:03:00Z\"}");

        Compiled rev1 = Compiled.compile(work.resolve("rev1"), Fixtures.REVENUE, Fixtures.binding(Fixtures.intentBus(1, Fixtures.TOTALS_INTENT), Fixtures.REVENUE));
        Object p = rev1.event(placed);
        assertEquals(List.of("AssignOrderTotal(120.00)", "AssignOrderCurrency(EUR)", "AssignOrderStatus(placed)"), Compiled.effects(p));
        Object a = rev1.event(amended);
        assertEquals(List.of("AssignOrderTotal(110.00)"), Compiled.effects(a));
        assertEquals("empty", Compiled.show(Compiled.call(a, "reason")));
        Object r = rev1.event(refund);
        assertEquals("o-1", Compiled.entityId(r));
        assertEquals(List.of("AdjustOrderTotal(-30.00)"), Compiled.effects(r));
        assertEquals("Optional[damaged]", Compiled.show(Compiled.call(r, "reason")));
        assertEquals("UnknownEvent(InvoiceAdjusted, 1)", Compiled.show(rev1.decode(invoice)));

        Compiled rev2 = Compiled.compile(work.resolve("rev2"), Fixtures.REVENUE, Fixtures.binding(Fixtures.intentBus(2, Fixtures.TOTALS_INTENT), Fixtures.REVENUE));
        assertEquals(2, rev2.constant("CATALOG_REVISION"));
        Object i = rev2.event(invoice);
        assertEquals("InvoiceAdjustedV1", i.getClass().getSimpleName());
        assertEquals("o-1", Compiled.entityId(i));
        assertEquals(List.of("AdjustOrderTotal(7.50)"), Compiled.effects(i), "only the intent entity's effects, not Invoice.balance");
        assertEquals("Adjustment(7.50, EUR, shipping)", Compiled.show(Compiled.call(i, "adjustment")));
        assertEquals(List.of("AdjustOrderTotal(-30.00)"), Compiled.effects(rev2.event(refund)));
    }

    /** Keywords and taken names as fields, widening, an int key, optional and nested sources, and a constant of each kind. */
    static final String EDGES = """
            {
              "revision": 3,
              "entities": {"Account": {"balance": "decimal", "count": "long", "level": "int", "active": "boolean", "seenAt": "timestamp", "label": "string"}},
              "events": [
                {"type": "Deposited", "version": 1, "topic": "bank.deposits", "producer": "bank",
                 "fields": {"accountId": "int", "class": "string", "amount": "int", "at": "timestamp", "entityId": "string?",
                            "extra": {"bonus": "long?", "note": "string?", "inner": {"default": "boolean"}}},
                 "keys": {"Account": "accountId"},
                 "effects": [
                   {"op": "add", "attribute": "Account.balance", "from": "amount"},
                   {"op": "subtract", "attribute": "Account.count", "from": "amount"},
                   {"op": "add", "attribute": "Account.balance", "from": "extra.bonus"},
                   {"op": "assign", "attribute": "Account.level", "value": 3},
                   {"op": "assign", "attribute": "Account.active", "from": "extra.inner.default"},
                   {"op": "assign", "attribute": "Account.seenAt", "from": "at"},
                   {"op": "assign", "attribute": "Account.label", "value": "dep\\"osit\\\\u0041 */"},
                   {"op": "assign", "attribute": "Account.count", "value": 9},
                   {"op": "assign", "attribute": "Account.balance", "value": "0.10"},
                   {"op": "assign", "attribute": "Account.seenAt", "value": "2026-01-01T00:00:00Z"}
                 ]}
              ],
              "intents": [{"name": "account-balance", "predicate": "changes(Account.balance)"}]
            }""";

    @Test
    void awkwardFieldNamesTypesAndConstantsCompile() throws Exception {
        Catalog c = Fixtures.parse(Json.parse(EDGES).value());
        BindingGenerator.Target target = new BindingGenerator.Target("bank.generated", "AccountBalance");
        String source = BindingGenerator.generate(c, "account-balance", target).value();
        Compiled b = Compiled.compile(work, target, source);

        String data = "{\"accountId\": 42, \"class\": \"gold\", \"amount\": 5, \"at\": \"2026-11-13T10:00:00Z\", "
                + "\"extra\": {\"bonus\": \"10\", \"inner\": {\"default\": true}}}";
        Object e = b.event(ce("Deposited", 1, data));
        assertEquals("42", Compiled.entityId(e));
        assertEquals(List.of("meta", "accountId", "class_", "amount", "at", "entityId_", "extra"), Compiled.componentNames(e));
        assertEquals(List.of("AdjustAccountBalance(5)", "AdjustAccountCount(-5)", "AdjustAccountBalance(10)", "AssignAccountLevel(3)",
                "AssignAccountActive(true)", "AssignAccountSeenAt(2026-11-13T10:00:00Z)", "AssignAccountLabel(dep\"osit\\u0041 */)",
                "AssignAccountCount(9)", "AssignAccountBalance(0.10)", "AssignAccountSeenAt(2026-01-01T00:00:00Z)"), Compiled.effects(e));
        assertEquals("Extra(Optional[10], empty, ExtraInner(true))", Compiled.show(Compiled.call(e, "extra")));

        Object without = b.event(ce("Deposited", 1, data.replace("\"bonus\": \"10\", ", "")));
        assertEquals(9, Compiled.effects(without).size(), "an optional source that is absent has no effect");
        assertEquals("Malformed(Deposited, 1, data.accountId: expected a 32-bit integer, got 4294967296)",
                Compiled.show(b.decode(ce("Deposited", 1, data.replace("42", "4294967296")))));
    }
}
