package inflight.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Minor units, a currency per event or per record, rates, and the JSON Patch the drafts are edited with. */
class AmountsTest {

    static Catalog catalog(String events, String extra) {
        String text = "{\"flow\":\"checkout\",\"currency\":\"USD\"" + extra + ",\"events\":[" + events + "]}";
        Result<Catalog> c = Catalog.parse(text);
        assertTrue(c.isOk(), c.error());
        return c.value();
    }

    static Result<Pricing.Priced> price(Catalog c, String data) {
        Records.Decoded d = new Records.Decoded(null, null, null, (ObjectNode) Json.parse(data).value(), null);
        return new Pricing(c).priceDecoded(d, List.of("orders"), "x");
    }

    @Test
    void minorUnitsAreMadeMajorByTheCurrencysDigits() {
        Catalog c = catalog("{\"type\":\"OrderPlaced\",\"version\":1,\"topic\":\"orders\",\"district\":\"orders\",\"value\":\"total_cents\","
                + "\"order\":\"id\",\"customer\":\"customer\",\"minorUnits\":true}", "");
        assertEquals(0, new BigDecimal("129.95").compareTo(price(c, "{\"id\":\"o1\",\"customer\":\"c1\",\"total_cents\":12995}").value().value()));
        // Yen have no minor unit: the amount is what it is.
        Catalog yen = catalog("{\"type\":\"OrderPlaced\",\"version\":1,\"topic\":\"orders\",\"district\":\"orders\",\"value\":\"total\","
                + "\"order\":\"id\",\"customer\":\"customer\",\"minorUnits\":true,\"currency\":\"JPY\"}", ",\"rates\":{\"JPY\":0.0065}");
        assertEquals(0, new BigDecimal("65.0000").compareTo(price(yen, "{\"id\":\"o1\",\"customer\":\"c1\",\"total\":10000}").value().value()));
    }

    @Test
    void aCurrencyFieldIsConvertedAtTheCatalogsRateAndUnknownWithoutOne() {
        Catalog c = catalog("{\"type\":\"OrderPlaced\",\"version\":1,\"topic\":\"orders\",\"district\":\"orders\",\"value\":\"amount\","
                + "\"order\":\"id\",\"customer\":\"customer\",\"currencyField\":\"currency\"}", ",\"rates\":{\"EUR\":1.10}");
        assertEquals(0, new BigDecimal("110").compareTo(price(c, "{\"id\":\"o1\",\"customer\":\"c1\",\"amount\":100,\"currency\":\"eur\"}").value().value()));
        assertEquals(0, new BigDecimal("100").compareTo(price(c, "{\"id\":\"o1\",\"customer\":\"c1\",\"amount\":100,\"currency\":\"USD\"}").value().value()));
        Result<Pricing.Priced> gbp = price(c, "{\"id\":\"o1\",\"customer\":\"c1\",\"amount\":100,\"currency\":\"GBP\"}");
        assertFalse(gbp.isOk());
        assertTrue(gbp.error().contains("no rate for GBP"), gbp.error());
    }

    @Test
    void theNewFieldsAreWrittenBackAsTheyWereRead() {
        Catalog c = catalog("{\"type\":\"OrderPlaced\",\"version\":1,\"topic\":\"orders\",\"district\":\"orders\",\"value\":\"amount\","
                + "\"order\":\"id\",\"customer\":\"customer\",\"minorUnits\":true,\"currencyField\":\"ccy\"}", ",\"rates\":{\"EUR\":1.1}");
        Catalog again = Catalog.fromJson(c.toJson()).value();
        assertEquals(c.events(), again.events());
        assertEquals(c.rates(), again.rates());
    }

    @Test
    void badAmountsAreNamed() {
        Result<Catalog> c = Catalog.parse("{\"currency\":\"USD\",\"rates\":{\"EUR\":-1},\"events\":[{\"type\":\"A\",\"version\":1,\"topic\":\"t\",\"district\":\"d\","
                + "\"minorUnits\":\"yes\",\"currency\":\"euro\"}]}");
        assertFalse(c.isOk());
        String all = c.error();
        assertTrue(all.contains("'minorUnits' must be true or false"), all);
        assertTrue(all.contains("three-letter currency code"), all);
        assertTrue(all.contains("EUR: must be a positive number"), all);
    }

    // ---- JSON Patch ----

    static JsonNode j(String s) {
        return Json.parse(s).value();
    }

    @Test
    void patchesAddReplaceRemoveMoveCopyAndTest() {
        JsonNode doc = j("{\"events\":[{\"value\":\"total\"}],\"currency\":\"USD\"}");
        JsonNode p = j("[{\"op\":\"replace\",\"path\":\"/events/0/value\",\"value\":\"total_cents\"},"
                + "{\"op\":\"add\",\"path\":\"/events/0/minorUnits\",\"value\":true},"
                + "{\"op\":\"add\",\"path\":\"/events/-\",\"value\":{\"value\":\"x\"}},"
                + "{\"op\":\"copy\",\"from\":\"/currency\",\"path\":\"/was\"},"
                + "{\"op\":\"move\",\"from\":\"/was\",\"path\":\"/old\"},"
                + "{\"op\":\"remove\",\"path\":\"/events/1\"},"
                + "{\"op\":\"test\",\"path\":\"/old\",\"value\":\"USD\"}]");
        Result<JsonNode> r = JsonPatch.apply(doc, p);
        assertTrue(r.isOk(), r.error());
        assertEquals(j("{\"events\":[{\"value\":\"total_cents\",\"minorUnits\":true}],\"currency\":\"USD\",\"old\":\"USD\"}"), r.value());
        assertEquals(j("{\"events\":[{\"value\":\"total\"}],\"currency\":\"USD\"}"), doc, "the original is untouched");
    }

    @Test
    void aPatchAppliesWholeOrNotAtAll() {
        Result<JsonNode> r = JsonPatch.apply(j("{\"a\":1}"), j("[{\"op\":\"replace\",\"path\":\"/a\",\"value\":2},{\"op\":\"remove\",\"path\":\"/nope\"}]"));
        assertFalse(r.isOk());
        assertTrue(r.error().startsWith("operation 1"), r.error());
        assertFalse(JsonPatch.apply(j("{}"), j("{\"op\":\"add\"}")).isOk());
        assertEquals(j("{\"b\":2}"), JsonPatch.apply(j("{\"a\":1}"), j("[{\"op\":\"replace\",\"path\":\"\",\"value\":{\"b\":2}}]")).value());
        assertEquals(Map.of("a/b", 1), Json.MAPPER.convertValue(JsonPatch.apply(j("{}"), j("[{\"op\":\"add\",\"path\":\"/a~1b\",\"value\":1}]")).value(), Map.class));
    }
}
