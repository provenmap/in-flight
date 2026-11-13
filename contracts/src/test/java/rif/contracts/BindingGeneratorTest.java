package rif.contracts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.node.ObjectNode;

import rif.catalog.Catalog;
import rif.catalog.Json;
import rif.catalog.Result;

/** What the generator refuses, how it names things, and the manifests that tell it where a binding goes. */
class BindingGeneratorTest {

    @Test
    void aFieldNamedMetaIsRefused() {
        ObjectNode json = Fixtures.edit(Fixtures.intentBusJson(1, Fixtures.TOTALS_INTENT),
                j -> ((ObjectNode) j.get("events").get(1).get("fields")).put("meta", "string"));
        Result<String> r = BindingGenerator.generate(Fixtures.parse(json), Fixtures.TOTALS, Fixtures.REVENUE);
        assertEquals(List.of("OrderAmended@v1 has a field named 'meta', which the binding reserves"), r.errors());
    }

    @Test
    void anUndeclaredIntentOrABadTargetIsRefused() {
        Catalog c = Fixtures.intentBus(1, Fixtures.TOTALS_INTENT);
        assertEquals(List.of("intent 'nope' is not declared in catalog revision 1"), BindingGenerator.generate(c, "nope", Fixtures.REVENUE).errors());
        Result<String> bad = BindingGenerator.generate(c, Fixtures.TOTALS, new BindingGenerator.Target("a.class.b", "orderTotals"));
        assertEquals(2, bad.errors().size(), bad.errors().toString());
        assertTrue(bad.errors().get(0).startsWith("'a.class.b' is not a Java package name"));
    }

    @Test
    void aNestedObjectThatWouldShadowATypeTheBindingUsesIsRefused() {
        ObjectNode json = Fixtures.edit(Fixtures.intentBusJson(2, Fixtures.TOTALS_INTENT), j -> {
            ObjectNode fields = (ObjectNode) j.get("events").get(4).get("fields");
            fields.set("effect", Json.object().put("kind", "string"));
        });
        Result<String> r = BindingGenerator.generate(Fixtures.parse(json), Fixtures.TOTALS, Fixtures.REVENUE);
        assertEquals(List.of("InvoiceAdjustedV1: a nested object would be named 'Effect', which the binding already uses; rename the field"), r.errors());
    }

    @Test
    void namesBecomeJavaIdentifiers() {
        assertEquals("orderId", BindingGenerator.ident("orderId"));
        assertEquals("class_", BindingGenerator.ident("class"));
        assertEquals("record_", BindingGenerator.ident("record"));
        assertEquals("hashCode_", BindingGenerator.ident("hashCode"));
        assertEquals("order_id", BindingGenerator.ident("order-id"));
        assertEquals("_2nd", BindingGenerator.ident("2nd"));
        assertEquals("SeenAt", BindingGenerator.cap("seen-at"));
        assertEquals("\"a\\\"b\\\\c\\n\"", BindingGenerator.lit("a\"b\\c\n"));
        assertEquals("x *&#47; \\\\u0041", BindingGenerator.doc("x */ \\u0041"));
    }

    @Test
    void theShippedManifestNamesPaymentsAndWhereItsBindingGoes() {
        List<ConsumerManifest> ms = ConsumerManifest.loadAll(Fixtures.MANIFESTS);
        assertEquals(List.of(new ConsumerManifest("payments-service", Fixtures.TOTALS,
                "simulator/src/main/java/rif/simulator/generated/OrderTotalChanges.java", Fixtures.SIMULATOR, null)), ms);
        assertTrue(Fixtures.shipped().consumersOf(Fixtures.TOTALS).stream().anyMatch(k -> k.name().equals("payments-service")),
                "the catalog declares the same consumer reading by the same intent");
    }

    @TempDir
    Path dir;

    @Test
    void manifestsReadIntentBusShapeAndRefuseAnIncompleteOne() throws Exception {
        Files.writeString(dir.resolve("revenue.json"), """
                {"name": "revenue-consumer", "intent": "order-total-changes", "repo": "provenmap/intent-bus",
                 "binding": {"path": "demo/X.scala", "package": "intentbus.demo.generated", "object": "OrderTotalChanges"}}""");
        Files.writeString(dir.resolve("notes.txt"), "not a manifest");
        ConsumerManifest m = ConsumerManifest.loadAll(dir).get(0);
        assertEquals("OrderTotalChanges", m.target().className());
        assertEquals("provenmap/intent-bus", m.repo());
        assertTrue(ConsumerManifest.loadAll(dir.resolve("absent")).isEmpty());

        Files.writeString(dir.resolve("broken.json"), "{\"name\": \"x\"}");
        ContractsException e = org.junit.jupiter.api.Assertions.assertThrows(ContractsException.class, () -> ConsumerManifest.loadAll(dir));
        assertTrue(e.getMessage().contains("broken.json: 'intent' must be a string"), e.getMessage());
        assertFalse(e.getMessage().contains("revenue.json"));
    }
}
