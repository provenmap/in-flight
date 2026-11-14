package inflight.contracts;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import inflight.catalog.Catalog;
import inflight.catalog.Json;
import inflight.catalog.Result;

/** Catalogs to test against: the shipped one, before and after OrderPlaced v2, and intent-bus's two demo revisions. */
final class Fixtures {

    private Fixtures() {}

    static final Path SHIPPED = Path.of("catalog/order-to-cash.json");
    static final Path INTENT_BUS = Path.of("catalog/src/test/resources/intent-bus");
    static final Path MANIFESTS = Path.of(ConsumerManifest.DEFAULT_DIR);

    static final String TOTALS = "order-total-changes";
    static final BindingGenerator.Target SIMULATOR = new BindingGenerator.Target("inflight.simulator.generated", "OrderTotalChanges");
    static final BindingGenerator.Target REVENUE = new BindingGenerator.Target("intentbus.demo.generated", "OrderTotalChanges");

    static ObjectNode read(Path file) {
        try {
            return (ObjectNode) Json.MAPPER.readTree(Files.readString(file));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static Catalog parse(JsonNode json) {
        Result<Catalog> c = Catalog.fromJson(json);
        if (!c.isOk()) {
            throw new AssertionError(c.error());
        }
        return c.value();
    }

    static ObjectNode edit(ObjectNode json, Consumer<ObjectNode> f) {
        ObjectNode copy = json.deepCopy();
        f.accept(copy);
        return copy;
    }

    /** Declares intents, each given as {@code name=predicate}. */
    static ObjectNode withIntents(ObjectNode json, String... intents) {
        return edit(json, j -> {
            ArrayNode is = j.putArray("intents");
            for (String i : intents) {
                String[] parts = i.split("=", 2);
                is.addObject().put("name", parts[0]).put("predicate", parts[1]);
            }
        });
    }

    static final String TOTALS_INTENT = TOTALS + "=changes(Order.total)";
    static final String STATUS_INTENT = "order-status=changes(Order.status)";

    /** intent-bus's demo catalog at a revision, with the intents given declared over it. */
    static ObjectNode intentBusJson(int rev, String... intents) {
        return withIntents(read(INTENT_BUS.resolve("rev" + rev + ".json")), intents);
    }

    static Catalog intentBus(int rev, String... intents) {
        return parse(intentBusJson(rev, intents));
    }

    /** The shipped catalog as it ships: revision 2, OrderPlaced v1 and v2. */
    static Catalog shipped() {
        return parse(read(SHIPPED));
    }

    /** The shipped flow before OrderPlaced v2, at revision 1: what the router has active. */
    static ObjectNode shippedActiveJson() {
        return edit(read(SHIPPED), j -> {
            j.put("revision", 1);
            ArrayNode events = (ArrayNode) j.get("events");
            for (int i = events.size() - 1; i >= 0; i--) {
                if (events.get(i).get("type").asText().equals("OrderPlaced") && events.get(i).get("version").asInt() == 2) {
                    events.remove(i);
                }
            }
        });
    }

    /** The shipped flow with OrderPlaced v2, as it ships: revision 2, proposed. */
    static ObjectNode shippedProposedJson() {
        return read(SHIPPED);
    }

    static ConsumerManifest revenueConsumer() {
        return new ConsumerManifest("revenue-consumer", TOTALS, "demo/src/main/java/intentbus/demo/generated/OrderTotalChanges.java", REVENUE,
                "provenmap/intent-bus");
    }

    static String binding(Catalog c, BindingGenerator.Target target) {
        Result<String> r = BindingGenerator.generate(c, TOTALS, target);
        if (!r.isOk()) {
            throw new AssertionError(r.error());
        }
        return r.value();
    }

    static List<String> lines(String s) {
        return s.lines().toList();
    }
}
