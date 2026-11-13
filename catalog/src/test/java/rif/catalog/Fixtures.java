package rif.catalog;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Catalogs to test against: the shipped one, intent-bus's two revisions, and a small one about meaning alone. */
final class Fixtures {

    private Fixtures() {}

    static final Path SHIPPED = Path.of("catalog/order-to-cash.json");

    static ObjectNode shippedJson() {
        try {
            return (ObjectNode) Json.MAPPER.readTree(Files.readString(SHIPPED));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The shipped catalog, edited. */
    static Result<Catalog> edit(Consumer<ObjectNode> f) {
        ObjectNode json = shippedJson();
        f.accept(json);
        return Catalog.fromJson(json);
    }

    static List<String> errors(Result<?> r) {
        return r.errors();
    }

    static String resource(String name) {
        try (InputStream in = Fixtures.class.getResourceAsStream(name)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** intent-bus's demo catalog at a revision, as intent-bus wrote it. */
    static Catalog intentBus(int rev) {
        return Catalog.parse(resource("/intent-bus/rev" + rev + ".json")).value();
    }

    static Result<Catalog> editJson(Catalog base, Consumer<ObjectNode> f) {
        ObjectNode json = (ObjectNode) base.toJson();
        f.accept(json);
        return Catalog.fromJson(json);
    }

    // ---- meaning alone: orders whose total is set, amended and refunded; shipments keyed by two entities ----

    static ObjectNode raw() {
        return (ObjectNode) Json.parse("""
                {
                  "entities": {"Order": {"total": "decimal", "status": "string"}, "Shipment": {"carrier": "string"}},
                  "intents": [{"name": "order-total-changes", "predicate": "changes(Order.total)"}],
                  "events": [
                    {"type": "OrderPlaced", "version": 1, "topic": "orders.placed",
                     "fields": {"orderId": "string", "total": "decimal", "note": "string?"}, "keys": {"Order": "orderId"},
                     "effects": [{"op": "assign", "attribute": "Order.total", "from": "total"}, {"op": "assign", "attribute": "Order.status", "value": "placed"}]},
                    {"type": "OrderAmended", "version": 1, "topic": "orders.amended",
                     "fields": {"orderId": "string", "newTotal": "decimal"}, "keys": {"Order": "orderId"},
                     "effects": [{"op": "assign", "attribute": "Order.total", "from": "newTotal"}]},
                    {"type": "RefundIssued", "version": 1, "topic": "billing.refunds",
                     "fields": {"refund": {"orderId": "string", "amount": "decimal"}}, "keys": {"Order": "refund.orderId"},
                     "effects": [{"op": "subtract", "attribute": "Order.total", "from": "refund.amount"}]},
                    {"type": "OrderShipped", "version": 1, "topic": "orders.shipped",
                     "fields": {"orderId": "string", "shipmentId": "string", "carrier": "string"}, "keys": {"Order": "orderId", "Shipment": "shipmentId"},
                     "effects": [{"op": "assign", "attribute": "Order.status", "value": "shipped"}, {"op": "assign", "attribute": "Shipment.carrier", "from": "carrier"}]},
                    {"type": "Heartbeat", "version": 1, "topic": "ops.heartbeat"}
                  ]
                }""").value();
    }

    static ArrayNode events(ObjectNode raw) {
        return (ArrayNode) raw.get("events");
    }

    static ObjectNode event(ObjectNode raw, int i) {
        return (ObjectNode) raw.get("events").get(i);
    }

    static Result<Semantics> parseSemantics(ObjectNode raw) {
        List<Model.RawEvent> events = new ArrayList<>();
        for (JsonNode e : raw.get("events")) {
            events.add(new Model.RawEvent(new EventRef(e.get("type").asText(), e.get("version").asInt()), e.get("topic").asText(), null,
                    e.get("fields"), e.get("keys"), e.get("effects")));
        }
        return Semantics.parse(new Semantics.Raw(raw.get("entities"), raw.get("intents"), events), 1);
    }

    static Result<Semantics> load(Consumer<ObjectNode> edit) {
        ObjectNode r = raw();
        edit.accept(r);
        return parseSemantics(r);
    }

    static Semantics sound(Consumer<ObjectNode> edit) {
        Result<Semantics> res = load(edit);
        if (!res.isOk() || res.value() == null) {
            throw new AssertionError(res.isOk() ? "no semantics" : res.error());
        }
        return res.value();
    }

    static Semantics sound() {
        return sound(r -> {});
    }
}
