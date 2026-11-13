package rif.catalog;

import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * A Kafka record as the observer reads it, whatever wrote it: a CloudEvent in JSON (structured mode) or in headers
 * (binary mode), or plain JSON. JSON written by a Schema Registry serializer carries a five-byte prefix, dropped here.
 */
public final class Records {

    private Records() {}

    /** What a record says about itself. What it leaves out (null), the catalog works out from the topic. */
    public record Decoded(String id, String type, Integer version, ObjectNode data, String redriveOf) {}

    public static Result<Decoded> decode(byte[] value, Map<String, String> headers) {
        // Schema Registry framing: a zero magic byte and a four-byte schema id. JSON never starts with a zero byte.
        boolean framed = value.length > 5 && value[0] == 0;
        Result<JsonNode> parsed = framed ? Json.parse(value, 5, value.length - 5) : Json.parse(value, 0, value.length);
        if (!parsed.isOk()) {
            return Result.err(parsed.errors());
        }
        JsonNode body = parsed.value();

        // Binary mode: the attributes are ce_ headers and the value is the data.
        String ceType = headers.get("ce_type");
        if (ceType != null) {
            if (!body.isObject()) {
                return Result.err(ceType + " has no data object");
            }
            String sv = headers.get("ce_schemaversion");
            Integer version = sv == null ? null : CloudEvent.wholeNumber(Json.tree(sv));
            return Result.ok(new Decoded(headers.get("ce_id"), ceType, version, (ObjectNode) body, headers.get("ce_redriveof")));
        }
        if (!body.isObject()) {
            return Result.err("the record is not a JSON object");
        }

        // Structured mode: the attributes and the data in one JSON object.
        JsonNode spec = body.get("specversion");
        if (spec != null && spec.isTextual()) {
            String type = Json.text(body, "type");
            if (type == null) {
                return Result.err("CloudEvent has no 'type'");
            }
            String id = Json.text(body, "id");
            if (!Json.isObject(body.get("data"))) {
                return Result.err(type + " (" + (id == null ? "no id" : id) + ") has no data object");
            }
            return Result.ok(new Decoded(id, type, CloudEvent.wholeNumber(body.get("schemaversion")), (ObjectNode) body.get("data"), Json.text(body, "redriveof")));
        }
        return Result.ok(new Decoded(null, null, null, (ObjectNode) body, null));
    }
}
