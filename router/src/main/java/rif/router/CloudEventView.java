package rif.router;

import java.time.Instant;
import java.time.format.DateTimeParseException;

import com.fasterxml.jackson.databind.JsonNode;

import rif.catalog.Json;
import rif.catalog.Result;

/**
 * The parts of a structured-mode CloudEvent (JSON) the router reads. The payload itself is forwarded untouched.
 *
 * @param sequence the producer's sequence extension, or null when it sent none
 */
public record CloudEventView(String id, String eventType, int schemaVersion, Instant time, Long sequence, JsonNode data) {

    public static Result<CloudEventView> parse(String text) {
        if (text == null) {
            return Result.err("not JSON: no value");
        }
        Result<JsonNode> json = Json.parse(text);
        if (!json.isOk()) {
            return Result.err(json.errors());
        }
        JsonNode obj = json.value();
        if (!obj.isObject()) {
            return Result.err("not a JSON object");
        }
        String id = Json.text(obj, "id");
        if (id == null) {
            return Result.err("missing 'id'");
        }
        String type = Json.text(obj, "type");
        if (type == null) {
            return Result.err("missing 'type'");
        }
        Long version = longValue(obj.get("schemaversion"));
        if (version == null) {
            return Result.err(type + ": missing 'schemaversion' extension");
        }
        String t = Json.text(obj, "time");
        if (t == null) {
            return Result.err(type + ": missing 'time'");
        }
        Instant time;
        try {
            time = Instant.parse(t);
        } catch (DateTimeParseException e) {
            return Result.err(type + ": 'time' is not RFC 3339: " + t);
        }
        JsonNode data = obj.get("data");
        if (data == null) {
            return Result.err(type + ": missing 'data'");
        }
        return Result.ok(new CloudEventView(id, type, version.intValue(), time, longValue(obj.get("sequence")), data));
    }

    /** CloudEvents extension values may arrive as numbers or as strings. */
    static Long longValue(JsonNode v) {
        if (v == null) {
            return null;
        }
        if (v.isIntegralNumber()) {
            return v.asLong();
        }
        if (v.isNumber() && v.decimalValue().stripTrailingZeros().scale() <= 0) {
            return v.decimalValue().longValue();
        }
        if (v.isTextual()) {
            try {
                return Long.parseLong(v.asText());
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    /** Entity ids are strings or integers in the catalog; both become the record key as text. Null otherwise. */
    public static String keyText(JsonNode v) {
        if (v == null) {
            return null;
        }
        if (v.isTextual()) {
            return v.asText().isEmpty() ? null : v.asText();
        }
        if (v.isNumber()) {
            Long n = longValue(v);
            return n == null ? null : n.toString();
        }
        return null;
    }
}
