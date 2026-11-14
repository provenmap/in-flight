package inflight.catalog;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * A structured-mode CloudEvent, with the extensions this flow reads: {@code schemaversion}, {@code sequence} and
 * {@code redriveof}. {@code redriveOf} is the id of the dead letter this event recovers, or null.
 */
public record CloudEvent(String id, String source, String type, int version, Instant time, long sequence, ObjectNode data, String redriveOf) {

    /** Timestamps as JavaScript's toISOString writes them: always milliseconds, always UTC. */
    public static final DateTimeFormatter ISO = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    public static String iso(Instant t) {
        return ISO.format(t);
    }

    public EventRef ref() {
        return new EventRef(type, version);
    }

    /** A new event with a fresh id. */
    public static CloudEvent create(String source, String type, int version, Instant time, long sequence, ObjectNode data, String redriveOf) {
        return new CloudEvent(UUID.randomUUID().toString(), source, type, version, time, sequence, data, redriveOf);
    }

    /** The JSON form, as it goes on the wire. */
    public String toJson() {
        ObjectNode o = Json.object();
        o.put("specversion", "1.0");
        o.put("id", id);
        o.put("source", source);
        o.put("type", type);
        o.put("time", iso(time));
        o.put("datacontenttype", "application/json");
        o.put("schemaversion", version);
        o.put("sequence", String.valueOf(sequence));
        o.set("data", data);
        if (redriveOf != null) {
            o.put("redriveof", redriveOf);
        }
        return Json.write(o);
    }

    public CloudEvent withId(String newId) {
        return new CloudEvent(newId, source, type, version, time, sequence, data, redriveOf);
    }

    public CloudEvent withVersion(int v) {
        return new CloudEvent(id, source, type, v, time, sequence, data, redriveOf);
    }

    public CloudEvent withType(String t) {
        return new CloudEvent(id, source, t, version, time, sequence, data, redriveOf);
    }

    public CloudEvent withData(ObjectNode d) {
        return new CloudEvent(id, source, type, version, time, sequence, d, redriveOf);
    }

    public CloudEvent withRedriveOf(String r) {
        return new CloudEvent(id, source, type, version, time, sequence, data, r);
    }

    public static Result<CloudEvent> parse(String text) {
        Result<JsonNode> json = Json.parse(text);
        return json.isOk() ? fromJson(json.value()) : Result.err(json.errors());
    }

    public static Result<CloudEvent> parse(byte[] bytes) {
        Result<JsonNode> json = Json.parse(bytes, 0, bytes.length);
        return json.isOk() ? fromJson(json.value()) : Result.err(json.errors());
    }

    public static Result<CloudEvent> fromJson(JsonNode json) {
        if (!Json.isObject(json)) {
            return Result.err("CloudEvent is not an object");
        }
        for (String key : new String[] {"id", "source", "type"}) {
            if (Json.text(json, key) == null) {
                return Result.err("CloudEvent has no '" + key + "'");
            }
        }
        String id = Json.text(json, "id");
        String source = Json.text(json, "source");
        String type = Json.text(json, "type");
        Integer version = wholeNumber(json.get("schemaversion"));
        if (version == null) {
            return Result.err(type + " (" + id + ") has no whole schemaversion");
        }
        JsonNode data = json.get("data");
        if (!Json.isObject(data)) {
            return Result.err(type + " (" + id + ") has no data object");
        }
        Instant time = Instant.EPOCH;
        String t = Json.text(json, "time");
        if (t != null) {
            try {
                time = Instant.parse(t);
            } catch (DateTimeParseException ignored) {
                // an unreadable time is the epoch, as Date.parse failing was in the TypeScript build
            }
        }
        long sequence = 0;
        String seq = Json.text(json, "sequence");
        if (seq != null && seq.matches("-?\\d+")) {
            sequence = Long.parseLong(seq);
        }
        return Result.ok(new CloudEvent(id, source, type, version, time, sequence, (ObjectNode) data, Json.text(json, "redriveof")));
    }

    /** A whole number, written as one or as a string of digits; null otherwise. */
    public static Integer wholeNumber(JsonNode v) {
        if (v == null) {
            return null;
        }
        if (v.isIntegralNumber()) {
            return v.asInt();
        }
        if (v.isNumber() && v.decimalValue().stripTrailingZeros().scale() <= 0) {
            return v.decimalValue().intValue();
        }
        if (v.isTextual() && v.asText().trim().matches("-?\\d+")) {
            return Integer.parseInt(v.asText().trim());
        }
        return null;
    }

    /** Headers a service sets when it dead-letters a record. */
    public static final class DeadLetter {
        public static final String ERROR = "in-flight-error";
        public static final String SOURCE_TOPIC = "in-flight-source-topic";
        public static final String GROUP = "in-flight-consumer-group";
        public static final String FAILED_AT = "in-flight-failed-at";

        private DeadLetter() {}
    }
}
