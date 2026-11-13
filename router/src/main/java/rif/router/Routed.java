package rif.router;

import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import rif.catalog.Json;

/**
 * One event on its way into one intent's stream, with its provenance. It travels between the routing and ordering
 * stages through the repartition topic, and waits in the ordering buffer, as JSON.
 */
public record Routed(String intent, int catalogRevision, String entityId, long eventTimeMs, long sequence, String eventId,
                     String sourceTopic, int sourcePartition, long sourceOffset, String payload) {

    /** The key between the routing and ordering stages: all events of one entity for one intent share it. */
    public static String compositeKey(String intent, String entityId) {
        return intent + "|" + entityId;
    }

    public String compositeKey() {
        return compositeKey(intent, entityId);
    }

    public String toJson() {
        ObjectNode o = Json.object();
        o.put("intent", intent).put("rev", catalogRevision).put("entity", entityId).put("t", eventTimeMs).put("seq", sequence)
                .put("id", eventId).put("topic", sourceTopic).put("partition", sourcePartition).put("offset", sourceOffset)
                .put("payload", payload);
        return Json.write(o);
    }

    public static Routed fromJson(String text) {
        JsonNode j = Json.parse(text).value();
        return new Routed(j.get("intent").asText(), j.get("rev").asInt(), j.get("entity").asText(), j.get("t").asLong(), j.get("seq").asLong(),
                j.get("id").asText(), j.get("topic").asText(), j.get("partition").asInt(), j.get("offset").asLong(), j.get("payload").asText());
    }

    public static final Serde<Routed> SERDE = Serdes.serdeFrom(
            (Serializer<Routed>) (topic, r) -> r == null ? null : r.toJson().getBytes(StandardCharsets.UTF_8),
            (Deserializer<Routed>) (topic, b) -> b == null ? null : fromJson(new String(b, StandardCharsets.UTF_8)));
}
