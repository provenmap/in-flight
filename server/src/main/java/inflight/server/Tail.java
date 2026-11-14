package inflight.server;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import inflight.broker.Broker;
import inflight.broker.Cursor;
import inflight.broker.Endpoint;
import inflight.broker.Message;
import inflight.broker.Shard;
import inflight.catalog.CloudEvent;
import inflight.catalog.Intent;
import inflight.catalog.Json;
import inflight.catalog.Result;

/** The latest events on an intent's topic, read straight from the broker, oldest first, with the router's provenance. */
final class Tail {

    private Tail() {}

    static ArrayNode of(Endpoint endpoint, String intent, int n) {
        String topic = Intent.topicFor(intent);
        ArrayNode out = Json.array();
        try (Broker broker = endpoint.open()) {
            List<Shard> parts = broker.shards(List.of(topic));
            if (parts.isEmpty()) {
                return out;
            }
            Map<Shard, Long> low = broker.starts(parts);
            Map<Shard, Long> end = broker.ends(parts);
            Map<Shard, Long> from = new java.util.LinkedHashMap<>();
            long want = 0;
            for (Shard tp : parts) {
                long f = Math.max(low.get(tp), end.get(tp) - n);
                from.put(tp, f);
                want += end.get(tp) - f;
            }
            List<Message> got = new ArrayList<>();
            try (Cursor c = broker.observe()) {
                c.seek(from);
                long until = System.currentTimeMillis() + 5000;
                while (got.size() < want && System.currentTimeMillis() < until) {
                    got.addAll(c.poll(Duration.ofMillis(200)));
                }
            }
            got.sort(Comparator.comparingLong(Message::timestamp));
            for (Message r : got.subList(Math.max(0, got.size() - n), got.size())) {
                Map<String, String> h = r.textHeaders();
                ObjectNode o = out.addObject();
                o.put("entity", h.getOrDefault("ib-entity", r.key() == null ? "" : new String(r.key(), StandardCharsets.UTF_8)));
                Result<CloudEvent> e = CloudEvent.parse(r.value());
                o.put("event", e.isOk() ? e.value().ref().show() : "unreadable");
                o.put("id", e.isOk() ? e.value().id() : "");
                o.put("at", CloudEvent.iso(java.time.Instant.ofEpochMilli(r.timestamp())));
                o.put("from", h.getOrDefault("ib-source-topic", ""));
                o.put("revision", h.getOrDefault("ib-catalog-revision", ""));
                o.put("late", "true".equals(h.get("ib-late")));
                JsonNode data = e.isOk() ? e.value().data() : Json.object();
                o.set("data", data);
            }
        }
        return out;
    }
}
