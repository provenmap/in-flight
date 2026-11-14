package inflight.server;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import inflight.catalog.CloudEvent;
import inflight.catalog.Intent;
import inflight.catalog.Json;
import inflight.catalog.Result;
import inflight.kafka.Kafka;

/** The latest events on an intent's topic, read straight from Kafka, oldest first, with the router's provenance. */
final class Tail {

    private Tail() {}

    static ArrayNode of(String bootstrap, String intent, int n) {
        String topic = Intent.topicFor(intent);
        ArrayNode out = Json.array();
        try (KafkaConsumer<byte[], byte[]> c = Kafka.consumer(bootstrap, null)) {
            List<PartitionInfo> infos = c.partitionsFor(topic, Duration.ofSeconds(5));
            if (infos == null || infos.isEmpty()) {
                return out;
            }
            List<TopicPartition> parts = infos.stream().map(i -> new TopicPartition(topic, i.partition())).toList();
            Map<TopicPartition, Long> low = c.beginningOffsets(parts);
            Map<TopicPartition, Long> end = c.endOffsets(parts);
            c.assign(parts);
            long want = 0;
            for (TopicPartition tp : parts) {
                long from = Math.max(low.get(tp), end.get(tp) - n);
                c.seek(tp, from);
                want += end.get(tp) - from;
            }
            List<ConsumerRecord<byte[], byte[]>> got = new ArrayList<>();
            long until = System.currentTimeMillis() + 5000;
            while (got.size() < want && System.currentTimeMillis() < until) {
                c.poll(Duration.ofMillis(200)).forEach(got::add);
            }
            got.sort(Comparator.comparingLong(ConsumerRecord::timestamp));
            for (ConsumerRecord<byte[], byte[]> r : got.subList(Math.max(0, got.size() - n), got.size())) {
                Map<String, String> h = Kafka.headers(r.headers());
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
