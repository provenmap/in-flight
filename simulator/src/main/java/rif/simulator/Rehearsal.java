package rif.simulator;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import rif.catalog.CloudEvent;
import rif.catalog.Intent;
import rif.catalog.Json;
import rif.catalog.Result;
import rif.kafka.Kafka;
import rif.simulator.Events.Emitter;
import rif.simulator.Events.Out;

/**
 * Rehearses a build of a consumer on the past, selected by intent: every event the router delivered on the intent's
 * topic in a time range goes through the build that runs now and through the candidate. The candidate's output goes to
 * a branch topic of its own, so no real reader sees it, and what comes back is the difference: how many each build
 * would have charged, declined and dead-lettered, and the events they disagree on.
 *
 * <p>Only the consumers whose code the simulator has can be rehearsed: payments, as {@code strict} (it reads OrderPlaced
 * v1 only) or {@code patched} (it reads through its regenerated binding).
 */
public final class Rehearsal {

    /** The builds of payments the simulator can run. */
    public static final Map<String, Boolean> BUILDS = Map.of("strict", false, "patched", true);

    /** What one build did with one event: the type it produced, or {@code dead-lettered}, and why. */
    public record Outcome(String result, String detail) {
        boolean same(Outcome o) {
            return result.equals(o.result);
        }
    }

    public record Difference(String eventId, String event, String orderId, Outcome baseline, Outcome candidate) {}

    public record Report(String id, String intent, String consumer, Instant from, Instant to, int events, String baseline, String candidate,
                         Map<String, Integer> baselineCounts, Map<String, Integer> candidateCounts, int differ, List<Difference> differences,
                         String branchTopic) {

        public String text() {
            if (events == 0) {
                return "No event reached intent." + intent + " between " + CloudEvent.iso(from) + " and " + CloudEvent.iso(to) + ".";
            }
            return "Rehearsed " + events + " events of intent." + intent + " through " + consumer + ": " + baseline + " " + baselineCounts + ", "
                    + candidate + " " + candidateCounts + ". They disagree on " + differ + (differ == 1 ? " event" : " events")
                    + ". The candidate's output is on " + branchTopic + ".";
        }

        public ObjectNode json() {
            ObjectNode o = Json.object();
            o.put("id", id).put("intent", intent).put("consumer", consumer).put("from", CloudEvent.iso(from)).put("to", CloudEvent.iso(to))
                    .put("events", events).put("baseline", baseline).put("candidate", candidate);
            ObjectNode b = o.putObject("baselineCounts");
            baselineCounts.forEach(b::put);
            ObjectNode c = o.putObject("candidateCounts");
            candidateCounts.forEach(c::put);
            o.put("differ", differ);
            ArrayNode ds = o.putArray("differences");
            for (Difference d : differences) {
                ObjectNode x = ds.addObject().put("eventId", d.eventId()).put("event", d.event()).put("orderId", d.orderId());
                x.putObject("baseline").put("result", d.baseline().result()).put("detail", d.baseline().detail());
                x.putObject("candidate").put("result", d.candidate().result()).put("detail", d.candidate().detail());
            }
            o.put("branchTopic", branchTopic);
            o.put("text", text());
            return o;
        }
    }

    private Rehearsal() {}

    /** The consumer that reads by an intent, as far as the simulator has its code. */
    static String consumerOf(String intent) {
        return intent.equals("order-total-changes") ? Services.Payments.GROUP : null;
    }

    public static Result<Report> run(String bootstrap, String intent, Instant from, Instant to, boolean runningPatched, String candidate) {
        String consumer = consumerOf(intent);
        if (consumer == null) {
            return Result.err("the simulator has the code of payments only, which reads by order-total-changes: it cannot rehearse intent " + intent);
        }
        if (!BUILDS.containsKey(candidate)) {
            return Result.err("no build '" + candidate + "' of " + consumer + ": there are " + String.join(" and ", BUILDS.keySet().stream().sorted().toList()));
        }
        String baseline = runningPatched ? "patched" : "strict";
        String topic = Intent.topicFor(intent);
        String id = "r" + UUID.randomUUID().toString().substring(0, 8);
        String branch = "rehearse." + id + "." + consumer;
        Emitter emit = new Emitter("/rehearsal/" + id);
        Function<Boolean, Function<CloudEvent, Outcome>> build = patched -> e -> {
            Result<List<Out>> r = Services.Payments.handle(emit, e, patched, Instant.now(), false);
            return r.isOk() ? new Outcome(r.value().isEmpty() ? "nothing" : r.value().get(0).event().type(), "")
                    : new Outcome("dead-lettered", r.error());
        };
        Function<CloudEvent, Outcome> before = build.apply(BUILDS.get(baseline));
        Function<CloudEvent, Outcome> after = build.apply(BUILDS.get(candidate));

        List<ConsumerRecord<byte[], byte[]>> past = new ArrayList<>();
        try (Admin admin = Kafka.admin(bootstrap); KafkaConsumer<byte[], byte[]> c = Kafka.consumer(bootstrap, null)) {
            if (!Kafka.listTopics(admin).contains(topic)) {
                return Result.err("intent " + intent + " has no topic yet: is the router running, and has it delivered anything?");
            }
            List<TopicPartition> parts = new ArrayList<>();
            for (PartitionInfo p : c.partitionsFor(topic)) {
                parts.add(new TopicPartition(topic, p.partition()));
            }
            Map<TopicPartition, Long> query = new HashMap<>();
            parts.forEach(tp -> query.put(tp, from.toEpochMilli()));
            Map<TopicPartition, OffsetAndTimestamp> starts = c.offsetsForTimes(query);
            Map<TopicPartition, Long> ends = c.endOffsets(parts);
            c.assign(parts);
            Map<TopicPartition, Long> remaining = new HashMap<>();
            for (TopicPartition tp : parts) {
                OffsetAndTimestamp s = starts.get(tp);
                long start = s == null ? ends.get(tp) : s.offset();
                c.seek(tp, start);
                remaining.put(tp, ends.get(tp) - start);
            }
            long until = System.currentTimeMillis() + 30_000;
            while (remaining.values().stream().anyMatch(n -> n > 0) && System.currentTimeMillis() < until) {
                for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofMillis(300))) {
                    TopicPartition tp = new TopicPartition(r.topic(), r.partition());
                    remaining.merge(tp, -1L, Long::sum);
                    if (r.timestamp() <= to.toEpochMilli()) {
                        past.add(r);
                    }
                }
            }
            Kafka.ensureTopics(admin, List.of(branch), 1);
        }

        Map<String, Integer> baseCounts = new LinkedHashMap<>();
        Map<String, Integer> candCounts = new LinkedHashMap<>();
        List<Difference> differences = new ArrayList<>();
        int differ = 0;
        try (KafkaProducer<String, byte[]> p = Kafka.producer(bootstrap)) {
            for (ConsumerRecord<byte[], byte[]> r : past) {
                Result<CloudEvent> e = CloudEvent.parse(r.value());
                if (!e.isOk()) {
                    continue;
                }
                Outcome b = before.apply(e.value());
                Outcome a = after.apply(e.value());
                baseCounts.merge(b.result(), 1, Integer::sum);
                candCounts.merge(a.result(), 1, Integer::sum);
                RecordHeaders hs = new RecordHeaders();
                hs.add("rif-rehearsal", id.getBytes(StandardCharsets.UTF_8));
                hs.add("rif-rehearsed-event", e.value().id().getBytes(StandardCharsets.UTF_8));
                hs.add("rif-outcome", (a.result() + (a.detail().isEmpty() ? "" : ": " + a.detail())).getBytes(StandardCharsets.UTF_8));
                p.send(new ProducerRecord<>(branch, null, r.key() == null ? null : new String(r.key(), StandardCharsets.UTF_8), r.value(), hs));
                if (!a.same(b)) {
                    differ++;
                    if (differences.size() < 20) {
                        differences.add(new Difference(e.value().id(), e.value().ref().show(), e.value().data().path("orderId").asText(""), b, a));
                    }
                }
            }
            p.flush();
        }
        return Result.ok(new Report(id, intent, consumer, from, to, past.size(), baseline, candidate, baseCounts, candCounts, differ, differences, branch));
    }
}
