package rif.simulator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import rif.catalog.CloudEvent;
import rif.catalog.Result;
import rif.kafka.Kafka;
import rif.simulator.Events.Out;

/**
 * One pass over a dead-letter queue, up to the end it had when the pass began, in its own consumer group. Each record
 * goes through the handler again; what it returns is published carrying the original event id. The first record that
 * still fails stops its partition, and the committed offset stays on it, so a later pass retries it. Nothing is
 * re-published to the source topic, so the other readers of that topic never see an event twice.
 *
 * <p>The group never subscribes: it assigns the partitions itself, so there is no rebalance and no member to wait for.
 */
public final class Redrive {

    /** How the last redrive went; {@code error} is null when nothing stopped it. */
    public record Status(boolean running, long redriven, long remaining, String error) {
        public static final Status IDLE = new Status(false, 0, 0, null);
    }

    private final String bootstrap;
    private final String dlq;
    private final String group;
    private final Function<CloudEvent, Result<List<Out>>> handler;

    public Redrive(String bootstrap, String dlq, String group, Function<CloudEvent, Result<List<Out>>> handler) {
        this.bootstrap = bootstrap;
        this.dlq = dlq;
        this.group = group;
        this.handler = handler;
    }

    public Status run(Consumer<Status> progress, long giveUpMs) {
        try (KafkaConsumer<byte[], byte[]> c = Kafka.consumer(bootstrap, group); KafkaProducer<String, byte[]> p = Kafka.producer(bootstrap)) {
            List<TopicPartition> parts = new ArrayList<>();
            for (PartitionInfo info : c.partitionsFor(dlq)) {
                parts.add(new TopicPartition(dlq, info.partition()));
            }
            Map<TopicPartition, Long> low = c.beginningOffsets(parts);
            Map<TopicPartition, Long> end = c.endOffsets(parts);
            Map<TopicPartition, OffsetAndMetadata> committed = c.committed(new LinkedHashSet<>(parts));
            Map<TopicPartition, Long> next = new HashMap<>();
            for (TopicPartition tp : parts) {
                OffsetAndMetadata om = committed.get(tp);
                next.put(tp, om != null ? Math.max(om.offset(), low.get(tp)) : low.get(tp));
            }
            c.assign(parts);
            parts.forEach(tp -> c.seek(tp, next.get(tp)));

            Set<TopicPartition> open = new LinkedHashSet<>();
            parts.stream().filter(tp -> next.get(tp) < end.get(tp)).forEach(open::add);
            long done = 0;
            String error = null;
            long giveUp = System.currentTimeMillis() + giveUpMs;

            while (!open.isEmpty() && System.currentTimeMillis() < giveUp) {
                Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
                for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofMillis(200))) {
                    TopicPartition tp = new TopicPartition(r.topic(), r.partition());
                    if (!open.contains(tp) || r.offset() >= end.get(tp)) {
                        continue;
                    }
                    Result<CloudEvent> e = CloudEvent.parse(r.value());
                    Result<List<Out>> res = e.isOk() ? handler.apply(e.value()) : Result.err(e.errors());
                    if (res.isOk()) {
                        res.value().forEach(o -> p.send(o.record()));
                        next.put(tp, r.offset() + 1);
                        commits.put(tp, new OffsetAndMetadata(r.offset() + 1));
                        done++;
                        if (r.offset() + 1 >= end.get(tp)) {
                            open.remove(tp);
                        }
                    } else {
                        error = "offset " + r.offset() + " on partition " + r.partition() + ": " + res.error();
                        c.pause(List.of(tp));
                        open.remove(tp);
                    }
                }
                if (!commits.isEmpty()) {
                    p.flush();
                    c.commitSync(commits);
                }
                progress.accept(new Status(true, done, remaining(parts, end, next), error));
            }
            return new Status(false, done, remaining(parts, end, next), error);
        }
    }

    private static long remaining(List<TopicPartition> parts, Map<TopicPartition, Long> end, Map<TopicPartition, Long> next) {
        return parts.stream().mapToLong(tp -> end.get(tp) - next.get(tp)).sum();
    }
}
