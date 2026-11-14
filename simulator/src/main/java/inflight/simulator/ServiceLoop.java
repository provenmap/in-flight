package inflight.simulator;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import inflight.catalog.CloudEvent;
import inflight.catalog.Result;
import inflight.kafka.Kafka;
import inflight.simulator.Events.Out;

/**
 * A Kafka service: reads its topics in its own consumer group, publishes what its handler returns, and dead-letters what
 * it cannot read. Offsets are committed after each batch's output is flushed, so consumer lag is real.
 */
public final class ServiceLoop {

    private static final Logger log = LoggerFactory.getLogger(ServiceLoop.class);

    /** What a service does with one event it read: the events to publish, or why it cannot read it. */
    @FunctionalInterface
    public interface Handler {
        Result<List<Out>> handle(CloudEvent e) throws InterruptedException;
    }

    private final String name;
    private final String bootstrap;
    private final String consumeFrom;
    private final String group;
    private final List<String> topics;
    private final String dlq;
    private final Handler handler;
    private final AtomicLong processed = new AtomicLong();
    private final AtomicLong deadLettered = new AtomicLong();
    private volatile boolean running = true;
    private Thread thread;
    private volatile KafkaConsumer<byte[], byte[]> consumer;

    public ServiceLoop(String name, String bootstrap, String group, List<String> topics, String dlq, Handler handler) {
        this(name, bootstrap, bootstrap, group, topics, dlq, handler);
    }

    /**
     * A service that reads from {@code consumeFrom} (the proxy, when it reads by intent) and writes to {@code bootstrap}
     * (Kafka itself, so the schema gate binds only the producers the demo puts behind it).
     */
    public ServiceLoop(String name, String bootstrap, String consumeFrom, String group, List<String> topics, String dlq, Handler handler) {
        this.name = name;
        this.bootstrap = bootstrap;
        this.consumeFrom = consumeFrom;
        this.group = group;
        this.topics = topics;
        this.dlq = dlq;
        this.handler = handler;
    }

    /** The record a service writes when it cannot read one: the original bytes and key, and why. */
    public static ProducerRecord<String, byte[]> deadLetter(String dlq, String group, ConsumerRecord<byte[], byte[]> r, String error, Instant at) {
        RecordHeaders headers = new RecordHeaders();
        headers.add(CloudEvent.DeadLetter.ERROR, error.getBytes(StandardCharsets.UTF_8));
        headers.add(CloudEvent.DeadLetter.SOURCE_TOPIC, r.topic().getBytes(StandardCharsets.UTF_8));
        headers.add(CloudEvent.DeadLetter.GROUP, group.getBytes(StandardCharsets.UTF_8));
        headers.add(CloudEvent.DeadLetter.FAILED_AT, CloudEvent.iso(at).getBytes(StandardCharsets.UTF_8));
        String key = r.key() == null ? null : new String(r.key(), StandardCharsets.UTF_8);
        return new ProducerRecord<>(dlq, null, key, r.value(), headers);
    }

    /** The offset to commit for each partition a batch touched: one past the last record handled. */
    public static Map<TopicPartition, OffsetAndMetadata> nextOffsets(Iterable<ConsumerRecord<byte[], byte[]>> recs) {
        Map<TopicPartition, OffsetAndMetadata> next = new HashMap<>();
        for (ConsumerRecord<byte[], byte[]> r : recs) {
            next.put(new TopicPartition(r.topic(), r.partition()), new OffsetAndMetadata(r.offset() + 1));
        }
        return next;
    }

    public void start() {
        thread = Thread.ofPlatform().name(name).start(this::run);
    }

    private void run() {
        try (KafkaConsumer<byte[], byte[]> c = Kafka.consumer(consumeFrom, group); KafkaProducer<String, byte[]> p = Kafka.producer(bootstrap)) {
            consumer = c;
            c.subscribe(topics);
            while (running) {
                ConsumerRecords<byte[], byte[]> recs = c.poll(Duration.ofMillis(200));
                for (ConsumerRecord<byte[], byte[]> r : recs) {
                    Result<CloudEvent> e = CloudEvent.parse(r.value());
                    Result<List<Out>> res = e.isOk() ? handler.handle(e.value()) : Result.err(e.errors());
                    if (res.isOk()) {
                        res.value().forEach(o -> p.send(o.record()));
                        processed.incrementAndGet();
                    } else {
                        p.send(deadLetter(dlq, group, r, res.error(), Instant.now()));
                        deadLettered.incrementAndGet();
                    }
                }
                if (!recs.isEmpty()) {
                    p.flush();
                    c.commitSync(nextOffsets(recs));
                }
            }
        } catch (WakeupException | InterruptedException e) {
            // stopping
        } catch (RuntimeException e) {
            log.error("{}: {}", name, e.getMessage(), e);
        } finally {
            log.info("{} stopped", name);
        }
    }

    /** Stops after the batch in hand, leaving the group so the next one gets the partitions at once. */
    public void stop() throws InterruptedException {
        running = false;
        KafkaConsumer<byte[], byte[]> c = consumer;
        if (c != null) {
            c.wakeup();
        }
        if (thread != null) {
            thread.join(30_000);
        }
    }

    public List<String> topics() {
        return topics;
    }

    public String group() {
        return group;
    }

    public long processed() {
        return processed.get();
    }

    public long deadLettered() {
        return deadLettered.get();
    }
}
