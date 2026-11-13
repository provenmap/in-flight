package rif.observer;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import rif.catalog.Catalog;
import rif.catalog.CloudEvent;
import rif.catalog.Json;
import rif.catalog.Pricing;
import rif.catalog.Records;
import rif.catalog.Result;
import rif.kafka.Kafka;
import rif.observer.Ledger.Msg;

/**
 * Reads every dead-letter queue from the start and every flow topic from the end, prices each record, and feeds the
 * ledger. It owns the ledger's feed: snapshots for new viewers are made between its batches, so a viewer that joins sees
 * every message after its snapshot exactly once.
 */
public final class Reader {

    private static final Logger log = LoggerFactory.getLogger(Reader.class);

    /** A viewer cannot show more red blocks than this usefully; the counter still counts them all. */
    public static final int MAX_STUCK = 3000;

    /** Every frame is a JSON array of messages. */
    public interface Publisher {
        void publish(ArrayNode frame);
    }

    /** Where a dead letter says why it failed, when, and where it came from, most specific first. */
    public record DeadLetterHeaders(List<String> error, List<String> failedAt, List<String> sourceTopic) {}

    /**
     * The catalog's own header names first, then this app's, Kafka Connect's (with
     * {@code errors.deadletterqueue.context.headers.enable}) and Spring Kafka's.
     */
    public static DeadLetterHeaders deadLetterHeaders(Catalog.DeadLetterHeaders own) {
        return new DeadLetterHeaders(
                with(own.error(), List.of(CloudEvent.DeadLetter.ERROR, "__connect.errors.exception.message", "kafka_dlt-exception-message")),
                with(own.failedAt(), List.of(CloudEvent.DeadLetter.FAILED_AT)),
                with(own.sourceTopic(), List.of(CloudEvent.DeadLetter.SOURCE_TOPIC, "__connect.errors.topic", "kafka_dlt-original-topic")));
    }

    private static List<String> with(String name, List<String> known) {
        if (name == null) {
            return known;
        }
        List<String> out = new ArrayList<>();
        out.add(name);
        out.addAll(known);
        return out;
    }

    private static String first(Map<String, String> headers, List<String> names) {
        for (String n : names) {
            String v = headers.get(n);
            if (v != null && !v.isEmpty()) {
                return v;
            }
        }
        return null;
    }

    private final String bootstrap;
    private final Catalog catalog;
    private final Publisher publish;
    private final Poller poller;
    private final boolean watching;
    private final Ledger ledger;
    private final Pricing pricing;
    private final Set<String> dlqs;
    /** The topics the router materialises intents on, for the intents services read by. */
    private final Set<String> intentTopics;
    private final DeadLetterHeaders headers;
    private final ConcurrentLinkedQueue<Consumer<ObjectNode>> joins = new ConcurrentLinkedQueue<>();
    private volatile boolean running = true;
    private volatile boolean reading;
    private volatile KafkaConsumer<byte[], byte[]> consumer;
    private Thread thread;

    public Reader(String bootstrap, Catalog catalog, Publisher publish, Poller poller, boolean watching) {
        this.bootstrap = bootstrap;
        this.catalog = catalog;
        this.publish = publish;
        this.poller = poller;
        this.watching = watching;
        this.ledger = new Ledger(catalog);
        this.pricing = new Pricing(catalog);
        this.dlqs = new HashSet<>(catalog.dlqs());
        this.intentTopics = new java.util.LinkedHashSet<>(catalog.intentTopicsByGroup().values());
        this.headers = deadLetterHeaders(catalog.deadLetterHeaders());
    }

    public Ledger ledger() {
        return ledger;
    }

    /** True once the reader has its partitions; until then a snapshot would be empty, so joins wait. */
    public boolean reading() {
        return reading;
    }

    public void start() {
        thread = Thread.ofPlatform().name("reader").start(this::run);
    }

    /** {@code send} gets a snapshot between two batches; every frame published after it is the viewer's to receive. */
    public void join(Consumer<ObjectNode> send) {
        joins.add(send);
    }

    private void run() {
        try (Admin admin = Kafka.admin(bootstrap); KafkaConsumer<byte[], byte[]> c = Kafka.consumer(bootstrap, null)) {
            consumer = c;
            List<String> topics = new ArrayList<>(catalog.topics());
            topics.addAll(catalog.dlqs());
            if (!waitFor(admin, topics)) {
                return;
            }
            for (String dlq : catalog.dlqs()) {
                String group = catalog.redriveGroupOf(dlq);
                if (group == null) {
                    continue;
                }
                Kafka.committed(admin, group).forEach((k, o) -> {
                    if (Kafka.topicOf(k).equals(dlq)) {
                        ledger.redriven(dlq, Integer.parseInt(k.substring(k.lastIndexOf(':') + 1)), o);
                    }
                });
            }
            // Intent topics the router already writes are read like flow topics; ones it creates later, from their start.
            Set<String> existing = Kafka.listTopics(admin);
            intentTopics.stream().filter(existing::contains).forEach(topics::add);
            List<TopicPartition> parts = new ArrayList<>(Kafka.partitions(admin, topics));
            // Dead letters from the start; flow topics from the end, pinned now, so nothing produced from here on is skipped.
            Map<TopicPartition, Long> low = c.beginningOffsets(parts);
            Map<TopicPartition, Long> high = c.endOffsets(parts);
            c.assign(parts);
            for (TopicPartition tp : parts) {
                c.seek(tp, dlqs.contains(tp.topic()) ? low.get(tp) : high.get(tp));
            }
            long lastLook = System.currentTimeMillis();
            log.info("observer: reading {} from the start and {} flow topics from the end", String.join(", ", catalog.dlqs()), catalog.topics().size());
            reading = true;

            Poller.Polled seen = null;
            long lastStats = 0;
            while (running) {
                if (System.currentTimeMillis() - lastLook > 5000 && !intentTopics.stream().allMatch(topics::contains)) {
                    lastLook = System.currentTimeMillis();
                    Set<String> now = Kafka.listTopics(admin);
                    List<String> fresh = intentTopics.stream().filter(t -> !topics.contains(t) && now.contains(t)).toList();
                    if (!fresh.isEmpty()) {
                        List<TopicPartition> more = Kafka.partitions(admin, fresh);
                        parts.addAll(more);
                        topics.addAll(fresh);
                        c.assign(parts);
                        c.seekToBeginning(more);
                        log.info("observer: reading intent topics {}", String.join(", ", fresh));
                    }
                }
                List<Msg> msgs = new ArrayList<>();
                for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofMillis(100))) {
                    msgs.addAll(read(r));
                }
                Poller.Polled polled = poller.latest();
                if (polled != null && polled != seen) {
                    seen = polled;
                    for (Map.Entry<String, Map<String, Long>> g : polled.committed().entrySet()) {
                        ledger.onOffsets(g.getKey(), g.getValue(), polled.ends());
                    }
                    for (Map.Entry<String, Map<String, Long>> q : polled.redriven().entrySet()) {
                        msgs.addAll(ledger.onRedriveOffsets(q.getKey(), q.getValue(), Instant.now()));
                    }
                }
                ArrayNode frame = Json.array();
                msgs.forEach(m -> frame.add(Messages.msg(m, catalog)));
                if (System.currentTimeMillis() - lastStats >= 500) {
                    lastStats = System.currentTimeMillis();
                    frame.add(statsJson());
                }
                if (!frame.isEmpty()) {
                    publish.publish(frame);
                }
                for (Consumer<ObjectNode> send; (send = joins.poll()) != null; ) {
                    send.accept(snapshot());
                }
            }
        } catch (WakeupException e) {
            // stopping
        } catch (RuntimeException e) {
            log.error("reader: {}", e.getMessage(), e);
        } finally {
            reading = false;
        }
    }

    public void stop() throws InterruptedException {
        running = false;
        KafkaConsumer<byte[], byte[]> c = consumer;
        if (c != null) {
            c.wakeup();
        }
        if (thread != null) {
            thread.join(15_000);
        }
    }

    private List<Msg> read(ConsumerRecord<byte[], byte[]> r) {
        Ledger.Pos pos = new Ledger.Pos(r.topic(), r.partition(), r.offset());
        if (intentTopics.contains(r.topic())) {
            byte[] v = r.value() == null ? new byte[0] : r.value();
            Result<Pricing.Priced> p = pricing.priceRecord(v, Kafka.headers(r.headers()), List.of(), Kafka.key(r.topic(), r.partition()) + ":" + r.offset());
            if (p.isOk()) {
                ledger.onIntent(pos, p.value());
            }
            return List.of();
        }
        boolean dead = dlqs.contains(r.topic());
        Map<String, String> hs = Kafka.headers(r.headers());
        // A dead letter came from the topic its headers name, or else from one of the topics its service reads.
        String source = dead ? first(hs, headers.sourceTopic()) : r.topic();
        List<String> topics = source != null ? List.of(source)
                : catalog.ownerOfDlq(r.topic()).map(Catalog.Consumer::reads).orElse(List.of());
        byte[] value = r.value() == null ? new byte[0] : r.value();
        Result<Records.Decoded> decoded = Records.decode(value, hs);
        Result<Pricing.Priced> priced = decoded.isOk()
                ? pricing.priceDecoded(decoded.value(), topics, Kafka.key(r.topic(), r.partition()) + ":" + r.offset())
                : Result.err(decoded.errors());
        if (!dead) {
            if (!priced.isOk()) {
                ledger.unreadable();
                return List.of();
            }
            return ledger.onFlow(pos, priced.value(), Instant.ofEpochMilli(r.timestamp()));
        }
        String failedAt = first(hs, headers.failedAt());
        Instant at = Instant.ofEpochMilli(r.timestamp());
        if (failedAt != null) {
            try {
                at = Instant.parse(failedAt);
            } catch (DateTimeParseException ignored) {
                // the record's own time, then
            }
        }
        String error = first(hs, headers.error());
        if (error == null) {
            error = "no error given";
        }
        if (priced.isOk()) {
            return List.of(ledger.onDead(r.topic(), pos, priced.value(), error, at));
        }
        // Stuck all the same: a dead letter nobody can price is still an order that did not get through.
        Records.Decoded d = decoded.isOk() ? decoded.value() : null;
        String event = d != null && d.type() != null ? (d.version() != null ? d.type() + "@v" + d.version() : d.type()) : "an unreadable record";
        return List.of(ledger.onDeadUnpriced(r.topic(), pos, event, priced.error(), error, at));
    }

    /** The stats message, with the simulator's state (or null) as the TypeScript build sent it. */
    public ObjectNode statsJson() {
        ObjectNode o = Messages.stats(ledger.stats());
        Poller.Polled p = poller.latest();
        JsonNode sim = p == null ? null : p.sim();
        if (sim == null) {
            o.putNull("sim");
        } else {
            o.set("sim", sim);
        }
        return o;
    }

    public List<Ledger.Stuck> stuck() {
        return ledger.stuckNow();
    }

    public ObjectNode snapshot() {
        List<Ledger.Stuck> stuck = ledger.stuckNow();
        ObjectNode o = Json.object();
        o.put("type", "snapshot");
        o.put("mode", watching ? "watch" : "demo");
        o.set("flow", Messages.flow(catalog));
        ArrayNode s = o.putArray("stuck");
        stuck.subList(Math.max(0, stuck.size() - MAX_STUCK), stuck.size()).forEach(x -> s.add(Messages.stuck(x, catalog)));
        o.put("stuckShown", Math.min(stuck.size(), MAX_STUCK));
        o.set("stats", statsJson());
        return o;
    }

    private boolean waitFor(Admin admin, List<String> topics) {
        boolean told = false;
        while (running) {
            Set<String> have;
            try {
                have = Kafka.listTopics(admin);
            } catch (RuntimeException e) {
                have = Set.of();
            }
            Set<String> haveTopics = have;
            List<String> missing = topics.stream().filter(t -> !haveTopics.contains(t)).toList();
            if (missing.isEmpty()) {
                return true;
            }
            if (!told) {
                log.info("observer: waiting for topics {}{}", String.join(", ", missing),
                        watching ? ": the catalog names them, and the observer reads them all" : " (start the simulator)");
            }
            told = true;
            long until = System.currentTimeMillis() + 2000;
            while (running && System.currentTimeMillis() < until) {
                LockSupport.parkNanos(50_000_000L);
            }
        }
        return false;
    }

    static Map<String, Object> plain(Map<String, Object> m) {
        return new LinkedHashMap<>(m);
    }
}
