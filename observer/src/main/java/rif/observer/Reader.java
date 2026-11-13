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
import rif.kafka.Decoders;
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
    private volatile List<String> missing = List.of();
    private volatile String problem;
    private final Decoders decoders;
    private final java.nio.file.Path stateFile;
    /** The chart: money stuck and in lag every few seconds, the last hour of it. Kept across restarts. */
    private final java.util.ArrayDeque<double[]> history = new java.util.ArrayDeque<>();
    static final long HISTORY_EVERY_MS = 5000;
    static final int HISTORY_POINTS = 720;
    static final long SAVE_EVERY_MS = 10_000;
    private Thread thread;

    public Reader(String bootstrap, Catalog catalog, Publisher publish, Poller poller, boolean watching) {
        this(bootstrap, catalog, publish, poller, watching, Decoders.plain());
    }

    public Reader(String bootstrap, Catalog catalog, Publisher publish, Poller poller, boolean watching, Decoders decoders) {
        this(bootstrap, catalog, publish, poller, watching, decoders, null);
    }

    /** {@code stateFile}: where the ledger and the chart are kept between runs, or null to keep nothing. */
    public Reader(String bootstrap, Catalog catalog, Publisher publish, Poller poller, boolean watching, Decoders decoders, java.nio.file.Path stateFile) {
        this.stateFile = stateFile;
        this.decoders = decoders;
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
        load();
        try (Admin admin = Kafka.admin(bootstrap); KafkaConsumer<byte[], byte[]> c = Kafka.consumer(bootstrap, null)) {
            consumer = c;
            List<String> wanted = new ArrayList<>(catalog.topics());
            wanted.addAll(catalog.dlqs());
            Set<String> existing = waitForCluster(admin);
            if (existing == null) {
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
            // Topics that do not exist yet are waited for, not fatal: a flow is often watched before all of it is deployed.
            List<String> topics = new ArrayList<>(wanted.stream().filter(existing::contains).toList());
            // Intent topics the router already writes are read like flow topics; ones it creates later, from their start.
            intentTopics.stream().filter(existing::contains).forEach(topics::add);
            missing = wanted.stream().filter(t -> !existing.contains(t)).toList();
            if (!missing.isEmpty()) {
                log.info("observer: waiting for topics {}: the catalog names them, and they do not exist yet", String.join(", ", missing));
            }
            List<TopicPartition> parts = new ArrayList<>(Kafka.partitions(admin, topics));
            // Dead letters from the start; flow topics from the end, pinned now, so nothing produced from here on is skipped.
            Map<TopicPartition, Long> low = c.beginningOffsets(parts);
            Map<TopicPartition, Long> high = c.endOffsets(parts);
            c.assign(parts);
            for (TopicPartition tp : parts) {
                c.seek(tp, dlqs.contains(tp.topic()) ? low.get(tp) : high.get(tp));
            }
            long lastLook = System.currentTimeMillis();
            log.info("observer: reading {} from the start and {} flow topics from the end", String.join(", ", catalog.dlqs()), catalog.topics().stream().filter(t -> !missing.contains(t)).count());
            reading = true;

            Poller.Polled seen = null;
            long lastStats = 0;
            long lastPoint = 0;
            long lastSave = System.currentTimeMillis();
            while (running) {
                if (System.currentTimeMillis() - lastLook > 5000 && !(intentTopics.stream().allMatch(topics::contains) && missing.isEmpty())) {
                    lastLook = System.currentTimeMillis();
                    Set<String> now = Kafka.listTopics(admin);
                    // Topics created since: everything on them is new, so they are read from their start.
                    List<String> fresh = new ArrayList<>(intentTopics.stream().filter(t -> !topics.contains(t) && now.contains(t)).toList());
                    missing.stream().filter(now::contains).forEach(fresh::add);
                    missing = missing.stream().filter(t -> !now.contains(t)).toList();
                    if (!fresh.isEmpty()) {
                        List<TopicPartition> more = Kafka.partitions(admin, fresh);
                        parts.addAll(more);
                        topics.addAll(fresh);
                        c.assign(parts);
                        c.seekToBeginning(more);
                        log.info("observer: reading new topics {}", String.join(", ", fresh));
                    }
                }
                List<Msg> msgs = new ArrayList<>();
                if (parts.isEmpty()) {
                    LockSupport.parkNanos(100_000_000L); // nothing to read until a topic appears
                } else {
                    for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofMillis(100))) {
                        msgs.addAll(read(r));
                    }
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
                    ObjectNode stats = statsJson();
                    frame.add(stats);
                    if (lastStats - lastPoint >= HISTORY_EVERY_MS) {
                        lastPoint = lastStats;
                        point(lastStats, stats);
                    }
                }
                if (stateFile != null && System.currentTimeMillis() - lastSave >= SAVE_EVERY_MS) {
                    lastSave = System.currentTimeMillis();
                    save();
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
            problem = e.getMessage();
            log.error("reader: {}", e.getMessage(), e);
        } finally {
            reading = false;
            save();
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
            Result<Records.Decoded> d = decoders.decode(v, Kafka.headers(r.headers()));
            Result<Pricing.Priced> p = d.isOk() ? pricing.priceDecoded(d.value(), List.of(), Kafka.key(r.topic(), r.partition()) + ":" + r.offset()) : Result.err(d.errors());
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
        Result<Records.Decoded> decoded = decoders.decode(value, hs);
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
        List<Msg> out = new ArrayList<>(1);
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
            Msg m = ledger.onDead(r.topic(), pos, priced.value(), error, at);
            if (m != null) {
                out.add(m);
            }
            return out;
        }
        // Stuck all the same: a dead letter nobody can price is still an order that did not get through.
        Records.Decoded d = decoded.isOk() ? decoded.value() : null;
        String event = d != null && d.type() != null ? (d.version() != null ? d.type() + "@v" + d.version() : d.type()) : "an unreadable record";
        Msg m = ledger.onDeadUnpriced(r.topic(), pos, event, priced.error(), error, at);
        if (m != null) {
            out.add(m);
        }
        return out;
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
        ArrayNode h = o.putArray("history");
        synchronized (history) {
            history.forEach(p -> h.addArray().add((long) p[0]).add(p[1]).add(p[2]));
        }
        return o;
    }

    private void point(long at, ObjectNode stats) {
        synchronized (history) {
            history.addLast(new double[] {at, stats.get("stuck").get("value").asDouble(), stats.get("lag").get("value").asDouble()});
            while (history.size() > HISTORY_POINTS) {
                history.removeFirst();
            }
        }
    }

    /** The ledger and the chart from the last run, if there was one. */
    private void load() {
        if (stateFile == null || !java.nio.file.Files.exists(stateFile)) {
            return;
        }
        try {
            Result<JsonNode> j = Json.parse(java.nio.file.Files.readString(stateFile));
            if (!j.isOk()) {
                log.warn("observer: {} is not JSON; starting afresh", stateFile);
                return;
            }
            ledger.restore(j.value().get("ledger"));
            synchronized (history) {
                j.value().path("history").forEach(p -> history.addLast(new double[] {p.path(0).asDouble(), p.path(1).asDouble(), p.path(2).asDouble()}));
            }
            log.info("observer: kept from the last run: {}", stateFile);
        } catch (java.io.IOException | RuntimeException e) {
            log.warn("observer: cannot read {}: {}", stateFile, e.getMessage());
        }
    }

    /** Writes the ledger and the chart, whole or not at all. */
    void save() {
        if (stateFile == null) {
            return;
        }
        try {
            ObjectNode o = Json.object();
            o.set("ledger", ledger.state());
            ArrayNode h = o.putArray("history");
            synchronized (history) {
                history.forEach(p -> h.addArray().add((long) p[0]).add(p[1]).add(p[2]));
            }
            java.nio.file.Files.createDirectories(stateFile.getParent());
            java.nio.file.Path tmp = stateFile.resolveSibling(stateFile.getFileName() + ".tmp");
            java.nio.file.Files.writeString(tmp, Json.write(o));
            java.nio.file.Files.move(tmp, stateFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        } catch (java.io.IOException | RuntimeException e) {
            log.warn("observer: cannot save {}: {}", stateFile, e.getMessage());
        }
    }

    /** The cluster's topics, once it answers; null when stopped first. */
    private Set<String> waitForCluster(Admin admin) {
        boolean told = false;
        while (running) {
            try {
                Set<String> have = Kafka.listTopics(admin);
                problem = null;
                return have;
            } catch (RuntimeException e) {
                problem = "Kafka at " + bootstrap + " does not answer: " + e.getMessage();
                if (!told) {
                    log.info("observer: {}", problem);
                }
                told = true;
            }
            long until = System.currentTimeMillis() + 2000;
            while (running && System.currentTimeMillis() < until) {
                LockSupport.parkNanos(50_000_000L);
            }
        }
        return null;
    }

    /** The catalog's topics that do not exist yet: they are read as soon as they do. */
    public List<String> missing() {
        return missing;
    }

    /** Why the reader is not reading, or null. */
    public String problem() {
        return problem;
    }

    static Map<String, Object> plain(Map<String, Object> m) {
        return new LinkedHashMap<>(m);
    }
}
