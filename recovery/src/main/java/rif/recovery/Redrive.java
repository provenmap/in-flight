package rif.recovery;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;

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

/**
 * A redrive for any dead-letter queue: reads it under a group of its own, from where that group last stopped, and
 * writes each matching record back to the topic it came from, as it was, with a {@code redriveof} header naming the
 * dead letter it recovers. Filtered by error, order or time; rate-limited; a dry run first, which reads and writes
 * nothing but says what a run would do.
 *
 * <p>The group's committed offset on each partition is where every record before it has been dealt with: the observer
 * counts dead letters below it recovered, after a restart too. A filter can leave records behind, so the commit stops at
 * the first one left; the ones redriven beyond it are listed in the commit's metadata and skipped by the next pass.
 */
public final class Redrive {

    /** Which dead letters: whose error contains this text, of this order, failed in this window. Null means any. */
    public record Filter(String errorContains, String orderId, Instant from, Instant to) {
        public static final Filter ALL = new Filter(null, null, null, null);

        public boolean isAll() {
            return errorContains == null && orderId == null && from == null && to == null;
        }
    }

    /**
     * One pass over {@code dlq} under {@code group}, to the end it has when the pass starts. {@code sources} are where a
     * dead letter goes when its headers do not say where it came from: the topics its consumer reads, used only when
     * there is exactly one. {@code perSecond} caps how fast it writes; {@code limit} how many it writes.
     */
    public record Options(String bootstrap, String dlq, String group, List<String> sources, Filter filter, double perSecond, int limit, Decoders decoders,
                          Catalog catalog) {
        public Options {
            filter = filter == null ? Filter.ALL : filter;
            perSecond = perSecond <= 0 ? 50 : Math.min(perSecond, 5000);
            limit = limit <= 0 ? Integer.MAX_VALUE : limit;
            decoders = decoders == null ? Decoders.plain() : decoders;
            sources = sources == null ? List.of() : List.copyOf(sources);
        }
    }

    /** What a pass did, or would do. */
    public record Report(boolean dryRun, int scanned, int matched, int written, int alreadyRedriven, int noTarget, Map<String, Integer> byError,
                         Map<String, Integer> byTarget, BigDecimal value, int unpriced, List<String> ids, boolean stoppedEarly, String note) {}

    /** The metadata a filtered pass leaves on its commit: the offsets beyond the commit it has already redriven. */
    static final String META = "rif-redriven:";

    private Redrive() {}

    public static Report dryRun(Options o) {
        return pass(o, true, new AtomicBoolean(false));
    }

    public static Report run(Options o, AtomicBoolean stop) {
        return pass(o, false, stop);
    }

    private static Report pass(Options o, boolean dry, AtomicBoolean stop) {
        Pricing pricing = o.catalog() == null ? null : new Pricing(o.catalog());
        List<String> headerNames = sourceHeaders(o.catalog());
        try (Admin admin = Kafka.admin(o.bootstrap()); KafkaConsumer<byte[], byte[]> c = Kafka.consumer(o.bootstrap(), dry ? null : o.group())) {
            if (!Kafka.listTopics(admin).contains(o.dlq())) {
                throw new IllegalArgumentException("there is no topic " + o.dlq());
            }
            List<TopicPartition> parts = Kafka.partitions(admin, List.of(o.dlq()));
            Map<TopicPartition, Long> low = c.beginningOffsets(parts);
            Map<TopicPartition, Long> high = c.endOffsets(parts);
            Map<String, OffsetAndMetadata> committed = committed(admin, o.group());
            c.assign(parts);
            Map<TopicPartition, TreeSet<Long>> done = new HashMap<>();
            for (TopicPartition tp : parts) {
                OffsetAndMetadata om = committed.get(Kafka.key(tp));
                long from = om == null ? low.get(tp) : Math.max(low.get(tp), om.offset());
                c.seek(tp, from);
                done.put(tp, om == null ? new TreeSet<>() : parse(om.metadata()));
            }
            KafkaProducer<String, byte[]> producer = dry ? null : Kafka.producer(o.bootstrap());
            try {
                return loop(o, dry, stop, c, producer, parts, high, done, pricing, headerNames);
            } finally {
                if (producer != null) {
                    producer.close(Duration.ofSeconds(10));
                }
            }
        }
    }

    private static Report loop(Options o, boolean dry, AtomicBoolean stop, KafkaConsumer<byte[], byte[]> c, KafkaProducer<String, byte[]> producer,
                               List<TopicPartition> parts, Map<TopicPartition, Long> high, Map<TopicPartition, TreeSet<Long>> done, Pricing pricing,
                               List<String> headerNames) {
        int scanned = 0;
        int matched = 0;
        int written = 0;
        int already = 0;
        int noTarget = 0;
        int unpriced = 0;
        BigDecimal value = BigDecimal.ZERO;
        Map<String, Integer> byError = new TreeMap<>();
        Map<String, Integer> byTarget = new TreeMap<>();
        List<String> ids = new ArrayList<>();
        // Per partition: the first record left behind (the commit stops there), and the next offset to commit otherwise.
        Map<TopicPartition, Long> leftAt = new HashMap<>();
        Map<TopicPartition, Long> next = new HashMap<>();
        long nanosPer = (long) (1_000_000_000L / o.perSecond());
        long nextWrite = System.nanoTime();
        boolean early = false;
        long idleSince = System.currentTimeMillis();
        while (!stop.get()) {
            boolean finished = parts.stream().allMatch(tp -> c.position(tp) >= high.get(tp));
            if (finished || System.currentTimeMillis() - idleSince > 15_000) {
                break;
            }
            var records = c.poll(Duration.ofMillis(200));
            if (!records.isEmpty()) {
                idleSince = System.currentTimeMillis();
            }
            for (ConsumerRecord<byte[], byte[]> r : records) {
                TopicPartition tp = new TopicPartition(r.topic(), r.partition());
                if (r.offset() >= high.get(tp)) {
                    continue;
                }
                scanned++;
                next.put(tp, r.offset() + 1);
                if (done.get(tp).contains(r.offset())) {
                    already++;
                    continue;
                }
                Map<String, String> hs = Kafka.headers(r.headers());
                String error = first(hs, List.of(CloudEvent.DeadLetter.ERROR, "__connect.errors.exception.message", "kafka_dlt-exception-message"));
                byte[] v = r.value() == null ? new byte[0] : r.value();
                Result<Records.Decoded> d = o.decoders().decode(v, hs);
                String id = d.isOk() && d.value().id() != null ? d.value().id() : Kafka.key(r.topic(), r.partition()) + ":" + r.offset();
                String source = first(hs, headerNames);
                if (!matches(o.filter(), error, failedAt(hs, r.timestamp()), d, o.catalog(), source)) {
                    leftAt.putIfAbsent(tp, r.offset());
                    continue;
                }
                if (written >= o.limit()) {
                    leftAt.putIfAbsent(tp, r.offset());
                    early = true;
                    continue;
                }
                String target = source != null ? source : o.sources().size() == 1 ? o.sources().get(0) : null;
                matched++;
                byError.merge(cause(error), 1, Integer::sum);
                if (target == null) {
                    noTarget++;
                    leftAt.putIfAbsent(tp, r.offset());
                    continue;
                }
                byTarget.merge(target, 1, Integer::sum);
                if (pricing != null && d.isOk()) {
                    Result<Pricing.Priced> p = pricing.priceDecoded(d.value(), List.of(target), id);
                    if (p.isOk() && p.value().known()) {
                        value = value.add(p.value().value());
                    } else {
                        unpriced++;
                    }
                } else {
                    unpriced++;
                }
                if (ids.size() < 20) {
                    ids.add(id);
                }
                if (dry) {
                    continue;
                }
                long wait = nextWrite - System.nanoTime();
                if (wait > 0) {
                    LockSupport.parkNanos(wait);
                }
                nextWrite = Math.max(nextWrite, System.nanoTime()) + nanosPer;
                RecordHeaders out = new RecordHeaders();
                for (Header h : r.headers()) {
                    if (!isDeadLetterHeader(h.key(), headerNames)) {
                        out.add(h);
                    }
                }
                out.remove(Records.REDRIVE_OF);
                out.add(Records.REDRIVE_OF, id.getBytes(StandardCharsets.UTF_8));
                String key = r.key() == null ? null : new String(r.key(), StandardCharsets.UTF_8);
                try {
                    producer.send(new ProducerRecord<>(target, null, key, r.value(), out)).get();
                } catch (Exception e) {
                    throw new IllegalStateException("could not write " + id + " back to " + target + ": " + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()), e);
                }
                written++;
                if (leftAt.containsKey(tp)) {
                    done.get(tp).add(r.offset());
                }
                if (written % 100 == 0) {
                    commit(c, next, leftAt, done);
                }
            }
        }
        if (!dry) {
            commit(c, next, leftAt, done);
        }
        String note = dry ? "A dry run: nothing was written." : stop.get() ? "Stopped before the end." : null;
        return new Report(dry, scanned, matched, written, already, noTarget, byError, byTarget, value, unpriced, ids, early, note);
    }

    private static void commit(KafkaConsumer<byte[], byte[]> c, Map<TopicPartition, Long> next, Map<TopicPartition, Long> leftAt,
                               Map<TopicPartition, TreeSet<Long>> done) {
        Map<TopicPartition, OffsetAndMetadata> commits = new LinkedHashMap<>();
        next.forEach((tp, n) -> {
            Long left = leftAt.get(tp);
            long at = left != null ? left : n;
            TreeSet<Long> beyond = new TreeSet<>(done.get(tp).tailSet(at, false));
            commits.put(tp, new OffsetAndMetadata(at, format(beyond)));
        });
        if (!commits.isEmpty()) {
            c.commitSync(commits);
        }
    }

    // ---- filters and causes ----------------------------------------------------------------------------------------

    static boolean matches(Filter f, String error, Instant at, Result<Records.Decoded> d, Catalog catalog, String source) {
        if (f.errorContains() != null && (error == null || !error.toLowerCase(Locale.ROOT).contains(f.errorContains().toLowerCase(Locale.ROOT)))) {
            return false;
        }
        if (f.from() != null && at.isBefore(f.from()) || f.to() != null && at.isAfter(f.to())) {
            return false;
        }
        if (f.orderId() != null) {
            if (!d.isOk() || catalog == null) {
                return false;
            }
            Pricing p = new Pricing(catalog);
            Result<Pricing.Priced> priced = p.priceDecoded(d.value(), source == null ? List.of() : List.of(source), "");
            return priced.isOk() && f.orderId().equals(priced.value().orderId());
        }
        return true;
    }

    /**
     * An error with what varies taken out (numbers, ids, quoted values), so the same failure on many records reads as
     * one cause: "cannot read total at offset 5 of 'ord-1'" and "... 9 of 'ord-2'" are the same thing.
     */
    public static String cause(String error) {
        if (error == null || error.isBlank()) {
            return "no error given";
        }
        String s = error.strip();
        s = s.replaceAll("'[^']*'|\"[^\"]*\"", "…");
        s = s.replaceAll("\\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\\b", "…");
        s = s.replaceAll("\\b[A-Za-z]*[-_]?\\d[\\w-]*\\b", "…");
        s = s.replaceAll("(…[\\s,]*)+…", "…");
        return s.length() > 160 ? s.substring(0, 160) + "…" : s;
    }

    static Instant failedAt(Map<String, String> hs, long timestamp) {
        String f = hs.get(CloudEvent.DeadLetter.FAILED_AT);
        if (f != null) {
            try {
                return Instant.parse(f);
            } catch (DateTimeParseException ignored) {
                // the record's own time
            }
        }
        return Instant.ofEpochMilli(timestamp);
    }

    private static List<String> sourceHeaders(Catalog c) {
        List<String> out = new ArrayList<>();
        if (c != null && c.deadLetterHeaders().sourceTopic() != null) {
            out.add(c.deadLetterHeaders().sourceTopic());
        }
        out.addAll(List.of(CloudEvent.DeadLetter.SOURCE_TOPIC, "__connect.errors.topic", "kafka_dlt-original-topic"));
        return out;
    }

    private static boolean isDeadLetterHeader(String key, List<String> sourceHeaders) {
        return key.startsWith("__connect.errors.") || key.startsWith("kafka_dlt-") || key.startsWith("rif-") || sourceHeaders.contains(key);
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

    private static Map<String, OffsetAndMetadata> committed(Admin admin, String group) {
        try {
            Map<TopicPartition, OffsetAndMetadata> got = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(15, java.util.concurrent.TimeUnit.SECONDS);
            Map<String, OffsetAndMetadata> out = new HashMap<>();
            got.forEach((tp, om) -> {
                if (om != null) {
                    out.put(Kafka.key(tp), om);
                }
            });
            return out;
        } catch (Exception e) {
            return Map.of();
        }
    }

    /** {@code rif-redriven:17-20,25}: offsets as ranges. */
    static String format(TreeSet<Long> offsets) {
        if (offsets.isEmpty()) {
            return "";
        }
        StringBuilder b = new StringBuilder(META);
        Long start = null;
        Long prev = null;
        for (long o : offsets) {
            if (start == null) {
                start = o;
            } else if (o != prev + 1) {
                b.append(start).append(start.equals(prev) ? "" : "-" + prev).append(',');
                start = o;
            }
            prev = o;
        }
        b.append(start).append(start.equals(prev) ? "" : "-" + prev);
        return b.toString();
    }

    static TreeSet<Long> parse(String meta) {
        TreeSet<Long> out = new TreeSet<>();
        if (meta == null || !meta.startsWith(META)) {
            return out;
        }
        for (String part : meta.substring(META.length()).split(",")) {
            if (part.isBlank()) {
                continue;
            }
            try {
                if (part.contains("-")) {
                    long a = Long.parseLong(part.substring(0, part.indexOf('-')));
                    long z = Long.parseLong(part.substring(part.indexOf('-') + 1));
                    for (long x = a; x <= z && out.size() < 1_000_000; x++) {
                        out.add(x);
                    }
                } else {
                    out.add(Long.parseLong(part));
                }
            } catch (NumberFormatException ignored) {
                // a commit someone else made: nothing of ours in it
            }
        }
        return out;
    }

    public static ObjectNode json(Report r) {
        ObjectNode o = Json.object();
        o.put("dryRun", r.dryRun()).put("scanned", r.scanned()).put("matched", r.matched()).put("written", r.written())
                .put("alreadyRedriven", r.alreadyRedriven()).put("noTarget", r.noTarget()).put("value", r.value()).put("unpriced", r.unpriced())
                .put("stoppedEarly", r.stoppedEarly());
        ObjectNode e = o.putObject("byError");
        r.byError().forEach(e::put);
        ObjectNode t = o.putObject("byTarget");
        r.byTarget().forEach(t::put);
        ArrayNode ids = o.putArray("ids");
        r.ids().forEach(ids::add);
        if (r.note() != null) {
            o.put("note", r.note());
        }
        return o;
    }

    /** One paragraph for a person, with what the dead letters are worth as the flow shows it. */
    public static String text(Report r, String dlq, rif.catalog.Display shown) {
        String money = r.value().signum() > 0 ? " worth " + (shown == null ? rif.catalog.Display.MONEY : shown).amount(r.value()) : "";
        StringBuilder b = new StringBuilder();
        if (r.dryRun()) {
            b.append("A redrive of ").append(dlq).append(" would write ").append(r.matched() - r.noTarget()).append(" of ").append(r.scanned())
                    .append(" dead letters back").append(money);
        } else {
            b.append("Redrove ").append(r.written()).append(" of ").append(r.scanned()).append(" dead letters from ").append(dlq).append(money);
        }
        if (!r.byTarget().isEmpty()) {
            b.append(" to ").append(String.join(", ", r.byTarget().keySet()));
        }
        b.append('.');
        if (r.noTarget() > 0) {
            b.append(' ').append(r.noTarget()).append(" say nowhere where they came from, and their consumer reads more than one topic: they stay.");
        }
        if (r.alreadyRedriven() > 0) {
            b.append(' ').append(r.alreadyRedriven()).append(" were redriven before and are skipped.");
        }
        if (r.matched() < r.scanned() - r.alreadyRedriven()) {
            b.append(' ').append(r.scanned() - r.alreadyRedriven() - r.matched()).append(" did not match the filter and stay.");
        }
        if (r.stoppedEarly()) {
            b.append(" The limit stopped it early.");
        }
        if (r.note() != null) {
            b.append(' ').append(r.note());
        }
        return b.toString();
    }
}
