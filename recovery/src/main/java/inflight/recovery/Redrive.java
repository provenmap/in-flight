package inflight.recovery;

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

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import inflight.broker.Broker;
import inflight.broker.Capability;
import inflight.broker.Commit;
import inflight.broker.Cursor;
import inflight.broker.Endpoint;
import inflight.broker.Header;
import inflight.broker.Message;
import inflight.broker.Shard;
import inflight.broker.Writer;
import inflight.catalog.Catalog;
import inflight.catalog.CloudEvent;
import inflight.catalog.Json;
import inflight.catalog.Pricing;
import inflight.catalog.Records;
import inflight.catalog.Result;
import inflight.codec.Decoders;

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
    public record Options(Endpoint endpoint, String dlq, String group, List<String> sources, Filter filter, double perSecond, int limit, Decoders decoders,
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
    static final String META = "in-flight-redriven:";

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
        try (Broker broker = o.endpoint().open()) {
            if (!broker.capabilities().contains(Capability.REDRIVE)) {
                throw new IllegalStateException(o.endpoint().show() + " does not support redrive: its adapter cannot write dead letters back");
            }
            if (!broker.streams().contains(o.dlq())) {
                throw new IllegalArgumentException("there is no topic " + o.dlq());
            }
            List<Shard> parts = broker.shards(List.of(o.dlq()));
            Map<Shard, Long> low = broker.starts(parts);
            Map<Shard, Long> high = broker.ends(parts);
            Map<Shard, Commit> committed = committed(broker, o.group());
            Map<Shard, TreeSet<Long>> done = new HashMap<>();
            Map<Shard, Long> from = new LinkedHashMap<>();
            for (Shard tp : parts) {
                Commit om = committed.get(tp);
                from.put(tp, om == null ? low.get(tp) : Math.max(low.get(tp), om.offset()));
                done.put(tp, om == null ? new TreeSet<>() : parse(om.note()));
            }
            // A dry run reads as an observer; a run reads under the redrive group, whose commits mark what is dealt with.
            try (Cursor c = dry ? broker.observe() : broker.consume(o.group()); Writer writer = dry ? null : broker.writer()) {
                c.seek(from);
                return loop(o, dry, stop, c, writer, parts, high, done, pricing, headerNames);
            }
        }
    }

    private static Report loop(Options o, boolean dry, AtomicBoolean stop, Cursor c, Writer writer,
                               List<Shard> parts, Map<Shard, Long> high, Map<Shard, TreeSet<Long>> done, Pricing pricing,
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
        Map<Shard, Long> leftAt = new HashMap<>();
        Map<Shard, Long> next = new HashMap<>();
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
            for (Message r : records) {
                Shard tp = r.shard();
                if (r.offset() >= high.get(tp)) {
                    continue;
                }
                scanned++;
                next.put(tp, r.offset() + 1);
                if (done.get(tp).contains(r.offset())) {
                    already++;
                    continue;
                }
                Map<String, String> hs = r.textHeaders();
                String error = first(hs, List.of(CloudEvent.DeadLetter.ERROR, "__connect.errors.exception.message", "kafka_dlt-exception-message"));
                Result<Records.Decoded> d = o.decoders().decode(r.valueOrEmpty(), hs);
                String id = d.isOk() && d.value().id() != null ? d.value().id() : r.place();
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
                List<Header> out = new ArrayList<>();
                for (Header h : r.headers()) {
                    if (!isDeadLetterHeader(h.key(), headerNames) && !h.key().equals(Records.REDRIVE_OF)) {
                        out.add(h);
                    }
                }
                out.add(new Header(Records.REDRIVE_OF, id.getBytes(StandardCharsets.UTF_8)));
                try {
                    writer.send(target, r.key(), r.value(), out);
                } catch (RuntimeException e) {
                    throw new IllegalStateException("could not write " + id + " back to " + target + ": " + e.getMessage(), e);
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

    private static void commit(Cursor c, Map<Shard, Long> next, Map<Shard, Long> leftAt, Map<Shard, TreeSet<Long>> done) {
        Map<Shard, Commit> commits = new LinkedHashMap<>();
        next.forEach((tp, n) -> {
            Long left = leftAt.get(tp);
            long at = left != null ? left : n;
            TreeSet<Long> beyond = new TreeSet<>(done.get(tp).tailSet(at, false));
            commits.put(tp, new Commit(at, format(beyond)));
        });
        if (!commits.isEmpty()) {
            c.commit(commits);
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
        return key.startsWith("__connect.errors.") || key.startsWith("kafka_dlt-") || key.startsWith("in-flight-") || sourceHeaders.contains(key);
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

    private static Map<Shard, Commit> committed(Broker broker, String group) {
        try {
            return broker.committed(group);
        } catch (RuntimeException e) {
            return Map.of();
        }
    }

    /** {@code in-flight-redriven:17-20,25}: offsets as ranges. */
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
    public static String text(Report r, String dlq, inflight.catalog.Display shown) {
        String money = r.value().signum() > 0 ? " worth " + (shown == null ? inflight.catalog.Display.MONEY : shown).amount(r.value()) : "";
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
