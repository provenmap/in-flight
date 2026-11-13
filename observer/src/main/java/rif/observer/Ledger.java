package rif.observer;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import rif.catalog.Catalog;
import rif.catalog.EventRef;
import rif.catalog.Pricing.Priced;
import rif.kafka.Kafka;

/**
 * Money at risk, from what the observer has read. One caller feeds it; any thread may ask it for stats.
 *
 * <ul>
 * <li>Stuck: every dead letter not yet recovered.</li>
 * <li>Lag: for each service's group, the events it has not committed that the observer saw arrive.</li>
 * <li>Recovered: a flow event whose {@code redriveof} names a stuck one; failing that, a flow event the stuck one's
 * district produced for the same order, which is how a redrive tool that does not mark its events shows; or a dead
 * letter below its redrive group's committed offset when the observer started.</li>
 * </ul>
 *
 * The headline counts each order once, at the most it is worth anywhere it is held up, and each customer once.
 */
public final class Ledger {

    /** A record's place in a topic. */
    public record Pos(String topic, int partition, long offset) {}

    /** Why a stuck dead letter has no value: what the record says it is, as far as it can be read, and the reason. */
    public record Unpriced(String event, String reason) {}

    /** A dead-lettered event: where it sits, what it is worth, and why it could not be read. {@code unpriced} may be null. */
    public record Stuck(String id, String dlq, String district, Pos pos, Priced priced, String error, Instant at, Unpriced unpriced) {}

    /** One message for the view. */
    public sealed interface Msg {}

    public record Event(Priced priced, List<String> to) implements Msg {}

    public record Dead(Stuck stuck) implements Msg {}

    /** {@code afterMs} is null for a dead letter found already redriven when the observer started. */
    public record Recovered(Stuck stuck, Long afterMs) implements Msg {}

    /** Consumer lag of one group: how many events it has not committed, and what the ones the observer saw are worth. */
    public record GroupLag(long events, BigDecimal value, int customers, BigDecimal estimated) {
        public GroupLag(long events, BigDecimal value, int customers) {
            this(events, value, customers, BigDecimal.ZERO);
        }
    }

    /** A stuck spell: from the first event stuck while none were, to the last recovery that emptied the pile. */
    public record Episode(BigDecimal value, long events, long tookMs) {}

    public record Stats(BigDecimal stuckValue, long stuckEvents, int stuckCustomers, long stuckUnpriced, Instant stuckSince,
                        Map<String, BigDecimal> stuckByDistrict, Map<String, GroupLag> lag, BigDecimal lagValue, long lagEvents,
                        BigDecimal recoveredValue, long recoveredEvents, BigDecimal atRiskValue, int atRiskOrders, int atRiskCustomers,
                        Episode lastEpisode, long unpriced, Map<String, IntentLag> byIntent, BigDecimal lagEstimated) {}

    /** Lag on one intent's topic: what the groups reading by it have not committed, and what that is worth. */
    public record IntentLag(String topic, List<String> groups, long events, BigDecimal value) {}

    private record Seen(long offset, Priced priced) {}

    private final Catalog catalog;
    private final int window;
    private final Map<String, Stuck> stuck = new LinkedHashMap<>();
    /** Stuck ids by district and order, for recoveries that do not name the event they recover. */
    private final Map<String, Set<String>> held = new HashMap<>();
    private final Set<String> recoveredIds = new HashSet<>();
    private final Map<String, Long> redrivenBelow = new HashMap<>();
    private final Map<String, ArrayDeque<Seen>> recent = new HashMap<>();
    private final Map<String, Map<String, Long>> committed = new HashMap<>();
    private final Map<String, Long> ends = new HashMap<>();
    /** Topics a service group reads: only those need their recent events remembered for lag. */
    private final Set<String> tracked = new HashSet<>();
    /** Each service's topics: what it lists, and the intent topic it may read instead, through the router. */
    private final Map<String, Set<String>> readsOf = new HashMap<>();
    private final Map<String, String> intentTopicOf;
    private BigDecimal recoveredValue = BigDecimal.ZERO;
    private long recoveredCount;
    private long unpriced;
    private Instant episodeStart;
    private BigDecimal episodeValue = BigDecimal.ZERO;
    private long episodeCount;
    private Episode lastEpisode;
    /** Every dead letter recovered, by its place in its queue, most recent last: what a restart must not count again. */
    private final Set<String> everRecovered = new LinkedHashSet<>();
    /** Dead letters recovered before a restart, by place: read again from the queue, they are skipped, not recounted. */
    private final Set<String> restored = new HashSet<>();
    private boolean episodeRestored;
    /** Each topic's recent average event value, to put a value on lag the observer did not see the events of. */
    private final Map<String, double[]> averages = new HashMap<>();

    static final int REMEMBER_RECOVERED = 200_000;
    static final int AVERAGE_OVER = 500;

    public Ledger(Catalog catalog) {
        this(catalog, 50_000);
    }

    public Ledger(Catalog catalog, int window) {
        this.catalog = catalog;
        this.window = window;
        this.intentTopicOf = catalog.intentTopicsByGroup();
        for (Catalog.Consumer s : catalog.services()) {
            Set<String> reads = new HashSet<>(s.reads());
            String it = intentTopicOf.get(s.group());
            if (it != null) {
                reads.add(it);
            }
            readsOf.put(s.group(), reads);
            tracked.addAll(reads);
        }
    }

    /**
     * An event read from an intent's topic, where the router delivered it. It is the same event the flow topic already
     * showed, so it makes no message: it is only remembered, so the lag of a group reading by intent has a value.
     */
    public synchronized void onIntent(Pos pos, Priced priced) {
        remember(pos, priced);
        average(pos.topic(), priced);
    }

    private void average(String topic, Priced p) {
        if (!p.known() || !tracked.contains(topic)) {
            return;
        }
        double[] a = averages.computeIfAbsent(topic, k -> new double[2]);
        a[1] = Math.min(AVERAGE_OVER, a[1] + 1);
        a[0] += (p.value().doubleValue() - a[0]) / a[1];
    }

    private static String place(Pos pos) {
        return Kafka.key(pos.topic(), pos.partition()) + ":" + pos.offset();
    }

    private void remember(Pos pos, Priced priced) {
        if (tracked.contains(pos.topic())) {
            ArrayDeque<Seen> q = recent.computeIfAbsent(Kafka.key(pos.topic(), pos.partition()), k -> new ArrayDeque<>());
            q.addLast(new Seen(pos.offset(), priced));
            while (q.size() > window) {
                q.removeFirst();
            }
        }
    }

    private static String heldKey(String district, String order) {
        return district + "\u0000" + order;
    }

    /** On start: the redrive group of {@code dlq} has dealt with everything on {@code partition} below {@code offset}. */
    public synchronized void redriven(String dlq, int partition, long offset) {
        redrivenBelow.put(Kafka.key(dlq, partition), offset);
    }

    public synchronized void unreadable() {
        unpriced++;
    }

    /** A dead letter the catalog cannot price: stuck, worth an unknown amount, recovered when its redrive group passes it. */
    public synchronized Msg onDeadUnpriced(String dlq, Pos pos, String event, String reason, String error, Instant failedAt) {
        String district = catalog.ownerOfDlq(dlq).map(Catalog.Consumer::district).orElse("");
        String id = Kafka.key(dlq, pos.partition()) + ":" + pos.offset();
        Priced priced = new Priced(id, new EventRef(event, 0), dlq, district, BigDecimal.ZERO, "", "", null, null);
        return hold(dlq, pos, priced, error, failedAt, new Unpriced(event, reason));
    }

    public synchronized Msg onDead(String dlq, Pos pos, Priced priced, String error, Instant failedAt) {
        if (!priced.known()) {
            // Whose it is is known, what it is worth is not: stuck all the same, its value unknown, never guessed.
            Priced zero = new Priced(priced.eventId(), priced.ref(), priced.topic(), priced.district(), BigDecimal.ZERO, priced.customer(),
                    priced.orderId(), priced.redriveOf(), priced.pricedAs());
            return hold(dlq, pos, zero, error, failedAt, new Unpriced(priced.ref().show(), "nothing has been seen of what order " + priced.orderId() + " is worth"));
        }
        return hold(dlq, pos, priced, error, failedAt, null);
    }

    private Msg hold(String dlq, Pos pos, Priced priced, String error, Instant failedAt, Unpriced unpricedWhy) {
        String district = catalog.ownerOfDlq(dlq).map(Catalog.Consumer::district).orElse(priced.district());
        Stuck s = new Stuck(priced.eventId(), dlq, district, pos, priced, error, failedAt, unpricedWhy);
        if (restored.remove(place(pos))) {
            return null; // recovered before a restart, and counted then
        }
        if (pos.offset() < redrivenBelow.getOrDefault(Kafka.key(dlq, pos.partition()), 0L) || recoveredIds.contains(s.id())) {
            return recover(s, null);
        }
        if (stuck.isEmpty()) {
            if (episodeRestored) {
                episodeRestored = false; // the spell in progress at the restart goes on
            } else {
                episodeStart = failedAt;
                episodeValue = BigDecimal.ZERO;
                episodeCount = 0;
            }
        }
        stuck.put(s.id(), s);
        if (!priced.orderId().isEmpty()) {
            held.computeIfAbsent(heldKey(s.district(), priced.orderId()), k -> new LinkedHashSet<>()).add(s.id());
        }
        return new Dead(s);
    }

    /**
     * The latest committed offsets of the redrive group of {@code dlq}, keyed by {@code topic:partition}. A dead letter
     * below them has been dealt with. That is the only sign of recovery a dead letter with no price, and no order, can give.
     */
    public synchronized List<Msg> onRedriveOffsets(String dlq, Map<String, Long> offsets, Instant at) {
        offsets.forEach((k, o) -> {
            if (Kafka.topicOf(k).equals(dlq)) {
                redrivenBelow.put(k, o);
            }
        });
        List<Stuck> passed = stuck.values().stream()
                .filter(s -> s.unpriced() != null && s.dlq().equals(dlq) && s.pos().offset() < redrivenBelow.getOrDefault(Kafka.key(dlq, s.pos().partition()), 0L))
                .toList();
        List<Msg> out = new ArrayList<>();
        passed.forEach(s -> out.add(resolve(s, at)));
        return out;
    }

    public synchronized List<Msg> onFlow(Pos pos, Priced priced, Instant at) {
        remember(pos, priced);
        average(pos.topic(), priced);
        List<Msg> out = new ArrayList<>();
        out.add(new Event(priced, catalog.readersOf(pos.topic()).stream().map(Catalog.Consumer::district).toList()));
        String orig = priced.redriveOf();
        if (orig != null) {
            Stuck s = stuck.get(orig);
            // A redriven event that overtook its dead letter: remember it, so the dead letter arrives recovered.
            if (s != null) {
                out.add(resolve(s, at));
            } else {
                recoveredIds.add(orig);
            }
        } else {
            Set<String> ids = held.get(heldKey(priced.district(), priced.orderId()));
            if (ids != null) {
                for (String id : List.copyOf(ids)) {
                    out.add(resolve(stuck.get(id), at));
                }
            }
        }
        return out;
    }

    /** A stuck event recovered at {@code at}: off the pile, into the episode, and the episode closed if the pile is empty. */
    private Msg resolve(Stuck s, Instant at) {
        stuck.remove(s.id());
        String k = heldKey(s.district(), s.priced().orderId());
        Set<String> ids = held.get(k);
        if (ids != null) {
            ids.remove(s.id());
            if (ids.isEmpty()) {
                held.remove(k);
            }
        }
        Msg msg = recover(s, at.toEpochMilli() - s.at().toEpochMilli());
        episodeValue = episodeValue.add(s.priced().valueOrZero());
        episodeCount++;
        if (stuck.isEmpty()) {
            if (episodeStart != null) {
                lastEpisode = new Episode(episodeValue, episodeCount, at.toEpochMilli() - episodeStart.toEpochMilli());
            }
            episodeStart = null;
        }
        return msg;
    }

    private Msg recover(Stuck s, Long afterMs) {
        recoveredValue = recoveredValue.add(s.priced().valueOrZero());
        recoveredCount++;
        recoveredIds.remove(s.id());
        everRecovered.add(place(s.pos()));
        if (everRecovered.size() > REMEMBER_RECOVERED) {
            var it = everRecovered.iterator();
            it.next();
            it.remove();
        }
        return new Recovered(s, afterMs);
    }

    /** The latest committed offsets of a service group, and the end of each partition, keyed by {@code topic:partition}. */
    public synchronized void onOffsets(String group, Map<String, Long> offsets, Map<String, Long> logEnds) {
        committed.put(group, Map.copyOf(offsets));
        ends.putAll(logEnds);
        // Forget events every group reading their topic has committed.
        for (Map.Entry<String, ArrayDeque<Seen>> e : recent.entrySet()) {
            String topic = Kafka.topicOf(e.getKey());
            List<Long> floors = new ArrayList<>();
            boolean unknown = false;
            for (Catalog.Consumer s : catalog.services()) {
                if (!readsOf.get(s.group()).contains(topic)) {
                    continue;
                }
                Long f = committed.getOrDefault(s.group(), Map.of()).get(e.getKey());
                if (f == null) {
                    unknown = true;
                } else {
                    floors.add(f);
                }
            }
            if (floors.isEmpty() || unknown) {
                continue;
            }
            long floor = floors.stream().mapToLong(Long::longValue).min().orElse(0);
            ArrayDeque<Seen> q = e.getValue();
            while (!q.isEmpty() && q.peekFirst().offset() < floor) {
                q.removeFirst();
            }
        }
    }

    private record Lag(long count, List<Priced> held, BigDecimal estimated) {}

    /**
     * Events a group has not committed, with the ones the observer saw, between its committed offset and the end polled
     * with it. Events newer than that end are in flight, not yet lag, so the count and the value agree.
     */
    private Lag lagOf(String group) {
        return lagOf(group, readsOf.getOrDefault(group, Set.of()));
    }

    private Lag lagOf(String group, Set<String> reads) {
        Map<String, Long> offs = committed.getOrDefault(group, Map.of());
        long count = 0;
        double estimated = 0;
        List<Priced> heldUp = new ArrayList<>();
        for (Map.Entry<String, Long> e : ends.entrySet()) {
            String k = e.getKey();
            if (!reads.contains(Kafka.topicOf(k))) {
                continue;
            }
            long end = e.getValue();
            long behind = Math.max(0, end - offs.getOrDefault(k, end));
            count += behind;
            long from = offs.getOrDefault(k, Long.MAX_VALUE);
            ArrayDeque<Seen> q = recent.get(k);
            long seen = 0;
            if (q != null) {
                for (Seen s : q) {
                    if (s.offset() >= from && s.offset() < end) {
                        heldUp.add(s.priced());
                        seen++;
                    }
                }
            }
            // Events behind the group that the observer never saw, from before it started: valued at the topic's average.
            double[] avg = averages.get(Kafka.topicOf(k));
            if (avg != null && behind > seen) {
                estimated += (behind - seen) * avg[0];
            }
        }
        return new Lag(count, heldUp, BigDecimal.valueOf(Math.round(estimated * 100) / 100.0));
    }

    public synchronized List<Stuck> stuckNow() {
        return List.copyOf(stuck.values());
    }

    public synchronized Stats stats() {
        List<Stuck> heldNow = List.copyOf(stuck.values());
        Map<String, Lag> groups = new LinkedHashMap<>();
        catalog.services().forEach(s -> groups.put(s.group(), lagOf(s.group())));
        List<Stuck> priced = heldNow.stream().filter(s -> s.unpriced() == null).toList();

        Map<String, Priced> byOrder = new HashMap<>();
        List<Priced> all = new ArrayList<>(priced.stream().map(Stuck::priced).toList());
        groups.values().forEach(l -> all.addAll(l.held()));
        for (Priced p : all) {
            Priced had = byOrder.get(p.orderId());
            if (had == null || p.valueOrZero().compareTo(had.valueOrZero()) > 0) {
                byOrder.put(p.orderId(), p);
            }
        }
        Map<String, BigDecimal> byDistrict = new TreeMap<>();
        heldNow.forEach(s -> byDistrict.merge(s.district(), s.priced().valueOrZero(), BigDecimal::add));
        Map<String, GroupLag> lag = new TreeMap<>();
        groups.forEach((g, l) -> lag.put(g, new GroupLag(l.count(), sum(l.held()).add(l.estimated()), (int) l.held().stream().map(Priced::customer).distinct().count(),
                l.estimated())));
        BigDecimal estimated = lag.values().stream().map(GroupLag::estimated).reduce(BigDecimal.ZERO, BigDecimal::add);

        Instant since = heldNow.stream().map(Stuck::at).min(Instant::compareTo).orElse(null);
        return new Stats(
                sum(heldNow.stream().map(Stuck::priced).toList()),
                heldNow.size(),
                (int) priced.stream().map(s -> s.priced().customer()).distinct().count(),
                heldNow.size() - priced.size(),
                since,
                byDistrict,
                lag,
                lag.values().stream().map(GroupLag::value).reduce(BigDecimal.ZERO, BigDecimal::add),
                lag.values().stream().mapToLong(GroupLag::events).sum(),
                recoveredValue,
                recoveredCount,
                sum(List.copyOf(byOrder.values())).add(estimated),
                byOrder.size(),
                (int) byOrder.values().stream().map(Priced::customer).distinct().count(),
                lastEpisode,
                unpriced,
                byIntent(),
                estimated);
    }

    /** Lag on each intent's topic, by intent, for the intents some service reads by. */
    private Map<String, IntentLag> byIntent() {
        Map<String, IntentLag> out = new TreeMap<>();
        for (Catalog.Consumer s : catalog.services()) {
            String topic = intentTopicOf.get(s.group());
            if (topic == null) {
                continue;
            }
            Lag l = lagOf(s.group(), Set.of(topic));
            IntentLag had = out.get(s.intent());
            List<String> groups = new ArrayList<>(had == null ? List.of() : had.groups());
            groups.add(s.group());
            out.put(s.intent(), new IntentLag(topic, groups, (had == null ? 0 : had.events()) + l.count(),
                    (had == null ? BigDecimal.ZERO : had.value()).add(sum(l.held())).add(l.estimated())));
        }
        return out;
    }

    // ---- across restarts ----------------------------------------------------------------------------------------------

    /**
     * What a restart must not lose: recoveries counted, the episode in progress and the last one, and the dead letters
     * already recovered (by place, so reading their queue again does not count them twice), and each topic's average.
     */
    public synchronized com.fasterxml.jackson.databind.node.ObjectNode state() {
        var o = rif.catalog.Json.object();
        o.put("version", 1);
        o.put("recoveredValue", recoveredValue).put("recoveredCount", recoveredCount).put("unpriced", unpriced);
        if (lastEpisode != null) {
            o.putObject("lastEpisode").put("value", lastEpisode.value()).put("events", lastEpisode.events()).put("tookMs", lastEpisode.tookMs());
        }
        if (episodeStart != null && !stuck.isEmpty()) {
            o.putObject("episode").put("start", episodeStart.toString()).put("value", episodeValue).put("count", episodeCount);
        }
        var rec = o.putArray("recovered");
        everRecovered.forEach(rec::add);
        restored.forEach(rec::add);
        var avg = o.putObject("averages");
        averages.forEach((t, a) -> avg.putArray(t).add(a[0]).add(a[1]));
        return o;
    }

    /** Takes back what {@link #state()} kept. Called before anything is read. */
    public synchronized void restore(com.fasterxml.jackson.databind.JsonNode o) {
        if (o == null || o.path("version").asInt() != 1) {
            return;
        }
        recoveredValue = o.path("recoveredValue").decimalValue();
        recoveredCount = o.path("recoveredCount").asLong();
        unpriced = o.path("unpriced").asLong();
        var e = o.get("lastEpisode");
        if (e != null && e.isObject()) {
            lastEpisode = new Episode(e.path("value").decimalValue(), e.path("events").asLong(), e.path("tookMs").asLong());
        }
        var ep = o.get("episode");
        if (ep != null && ep.isObject()) {
            episodeStart = Instant.parse(ep.get("start").asText());
            episodeValue = ep.path("value").decimalValue();
            episodeCount = ep.path("count").asLong();
            episodeRestored = true;
        }
        o.path("recovered").forEach(x -> restored.add(x.asText()));
        o.path("averages").properties().forEach(x -> averages.put(x.getKey(), new double[] {x.getValue().path(0).asDouble(), x.getValue().path(1).asDouble()}));
    }

    private static BigDecimal sum(List<Priced> ps) {
        BigDecimal total = BigDecimal.ZERO;
        for (Iterator<Priced> it = ps.iterator(); it.hasNext(); ) {
            total = total.add(it.next().valueOrZero());
        }
        return total;
    }
}
