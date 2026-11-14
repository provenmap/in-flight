package inflight.router;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLongArray;

import com.fasterxml.jackson.databind.node.ObjectNode;

import inflight.catalog.Json;

/**
 * Counters the control API reports, for one router: in total and per intent. Updated from stream threads.
 *
 * <p>Routed, delivered, late and unkeyed are counted per copy, so per intent. Unparseable, uncatalogued and unrouted
 * are about a source record, and each intent's application reads its source topics itself: such a record is counted
 * once in the total for each application that reads it, and once under each intent that reads its topic.
 */
public final class RouterStats {

    public enum Counter {
        ROUTED, DELIVERED, LATE, UNROUTED, UNKEYED, UNPARSEABLE,
        /** Events whose type and version the active catalog does not know: produced around the schema gate. */
        UNCATALOGUED;

        String wire() {
            return name().toLowerCase();
        }
    }

    private static final class Counts {
        final AtomicLongArray values = new AtomicLongArray(Counter.values().length);

        ObjectNode json() {
            ObjectNode o = Json.object();
            for (Counter c : Counter.values()) {
                o.put(c.wire(), values.get(c.ordinal()));
            }
            return o;
        }
    }

    private final Counts total = new Counts();
    private final ConcurrentMap<String, Counts> byIntent = new ConcurrentHashMap<>();
    private final Set<String> warnedUncatalogued = ConcurrentHashMap.newKeySet();

    /** Counts one copy for one intent. */
    public void count(Counter c, String intent) {
        total.values.incrementAndGet(c.ordinal());
        counts(intent).values.incrementAndGet(c.ordinal());
    }

    /** Counts one source record once in the total, and under each intent that read it. */
    public void countSource(Counter c, Collection<String> intents) {
        total.values.incrementAndGet(c.ordinal());
        intents.forEach(i -> counts(i).values.incrementAndGet(c.ordinal()));
    }

    public long get(Counter c) {
        return total.values.get(c.ordinal());
    }

    public long get(Counter c, String intent) {
        Counts k = byIntent.get(intent);
        return k == null ? 0 : k.values.get(c.ordinal());
    }

    /** True the first time an uncatalogued type and version is seen, so it is logged once. */
    public boolean firstUncatalogued(String typeAndVersion) {
        return warnedUncatalogued.add(typeAndVersion);
    }

    private Counts counts(String intent) {
        return byIntent.computeIfAbsent(intent, k -> new Counts());
    }

    /** The totals, and under {@code intents} the same counters for each intent, by name. */
    public ObjectNode snapshot() {
        ObjectNode o = total.json();
        ObjectNode intents = o.putObject("intents");
        for (Map.Entry<String, Counts> e : new TreeMap<>(byIntent).entrySet()) {
            intents.set(e.getKey(), e.getValue().json());
        }
        return o;
    }

    public void reset() {
        for (Counter c : Counter.values()) {
            total.values.set(c.ordinal(), 0);
        }
        byIntent.clear();
        warnedUncatalogued.clear();
    }
}
