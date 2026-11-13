package rif.catalog;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;

import rif.catalog.Catalog.EventSpec;
import rif.catalog.Records.Decoded;

/**
 * Puts a value on events, by what the catalog declares. Nothing is guessed: an undeclared type or a missing path is an
 * error, and a worth that cannot be established is unknown, never zero.
 *
 * <p>An event's worth is, in order: the value its declared {@code value} path holds; else its own effect on the
 * catalog's measure (assign from a field, or the case's last known worth plus or minus a delta); else what has been
 * seen for its case. That last is why one instance should price a whole flow in order: it remembers each case's worth.
 *
 * <p>A record that does not say its type and version is priced as one of the catalog's types for its topic, newest
 * version first: the first whose order, customer and value all resolve. A version the catalog does not have yet, the
 * usual way a schema change arrives, is priced the same way from the versions of its type it has.
 */
public final class Pricing {

    /** What one event is worth, to whom, and where it sits on the map. */
    public record Priced(String eventId, EventRef ref, String topic, String district, BigDecimal value, String customer, String orderId,
                         String redriveOf, EventRef pricedAs) {

        /** False when the catalog could say whose it is but not what it is worth. */
        public boolean known() {
            return value != null;
        }

        public BigDecimal valueOrZero() {
            return value == null ? BigDecimal.ZERO : value;
        }

        public Priced as(EventRef actual, EventRef by) {
            return new Priced(eventId, actual, topic, district, value, customer, orderId, redriveOf, by);
        }
    }

    private final Catalog catalog;
    /** The last known worth of each case, most recent last; bounded. */
    private final Map<String, BigDecimal> worths;

    public Pricing(Catalog catalog) {
        this(catalog, 200_000);
    }

    public Pricing(Catalog catalog, int remember) {
        this.catalog = catalog;
        this.worths = new LinkedHashMap<>(1024, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, BigDecimal> eldest) {
                return size() > remember;
            }
        };
    }

    public Catalog catalog() {
        return catalog;
    }

    /** An amount, written as a decimal string or a JSON number; null otherwise. */
    public static BigDecimal money(JsonNode v) {
        if (v == null) {
            return null;
        }
        if (v.isNumber()) {
            return v.decimalValue();
        }
        if (v.isTextual()) {
            try {
                return new BigDecimal(v.asText().trim());
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    public Result<Priced> price(CloudEvent event) {
        return priceDecoded(new Decoded(event.id(), event.type(), event.version(), event.data(), event.redriveOf()), List.of(), event.id());
    }

    public Result<Priced> priceBytes(byte[] bytes) {
        Result<CloudEvent> e = CloudEvent.parse(bytes);
        return e.isOk() ? price(e.value()) : Result.err(e.errors());
    }

    /** A record from {@code topics}: the topic it was read from, or for a dead letter the topics it may have come from. */
    public Result<Priced> priceRecord(byte[] value, Map<String, String> headers, List<String> topics, String fallbackId) {
        Result<Decoded> d = Records.decode(value, headers);
        return d.isOk() ? priceDecoded(d.value(), topics, fallbackId) : Result.err(d.errors());
    }

    public synchronized Result<Priced> priceDecoded(Decoded d, List<String> topics, String fallbackId) {
        String id = d.id() != null ? d.id() : fallbackId;
        if (d.type() != null && d.version() != null) {
            EventRef ref = new EventRef(d.type(), d.version());
            Optional<EventSpec> spec = catalog.find(ref);
            if (spec.isPresent()) {
                return remember(priceAs(spec.get(), d, id));
            }
            List<EventSpec> known = catalog.events().stream().filter(e -> e.ref().name().equals(ref.name()))
                    .sorted(Comparator.comparingInt((EventSpec e) -> e.ref().version()).reversed()).toList();
            if (known.isEmpty()) {
                return Result.err(ref.show() + " (" + id + ") is not in the catalog");
            }
            for (EventSpec k : known) {
                Attempt p = priceAs(k, d, id);
                if (p.priced().isOk()) {
                    return remember(new Attempt(Result.ok(p.priced().value().as(ref, k.ref())), p.measured()));
                }
            }
            return Result.err(ref.show() + " (" + id + ") is not in the catalog, and no version of " + ref.name() + " it has can read it");
        }
        List<EventSpec> specs = catalog.events().stream()
                .filter(e -> d.type() != null ? e.ref().name().equals(d.type()) : topics.contains(e.topic()))
                .sorted(Comparator.comparingInt((EventSpec e) -> e.ref().version()).reversed()).toList();
        if (specs.isEmpty()) {
            return Result.err(d.type() != null ? d.type() + " (" + id + ") is not in the catalog"
                    : "a record on " + (topics.isEmpty() ? "no topic" : String.join(", ", topics)) + " (" + id + ") has no event type in the catalog");
        }
        Attempt first = null;
        for (EventSpec spec : specs) {
            Attempt p = priceAs(spec, d, id);
            if (p.priced().isOk()) {
                return remember(p);
            }
            if (first == null) {
                first = p;
            }
        }
        return first.priced();
    }

    /** A priced event, and the measure its case now has when the event set it. */
    private record Attempt(Result<Priced> priced, BigDecimal measured) {}

    private Result<Priced> remember(Attempt a) {
        if (a.priced().isOk() && a.measured() != null) {
            worths.put(a.priced().value().orderId(), a.measured());
        }
        return a.priced();
    }

    /** The last worth seen for a case, or null. */
    public synchronized BigDecimal worthOf(String caseId) {
        return worths.get(caseId);
    }

    private Attempt priceAs(EventSpec spec, Decoded d, String id) {
        String where = spec.ref().show() + " (" + id + ")";
        FieldPath orderPath = catalog.orderPath(spec);
        FieldPath customerPath = catalog.customerPath(spec);
        boolean hasWorth = spec.value() != null || (catalog.roles() != null && catalog.roles().measure() != null);
        if (!hasWorth || orderPath == null || customerPath == null) {
            return failed(where + ": the catalog declares no value for it");
        }

        // The value first, so an event read with the wrong version's paths fails on the amount, as it always has.
        BigDecimal explicit = null;
        if (spec.value() != null) {
            explicit = money(spec.value().read(d.data()).orElse(null));
            if (explicit == null) {
                return failed(where + ": no amount at '" + spec.value().show() + "'");
            }
        }
        String customer = key(customerPath.read(d.data()).orElse(null));
        if (customer == null) {
            return failed(where + ": no customer at '" + customerPath.show() + "'");
        }
        String order = key(orderPath.read(d.data()).orElse(null));
        if (order == null) {
            return failed(where + ": no order at '" + orderPath.show() + "'");
        }

        // What the event does to the measure, from the case's last known worth.
        BigDecimal measured = worths.get(order);
        boolean touched = false;
        List<String> problems = new ArrayList<>();
        for (Effect eff : catalog.measureEffects(spec)) {
            BigDecimal amount = switch (eff.source()) {
                case Effect.FromField f -> money(f.field().read(d.data()).orElse(null));
                case Effect.Const c -> money(c.value());
            };
            if (amount == null) {
                problems.add("no amount at '" + (eff.source() instanceof Effect.FromField f ? f.field().show() : eff.source().show()) + "'");
                continue;
            }
            touched = true;
            measured = switch (eff.op()) {
                case ASSIGN -> amount;
                case ADD -> measured == null ? null : measured.add(amount);
                case SUBTRACT -> measured == null ? null : measured.subtract(amount);
            };
        }
        if (explicit == null && !problems.isEmpty()) {
            return failed(where + ": " + problems.get(0));
        }
        BigDecimal value = explicit != null ? explicit : measured;
        Priced p = new Priced(id, spec.ref(), spec.topic(), spec.district(), value, customer, order, d.redriveOf(), null);
        return new Attempt(Result.ok(p), touched ? measured : null);
    }

    private static Attempt failed(String why) {
        return new Attempt(Result.err(why), null);
    }

    /** A key as text: a string, or a whole number. */
    private static String key(JsonNode v) {
        if (v == null) {
            return null;
        }
        if (v.isTextual()) {
            return v.asText();
        }
        return v.isIntegralNumber() ? v.asText() : null;
    }

    static String utf8(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }
}
