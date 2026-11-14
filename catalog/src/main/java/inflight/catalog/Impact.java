package inflight.catalog;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Everything a change to the catalog's meaning would do, worked out before a single event is produced: which intents it
 * reaches, who reads by them, and who must change first.
 *
 * @param changes what changed, in words
 * @param intents each intent whose matches the change alters
 * @param unaffected the intents it leaves alone
 * @param errors why the change cannot stand at all: a published version edited in place, or an intent that no longer compiles
 * @param breaks the consumers that will meet events they cannot read unless they change first
 */
public record Impact(List<String> changes, List<IntentImpact> intents, List<String> unaffected, List<String> errors, List<Breakage> breaks) {

    /** Someone who reads by an intent, and where what they cannot read ends up ({@code dlq} may be null). */
    public record Reader(String name, String dlq) {}

    /**
     * How an event version newly reaching an intent lands on its consumers.
     * <ul>
     * <li>{@code SAME_READS}: a new version of a type they already read, and they read the same fields from it.</li>
     * <li>{@code NEW_READS}: a new version whose fields they read have moved or changed type. Their code must change first.</li>
     * <li>{@code NEW_TYPE}: an event type they have never been sent.</li>
     * </ul>
     */
    public enum Arrival {
        SAME_READS("same-reads"), NEW_READS("new-reads"), NEW_TYPE("new-type");

        public final String wire;

        Arrival(String wire) {
            this.wire = wire;
        }
    }

    /** One route newly reaching an intent; {@code previous} is the version it is compared with, or null. */
    public record RouteArrival(RoutingTable.Route route, Arrival arrival, EventRef previous, List<String> differences) {}

    public record IntentImpact(String intent, String predicate, String entity, List<RouteArrival> arrivals, List<RoutingTable.Route> removed,
                               List<RoutingTable.Route> rekeyed, List<Reader> consumers) {}

    public record Breakage(String consumer, String dlq, String intent, EventRef event, Arrival arrival, List<String> differences) {}

    /** Nobody must change, and nothing blocks it. */
    public boolean safe() {
        return breaks.isEmpty() && errors.isEmpty();
    }

    // ---- working it out ------------------------------------------------------------------------------------------

    private static String showRead(RoutingTable.Read r) {
        return r.source() + " (" + r.type() + ")";
    }

    private static List<String> differences(List<RoutingTable.Read> before, List<RoutingTable.Read> after) {
        List<String> out = new ArrayList<>();
        Set<String> whats = new LinkedHashSet<>();
        before.forEach(r -> whats.add(r.what()));
        after.forEach(r -> whats.add(r.what()));
        for (String what : whats) {
            String b = before.stream().filter(r -> r.what().equals(what)).map(Impact::showRead).collect(Collectors.joining(", "));
            String a = after.stream().filter(r -> r.what().equals(what)).map(Impact::showRead).collect(Collectors.joining(", "));
            if (!a.equals(b)) {
                out.add(what + ": " + (b.isEmpty() ? "not read" : b) + " -> " + (a.isEmpty() ? "gone" : a));
            }
        }
        return out;
    }

    private static RouteArrival arrival(RoutingTable.Route route, RoutingTable before) {
        List<RoutingTable.Route> earlier = before == null ? List.of()
                : before.routes().stream().filter(r -> r.event().name().equals(route.event().name()))
                        .sorted(Comparator.comparingInt((RoutingTable.Route r) -> r.event().version()).reversed()).toList();
        RoutingTable.Route previous = earlier.stream().filter(r -> r.event().version() < route.event().version()).findFirst()
                .orElse(earlier.isEmpty() ? null : earlier.get(0));
        if (previous == null) {
            return new RouteArrival(route, Arrival.NEW_TYPE, null, List.of());
        }
        List<String> diffs = differences(previous.reads(), route.reads());
        return new RouteArrival(route, diffs.isEmpty() ? Arrival.SAME_READS : Arrival.NEW_READS, previous.event(), diffs);
    }

    private static List<String> redefinitions(Model.Event old, Model.Event now) {
        Function<Model.Event, String> fields = e -> e.fields().entrySet().stream().map(x -> x.getKey() + ": " + x.getValue().show()).collect(Collectors.joining(", "));
        Function<Model.Event, String> keys = e -> e.keys().entrySet().stream().map(x -> x.getKey() + ": " + x.getValue().show()).collect(Collectors.joining(", "));
        Function<Model.Event, String> effects = e -> e.effects().stream().map(Effect::show).collect(Collectors.joining("; "));
        List<String> out = new ArrayList<>();
        if (!old.topic().equals(now.topic())) {
            out.add("topic " + old.topic() + " -> " + now.topic());
        }
        if (!fields.apply(old).equals(fields.apply(now))) {
            out.add("fields changed");
        }
        if (!keys.apply(old).equals(keys.apply(now))) {
            out.add("keys changed");
        }
        if (!effects.apply(old).equals(effects.apply(now))) {
            out.add("effects changed");
        }
        return out;
    }

    /** What changed between two versions of the catalog's meaning, in words, and what cannot change at all. */
    public record Diff(List<String> changes, List<String> errors, List<Model.Event> added) {}

    public static Diff diff(Model before, Model after) {
        List<String> changes = new ArrayList<>();
        List<String> errors = new ArrayList<>();
        List<Model.Event> added = new ArrayList<>();
        for (Model.Event e : after.events()) {
            var old = before.find(e.ref());
            if (old.isEmpty()) {
                added.add(e);
                String from = e.district() == null ? "" : " from " + e.district();
                changes.add(before.events().stream().anyMatch(x -> x.ref().name().equals(e.ref().name()))
                        ? "new version " + e.ref().show() + " on " + e.topic()
                        : "new event type " + e.ref().show() + " on " + e.topic() + from);
                continue;
            }
            List<String> details = redefinitions(old.get(), e);
            if (!details.isEmpty()) {
                changes.add(e.ref().show() + " redefined in place: " + String.join("; ", details));
                errors.add(e.ref().show() + " is already published and cannot change (" + String.join(", ", details) + "); add version "
                        + (e.ref().version() + 1) + " instead");
            }
        }
        for (Model.Event e : before.events()) {
            if (after.find(e.ref()).isEmpty()) {
                changes.add("removed " + e.ref().show());
            }
        }
        for (Map.Entry<String, Map<String, FieldType>> ent : after.entities().entrySet()) {
            for (Map.Entry<String, FieldType> attr : ent.getValue().entrySet()) {
                AttrRef ref = new AttrRef(ent.getKey(), attr.getKey());
                FieldType old = before.attributeType(ref);
                if (old == null) {
                    changes.add("new attribute " + ref.show() + ": " + attr.getValue().show());
                } else if (!FieldType.same(old, attr.getValue())) {
                    changes.add(ref.show() + " retyped " + old.show() + " -> " + attr.getValue().show());
                }
            }
        }
        for (Map.Entry<String, Map<String, FieldType>> ent : before.entities().entrySet()) {
            for (String attr : ent.getValue().keySet()) {
                if (after.attributeType(new AttrRef(ent.getKey(), attr)) == null) {
                    changes.add("removed attribute " + new AttrRef(ent.getKey(), attr).show());
                }
            }
        }
        return new Diff(changes, errors, added);
    }

    /** Recompiles every intent against both models and says which consumers the difference reaches, and how. */
    public static Impact analyse(Model before, Semantics after, Function<String, List<Reader>> consumersOf) {
        Diff d = diff(before, after.model());
        List<String> errors = new ArrayList<>(d.errors());
        List<IntentImpact> intents = new ArrayList<>();
        List<String> unaffected = new ArrayList<>();

        for (Intent intent : after.intents().stream().sorted(Comparator.comparing(Intent::name)).toList()) {
            Result<RoutingTable> now = IntentCompiler.compile(intent, after.model(), after.revision());
            if (!now.isOk()) {
                now.errors().forEach(e -> errors.add("would break " + e));
                continue;
            }
            Result<RoutingTable> then = IntentCompiler.compile(intent, before);
            RoutingTable was = then.isOk() ? then.value() : null;
            List<RoutingTable.Route> wasRoutes = was == null ? List.of() : was.routes();
            List<RouteArrival> arrivals = now.value().routes().stream()
                    .filter(r -> wasRoutes.stream().noneMatch(o -> o.event().equals(r.event())))
                    .map(r -> arrival(r, was)).toList();
            List<RoutingTable.Route> removed = wasRoutes.stream()
                    .filter(o -> now.value().routes().stream().noneMatch(r -> r.event().equals(o.event()))).toList();
            List<RoutingTable.Route> rekeyed = now.value().routes().stream()
                    .filter(r -> wasRoutes.stream().anyMatch(o -> o.event().equals(r.event())
                            && (!o.keyPath().equals(r.keyPath()) || !o.topic().equals(r.topic()))))
                    .toList();
            if (arrivals.isEmpty() && removed.isEmpty() && rekeyed.isEmpty()) {
                unaffected.add(intent.name());
            } else {
                intents.add(new IntentImpact(intent.name(), now.value().predicate(), now.value().entity(), arrivals, removed, rekeyed,
                        consumersOf.apply(intent.name())));
            }
        }

        List<Breakage> breaks = new ArrayList<>();
        for (IntentImpact i : intents) {
            for (RouteArrival a : i.arrivals()) {
                if (a.arrival() == Arrival.SAME_READS) {
                    continue;
                }
                for (Reader c : i.consumers()) {
                    breaks.add(new Breakage(c.name(), c.dlq(), i.intent(), a.route().event(), a.arrival(), a.differences()));
                }
            }
        }
        return new Impact(d.changes(), intents, unaffected, errors, breaks);
    }

    /** The impact of one event version: the catalog with it against the catalog without it. */
    public static Result<Impact> of(EventRef ref, Semantics semantics, Function<String, List<Reader>> consumersOf) {
        if (semantics.model().find(ref).isEmpty()) {
            return Result.err(ref.show() + " is not in the catalog, or the catalog does not say what it means (fields, keys, effects)");
        }
        return Result.ok(analyse(semantics.model().without(ref), semantics, consumersOf));
    }

    /** The impact as a report a person reads top to bottom. */
    public String report() {
        List<String> lines = new ArrayList<>();
        lines.add("Catalog changes:");
        if (changes.isEmpty()) {
            lines.add("  (none)");
        }
        changes.forEach(c -> lines.add("  - " + c));

        lines.add("Intents affected:");
        if (intents.isEmpty()) {
            lines.add("  (none)");
        }
        for (IntentImpact i : intents) {
            lines.add("  - " + i.intent() + ": " + i.predicate());
            for (RouteArrival a : i.arrivals()) {
                String why = String.join("; ", a.route().reasons());
                String ev = a.route().event().show() + " from " + a.route().topic();
                switch (a.arrival()) {
                    case NEW_TYPE -> lines.add("      + " + ev + ", a type its consumers have never read: " + why);
                    case SAME_READS -> lines.add("      + " + ev + ", read the same way as " + a.previous().show());
                    case NEW_READS -> {
                        lines.add("      ! " + ev + ", read differently from " + a.previous().show() + ":");
                        a.differences().forEach(d -> lines.add("          " + d));
                    }
                }
            }
            i.removed().forEach(r -> lines.add("      - " + r.event().show() + " no longer matches"));
            i.rekeyed().forEach(r -> lines.add("      ~ " + r.event().show() + " is now keyed by " + r.keyPath().show() + " from " + r.topic()));
            lines.add("      consumers: " + (i.consumers().isEmpty() ? "none declared" : i.consumers().stream().map(Reader::name).collect(Collectors.joining(", "))));
        }
        if (!unaffected.isEmpty()) {
            lines.add("Intents unaffected: " + String.join(", ", unaffected));
        }
        if (!breaks.isEmpty()) {
            lines.add("Must change before it ships:");
            for (Breakage b : breaks) {
                String fate = b.dlq() != null ? "; until then every " + b.event().show() + " it meets goes to " + b.dlq() : "";
                lines.add("  ! " + b.consumer() + " (" + b.intent() + ") " + (b.arrival() == Arrival.NEW_TYPE ? "has no code for" : "reads") + " "
                        + b.event().show() + (b.arrival() == Arrival.NEW_READS ? " differently" : "") + fate);
            }
        }
        if (!errors.isEmpty()) {
            lines.add("Blocked:");
            errors.forEach(e -> lines.add("  ! " + e));
        }
        return String.join("\n", lines);
    }

    /** The headline: who must change first, or that nobody must. */
    public String headline() {
        if (!errors.isEmpty()) {
            return "Blocked: " + errors.get(0) + (errors.size() > 1 ? " (and " + (errors.size() - 1) + " more)" : "");
        }
        if (breaks.isEmpty()) {
            List<String> reached = intents.stream().flatMap(i -> i.consumers().stream().map(Reader::name)).distinct().toList();
            return reached.isEmpty() ? "Safe to ship: no declared consumer is reached by it."
                    : "Safe to ship: it reaches " + String.join(", ", reached) + ", and each reads it the way it reads what it gets today.";
        }
        List<String> who = breaks.stream().map(Breakage::consumer).distinct().toList();
        List<String> queues = breaks.stream().map(Breakage::dlq).filter(Objects::nonNull).distinct().toList();
        return String.join(", ", who) + " must change before this ships" + (queues.isEmpty() ? "" : ", or its events pile up in " + String.join(", ", queues)) + ".";
    }
}
