package rif.catalog;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/** Compiles an intent against the catalog's meaning. It fails on anything it cannot resolve; nothing is guessed. */
public final class IntentCompiler {

    private IntentCompiler() {}

    public static Result<RoutingTable> compile(Intent intent, Model model) {
        return compile(intent, model, 1);
    }

    public static Result<RoutingTable> compile(Intent intent, Model model, int revision) {
        String name = intent.name();
        List<String> refErrors = references(intent.predicate(), model);
        if (!refErrors.isEmpty()) {
            return Result.err(refErrors.stream().map(e -> "intent '" + name + "': " + e).toList());
        }
        List<Model.Event> matched = model.events().stream().filter(intent.predicate()::matches)
                .sorted(Comparator.comparing((Model.Event e) -> e.ref().name()).thenComparingInt(e -> e.ref().version())).toList();
        if (matched.isEmpty()) {
            return Result.err("intent '" + name + "': " + intent.predicate().show() + " matches no event type in the catalog");
        }
        Result<String> entity = keyEntity(intent, matched, model);
        if (!entity.isOk()) {
            return Result.err(entity.errors());
        }
        String by = entity.value();
        List<String> unkeyed = matched.stream().filter(e -> !e.keys().containsKey(by))
                .map(e -> "intent '" + name + "': " + e.ref().show() + " matches but carries no " + by + " key, so it cannot be ordered by " + by)
                .toList();
        if (!unkeyed.isEmpty()) {
            return Result.err(unkeyed);
        }
        List<RoutingTable.Route> routes = matched.stream().map(e -> {
            FieldPath keyPath = e.keys().get(by);
            return new RoutingTable.Route(e.ref(), e.topic(), keyPath, reasons(intent.predicate(), e), reads(intent.predicate(), e, by, keyPath));
        }).toList();
        List<String> topics = new ArrayList<>(new TreeSet<>(routes.stream().map(RoutingTable.Route::topic).toList()));
        return Result.ok(new RoutingTable(name, intent.predicate().show(), by, revision, routes, topics));
    }

    /** Compiles every intent; the set is acceptable only if all of them compile. */
    public static Result<List<RoutingTable>> compileAll(List<Intent> intents, Model model, int revision) {
        List<String> errors = new ArrayList<>();
        List<RoutingTable> tables = new ArrayList<>();
        for (Intent i : intents) {
            Result<RoutingTable> t = compile(i, model, revision);
            if (t.isOk()) {
                tables.add(t.value());
            } else {
                errors.addAll(t.errors());
            }
        }
        return errors.isEmpty() ? Result.ok(tables) : Result.err(errors);
    }

    private static List<String> references(Predicate p, Model m) {
        return switch (p) {
            case Predicate.Changes c -> {
                var attrs = m.entities().get(c.attribute().entity());
                if (attrs == null) {
                    yield List.of("unknown entity '" + c.attribute().entity() + "' in " + p.show());
                }
                yield attrs.containsKey(c.attribute().attribute()) ? List.of()
                        : List.of(c.attribute().entity() + " has no attribute '" + c.attribute().attribute() + "' (has: " + String.join(", ", attrs.keySet()) + ")");
            }
            case Predicate.Emits e -> m.events().stream().anyMatch(x -> x.ref().name().equals(e.eventType())) ? List.of()
                    : List.of("unknown event type '" + e.eventType() + "' in " + p.show());
            case Predicate.About a -> m.entities().containsKey(a.entity()) ? List.of() : List.of("unknown entity '" + a.entity() + "' in " + p.show());
            case Predicate.And a -> concat(references(a.left(), m), references(a.right(), m));
            case Predicate.Or o -> concat(references(o.left(), m), references(o.right(), m));
            case Predicate.Not n -> references(n.inner(), m);
        };
    }

    private static <T> List<T> concat(List<T> a, List<T> b) {
        List<T> out = new ArrayList<>(a);
        out.addAll(b);
        return out;
    }

    private static Result<String> keyEntity(Intent intent, List<Model.Event> matched, Model m) {
        if (intent.by() != null) {
            return m.entities().containsKey(intent.by()) ? Result.ok(intent.by())
                    : Result.err("intent '" + intent.name() + "': unknown key entity '" + intent.by() + "'");
        }
        Set<String> named = intent.predicate().namedEntities();
        List<String> candidates;
        if (!named.isEmpty()) {
            candidates = new ArrayList<>(named);
        } else {
            Set<String> common = new LinkedHashSet<>(matched.get(0).keys().keySet());
            for (Model.Event e : matched) {
                common.retainAll(e.keys().keySet());
            }
            candidates = new ArrayList<>(common);
        }
        if (candidates.size() == 1) {
            return Result.ok(candidates.get(0));
        }
        return candidates.isEmpty()
                ? Result.err("intent '" + intent.name() + "': no entity keys every match; say which with 'by'")
                : Result.err("intent '" + intent.name() + "': key entity is ambiguous (" + String.join(", ", candidates.stream().sorted().toList()) + "); say which with 'by'");
    }

    private static List<String> reasons(Predicate p, Model.Event e) {
        return switch (p) {
            case Predicate.Changes c -> e.affects(c.attribute()).stream().map(Effect::show).toList();
            case Predicate.Emits em -> e.ref().name().equals(em.eventType()) ? List.of("emits " + e.ref().name()) : List.of();
            case Predicate.About a -> e.keys().containsKey(a.entity()) ? List.of("about " + a.entity()) : List.of();
            case Predicate.And a -> List.copyOf(new LinkedHashSet<>(concat(reasons(a.left(), e), reasons(a.right(), e))));
            case Predicate.Or o -> List.copyOf(new LinkedHashSet<>(concat(reasons(o.left(), e), reasons(o.right(), e))));
            case Predicate.Not n -> List.of();
        };
    }

    private static List<RoutingTable.Read> reads(Predicate p, Model.Event e, String entity, FieldPath keyPath) {
        FieldType keyType = FieldType.at(e.fields(), keyPath);
        List<RoutingTable.Read> out = new ArrayList<>();
        out.add(new RoutingTable.Read(entity + " key", "from " + keyPath.show(), keyType == null ? "unknown" : keyType.show()));
        for (AttrRef attr : new LinkedHashSet<>(p.watchedAttributes())) {
            for (Effect effect : e.affects(attr)) {
                FieldType t = effect.source() instanceof Effect.FromField f ? FieldType.at(e.fields(), f.field()) : null;
                String source = (effect.op() == Effect.Op.ASSIGN ? "" : effect.op().show() + " ") + effect.source().show();
                out.add(new RoutingTable.Read(attr.show(), source, t == null ? "constant" : t.show()));
            }
        }
        return out;
    }
}
