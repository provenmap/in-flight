package inflight.catalog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * What the catalog's events mean, and the intents declared over them, each compiled. This is the part of the catalog
 * that lets a consumer say what it cares about instead of which topics it reads.
 */
public record Semantics(Model model, List<Intent> intents, Map<String, RoutingTable> tables, int revision) {

    public Semantics {
        intents = List.copyOf(intents);
        tables = Collections.unmodifiableMap(new LinkedHashMap<>(tables));
    }

    /** A declared intent, compiled; null when none has that name. */
    public RoutingTable table(String intent) {
        return tables.get(intent);
    }

    /** Every declared intent, compiled, in the order they were declared. */
    public List<RoutingTable> compiled() {
        return intents.stream().map(i -> tables.get(i.name())).toList();
    }

    /** Compiles a predicate nobody has declared, to see what it would match. */
    public Result<RoutingTable> explain(String predicate, String by) {
        Result<Intent> intent = Intent.parse("explain", predicate, by);
        if (!intent.isOk()) {
            return Result.err(intent.errors().stream().map(Semantics::unprefixed).toList());
        }
        Result<RoutingTable> table = IntentCompiler.compile(intent.value(), model, revision);
        return table.isOk() ? table : Result.err(table.errors().stream().map(Semantics::unprefixed).toList());
    }

    /** The same meaning with one more intent, compiled; fails when it does not compile or the name is taken. */
    public Result<Semantics> declare(Intent intent) {
        if (tables.containsKey(intent.name())) {
            return Result.err("intent '" + intent.name() + "' is declared twice");
        }
        Result<RoutingTable> t = IntentCompiler.compile(intent, model, revision);
        if (!t.isOk()) {
            return Result.err(t.errors());
        }
        List<Intent> more = new ArrayList<>(intents);
        more.add(intent);
        Map<String, RoutingTable> all = new LinkedHashMap<>(tables);
        all.put(intent.name(), t.value());
        return Result.ok(new Semantics(model, more, all, revision));
    }

    private static String unprefixed(String e) {
        return e.replaceFirst("^intent 'explain': ", "");
    }

    /** The catalog's meaning as written in the file: entities, and each event's fields, keys and effects. */
    public record Raw(JsonNode entities, JsonNode intents, List<Model.RawEvent> events) {}

    /**
     * Null when the catalog says nothing about meaning: no entities, no intents, and no event with fields, keys or
     * effects.
     */
    public static Result<Semantics> parse(Raw raw, int revision) {
        boolean typed = raw.events().stream().anyMatch(e -> !e.saysNothing());
        if (raw.entities() == null && raw.intents() == null && !typed) {
            return Result.ok(null);
        }
        Model.Parsed parsed = Model.parse(raw.entities(), raw.events());
        List<String> errors = new ArrayList<>(parsed.errors());

        List<Intent> intents = new ArrayList<>();
        if (raw.intents() != null && !raw.intents().isArray()) {
            errors.add("'intents' must be an array");
        }
        if (raw.intents() != null && raw.intents().isArray()) {
            int i = 0;
            for (JsonNode item : raw.intents()) {
                String name = Json.text(item, "name");
                String predicate = Json.text(item, "predicate");
                if (name == null || predicate == null) {
                    errors.add("intents[" + i++ + "]: needs a 'name' and a 'predicate'");
                    continue;
                }
                i++;
                Result<Intent> intent = Intent.parse(name, predicate, Json.text(item, "by"));
                if (!intent.isOk()) {
                    errors.addAll(intent.errors());
                } else if (intents.stream().anyMatch(x -> x.name().equals(intent.value().name()))) {
                    errors.add("intent '" + intent.value().name() + "' is declared twice");
                } else {
                    intents.add(intent.value());
                }
            }
        }
        // An intent is only compiled against a model that is itself sound, so its errors are about the intent.
        if (!errors.isEmpty()) {
            return Result.err(errors);
        }
        Map<String, RoutingTable> tables = new LinkedHashMap<>();
        for (Intent intent : intents) {
            Result<RoutingTable> t = IntentCompiler.compile(intent, parsed.model(), revision);
            if (t.isOk()) {
                tables.put(intent.name(), t.value());
            } else {
                errors.addAll(t.errors());
            }
        }
        return errors.isEmpty() ? Result.ok(new Semantics(parsed.model(), intents, tables, revision)) : Result.err(errors);
    }
}
