package inflight.router;

import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import inflight.catalog.Catalog;
import inflight.catalog.Intent;
import inflight.catalog.Json;
import inflight.catalog.Result;

/**
 * Where the router keeps its state between restarts: the active catalog, and the intents declared at runtime. Intents
 * the catalog declares itself travel inside the catalog, so they are not stored apart.
 */
public sealed interface Registry extends AutoCloseable permits InMemoryRegistry, KafkaRegistry {

    /** The active catalog (null when none has been activated) and the runtime-declared intents, by name. */
    record State(Catalog catalog, Map<String, Intent> intents) {
        public State {
            intents = java.util.Collections.unmodifiableMap(new LinkedHashMap<>(intents));
        }
    }

    State load();

    void saveCatalog(Catalog catalog);

    void saveIntent(Intent intent);

    /** Drops a runtime declaration, as when the catalog comes to declare an intent of the same name. */
    void forgetIntent(String name);

    @Override
    default void close() {}

    /** An intent as the registry stores it: its name, predicate and key entity. */
    static String intentJson(Intent i) {
        ObjectNode o = Json.object().put("name", i.name()).put("predicate", i.predicate().show());
        if (i.by() != null) {
            o.put("by", i.by());
        }
        return Json.write(o);
    }

    static Result<Intent> readIntent(JsonNode j) {
        String name = Json.text(j, "name");
        String predicate = Json.text(j, "predicate");
        if (name == null || predicate == null) {
            return Result.err("an intent needs a 'name' and a 'predicate'");
        }
        return Intent.parse(name, predicate, Json.text(j, "by"));
    }
}
