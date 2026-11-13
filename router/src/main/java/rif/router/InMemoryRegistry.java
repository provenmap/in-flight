package rif.router;

import java.util.LinkedHashMap;
import java.util.Map;

import rif.catalog.Catalog;
import rif.catalog.Intent;

/** A registry that lives as long as the process: for tests, and for a router nobody restarts. */
public final class InMemoryRegistry implements Registry {

    private volatile State state = new State(null, Map.of());

    @Override
    public State load() {
        return state;
    }

    @Override
    public synchronized void saveCatalog(Catalog catalog) {
        state = new State(catalog, state.intents());
    }

    @Override
    public synchronized void saveIntent(Intent intent) {
        Map<String, Intent> next = new LinkedHashMap<>(state.intents());
        next.put(intent.name(), intent);
        state = new State(state.catalog(), next);
    }

    @Override
    public synchronized void forgetIntent(String name) {
        Map<String, Intent> next = new LinkedHashMap<>(state.intents());
        next.remove(name);
        state = new State(state.catalog(), next);
    }
}
