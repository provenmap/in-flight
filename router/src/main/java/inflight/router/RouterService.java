package inflight.router;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import inflight.catalog.Catalog;
import inflight.catalog.Describe;
import inflight.catalog.EventRef;
import inflight.catalog.FieldPath;
import inflight.catalog.Impact;
import inflight.catalog.Intent;
import inflight.catalog.IntentCompiler;
import inflight.catalog.Result;
import inflight.catalog.RoutingTable;
import inflight.catalog.Semantics;

/**
 * Holds the active catalog and the intents, and runs one Streams application per intent.
 *
 * <p>An intent is declared once: by the catalog, in its {@code intents}, or at runtime through {@link #declareIntent}.
 * Runtime declarations are kept in the registry; the catalog's travel with the catalog. A catalog that comes to declare
 * a name declared at runtime takes it over, and the runtime declaration is dropped.
 *
 * <p>Every change is checked first: a catalog that would break any runtime-declared intent is refused, a published event
 * version may not be redefined in place, and revisions only move forward. A change restarts only the intents whose
 * routes it changes; the rest pick up the new revision while they run.
 */
public final class RouterService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RouterService.class);

    private final Registry registry;
    private final TopicManager topics;
    private final StreamsSettings settings;
    private final IntentTopology.Ordering ordering;
    private final boolean runStreams;
    private final RouterStats stats = new RouterStats();

    private volatile Catalog catalog;
    private volatile Map<String, Intent> runtime;
    private volatile Map<String, IntentApplication> apps = Map.of();

    /**
     * @param topics creates the topics an application needs before it starts; null when streams do not run
     * @param runStreams false builds each intent's topology without starting it, for tests
     */
    public RouterService(Registry registry, TopicManager topics, StreamsSettings settings, IntentTopology.Ordering ordering, boolean runStreams) {
        this.registry = registry;
        this.topics = topics;
        this.settings = settings;
        this.ordering = ordering;
        this.runStreams = runStreams;
        Registry.State state = registry.load();
        this.catalog = state.catalog();
        this.runtime = new LinkedHashMap<>(state.intents());
    }

    // ---- what it holds -----------------------------------------------------------------------------------------------

    public Optional<Catalog> catalog() {
        return Optional.ofNullable(catalog);
    }

    /** The intents declared at runtime, by name. */
    public Map<String, Intent> runtimeIntents() {
        return Collections.unmodifiableMap(runtime);
    }

    /** True when the active catalog declares this intent itself. */
    public boolean declaredByCatalog(String intent) {
        Catalog c = catalog;
        return c != null && c.semantics() != null && c.semantics().table(intent) != null;
    }

    /** Every intent's routing table, by name. */
    public List<RoutingTable> routingTables() {
        return apps.values().stream().map(IntentApplication::table).toList();
    }

    public Optional<RoutingTable> routingTable(String intent) {
        IntentApplication app = apps.get(intent);
        return app == null ? Optional.empty() : Optional.of(app.table());
    }

    /** Each intent's application, by intent name. */
    public Map<String, IntentApplication> applications() {
        return apps;
    }

    public RouterStats stats() {
        return stats;
    }

    /** Each intent's Streams state, by name. */
    public Map<String, String> streamsStates() {
        Map<String, String> out = new TreeMap<>();
        apps.forEach((name, app) -> out.put(name, app.state()));
        return out;
    }

    /**
     * One word for all of them: {@code NOT_RUNNING} with no intents, the shared state when they agree, {@code ERROR} if any
     * failed, else the first that is not running.
     */
    public String streamsState() {
        Set<String> states = new LinkedHashSet<>(streamsStates().values());
        if (states.isEmpty()) {
            return "NOT_RUNNING";
        }
        if (states.size() == 1) {
            return states.iterator().next();
        }
        return states.contains("ERROR") ? "ERROR" : states.stream().filter(s -> !s.equals("RUNNING")).findFirst().orElse("RUNNING");
    }

    // ---- changing it -------------------------------------------------------------------------------------------------

    /** Starts every intent from the state the registry held. An intent that no longer compiles is logged and left out. */
    public synchronized void start() {
        Compiled c = compile(catalog, runtime);
        c.errors().forEach(e -> log.error("not starting: {}", e));
        reconcile(c.tables());
    }

    public synchronized Result<Catalog> activateCatalog(Catalog next) {
        List<String> invalid = Catalog.validate(next);
        if (!invalid.isEmpty()) {
            return Result.err(invalid);
        }
        Catalog cur = catalog;
        if (cur != null && next.revision() <= cur.revision()) {
            return Result.err("revision " + next.revision() + " is not newer than the active revision " + cur.revision());
        }
        if (cur != null) {
            List<String> redefined = redefinitions(cur, next);
            if (!redefined.isEmpty()) {
                return Result.err(redefined);
            }
        }
        Map<String, Intent> kept = new LinkedHashMap<>();
        List<String> takenOver = new ArrayList<>();
        runtime.forEach((name, intent) -> {
            if (next.semantics() != null && next.semantics().table(name) != null) {
                takenOver.add(name);
            } else {
                kept.put(name, intent);
            }
        });
        Compiled compiled = compile(next, kept);
        if (!compiled.errors().isEmpty()) {
            return Result.err(compiled.errors());
        }

        registry.saveCatalog(next);
        for (String name : takenOver) {
            registry.forgetIntent(name);
            log.warn("intent {} is now declared by catalog revision {}; its runtime declaration is dropped", name, next.revision());
        }
        catalog = next;
        runtime = kept;
        log.info("catalog revision {} active; {} intents", next.revision(), compiled.tables().size());
        reconcile(compiled.tables());
        return Result.ok(next);
    }

    public synchronized Result<RoutingTable> declareIntent(Intent intent) {
        Catalog c = catalog;
        if (c == null) {
            return Result.err("no catalog is active yet; PUT /catalog first");
        }
        Semantics s = c.semantics();
        if (s == null) {
            return Result.err(Describe.NO_SEMANTICS);
        }
        RoutingTable ofCatalog = s.table(intent.name());
        if (ofCatalog != null) {
            Intent declared = s.intents().stream().filter(i -> i.name().equals(intent.name())).findFirst().orElseThrow();
            if (sameDeclaration(declared, intent)) {
                return Result.ok(apps.containsKey(intent.name()) ? apps.get(intent.name()).table() : ofCatalog);
            }
            return Result.err("intent '" + intent.name() + "' is declared by catalog revision " + c.revision() + " as " + declared.predicate().show()
                    + "; change it in the catalog");
        }
        Result<RoutingTable> t = IntentCompiler.compile(intent, s.model(), c.revision());
        if (!t.isOk()) {
            return t;
        }
        registry.saveIntent(intent);
        Map<String, Intent> next = new LinkedHashMap<>(runtime);
        next.put(intent.name(), intent);
        runtime = next;

        Map<String, RoutingTable> tables = new TreeMap<>();
        apps.forEach((name, app) -> tables.put(name, app.table()));
        tables.put(intent.name(), t.value());
        log.info("intent {} declared: {} -> {}", intent.name(), intent.predicate().show(),
                t.value().routes().stream().map(r -> r.event().show()).collect(Collectors.joining(", ")));
        reconcile(tables);
        return Result.ok(t.value());
    }

    private static boolean sameDeclaration(Intent a, Intent b) {
        return a.predicate().show().equals(b.predicate().show()) && java.util.Objects.equals(a.by(), b.by());
    }

    // ---- checks ------------------------------------------------------------------------------------------------------

    /** The catalog's own intents, compiled when it was read, and the runtime ones compiled against it. */
    private record Compiled(Map<String, RoutingTable> tables, List<String> errors) {}

    private static Compiled compile(Catalog c, Map<String, Intent> runtimeIntents) {
        Map<String, RoutingTable> tables = new TreeMap<>();
        List<String> errors = new ArrayList<>();
        if (c == null) {
            return new Compiled(tables, errors);
        }
        Semantics s = c.semantics();
        if (s != null) {
            s.compiled().forEach(t -> tables.put(t.intent(), t));
        }
        for (Intent i : runtimeIntents.values().stream().sorted(java.util.Comparator.comparing(Intent::name)).toList()) {
            if (tables.containsKey(i.name())) {
                continue;
            }
            if (s == null) {
                errors.add("would break intent '" + i.name() + "': catalog revision " + c.revision() + " does not say what its events mean");
                continue;
            }
            Result<RoutingTable> t = IntentCompiler.compile(i, s.model(), c.revision());
            if (t.isOk()) {
                tables.put(i.name(), t.value());
            } else {
                t.errors().forEach(e -> errors.add("would break " + e));
            }
        }
        return new Compiled(tables, errors);
    }

    /**
     * A published event version may not change in place. With meaning on both sides, impact analysis says what changed;
     * without, the topic is all there is to compare, and meaning may not be added to or taken from a published version.
     */
    static List<String> redefinitions(Catalog cur, Catalog next) {
        if (cur.semantics() != null && next.semantics() != null) {
            return Impact.diff(cur.semantics().model(), next.semantics().model()).errors();
        }
        List<String> out = new ArrayList<>();
        for (Catalog.EventSpec e : next.events()) {
            Optional<Catalog.EventSpec> old = cur.find(e.ref());
            if (old.isEmpty()) {
                continue;
            }
            List<String> details = new ArrayList<>();
            if (!old.get().topic().equals(e.topic())) {
                details.add("topic " + old.get().topic() + " -> " + e.topic());
            }
            if (cur.meaning(e.ref()).isPresent() != next.meaning(e.ref()).isPresent()) {
                details.add(cur.meaning(e.ref()).isPresent() ? "meaning removed" : "meaning added");
            }
            if (!details.isEmpty()) {
                out.add(e.ref().show() + " is already published and cannot change (" + String.join(", ", details) + "); add version "
                        + (e.ref().version() + 1) + " instead");
            }
        }
        return out;
    }

    // ---- running it --------------------------------------------------------------------------------------------------

    /** What routing depends on in a table: the key entity, the topics, and each route's event, topic and key path. */
    private record RouteShape(EventRef event, String topic, FieldPath keyPath) {}

    static boolean sameRouting(RoutingTable a, RoutingTable b) {
        return a.entity().equals(b.entity()) && a.sourceTopics().equals(b.sourceTopics()) && shape(a).equals(shape(b));
    }

    private static List<RouteShape> shape(RoutingTable t) {
        return t.routes().stream().map(r -> new RouteShape(r.event(), r.topic(), r.keyPath())).toList();
    }

    /**
     * Brings the running applications in line with these tables: stops intents that are gone, starts new ones, restarts
     * those whose routes changed or that failed to start, and hands the rest the new revision and known versions without
     * a restart.
     */
    private void reconcile(Map<String, RoutingTable> tables) {
        Set<EventRef> known = catalog == null ? Set.of() : catalog.events().stream().map(Catalog.EventSpec::ref).collect(Collectors.toSet());
        Map<String, IntentApplication> next = new TreeMap<>();
        for (Map.Entry<String, IntentApplication> e : apps.entrySet()) {
            if (!tables.containsKey(e.getKey())) {
                log.info("intent {} is no longer declared; stopping {}", e.getKey(), e.getValue().applicationId());
                e.getValue().close();
            }
        }
        for (Map.Entry<String, RoutingTable> e : tables.entrySet()) {
            String name = e.getKey();
            RoutingTable table = e.getValue();
            IntentTopology.Routing routing = new IntentTopology.Routing(List.of(table), known);
            IntentApplication app = apps.get(name);
            if (app != null && app.failure() == null && sameRouting(app.table(), table)) {
                app.update(routing);
                next.put(name, app);
                continue;
            }
            if (app != null) {
                log.info("intent {} routes differently; restarting {}", name, app.applicationId());
                app.close();
            }
            next.put(name, launch(table, routing, app == null ? 1 : app.generation() + 1));
        }
        apps = Collections.unmodifiableMap(next);
    }

    private IntentApplication launch(RoutingTable table, IntentTopology.Routing routing, int generation) {
        String name = table.intent();
        AtomicReference<IntentTopology.Routing> ref = new AtomicReference<>(routing);
        Topology topology = IntentTopology.build(ref::get, ordering, stats);
        KafkaStreams streams = null;
        String failure = null;
        if (runStreams) {
            try {
                if (topics != null) {
                    List<String> needed = new ArrayList<>(catalog.topics());
                    needed.addAll(table.sourceTopics());
                    topics.ensure(needed);
                    topics.ensure(List.of(table.topic()));
                }
                streams = new KafkaStreams(topology, settings.propertiesFor(name));
                streams.setUncaughtExceptionHandler(ex -> {
                    log.error("intent {}: stream thread failed", name, ex);
                    return StreamThreadExceptionResponse.REPLACE_THREAD;
                });
                streams.start();
                log.info("intent {} running as {} over {}", name, settings.applicationIdFor(name), String.join(", ", table.sourceTopics()));
            } catch (RuntimeException e) {
                // One intent that cannot start does not stop the others; health reports it as ERROR.
                log.error("intent {} could not start as {}", name, settings.applicationIdFor(name), e);
                if (streams != null) {
                    streams.close(Duration.ofSeconds(5));
                    streams = null;
                }
                failure = e.toString();
            }
        }
        return new IntentApplication(name, settings.applicationIdFor(name), settings.stateDirFor(name), generation, ref, topology, streams, failure);
    }

    @Override
    public synchronized void close() {
        apps.values().forEach(IntentApplication::close);
        apps = Map.of();
        registry.close();
        if (topics != null) {
            topics.close();
        }
    }
}
