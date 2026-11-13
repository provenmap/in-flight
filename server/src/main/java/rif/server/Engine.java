package rif.server;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import rif.catalog.Catalog;
import rif.catalog.Json;
import rif.catalog.Result;
import rif.kafka.Decoders;
import rif.kafka.Kafka;
import rif.kafka.Profile;
import rif.kafka.SchemaRegistry;
import rif.observer.Observer;

/**
 * Owns the running observer and swaps it when the connection or the catalog changes, without a restart: viewers get a
 * new snapshot, tools read the new figures. With nothing configured there is no observer, and the console opens on
 * Connect.
 *
 * <p>Fixed engines (the sample data under {@code try}, and {@code watch} of one catalog file) run one observer for
 * their whole life and refuse to be reconfigured.
 */
public final class Engine implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Engine.class);

    /** How it was started: from the workspace, on sample data, or watching one catalog file. */
    public enum Mode {
        APP, TRY, WATCH;

        public String show() {
            return name().toLowerCase(java.util.Locale.ROOT);
        }
    }

    /** What is running: the profile (null when fixed), the flow, and the observer reading it. */
    public record Running(Profile profile, String flow, Catalog catalog, Observer observer, SchemaRegistry registry) {}

    private final Mode mode;
    private final Workspace workspace;
    private final Observer.Options fixed;
    private final List<Consumer<Observer>> listeners = new CopyOnWriteArrayList<>();
    private volatile Running running;
    private volatile String problem;

    private Engine(Mode mode, Workspace workspace, Observer.Options fixed) {
        this.mode = mode;
        this.workspace = workspace;
        this.fixed = fixed;
    }

    /** From the workspace: the active profile and flow, if there are any. */
    public static Engine app(Workspace ws) {
        return new Engine(Mode.APP, ws, null);
    }

    /** One observer for good: the sample data ({@code try}) or one catalog ({@code watch}). */
    public static Engine fixed(Mode mode, Observer.Options opts, Workspace ws) {
        return new Engine(mode, ws, opts);
    }

    /** Around an observer someone else made and started: fixed, for tests and embedding. */
    public static Engine around(Observer o) {
        Engine e = new Engine(o.watching() ? Mode.WATCH : Mode.TRY, null, null);
        e.running = new Running(null, o.catalog().flow(), o.catalog(), o, null);
        return e;
    }

    public Engine start() {
        if (fixed != null) {
            Observer o = new Observer(fixed).start();
            running = new Running(null, fixed.catalog().flow(), fixed.catalog(), o, null);
            listeners.forEach(l -> l.accept(o));
            return this;
        }
        Workspace.Config c = workspace.load();
        if (c.active() != null && c.activeFlow() != null) {
            try {
                switchTo(c.active(), c.activeFlow());
            } catch (RuntimeException e) {
                problem = e.getMessage();
                log.warn("engine: {}", e.getMessage());
            }
        }
        return this;
    }

    public Mode mode() {
        return mode;
    }

    public boolean reconfigurable() {
        return mode == Mode.APP;
    }

    public Workspace workspace() {
        return workspace;
    }

    /** The observer, or null when nothing is being watched yet. */
    public Observer observer() {
        Running r = running;
        return r == null ? null : r.observer();
    }

    public Running running() {
        return running;
    }

    /** Called with each new observer (or null when it stops), on the thread that swapped it. */
    public void onSwitch(Consumer<Observer> listener) {
        listeners.add(listener);
    }

    // ---- connecting --------------------------------------------------------------------------------------------------

    /** What a connection test found: whether it works, the cluster, and the registry. */
    public record Tested(boolean ok, String text, ObjectNode data) {}

    /**
     * Tests a profile: its fields, its secrets (resolved, never shown), the cluster, and its registry. Nothing is saved.
     */
    public static Tested test(Profile p) {
        ObjectNode data = Json.object();
        data.set("profile", Workspace.profileJson(p));
        List<String> problems = p.problems();
        if (!problems.isEmpty()) {
            ArrayNode a = data.putArray("problems");
            problems.forEach(a::add);
            return new Tested(false, "The profile is not usable:\n  " + String.join("\n  ", problems), data);
        }
        java.util.Map<String, String> settings;
        try {
            settings = p.clientSettings();
        } catch (IllegalStateException e) {
            data.putArray("problems").add(e.getMessage());
            return new Tested(false, "A secret cannot be read: " + e.getMessage(), data);
        }
        Kafka.settings(p.bootstrap(), settings);
        try (Admin admin = Kafka.quickAdmin(p.bootstrap(), Duration.ofSeconds(8))) {
            DescribeClusterResult d = admin.describeCluster();
            String id = d.clusterId().get(10, TimeUnit.SECONDS);
            int brokers = d.nodes().get(10, TimeUnit.SECONDS).size();
            int topics = Kafka.listTopics(admin).size();
            data.putObject("cluster").put("id", id).put("brokers", brokers).put("topics", topics);
        } catch (Exception e) {
            String why = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
            data.putArray("problems").add("cluster: " + why);
            return new Tested(false, "Kafka at " + p.bootstrap() + " does not answer (" + p.auth() + "): " + why, data);
        }
        String registryLine = "";
        if (p.registry() != null) {
            try {
                SchemaRegistry r = SchemaRegistry.of(p.registry());
                String why = r.check();
                ObjectNode reg = data.putObject("registry").put("url", r.url()).put("ok", why == null);
                if (why != null) {
                    reg.put("problem", why);
                    registryLine = " The registry at " + r.url() + " does not answer: " + why + ".";
                } else {
                    registryLine = " The registry at " + r.url() + " answers.";
                }
            } catch (IllegalStateException e) {
                data.putObject("registry").put("url", p.registry().url()).put("ok", false).put("problem", e.getMessage());
                registryLine = " The registry's secret cannot be read: " + e.getMessage() + ".";
            }
        }
        JsonNode cl = data.get("cluster");
        return new Tested(true, "Connected to " + p.bootstrap() + ": cluster " + cl.get("id").asText() + ", " + cl.get("brokers").asInt() + " broker"
                + (cl.get("brokers").asInt() == 1 ? "" : "s") + ", " + cl.get("topics").asInt() + " topics." + registryLine, data);
    }

    /** Saves a profile that tests well, and with {@code use} makes it the active one, keeping the active flow. */
    public synchronized Tested connect(Profile p, boolean use) {
        requireApp();
        Tested t = test(p);
        if (!t.ok()) {
            return t;
        }
        Workspace.Config c = workspace.load().withProfile(p);
        if (use) {
            c = c.withActive(p.name(), c.activeFlow());
        }
        workspace.save(c);
        if (use) {
            if (c.activeFlow() != null && workspace.flow(c.activeFlow()).isOk()) {
                switchTo(p, c.activeFlow());
            } else {
                stopRunning();
            }
        }
        t.data().put("saved", true).put("active", use);
        return new Tested(true, t.text() + " Saved as " + p.name() + (use ? ", and in use." : "."), t.data());
    }

    /** Makes a saved profile the active one. */
    public synchronized void use(String name) {
        requireApp();
        Workspace.Config c = workspace.load();
        Profile p = c.profile(name);
        if (p == null) {
            throw new IllegalArgumentException("there is no profile " + name);
        }
        workspace.save(c.withActive(name, c.activeFlow()));
        if (c.activeFlow() != null && workspace.flow(c.activeFlow()).isOk()) {
            switchTo(p, c.activeFlow());
        }
    }

    /** The active profile, or null: what discovery, sampling and redrive connect with. */
    public Profile profile() {
        if (mode != Mode.APP) {
            return running == null ? null : Profile.plain("this", running.observer().bootstrap());
        }
        return workspace.load().active();
    }

    /** The active profile with its client settings registered, ready to make clients for. */
    public Profile connected() {
        Profile p = profile();
        if (p == null) {
            throw new IllegalStateException("not connected: connect to a cluster first");
        }
        if (mode == Mode.APP) {
            Kafka.settings(p.bootstrap(), p.clientSettings());
        }
        return p;
    }

    /** The decoders for the active profile: through its registry when it has one. */
    public Decoders decoders() {
        Running r = running;
        if (r != null && r.registry() != null) {
            return new Decoders(r.registry());
        }
        Profile p = profile();
        return p == null || p.registry() == null || mode != Mode.APP ? Decoders.plain() : new Decoders(SchemaRegistry.of(p.registry()));
    }

    // ---- flows ------------------------------------------------------------------------------------------------------

    /** Saves a catalog as a flow and switches the running observer to it, no restart. */
    public synchronized Result<Catalog> activate(String flow, JsonNode catalogJson) {
        requireApp();
        if (!Workspace.validName(flow)) {
            return Result.err("a flow's name is lowercase letters, digits and hyphens, such as checkout");
        }
        Result<Catalog> c = Catalog.fromJson(catalogJson);
        if (!c.isOk()) {
            return c;
        }
        Workspace.Config cfg = workspace.load();
        if (cfg.active() == null) {
            return Result.err("not connected: connect to a cluster first");
        }
        workspace.saveFlow(flow, catalogJson);
        workspace.deleteDraft(flow);
        workspace.save(cfg.withActive(cfg.activeProfile(), flow));
        switchTo(cfg.active(), flow);
        return c;
    }

    private void switchTo(Profile p, String flow) {
        Result<Catalog> c = workspace.flow(flow);
        if (!c.isOk()) {
            throw new IllegalStateException("flow " + flow + " is not usable: " + c.error());
        }
        Kafka.settings(p.bootstrap(), p.clientSettings());
        SchemaRegistry registry = SchemaRegistry.of(p.registry());
        Observer.Options opts = new Observer.Options(p.bootstrap(), null, c.value(), registry == null ? Decoders.plain() : new Decoders(registry),
                workspace.stateFile(p.name(), flow));
        stopRunning();
        Observer o = new Observer(opts).start();
        running = new Running(p, flow, c.value(), o, registry);
        problem = null;
        log.info("engine: watching {} on {} ({})", flow, p.bootstrap(), p.name());
        listeners.forEach(l -> l.accept(o));
    }

    private void stopRunning() {
        Running r = running;
        running = null;
        if (r != null) {
            listeners.forEach(l -> l.accept(null));
            try {
                r.observer().close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void requireApp() {
        if (mode != Mode.APP) {
            throw new IllegalStateException(mode == Mode.TRY
                    ? "this is the sample data (revenue-in-flight try): start revenue-in-flight on its own to connect to your Kafka"
                    : "this instance watches one catalog file (revenue-in-flight watch): start revenue-in-flight on its own to set it up");
        }
    }

    // ---- status -----------------------------------------------------------------------------------------------------

    /** Where things stand, and what to do next. */
    public ObjectNode status() {
        ObjectNode o = Json.object();
        o.put("mode", mode.show());
        Running r = running;
        Observer obs = r == null ? null : r.observer();
        Workspace.Config c = mode == Mode.APP ? workspace.load() : null;
        Profile p = mode == Mode.APP ? c.active() : null;
        String stage = obs != null ? "watching" : p == null ? "connect" : "discover";
        o.put("stage", stage);
        if (workspace != null) {
            o.put("home", workspace.home().toString());
        }
        if (p != null) {
            o.set("profile", Workspace.profileJson(p));
        } else {
            o.putNull("profile");
        }
        ArrayNode ps = o.putArray("profiles");
        if (c != null) {
            c.profiles().forEach(x -> ps.add(x.name()));
        }
        if (r != null) {
            o.put("flow", r.flow());
            o.put("bootstrap", obs.bootstrap());
        } else {
            o.putNull("flow");
        }
        ArrayNode fs = o.putArray("flows");
        ArrayNode ds = o.putArray("drafts");
        if (mode == Mode.APP) {
            workspace.flows().forEach(fs::add);
            try (var s = java.nio.file.Files.list(workspace.home().resolve("flows"))) {
                s.map(x -> x.getFileName().toString()).filter(n -> n.endsWith(".draft.json")).sorted()
                        .forEach(n -> ds.add(n.substring(0, n.length() - ".draft.json".length())));
            } catch (java.io.IOException ignored) {
                // no flows yet
            }
        }
        o.put("observing", obs != null && obs.reader().reading());
        ArrayNode missing = o.putArray("missingTopics");
        if (obs != null) {
            obs.reader().missing().forEach(missing::add);
        }
        String why = obs != null ? obs.reader().problem() : problem;
        if (why != null) {
            o.put("problem", why);
        }
        o.put("canWrite", mode == Mode.APP && p != null && p.allowWrites());
        o.put("next", next(stage, ds.size() > 0, why));
        return o;
    }

    private String next(String stage, boolean drafts, String why) {
        return switch (stage) {
            case "connect" -> "Connect to a cluster: give its bootstrap servers, an auth preset and secret references (env:NAME, file:/path, keychain:service).";
            case "discover" -> drafts ? "A draft is waiting: answer its questions, validate it, then activate it."
                    : "Discover what is on the cluster, sample the topics of the flow, and draft a catalog from them.";
            default -> why != null ? "Watching, but: " + why : mode == Mode.TRY ? "Sample data: break the schema and watch money get stuck." : "Watching. value_at_risk is the headline.";
        };
    }

    @Override
    public synchronized void close() {
        stopRunning();
    }
}
