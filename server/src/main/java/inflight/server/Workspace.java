package inflight.server;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import inflight.catalog.Catalog;
import inflight.catalog.Json;
import inflight.catalog.Result;
import inflight.kafka.Profile;

/**
 * The tool's home: {@code ~/.in-flight}, or {@code --home}, or {@code INFLIGHT_HOME}.
 *
 * <pre>
 *   config.json            connection profiles, the active profile and flow, alerts
 *   flows/&lt;name&gt;.json      catalogs, the active one among them: plain catalog files, to keep in a repository
 *   flows/&lt;name&gt;.draft.json a draft and its open questions, until it is activated
 *   state/                 the ledger's snapshots, one per profile and flow
 * </pre>
 *
 * Secrets are never written here: profiles hold references to them.
 */
public final class Workspace {

    /** Where to post when money at risk crosses a threshold; null url when off. */
    public record Alerts(String webhook, double threshold) {
        public static final Alerts OFF = new Alerts(null, 0);
    }

    /** What config.json holds. */
    public record Config(List<Profile> profiles, String activeProfile, String activeFlow, Alerts alerts) {
        public Config {
            profiles = List.copyOf(profiles);
            alerts = alerts == null ? Alerts.OFF : alerts;
        }

        public static Config empty() {
            return new Config(List.of(), null, null, Alerts.OFF);
        }

        public Profile profile(String name) {
            return profiles.stream().filter(p -> p.name().equals(name)).findFirst().orElse(null);
        }

        public Profile active() {
            return activeProfile == null ? null : profile(activeProfile);
        }

        public Config withProfile(Profile p) {
            List<Profile> ps = new ArrayList<>(profiles.stream().filter(x -> !x.name().equals(p.name())).toList());
            ps.add(p);
            return new Config(ps, activeProfile, activeFlow, alerts);
        }

        public Config withoutProfile(String name) {
            return new Config(profiles.stream().filter(x -> !x.name().equals(name)).toList(), name.equals(activeProfile) ? null : activeProfile,
                    name.equals(activeProfile) ? null : activeFlow, alerts);
        }

        public Config withActive(String profile, String flow) {
            return new Config(profiles, profile, flow, alerts);
        }

        public Config withAlerts(Alerts a) {
            return new Config(profiles, activeProfile, activeFlow, a);
        }
    }

    private final Path home;

    public Workspace(Path home) {
        this.home = home.toAbsolutePath();
    }

    /**
     * {@code INFLIGHT_HOME}, else {@code ~/.in-flight}; else, when only that exists, {@code ~/.revenue-in-flight}, the
     * workspace from before the rename, so a setup made then is still found.
     */
    public static Workspace fromEnv() {
        String h = inflight.catalog.Env.get("INFLIGHT_HOME");
        if (h != null && !h.isBlank()) {
            return new Workspace(Path.of(h));
        }
        Path user = Path.of(System.getProperty("user.home"));
        Path now = user.resolve(".in-flight"), before = user.resolve(".revenue-in-flight");
        return new Workspace(!java.nio.file.Files.exists(now) && java.nio.file.Files.isDirectory(before) ? before : now);
    }

    public Path home() {
        return home;
    }

    public Path configFile() {
        return home.resolve("config.json");
    }

    // ---- config -----------------------------------------------------------------------------------------------------

    public synchronized Config load() {
        Path f = configFile();
        if (!Files.exists(f)) {
            return Config.empty();
        }
        try {
            Result<JsonNode> j = Json.parse(Files.readString(f));
            if (!j.isOk()) {
                throw new IllegalStateException(f + " is not JSON: " + j.error());
            }
            return read(j.value());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public synchronized void save(Config c) {
        write(configFile(), Json.pretty(write(c)) + "\n");
    }

    static Config read(JsonNode j) {
        List<Profile> ps = new ArrayList<>();
        j.path("profiles").forEach(p -> ps.add(profile(p)));
        JsonNode a = j.path("active");
        JsonNode al = j.path("alerts");
        Alerts alerts = al.isObject() && al.path("webhook").isTextual() ? new Alerts(al.get("webhook").asText(), al.path("threshold").asDouble(0)) : Alerts.OFF;
        return new Config(ps, a.path("profile").asText(null), a.path("flow").asText(null), alerts);
    }

    static ObjectNode write(Config c) {
        ObjectNode o = Json.object();
        ArrayNode ps = o.putArray("profiles");
        c.profiles().forEach(p -> ps.add(profileJson(p)));
        ObjectNode a = o.putObject("active");
        if (c.activeProfile() != null) {
            a.put("profile", c.activeProfile());
        }
        if (c.activeFlow() != null) {
            a.put("flow", c.activeFlow());
        }
        if (c.alerts().webhook() != null) {
            o.putObject("alerts").put("webhook", c.alerts().webhook()).put("threshold", c.alerts().threshold());
        }
        return o;
    }

    /** A profile from JSON, as config.json and the connect tool give it. */
    public static Profile profile(JsonNode p) {
        Map<String, String> secrets = strings(p.path("secrets"));
        Map<String, String> settings = strings(p.path("settings"));
        JsonNode r = p.path("registry");
        Profile.Registry registry = null;
        if (r.isTextual() && !r.asText().isBlank()) {
            registry = new Profile.Registry(r.asText(), null, null);
        } else if (r.isObject() && r.path("url").isTextual()) {
            registry = new Profile.Registry(r.get("url").asText(), r.path("username").asText(null), r.path("password").asText(null));
        }
        return new Profile(p.path("name").asText(null), p.path("bootstrap").asText(null), p.path("auth").asText("none"), secrets, settings, registry,
                p.path("allowWrites").asBoolean(false));
    }

    public static ObjectNode profileJson(Profile p) {
        ObjectNode o = Json.object();
        o.put("name", p.name()).put("bootstrap", p.bootstrap()).put("auth", p.auth());
        if (!p.secrets().isEmpty()) {
            ObjectNode s = o.putObject("secrets");
            p.secrets().forEach(s::put);
        }
        if (!p.settings().isEmpty()) {
            ObjectNode s = o.putObject("settings");
            p.settings().forEach(s::put);
        }
        if (p.registry() != null) {
            ObjectNode r = o.putObject("registry").put("url", p.registry().url());
            if (p.registry().username() != null) {
                r.put("username", p.registry().username());
            }
            if (p.registry().password() != null) {
                r.put("password", p.registry().password());
            }
        }
        o.put("allowWrites", p.allowWrites());
        return o;
    }

    private static Map<String, String> strings(JsonNode o) {
        Map<String, String> m = new LinkedHashMap<>();
        if (o.isObject()) {
            o.properties().forEach(e -> {
                if (e.getValue().isValueNode() && !e.getValue().isNull()) {
                    m.put(e.getKey(), e.getValue().asText());
                }
            });
        }
        return m;
    }

    // ---- flows ------------------------------------------------------------------------------------------------------

    public static boolean validName(String flow) {
        return flow != null && flow.matches("[a-z0-9][a-z0-9-]{0,62}");
    }

    public Path flowFile(String flow) {
        return home.resolve("flows").resolve(flow + ".json");
    }

    public Path draftFile(String flow) {
        return home.resolve("flows").resolve(flow + ".draft.json");
    }

    public Path stateFile(String profile, String flow) {
        return home.resolve("state").resolve(profile + "--" + flow + ".json");
    }

    public List<String> flows() {
        Path dir = home.resolve("flows");
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        try (var s = Files.list(dir)) {
            return s.map(p -> p.getFileName().toString()).filter(n -> n.endsWith(".json") && !n.endsWith(".draft.json"))
                    .map(n -> n.substring(0, n.length() - 5)).sorted().toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Result<Catalog> flow(String flow) {
        Path f = flowFile(flow);
        if (!Files.exists(f)) {
            return Result.err("there is no flow " + flow + " in " + home.resolve("flows"));
        }
        return Catalog.load(f);
    }

    public JsonNode flowJson(String flow) {
        return readJson(flowFile(flow));
    }

    public void saveFlow(String flow, JsonNode catalog) {
        write(flowFile(flow), Json.pretty(catalog) + "\n");
    }

    /** The draft and its guesses, or null. */
    public JsonNode draft(String flow) {
        return readJson(draftFile(flow));
    }

    public void saveDraft(String flow, JsonNode draft) {
        write(draftFile(flow), Json.pretty(draft) + "\n");
    }

    public void deleteDraft(String flow) {
        try {
            Files.deleteIfExists(draftFile(flow));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static JsonNode readJson(Path f) {
        if (!Files.exists(f)) {
            return null;
        }
        try {
            Result<JsonNode> j = Json.parse(Files.readString(f));
            return j.isOk() ? j.value() : null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Written whole or not at all: to a temporary file, then moved over the old one. Readable by its owner only. */
    static void write(Path f, String text) {
        try {
            Files.createDirectories(f.getParent());
            Path tmp = f.resolveSibling(f.getFileName() + ".tmp");
            Files.writeString(tmp, text);
            try {
                Files.setPosixFilePermissions(tmp, PosixFilePermissions.fromString("rw-------"));
            } catch (UnsupportedOperationException ignored) {
                // not a POSIX file system
            }
            Files.move(tmp, f, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
