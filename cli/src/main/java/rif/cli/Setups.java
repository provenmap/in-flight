package rif.cli;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import rif.catalog.Catalog;
import rif.catalog.Json;
import rif.catalog.Result;
import rif.kafka.Profile;
import rif.recovery.Redrive;
import rif.server.Engine;
import rif.server.Setup;
import rif.server.Workspace;

/**
 * The setup commands, for scripts and CI: the same actions as the MCP tools and the console's screens, on the
 * workspace, with no server running. Each prints a line or a few for a person, and the data with {@code --json}.
 */
final class Setups {

    private Setups() {}

    /** Flags out of the arguments: {@code --name value} pairs and lone {@code --flag}s; the rest stay in order. */
    record Args(List<String> positional, Map<String, List<String>> flags) {
        static Args of(String[] argv, String... valued) {
            List<String> pos = new ArrayList<>();
            Map<String, List<String>> flags = new LinkedHashMap<>();
            List<String> takesValue = Arrays.asList(valued);
            for (int i = 0; i < argv.length; i++) {
                String a = argv[i];
                if (a.startsWith("--")) {
                    String name = a.substring(2);
                    String value = "true";
                    if (name.contains("=")) {
                        value = name.substring(name.indexOf('=') + 1);
                        name = name.substring(0, name.indexOf('='));
                    } else if (takesValue.contains(name)) {
                        if (i + 1 >= argv.length) {
                            throw new Main.Failure("--" + name + " needs a value");
                        }
                        value = argv[++i];
                    }
                    flags.computeIfAbsent(name, k -> new ArrayList<>()).add(value);
                } else {
                    pos.add(a);
                }
            }
            return new Args(pos, flags);
        }

        boolean has(String flag) {
            return flags.containsKey(flag);
        }

        String one(String flag) {
            List<String> v = flags.get(flag);
            return v == null ? null : v.get(v.size() - 1);
        }

        List<String> all(String flag) {
            return flags.getOrDefault(flag, List.of());
        }
    }

    private static int print(Setup.Answer a, boolean json) {
        if (json) {
            ObjectNode o = a.data().deepCopy();
            o.put("text", a.text());
            System.out.println(Json.pretty(o));
        } else {
            System.out.println(a.text());
        }
        return a.ok() ? 0 : 1;
    }

    private static Setup setup(Workspace ws) {
        return new Setup(Engine.app(ws));
    }

    static int connect(Workspace ws, String[] argv) {
        Args a = Args.of(argv, "auth", "secret", "setting", "registry", "registry-username", "registry-password");
        if (a.positional().size() < 2) {
            throw new Main.Failure("connect needs a name and bootstrap servers, such as: connect staging kafka-1:9092 --auth scram-sha-512 "
                    + "--secret username=alice --secret password=env:KAFKA_PASSWORD");
        }
        Map<String, String> secrets = pairs(a.all("secret"), "--secret");
        Map<String, String> settings = pairs(a.all("setting"), "--setting");
        Profile.Registry registry = a.one("registry") == null ? null
                : new Profile.Registry(a.one("registry"), a.one("registry-username"), a.one("registry-password"));
        Profile p = new Profile(a.positional().get(0), a.positional().get(1), a.one("auth"), secrets, settings, registry, a.has("allow-writes"));
        Engine.Tested t = a.has("test") ? Engine.test(p) : Engine.app(ws).connect(p, true);
        System.out.println(t.text());
        return t.ok() ? 0 : 1;
    }

    private static Map<String, String> pairs(List<String> xs, String flag) {
        Map<String, String> m = new LinkedHashMap<>();
        for (String x : xs) {
            int eq = x.indexOf('=');
            if (eq <= 0) {
                throw new Main.Failure(flag + " takes key=value, such as " + flag + " password=env:KAFKA_PASSWORD");
            }
            m.put(x.substring(0, eq), x.substring(eq + 1));
        }
        return m;
    }

    static int discover(Workspace ws, String[] argv) {
        Args a = Args.of(argv);
        Setup.Answer r = setup(ws).discover();
        if (!a.has("json") && r.ok()) {
            System.out.println(r.text());
            for (JsonNode t : r.data().path("topics")) {
                if (!t.path("kind").asText().equals("internal")) {
                    System.out.printf("  %-40s %-6s %3d partitions %,12d records%n", t.path("name").asText(), t.path("kind").asText(), t.path("partitions").asInt(),
                            t.path("records").asLong());
                }
            }
            for (JsonNode g : r.data().path("groups")) {
                System.out.printf("  group %-34s reads %s (lag %,d)%n", g.path("group").asText(), String.join(", ", Main.list(g.path("reads"))), g.path("lag").asLong());
            }
            for (JsonNode q : r.data().path("dlqs")) {
                System.out.printf("  dead letters %-27s %s%s%n", q.path("topic").asText(), String.join("; ", Main.list(q.path("reasons"))),
                        q.path("owner").isNull() ? "" : ", written by " + q.path("owner").asText());
            }
            return 0;
        }
        return print(r, a.has("json"));
    }

    static int sample(Workspace ws, String[] argv) {
        Args a = Args.of(argv);
        if (a.positional().isEmpty()) {
            throw new Main.Failure("sample needs a topic");
        }
        int n = a.positional().size() > 1 ? Integer.parseInt(a.positional().get(1)) : 50;
        Setup.Answer r = setup(ws).sample(a.positional().get(0), n, a.has("reveal"));
        if (!a.has("json") && r.ok()) {
            System.out.println(r.text());
            for (JsonNode f : r.data().path("fields")) {
                System.out.printf("  %-36s %-18s %5.0f%%  %s%n", f.path("path").asText(), String.join("|", Main.list(f.path("types"))),
                        f.path("presence").asDouble() * 100, f.path("example").asText(""));
            }
            return 0;
        }
        return print(r, a.has("json"));
    }

    static int draft(Workspace ws, String[] argv) {
        Args a = Args.of(argv);
        if (a.positional().isEmpty()) {
            throw new Main.Failure("draft needs a flow's name, such as: draft checkout");
        }
        List<String> topics = a.positional().subList(1, a.positional().size());
        Setup.Answer r = setup(ws).draft(a.positional().get(0), topics);
        if (!a.has("json") && r.ok()) {
            System.out.println(r.text());
            for (JsonNode g : r.data().path("guesses")) {
                if (g.path("confidence").asDouble() < 0.9) {
                    System.out.printf("  [%s] %s (%.0f%% sure)%n", g.path("id").asText(), g.path("question").asText(), g.path("confidence").asDouble() * 100);
                    for (JsonNode o : g.path("options")) {
                        System.out.println("      - " + o.path("label").asText());
                    }
                }
            }
            System.out.println("The draft is " + ws.draftFile(a.positional().get(0)) + ". Answer in the console, or edit its catalog, then validate and activate.");
            return 0;
        }
        return print(r, a.has("json"));
    }

    static int validate(Workspace ws, String[] argv) throws java.io.IOException {
        Args a = Args.of(argv);
        if (a.positional().isEmpty()) {
            throw new Main.Failure("validate needs a flow or a catalog file");
        }
        String target = a.positional().get(0);
        Setup s = setup(ws);
        Setup.Answer r;
        if (target.endsWith(".json") || Files.exists(Path.of(target))) {
            Result<JsonNode> j = Json.parse(Files.readString(Path.of(target)));
            if (!j.isOk()) {
                throw new Main.Failure(target + " is not JSON: " + j.error());
            }
            r = s.validate(null, j.value());
        } else {
            r = s.validate(target, null);
        }
        return print(r, a.has("json"));
    }

    static int activate(Workspace ws, String[] argv) throws java.io.IOException {
        Args a = Args.of(argv);
        if (a.positional().isEmpty()) {
            throw new Main.Failure("activate needs a flow, and optionally its catalog file");
        }
        String flow = a.positional().get(0);
        if (!Workspace.validName(flow)) {
            throw new Main.Failure("a flow's name is lowercase letters, digits and hyphens, such as checkout");
        }
        JsonNode c;
        if (a.positional().size() > 1) {
            Result<JsonNode> j = Json.parse(Files.readString(Path.of(a.positional().get(1))));
            if (!j.isOk()) {
                throw new Main.Failure(a.positional().get(1) + " is not JSON: " + j.error());
            }
            c = j.value();
        } else {
            JsonNode d = ws.draft(flow);
            c = d != null ? d.get("catalog") : ws.flowJson(flow);
        }
        if (c == null) {
            throw new Main.Failure("nothing to activate for " + flow + ": draft it first, or give a catalog file");
        }
        Result<Catalog> parsed = Catalog.fromJson(c);
        if (!parsed.isOk()) {
            throw new Main.Failure("not a usable catalog:\n  " + String.join("\n  ", parsed.errors()));
        }
        Workspace.Config cfg = ws.load();
        if (cfg.active() == null) {
            throw new Main.Failure("not connected: run connect first");
        }
        ws.saveFlow(flow, c);
        ws.deleteDraft(flow);
        ws.save(cfg.withActive(cfg.activeProfile(), flow));
        System.out.println("Activated " + flow + " (" + ws.flowFile(flow) + "). Start revenue-in-flight to watch it.");
        return 0;
    }

    static int redrive(Workspace ws, String[] argv) throws InterruptedException {
        Args a = Args.of(argv, "error", "order", "from", "to", "limit", "rate");
        if (a.positional().isEmpty()) {
            throw new Main.Failure("redrive needs a dead-letter queue");
        }
        Engine engine = Engine.app(ws).start();
        try {
            Setup s = new Setup(engine);
            Redrive.Filter f = Setup.filter(a.one("error"), a.one("order"), a.one("from"), a.one("to"));
            double rate = a.one("rate") == null ? 50 : Double.parseDouble(a.one("rate"));
            int limit = a.one("limit") == null ? 0 : Integer.parseInt(a.one("limit"));
            Setup.Answer r = s.redrive(a.positional().get(0), f, !a.has("run"), rate, limit, Long.MAX_VALUE);
            return print(r, a.has("json"));
        } finally {
            engine.close();
        }
    }
}
