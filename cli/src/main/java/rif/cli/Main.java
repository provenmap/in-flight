package rif.cli;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import rif.catalog.Catalog;
import rif.catalog.Describe;
import rif.catalog.EventRef;
import rif.catalog.Impact;
import rif.catalog.Result;
import rif.catalog.RoutingTable;
import rif.router.Router;
import com.fasterxml.jackson.databind.node.ObjectNode;

import rif.catalog.Json;
import rif.server.Changes;
import rif.server.Engine;
import rif.server.Mcp;
import rif.server.Server;
import rif.server.Setup;
import rif.server.Workspace;
import rif.simulator.Simulator;

/** revenue-in-flight: the app, try, mcp, connect · discover · draft · validate · activate · status, and the rest. */
public final class Main {

    static final String USAGE = """
            usage: revenue-in-flight [command] [--home DIR]

            Watch the money moving through your own Kafka:

              (no command)  start the app and open the console on http://localhost:8088. With nothing configured it opens
                            on Connect: give a cluster, discover its topics, confirm what the money is, watch.
              mcp           the MCP server over stdio, for Claude Code or any MCP client; it serves the console on :8088
                            too. Set it up by asking: claude mcp add revenue-in-flight -- revenue-in-flight mcp
              status        what is connected, which flow is active, and what to do next

            The same setup from a script or CI, on the workspace (~/.revenue-in-flight, or --home, or RIF_HOME):

              connect <name> <bootstrap> [--auth PRESET] [--secret ROLE=REF]... [--setting KEY=VALUE]...
                      [--registry URL] [--allow-writes] [--test]
                            test a connection and save it as the profile in use. Secrets are references, never values:
                            env:NAME, file:/path or keychain:service[/account]. Presets: none, ssl, sasl-plain (Confluent
                            Cloud), scram-sha-256, scram-sha-512, mtls, msk-iam
              discover [--json]
                            topics, consumer groups, dead-letter queues and registry subjects on the cluster in use
              sample <topic> [n] [--reveal]
                            the field shapes of a topic's latest records, values masked unless --reveal
              draft <flow> [topic...]
                            draft a catalog from the cluster, with a question for every guess; saved as the flow's draft
              validate <flow | catalog.json>
                            check a catalog and dry-run it on records sampled now. Exits 1 when it does not price them
              activate <flow> [catalog.json]
                            save the catalog (or the draft) as the flow in use
              redrive <dlq> [--run] [--error TEXT] [--order ID] [--limit N] [--rate N]
                            what a redrive of a dead-letter queue would do; with --run, do it (the profile must allow writes)
              impact <Type@vN> [catalog.json]
                            what one event version reaches, and who must change before it ships. Exits 1 when someone
                            must: for CI. Needs no Kafka
              impact --since <base.json> [catalog.json]
                            the same for every event version the catalog adds since base: what a pull request changes
              rehearse --topic T | --intent I --candidate "<cmd>" | --candidate-image IMG [--baseline "<cmd>"]
                       [--minutes N] [--limit N]
                            run the last minutes of a topic through a consumer given as a command or an image, one
                            JSON line in and one out per record, beside a baseline, and show where they differ
              watch <catalog.json>
                            watch one catalog file on BOOTSTRAP_SERVERS, with no workspace

            Try it with sample data (a simulated order-to-cash business, clearly labelled):

              try           the sample data in one process: the router, a simulated business, and the console with
                            contracts in the loop, on the stack up starts
              up [kafka]    start the sample data's stack: Kafka, the schema registry and the proxy (Docker Compose)
              down          stop it and delete its data
              simulate      run the simulated business alone, control API on :8090
              serve         the console on the simulated business, with its controls
              router        run the intent router: one Kafka Streams application per intent, control API on :8085
              intents [catalog.json]    the catalog's declared intents
              explain "<predicate>" [catalog.json]
                            compile a predicate, such as "changes(Order.total)", and show what it would match
              rehearse <intent> <build> [minutes]
                            run the intent's recent events through a build of its consumer, beside the one running now
              contracts propose <catalog.json> [--push] | approve <id> | status [<id>] | generate | register

            environment: PORT (8088), HOST (localhost; beyond it the console needs RIF_TOKEN, or prints one), RIF_HOME,
                         RIF_MCP_TOKEN (MCP over HTTP beyond localhost), BOOTSTRAP_SERVERS (watch, try), RIF_KAFKA_*
                         (extra Java client settings), SIMULATOR_URL, ROUTER_URL, APICURIO_URL""";

    private Main() {}

    public static void main(String[] args) {
        try {
            System.exit(run(args));
        } catch (Failure e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            System.err.println(e.getMessage() == null ? e.toString() : e.getMessage());
            System.exit(1);
        }
    }

    /** A failure the person can act on: printed as it is, exit 1. */
    static final class Failure extends RuntimeException {
        Failure(String msg) {
            super(msg, null, false, false);
        }
    }

    static int run(String[] argv) throws Exception {
        List<String> args0 = new ArrayList<>(Arrays.asList(argv));
        int h = args0.indexOf("--home");
        if (h >= 0) {
            if (h + 1 >= args0.size()) {
                throw new Failure("--home needs a directory");
            }
            home = Path.of(args0.get(h + 1));
            args0.remove(h + 1);
            args0.remove(h);
        }
        boolean noOpen = args0.remove("--no-open");
        argv = args0.toArray(String[]::new);
        String command = argv.length == 0 ? "app" : argv[0];
        String[] rest = Arrays.copyOfRange(argv, Math.min(1, argv.length), argv.length);
        switch (command) {
            case "app", "start" -> {
                Server server = Server.start(Server.Options.fromEnv().app(workspace()));
                System.err.println("revenue-in-flight: the console is on " + server.loginUrl()
                        + (server.engine().observer() == null ? " (nothing configured yet: it opens on Connect)" : ""));
                if (!noOpen) {
                    open(server.loginUrl());
                }
                return untilSignal(server);
            }
            case "try" -> {
                System.err.println("revenue-in-flight: sample data. A simulated order-to-cash business on the stack `up` starts; nothing here is yours.");
                Demo demo = Demo.start(Demo.Options.fromEnv());
                return untilSignal(demo);
            }
            case "status" -> {
                ObjectNode s = Engine.app(workspace()).status();
                System.out.println(s.get("next").asText());
                System.out.println(Json.pretty(s));
                return 0;
            }
            case "connect" -> {
                return Setups.connect(workspace(), rest);
            }
            case "discover" -> {
                return Setups.discover(workspace(), rest);
            }
            case "sample" -> {
                return Setups.sample(workspace(), rest);
            }
            case "draft" -> {
                return Setups.draft(workspace(), rest);
            }
            case "validate" -> {
                return Setups.validate(workspace(), rest);
            }
            case "activate" -> {
                return Setups.activate(workspace(), rest);
            }
            case "redrive" -> {
                return Setups.redrive(workspace(), rest);
            }
            case "up" -> {
                // `up kafka` starts Kafka alone: enough for simulate and serve, the incident without the proxy or registry.
                List<String> args = new ArrayList<>(List.of("up", "-d", "--wait"));
                args.addAll(Arrays.asList(rest));
                return Compose.run(args);
            }
            case "down" -> {
                int code = Compose.run(List.of("down", "-v"));
                deleteTree(Path.of(".rif/demo"));
                return code;
            }
            case "demo" -> {
                System.err.println("revenue-in-flight: demo is now called try. Running the sample data.");
                Demo demo = Demo.start(Demo.Options.fromEnv());
                return untilSignal(demo);
            }
            case "simulate" -> {
                Simulator sim = Simulator.start(Simulator.Options.fromEnv());
                return untilSignal(sim);
            }
            case "serve" -> {
                Server server = Server.start(Server.Options.fromEnv());
                return untilSignal(server);
            }
            case "watch" -> {
                if (rest.length == 0) {
                    throw new Failure("watch needs a catalog file. To set one up from your cluster, run revenue-in-flight with no command.");
                }
                Catalog catalog = loadCatalog(rest[0]);
                Server.Options opts = Server.Options.fromEnv();
                Server server = Server.start(opts.withObserver(opts.observer().withCatalog(catalog).withSimulator(null)));
                return untilSignal(server);
            }
            case "router" -> {
                Router router = Router.start(Router.Config.fromEnv(System.getenv()));
                return untilSignal(router);
            }
            case "mcp" -> {
                // stdout is the protocol: everything else goes to stderr. The console is served too, when its port is free.
                // With SIMULATOR_URL set (or --try), the sample data's tools; otherwise the workspace's.
                String simEnv = System.getenv("SIMULATOR_URL");
                boolean sample = Arrays.asList(rest).contains("--try") || simEnv != null && !simEnv.equals("none");
                Server.Options opts = sample ? Server.Options.fromEnv() : Server.Options.fromEnv().app(workspace());
                Server server = null;
                Engine engine;
                if (portFree(opts.host(), opts.port())) {
                    server = Server.start(opts);
                    engine = server.engine();
                    System.err.println("revenue-in-flight: MCP on stdio; the console is on " + server.loginUrl());
                } else {
                    System.err.println("revenue-in-flight: MCP on stdio; port " + opts.port() + " is taken, so the console is not served");
                    engine = sample ? Engine.fixed(Engine.Mode.TRY, opts.observer(), workspace()).start() : Engine.app(workspace()).start();
                }
                Engine eng = engine;
                Changes changes = server != null ? server.changes()
                        : new Changes(new Changes.Options(rif.contracts.Contracts.Settings.fromEnv(Path.of("").toAbsolutePath()), false),
                                sample && eng.observer() != null ? eng.observer().simulator() : null);
                var mcp = Mcp.stdio(eng, server != null ? server.setup() : new Setup(eng), changes);
                Server running = server;
                return untilSignal(() -> {
                    mcp.closeGracefully();
                    if (running != null) {
                        running.close();
                    } else {
                        eng.close();
                    }
                });
            }
            case "intents" -> {
                Catalog catalog = loadCatalog(rest.length > 0 ? rest[0] : null);
                if (catalog.semantics() == null) {
                    throw new Failure(Describe.NO_SEMANTICS);
                }
                List<RoutingTable> tables = catalog.semantics().compiled();
                System.out.println(tables.isEmpty() ? "The catalog declares no intents."
                        : String.join("\n\n", tables.stream().map(t -> Describe.table(t, catalog.readers().apply(t.intent()))).toList()));
                return 0;
            }
            case "explain" -> {
                if (rest.length == 0) {
                    throw new Failure("explain needs a predicate, such as \"changes(Order.total)\"");
                }
                Catalog catalog = loadCatalog(rest.length > 1 ? rest[1] : null);
                if (catalog.semantics() == null) {
                    throw new Failure(Describe.NO_SEMANTICS);
                }
                Result<RoutingTable> table = catalog.semantics().explain(rest[0], null);
                if (!table.isOk()) {
                    throw new Failure(String.join("\n", table.errors()));
                }
                System.out.println(Describe.table(table.value().named("matches"), null));
                return 0;
            }
            case "impact" -> {
                if (rest.length > 0 && rest[0].equals("--since")) {
                    return ImpactSince.run(Arrays.copyOfRange(rest, 1, rest.length));
                }
                Result<EventRef> ref = EventRef.parse(rest.length > 0 ? rest[0] : "");
                if (!ref.isOk()) {
                    throw new Failure("impact needs an event version: " + ref.error());
                }
                Catalog catalog = loadCatalog(rest.length > 1 ? rest[1] : null);
                if (catalog.semantics() == null) {
                    throw new Failure(Describe.NO_SEMANTICS);
                }
                Result<Impact> impact = Impact.of(ref.value(), catalog.semantics(), catalog.readers());
                if (!impact.isOk()) {
                    throw new Failure(impact.error());
                }
                System.out.println(impact.value().headline() + "\n\n" + impact.value().report());
                return impact.value().safe() ? 0 : 1;
            }
            case "rehearse" -> {
                if (Arrays.stream(rest).anyMatch(x -> x.startsWith("--candidate"))) {
                    return Rehearse.run(workspace(), rest);
                }
                if (rest.length < 2) {
                    throw new Failure("rehearse needs an intent and a build, such as: rehearse order-total-changes patched 10");
                }
                String url = rif.observer.SimulatorClient.urlFromEnv();
                if (url == null) {
                    throw new Failure("rehearse needs the simulator: SIMULATOR_URL is none");
                }
                var reply = new rif.observer.SimulatorClient(url).call("POST", "/rehearse?intent=" + rest[0] + "&build=" + rest[1]
                        + "&minutes=" + (rest.length > 2 ? rest[2] : "10"), java.time.Duration.ofSeconds(60));
                var body = reply.json();
                if (reply.status() != 200) {
                    throw new Failure(body.path("error").asText("the simulator answered " + reply.status()));
                }
                System.out.println(body.get("text").asText());
                body.get("differences").forEach(d -> System.out.println("  " + d.get("event").asText() + " " + d.get("orderId").asText() + ": "
                        + d.get("baseline").get("result").asText() + " -> " + d.get("candidate").get("result").asText()));
                return 0;
            }
            case "contracts" -> {
                return ContractsCommand.run(rest);
            }
            case "version", "--version" -> {
                System.out.println("revenue-in-flight " + Mcp.VERSION);
                return 0;
            }
            case "help", "--help", "-h" -> {
                System.out.println(USAGE);
                return 0;
            }
            default -> {
                System.err.println("unknown command '" + command + "'\n\n" + USAGE);
                return 2;
            }
        }
    }

    static List<String> list(com.fasterxml.jackson.databind.JsonNode a) {
        List<String> out = new ArrayList<>();
        a.forEach(x -> out.add(x.asText()));
        return out;
    }

    /** The workspace: --home, else RIF_HOME, else ~/.revenue-in-flight. */
    static Path home;

    static Workspace workspace() {
        return home != null ? new Workspace(home) : Workspace.fromEnv();
    }

    static boolean portFree(String host, int port) {
        try (java.net.ServerSocket s = new java.net.ServerSocket()) {
            s.setReuseAddress(true);
            s.bind(new java.net.InetSocketAddress(host, port));
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /** Opens the console in a browser, when there is one to open. */
    static void open(String url) {
        String os = System.getProperty("os.name", "").toLowerCase(java.util.Locale.ROOT);
        List<String> cmd = os.contains("mac") ? List.of("open", url) : os.contains("win") ? List.of("rundll32", "url.dll,FileProtocolHandler", url)
                : List.of("xdg-open", url);
        if (System.console() == null || System.getenv("CI") != null) {
            return;
        }
        try {
            new ProcessBuilder(cmd).redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
        } catch (IOException e) {
            // no browser here: the address is printed
        }
    }

    /** Removes a directory and everything under it, if it is there. */
    static void deleteTree(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        try (var paths = Files.walk(dir)) {
            for (Path p : paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
                Files.delete(p);
            }
        }
    }

    static Catalog loadCatalog(String file) {
        if (file == null) {
            return Catalog.defaultCatalog();
        }
        Result<Catalog> c = Catalog.load(Path.of(file));
        if (!c.isOk()) {
            throw new Failure(file + " is not a usable catalog:\n  " + String.join("\n  ", c.errors()));
        }
        return c.value();
    }

    /** Runs until SIGINT or SIGTERM, then closes cleanly, so services leave their consumer groups. */
    static int untilSignal(AutoCloseable running) throws InterruptedException {
        CountDownLatch closed = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                running.close();
            } catch (Exception e) {
                System.err.println(e.getMessage());
            } finally {
                closed.countDown();
            }
        }));
        closed.await();
        return 0;
    }

    /** Docker Compose, with the stack this build ships. */
    static final class Compose {
        private Compose() {}

        /**
         * The Compose file in the working directory when there is one (a checkout), else the shipped stack, unpacked.
         * Either way the proxy's filter jar is put beside it from this jar when it is missing, so {@code up} works on a
         * fresh checkout before anything else is built.
         */
        static Path file() throws IOException {
            Path here = Path.of("docker-compose.yml");
            if (Files.exists(here) && Files.readString(here).contains("revenue-in-flight")) {
                Path filter = Path.of("stack/rif-proxy-filter.jar");
                if (!Files.exists(filter)) {
                    try (InputStream in = Main.class.getResourceAsStream("/rif/stack/stack/rif-proxy-filter.jar")) {
                        if (in != null) {
                            Files.createDirectories(filter.getParent());
                            Files.copy(in, filter);
                        }
                    }
                }
                return here.toAbsolutePath();
            }
            Path home = Path.of(System.getProperty("user.home"), ".revenue-in-flight");
            Files.createDirectories(home.resolve("stack"));
            for (String name : List.of("docker-compose.yml", "stack/proxy.yaml", "stack/rif-proxy-filter.jar")) {
                try (InputStream in = Main.class.getResourceAsStream("/rif/stack/" + name)) {
                    if (in != null) {
                        Files.copy(in, home.resolve(name), StandardCopyOption.REPLACE_EXISTING);
                    }
                }
            }
            return home.resolve("docker-compose.yml");
        }

        static int run(List<String> args) throws IOException, InterruptedException {
            List<String> cmd = new ArrayList<>(List.of("docker", "compose", "-f", file().toString()));
            cmd.addAll(args);
            try {
                return new ProcessBuilder(cmd).inheritIO().start().waitFor();
            } catch (IOException e) {
                throw new Failure("docker is not available: " + e.getMessage());
            }
        }
    }
}
