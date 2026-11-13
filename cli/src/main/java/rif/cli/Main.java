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
import rif.observer.Observer;
import rif.router.Router;
import rif.server.Mcp;
import rif.server.Server;
import rif.simulator.Simulator;

/** revenue-in-flight: up · simulate · serve · watch · router · mcp · intents · explain · impact · contracts · down */
public final class Main {

    static final String USAGE = """
            usage: revenue-in-flight <command>

              up [kafka] start Kafka, the schema registry and the proxy (Docker Compose) and wait until they are healthy;
                         with kafka, only Kafka, which simulate and serve need
              demo       the whole demo in one process, on the stack up starts: the router, the simulator reading by
                         intent through the proxy, and the console with contracts in the loop, on :8088
              simulate   run order traffic and the payments, inventory and shipping services, control API on :8090
              serve      run the observer and the console on :8088, with MCP over HTTP on /mcp
              watch [catalog.json]
                         serve with no simulator, for a flow of your own on your own Kafka: the figures, no controls
              router     run the intent router: one Kafka Streams application per intent, control API on :8085
              mcp        run the MCP server over stdio, with an observer of its own
              intents [catalog.json]
                         the catalog's declared intents: what each matches, why, and who reads by it
              explain "<predicate>" [catalog.json]
                         compile a predicate, such as "changes(Order.total)", and show what it would match
              impact <Type@vN> [catalog.json]
                         what one event version reaches: which intents, which consumers, and who must change before it
                         ships. Exits 1 when someone must. Needs no Kafka.
              rehearse <intent> <build> [minutes]
                         run the intent's events of the last minutes (10) through a build of its consumer (payments:
                         strict or patched) on a branch topic, beside the build running now, and show the difference
              contracts propose <catalog.json> [--push] | approve <id> | status [<id>] | generate | register
                         contracts ahead of breakage: impact, generated bindings, pull requests, the gate
              down       stop the Compose stack and delete its data

            The demo is two terminals: up, then demo, and open http://localhost:8088. Without the proxy and the registry,
            simulate and serve in two terminals run the incident on Kafka alone.

            environment: BOOTSTRAP_SERVERS (localhost:9092), PORT (8088), CONTROL_PORT (8090), ROUTER_PORT (8085),
                         SIMULATOR_URL (http://localhost:8090, or none to watch), HOST (localhost), RIF_MCP_TOKEN,
                         RIF_CATALOG (the flow), ROUTER_URL, APICURIO_URL,
                         RIF_KAFKA_* (Java client settings, RIF_KAFKA_SECURITY_PROTOCOL=SASL_SSL)""";

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
        String command = argv.length == 0 ? "help" : argv[0];
        String[] rest = Arrays.copyOfRange(argv, Math.min(1, argv.length), argv.length);
        switch (command) {
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
                Catalog catalog = loadCatalog(rest.length > 0 ? rest[0] : null);
                Server.Options opts = Server.Options.fromEnv();
                Server server = Server.start(opts.withObserver(opts.observer().withCatalog(catalog).withSimulator(null)));
                return untilSignal(server);
            }
            case "router" -> {
                Router router = Router.start(Router.Config.fromEnv(System.getenv()));
                return untilSignal(router);
            }
            case "mcp" -> {
                Observer observer = new Observer(Observer.Options.fromEnv()).start();
                var changes = new rif.server.Changes(new rif.server.Changes.Options(rif.contracts.Contracts.Settings.fromEnv(Path.of("").toAbsolutePath()), false),
                        observer.simulator());
                var mcp = Mcp.stdio(observer, changes);
                return untilSignal(() -> {
                    mcp.closeGracefully();
                    observer.close();
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
