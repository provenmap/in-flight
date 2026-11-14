package inflight.cli;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import inflight.catalog.Catalog;
import inflight.catalog.EventRef;
import inflight.catalog.Json;
import inflight.contracts.BindingGenerator;
import inflight.contracts.Contracts;
import inflight.router.Router;
import inflight.server.Changes;
import inflight.server.Server;
import inflight.simulator.Simulator;

/**
 * The whole demo in one process, on the stack {@code up} starts: the router, the simulator reading by intent through the
 * proxy, and the console with contracts in the loop. One schema change, OrderPlaced v2, can then ship two ways:
 *
 * <ol>
 * <li>Around the catalog: the orders team writes v2 straight to Kafka. Payments dead-letters every order, money at
 * risk climbs, payments is patched, the queue is redriven.</li>
 * <li>Through the catalog: the change is proposed. Payments must change, inventory need not; payments' regenerated
 * binding is a migration. The gate refuses v2 until it merges and the change is approved; then v2 ships and nothing
 * gets stuck.</li>
 * </ol>
 *
 * The router starts at revision 1, the flow before v2; the shipped catalog, revision 2, is the proposal. The consumers'
 * repository is a scratch one under {@code .in-flight/demo/repo}, where merging a migration is a button in the console.
 */
public final class Demo implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Demo.class);

    static final EventRef CHANGE = new EventRef("OrderPlaced", 2);

    /** Where everything is. */
    public record Options(Path dir, String bootstrap, String proxy, String registry, int routerPort, int simulatorPort, int serverPort, String host) {
        public static Options fromEnv() {
            Map<String, String> env = inflight.catalog.Env.all();
            return new Options(Path.of(".in-flight/demo"), env.getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092"), env.getOrDefault("INFLIGHT_PROXY", "localhost:9192"),
                    env.getOrDefault("APICURIO_URL", "http://localhost:8080/apis/registry/v3"),
                    Integer.parseInt(env.getOrDefault("ROUTER_PORT", "8085")), Integer.parseInt(env.getOrDefault("CONTROL_PORT", "8090")),
                    Integer.parseInt(env.getOrDefault("PORT", "8088")), env.getOrDefault("HOST", "localhost"));
        }
    }

    private final Router router;
    private final Simulator simulator;
    private final Server server;

    private Demo(Router router, Simulator simulator, Server server) {
        this.router = router;
        this.simulator = simulator;
        this.server = server;
    }

    public Server server() {
        return server;
    }

    public Simulator simulator() {
        return simulator;
    }

    public Router router() {
        return router;
    }

    /** The flow before the change: the shipped catalog without OrderPlaced v2, at revision 1. */
    static Catalog baseline(Catalog shipped) {
        return shipped.withoutEvent(CHANGE).value().withRevision(1);
    }

    public static Demo start(Options o) throws IOException, InterruptedException {
        Catalog shipped = Catalog.defaultCatalog();
        Catalog before = baseline(shipped);
        Path dir = o.dir().toAbsolutePath();
        Files.createDirectories(dir);
        Path rev1 = dir.resolve("order-to-cash.rev1.json");
        Files.writeString(rev1, Json.pretty(before.toJson()));

        Path repo = dir.resolve("repo");
        Contracts.Settings settings = new Contracts.Settings(repo, "http://localhost:" + o.routerPort(), o.registry(), repo.resolve("contracts/consumers"));
        if (!Files.exists(repo.resolve(".git"))) {
            consumersRepo(repo, before);
        }

        Map<String, String> env = new HashMap<>();
        env.put("BOOTSTRAP_SERVERS", o.bootstrap());
        env.put("ROUTER_PORT", String.valueOf(o.routerPort()));
        env.put("STATE_DIR", dir.resolve("router-state").toString());
        Router router = Router.start(Router.Config.fromEnv(env).withSeedCatalog(rev1));
        Catalog active = router.service().catalog().orElse(before);
        // What the router routes by is what the gate lets through: register it, so the orders team can ship v1.
        new Contracts(settings).register(active);
        log.info("demo: the router routes by catalog revision {}; the change to propose is revision {}", active.revision(), shipped.revision());

        Simulator sim = Simulator.start(Simulator.Options.fromEnv().withBootstrap(o.bootstrap()).withPort(o.simulatorPort()).withIntentsVia(o.proxy()));
        Server.Options so = Server.Options.fromEnv();
        Server server = Server.start(new Server.Options(o.serverPort(), o.host(), so.uiDir(), so.mcpToken(),
                so.observer().withBootstrap(o.bootstrap()).withSimulator(sim.url()), new Changes.Options(settings, true)));
        log.info("demo: open {}", server.url());
        return new Demo(router, sim, server);
    }

    /** The consumers' repository: payments' binding as it is at revision 1, committed, with the manifest naming it. */
    static void consumersRepo(Path repo, Catalog before) throws IOException, InterruptedException {
        Files.createDirectories(repo.resolve("contracts/consumers"));
        String manifest = """
                {
                  "name": "payments-service",
                  "intent": "order-total-changes",
                  "binding": {
                    "path": "simulator/src/main/java/inflight/simulator/generated/OrderTotalChanges.java",
                    "package": "inflight.simulator.generated",
                    "class": "OrderTotalChanges"
                  }
                }
                """;
        Files.writeString(repo.resolve("contracts/consumers/payments-service.json"), manifest);
        Path binding = repo.resolve("simulator/src/main/java/inflight/simulator/generated/OrderTotalChanges.java");
        Files.createDirectories(binding.getParent());
        Files.writeString(binding, BindingGenerator.generate(before, "order-total-changes",
                new BindingGenerator.Target("inflight.simulator.generated", "OrderTotalChanges")).value());
        Files.writeString(repo.resolve(".gitignore"), ".in-flight/\n");
        git(repo, "init", "-q", "-b", "main");
        git(repo, "add", "-A");
        git(repo, "commit", "-q", "-m", "payments-service at catalog revision 1");
    }

    static void git(Path repo, String... args) throws IOException, InterruptedException {
        List<String> cmd = new ArrayList<>(List.of("git", "-C", repo.toString(), "-c", "user.name=in-flight-demo", "-c", "user.email=in-flight-demo@users.noreply.github.com"));
        cmd.addAll(List.of(args));
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (p.waitFor() != 0) {
            throw new IOException("git " + String.join(" ", args) + ": " + out.trim());
        }
    }

    @Override
    public void close() throws Exception {
        server.close();
        simulator.close();
        router.close();
    }
}
