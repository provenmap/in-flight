package rif.server;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.staticfiles.Location;
import io.modelcontextprotocol.server.McpStatelessSyncServer;
import rif.catalog.Json;
import rif.observer.Observer;
import rif.observer.SimulatorClient;

/**
 * Prices every event in the flow, tracks money at risk, and streams it to the console, which it also serves.
 *
 * <pre>
 *   GET  /                         the console
 *   WS   /ws                       a snapshot, then frames of event, dead, recovered and stats messages
 *   GET  /api/sim/state            the simulator's state
 *   POST /api/sim/{action}         break | restore | patch | unpatch | redrive | rate?perSecond=N, passed to the simulator
 *                                  (both 404 when the observer only watches)
 *   POST /mcp                      the MCP server, over Streamable HTTP
 * </pre>
 */
public final class Server implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Server.class);

    /** Where to listen, the built console (null to find it), and the observer's options. */
    public record Options(int port, String host, String uiDir, String mcpToken, Observer.Options observer, Changes.Options changes) {
        public static Options fromEnv() {
            String port = System.getenv("PORT");
            String host = System.getenv("HOST");
            return new Options(port == null ? 8088 : Integer.parseInt(port), host == null ? "localhost" : host, System.getenv("RIF_UI_DIR"),
                    System.getenv("RIF_MCP_TOKEN"), Observer.Options.fromEnv(),
                    new Changes.Options(rif.contracts.Contracts.Settings.fromEnv(Changes.cwd()), false));
        }

        public Options withPort(int p) {
            return new Options(p, host, uiDir, mcpToken, observer, changes);
        }

        public Options withObserver(Observer.Options o) {
            return new Options(port, host, uiDir, mcpToken, o, changes);
        }

        public Options withChanges(Changes.Options c) {
            return new Options(port, host, uiDir, mcpToken, observer, c);
        }
    }

    private final Observer observer;
    private final Hub hub;
    private final Javalin app;
    private final McpStatelessSyncServer mcp;
    private final Changes changes;
    private final String url;

    private Server(Options opts) {
        observer = new Observer(opts.observer()).start();
        hub = new Hub(observer.reader());
        observer.onFrame(hub::publish);
        SimulatorClient sim = observer.simulator();
        changes = new Changes(opts.changes(), sim);
        McpHttp mcpHttp = new McpHttp(opts.host(), opts.mcpToken());
        mcp = Mcp.stateless(observer, changes, mcpHttp);
        Api api = new Api(observer, changes);
        String ui = uiDir(opts.uiDir());

        app = Javalin.create(config -> {
            config.startup.showJavalinBanner = false;
            config.routes.ws("/ws", ws -> {
                ws.onConnect(ctx -> {
                    ctx.enableAutomaticPings(15, TimeUnit.SECONDS);
                    hub.join(ctx);
                });
                ws.onClose(hub::leave);
                ws.onError(hub::leave);
            });
            config.routes.get("/api/sim/state", ctx -> {
                if (sim == null) {
                    noSimulator(ctx);
                    return;
                }
                relay(ctx, sim.call("GET", "/state"));
            });
            config.routes.post("/api/sim/{action}", ctx -> {
                if (sim == null) {
                    noSimulator(ctx);
                    return;
                }
                String q = ctx.queryString();
                relay(ctx, sim.call("POST", "/" + URLEncoder.encode(ctx.pathParam("action"), StandardCharsets.UTF_8) + (q == null ? "" : "?" + q)));
            });
            api.register(config.routes);
            config.routes.post("/mcp", mcpHttp::post);
            config.routes.get("/mcp", mcpHttp::other);
            config.routes.delete("/mcp", mcpHttp::other);
            if (ui != null) {
                config.staticFiles.add(ui, Location.EXTERNAL);
            } else if (Server.class.getResource("/rif/ui/index.html") != null) {
                config.staticFiles.add("/rif/ui", Location.CLASSPATH);
            } else {
                config.routes.get("/", ctx -> ctx.contentType("text/plain").result("The console is not built: run npm run build.\n"));
            }
        });
        app.start(opts.host(), opts.port());
        url = "http://localhost:" + app.port();
        log.info("observer: Kafka at {}, {}, console on {}", observer.bootstrap(),
                sim != null ? "simulator at " + sim.url() : "watching " + observer.catalog().flow() + " with no simulator", url);
    }

    public static Server start(Options opts) {
        return new Server(opts);
    }

    /** {@code RIF_UI_DIR}, or {@code dist/ui} under the working directory, when it holds a built console. */
    static String uiDir(String given) {
        for (String dir : new String[] {given, "dist/ui"}) {
            if (dir != null && Files.exists(Path.of(dir, "index.html"))) {
                return Path.of(dir).toAbsolutePath().toString();
            }
        }
        return null;
    }

    private static void noSimulator(Context ctx) {
        ctx.status(404).contentType("application/json").result(Json.write(Json.object().put("error", "there is no simulator: this observer only watches")));
    }

    private static void relay(Context ctx, SimulatorClient.Reply r) {
        ctx.status(r.status()).contentType("application/json").result(r.body());
    }

    public String url() {
        return url;
    }

    public Observer observer() {
        return observer;
    }

    public Changes changes() {
        return changes;
    }

    public Hub hub() {
        return hub;
    }

    @Override
    public void close() throws InterruptedException {
        hub.close();
        app.stop();
        mcp.close();
        observer.close();
    }
}
