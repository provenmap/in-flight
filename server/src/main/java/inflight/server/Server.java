package inflight.server;

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
import inflight.catalog.Json;
import inflight.observer.Observer;
import inflight.observer.SimulatorClient;

/**
 * Prices every event in the flow, tracks money at risk, and streams it to the console, which it also serves. The flow
 * comes from the engine: the workspace's active one, or the sample data under {@code try}, or one catalog file under
 * {@code watch}.
 *
 * <pre>
 *   GET  /                         the console
 *   WS   /ws                       a snapshot, then frames of event, dead, recovered and stats messages
 *   GET  /api/status               what is connected, which flow is active, what is missing, what to do next
 *   ...  /api/...                  setup, recovery and the views beyond the flow: see {@link Api}
 *   GET  /api/sim/state            the simulator's state            (try only)
 *   POST /api/sim/{action}         passed to the simulator          (try only)
 *   POST /mcp                      the MCP server, over Streamable HTTP
 * </pre>
 *
 * Bound beyond localhost, the console and its API need a login token ({@code INFLIGHT_TOKEN}, or one made up and printed at
 * start), as MCP over HTTP needs {@code INFLIGHT_MCP_TOKEN}.
 */
public final class Server implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Server.class);

    /**
     * Where to listen, the built console (null to find it), the observer's options for a fixed engine, contracts, and how
     * the engine is made: from the workspace ({@link Engine.Mode#APP}), or fixed on the observer's options (null mode:
     * {@code TRY} with a simulator, else {@code WATCH}).
     */
    public record Options(int port, String host, String uiDir, String mcpToken, Observer.Options observer, Changes.Options changes, Engine.Mode mode,
                          Workspace workspace, String token) {

        public Options(int port, String host, String uiDir, String mcpToken, Observer.Options observer, Changes.Options changes) {
            this(port, host, uiDir, mcpToken, observer, changes, null, null, inflight.catalog.Env.get("INFLIGHT_TOKEN"));
        }

        public static Options fromEnv() {
            String port = System.getenv("PORT");
            String host = System.getenv("HOST");
            return new Options(port == null ? 8088 : Integer.parseInt(port), host == null ? "localhost" : host, inflight.catalog.Env.get("INFLIGHT_UI_DIR"),
                    inflight.catalog.Env.get("INFLIGHT_MCP_TOKEN"), Observer.Options.fromEnv(),
                    new Changes.Options(inflight.contracts.Contracts.Settings.fromEnv(Changes.cwd()), false));
        }

        public Options withPort(int p) {
            return new Options(p, host, uiDir, mcpToken, observer, changes, mode, workspace, token);
        }

        public Options withObserver(Observer.Options o) {
            return new Options(port, host, uiDir, mcpToken, o, changes, mode, workspace, token);
        }

        public Options withChanges(Changes.Options c) {
            return new Options(port, host, uiDir, mcpToken, observer, c, mode, workspace, token);
        }

        /** From the workspace: starts on its active flow, or on Connect. */
        public Options app(Workspace ws) {
            return new Options(port, host, uiDir, mcpToken, observer, changes, Engine.Mode.APP, ws, token);
        }

        public Options withToken(String t) {
            return new Options(port, host, uiDir, mcpToken, observer, changes, mode, workspace, t);
        }

        Engine.Mode effectiveMode() {
            return mode != null ? mode : observer.simulatorUrl() != null ? Engine.Mode.TRY : Engine.Mode.WATCH;
        }
    }

    private final Engine engine;
    private final Hub hub;
    private final Javalin app;
    private final McpStatelessSyncServer mcp;
    private final Changes changes;
    private final Setup setup;
    private final Alerts alerts;
    private final String url;
    private final ConsoleAuth auth;

    private Server(Options opts) {
        this(opts, null);
    }

    private Server(Options opts, Engine given) {
        Engine.Mode mode = opts.effectiveMode();
        Workspace ws = opts.workspace() != null ? opts.workspace() : Workspace.fromEnv();
        engine = given != null ? given : mode == Engine.Mode.APP ? Engine.app(ws) : Engine.fixed(mode, opts.observer(), ws);
        hub = new Hub(null);
        engine.onSwitch(o -> {
            if (o != null) {
                o.onFrame(hub::publish);
            }
            hub.rebind(o == null ? null : o.reader());
        });
        if (engine.observer() == null) {
            engine.start();
        } else {
            engine.observer().onFrame(hub::publish);
            hub.rebind(engine.observer().reader());
        }
        SimulatorClient sim = mode == Engine.Mode.TRY && engine.observer() != null ? engine.observer().simulator() : null;
        changes = new Changes(opts.changes(), sim);
        setup = new Setup(engine);
        alerts = new Alerts(engine);
        McpHttp mcpHttp = new McpHttp(opts.host(), opts.mcpToken());
        mcp = Mcp.stateless(engine, setup, changes, mcpHttp);
        Api api = new Api(engine, setup, changes);
        auth = new ConsoleAuth(opts.host(), opts.token());
        String ui = uiDir(opts.uiDir());

        app = Javalin.create(config -> {
            config.startup.showJavalinBanner = false;
            config.routes.before(auth::guard);
            config.routes.wsBeforeUpgrade(auth::guard);
            config.routes.ws("/ws", ws2 -> {
                ws2.onConnect(ctx -> {
                    ctx.enableAutomaticPings(15, TimeUnit.SECONDS);
                    hub.join(ctx);
                });
                ws2.onClose(hub::leave);
                ws2.onError(hub::leave);
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
            } else if (Server.class.getResource("/in-flight/ui/index.html") != null) {
                config.staticFiles.add("/in-flight/ui", Location.CLASSPATH);
            } else {
                config.routes.get("/", ctx -> ctx.contentType("text/plain").result("The console is not built: run npm run build.\n"));
            }
        });
        app.start(opts.host(), opts.port());
        url = "http://" + (McpHttp.isLoopback(opts.host()) ? "localhost" : opts.host()) + ":" + app.port();
        alerts.start();
        Observer o = engine.observer();
        String what = switch (mode) {
            case TRY -> "sample data, simulator at " + (sim == null ? "?" : sim.url());
            case WATCH -> "watching " + (o == null ? "?" : o.catalog().flow()) + " with no simulator";
            case APP -> o == null ? "nothing configured yet: the console opens on Connect" : "watching " + engine.running().flow() + " on " + o.bootstrap();
        };
        log.info("in-flight: {}; console on {}{}", what, url, auth.required() ? "/?token=" + auth.token() : "");
    }

    public static Server start(Options opts) {
        return new Server(opts);
    }

    /** {@code INFLIGHT_UI_DIR}, or {@code dist/ui} under the working directory, when it holds a built console. */
    static String uiDir(String given) {
        for (String dir : new String[] {given, "dist/ui"}) {
            if (dir != null && Files.exists(Path.of(dir, "index.html"))) {
                return Path.of(dir).toAbsolutePath().toString();
            }
        }
        return null;
    }

    private static void noSimulator(Context ctx) {
        ctx.status(404).contentType("application/json").result(Json.write(Json.object().put("error", "there is no simulator: it runs only under try, with sample data")));
    }

    private static void relay(Context ctx, SimulatorClient.Reply r) {
        ctx.status(r.status()).contentType("application/json").result(r.body());
    }

    /** The console's address, with its login token when it needs one. */
    public String url() {
        return url;
    }

    public String loginUrl() {
        return auth.required() ? url + "/?token=" + auth.token() : url;
    }

    /** The observer now: null when nothing is configured. */
    public Observer observer() {
        return engine.observer();
    }

    public Engine engine() {
        return engine;
    }

    public Setup setup() {
        return setup;
    }

    public Changes changes() {
        return changes;
    }

    public Hub hub() {
        return hub;
    }

    @Override
    public void close() throws InterruptedException {
        alerts.close();
        hub.close();
        app.stop();
        mcp.close();
        engine.close();
    }
}
