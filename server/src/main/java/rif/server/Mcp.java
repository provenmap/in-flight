package rif.server;

import java.text.NumberFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.json.jackson2.JacksonMcpJsonMapper;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.McpStatelessServerFeatures;
import io.modelcontextprotocol.server.McpStatelessSyncServer;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.server.transport.StdioServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpStatelessServerTransport;
import rif.catalog.Catalog;
import rif.catalog.Describe;
import rif.catalog.EventRef;
import rif.catalog.Impact;
import rif.catalog.Json;
import rif.catalog.Result;
import rif.catalog.RoutingTable;
import rif.catalog.Semantics;
import rif.observer.Messages;
import rif.observer.Observer;
import rif.observer.SimulatorClient;

/**
 * The MCP server: what the console shows and the controls it has, on the same observer and the same simulator API,
 * never by driving the console. The same tools are served over stdio and over Streamable HTTP.
 */
public final class Mcp {

    public static final String NAME = "revenue-in-flight";
    public static final String VERSION = "0.2.0";

    public static final McpJsonMapper JSON = new JacksonMcpJsonMapper(Json.MAPPER);

    /** One tool: what it is, what it takes, and what it does with its arguments. */
    public record Tool(String name, String title, String description, Map<String, Object> inputSchema, McpSchema.ToolAnnotations annotations,
                       Function<Map<String, Object>, McpSchema.CallToolResult> handler) {
        McpSchema.Tool spec() {
            return new McpSchema.Tool(name, title, description, inputSchema, null, annotations, null, null);
        }
    }

    private static final NumberFormat USD = NumberFormat.getCurrencyInstance(Locale.US);
    private static final NumberFormat COUNT = NumberFormat.getIntegerInstance(Locale.US);

    static String usd(double v) {
        synchronized (USD) {
            return USD.format(v);
        }
    }

    static String count(long n, String noun) {
        synchronized (COUNT) {
            return COUNT.format(n) + " " + noun + (n == 1 ? "" : "s");
        }
    }

    /** A tool answer: a line for a person to read, and the same facts as data. */
    static McpSchema.CallToolResult answer(String line, JsonNode data) {
        return new McpSchema.CallToolResult(List.of(new McpSchema.TextContent(null, line + "\n\n" + Json.indented(data), null)), false, Json.plain(data), null);
    }

    static McpSchema.CallToolResult failure(String text) {
        return new McpSchema.CallToolResult(List.of(new McpSchema.TextContent(null, text, null)), true, null, null);
    }

    // ---- input schemas ----------------------------------------------------------------------------------------------

    record Param(String name, String type, String description, boolean required, Double min, Double max) {}

    static Map<String, Object> schema(Param... params) {
        Map<String, Object> props = new LinkedHashMap<>();
        List<String> required = new ArrayList<>();
        for (Param p : params) {
            Map<String, Object> prop = new LinkedHashMap<>();
            prop.put("type", p.type());
            prop.put("description", p.description());
            if (p.min() != null) {
                prop.put("minimum", p.min());
            }
            if (p.max() != null) {
                prop.put("maximum", p.max());
            }
            props.put(p.name(), prop);
            if (p.required()) {
                required.add(p.name());
            }
        }
        Map<String, Object> s = new LinkedHashMap<>();
        s.put("type", "object");
        s.put("properties", props);
        if (!required.isEmpty()) {
            s.put("required", required);
        }
        s.put("additionalProperties", false);
        return s;
    }

    static McpSchema.ToolAnnotations readOnly(String title) {
        return new McpSchema.ToolAnnotations(title, true, false, true, false, null);
    }

    static McpSchema.ToolAnnotations writes(String title, boolean idempotent) {
        return new McpSchema.ToolAnnotations(title, false, false, idempotent, false, null);
    }

    /** A string argument, or null. */
    static String text(Map<String, Object> args, String key) {
        Object v = args == null ? null : args.get(key);
        return v instanceof String s ? s : null;
    }

    /** A number argument, or null when it is absent or not a number. */
    static Double number(Map<String, Object> args, String key) {
        Object v = args == null ? null : args.get(key);
        return v instanceof Number n ? n.doubleValue() : null;
    }

    // ---- the tools --------------------------------------------------------------------------------------------------

    public static String instructions(Observer observer) {
        return observer.simulator() != null
                ? "Order-to-cash, live from Kafka. value_at_risk is the headline: money held up in dead-letter queues (stuck) and in "
                        + "consumer lag. The demo: break_schema ships OrderPlaced v2, which payments cannot read, so orders pile up stuck; "
                        + "patch_payments makes payments read both shapes; redrive replays the dead letters; last_episode reports the recovery. "
                        + "impact of OrderPlaced@v2 says, before anything is produced, who that change breaks."
                : "The " + observer.catalog().flow() + " flow, live from Kafka, watched: there are no controls. value_at_risk is the headline: money "
                        + "held up in dead-letter queues (stuck) and in consumer lag. stuck_events says why each one failed; dead letters the "
                        + "catalog cannot price are counted with their value unknown. last_episode reports the latest full recovery.";
    }

    public static List<Tool> tools(Observer observer) {
        return tools(observer, null);
    }

    public static List<Tool> tools(Observer observer, Changes changes) {
        List<Tool> tools = new ArrayList<>();
        Function<Void, String> notReady = v -> observer.reader().reading() ? null : "The observer is still connecting to Kafka; its figures will start at zero.";

        tools.add(new Tool("value_at_risk", "Money at risk",
                "The headline: money, orders and customers held up right now, split into stuck (dead-lettered, not yet recovered) and consumer lag, by district and by consumer group.",
                schema(), readOnly("Money at risk"), args -> {
                    ObjectNode s = observer.reader().statsJson();
                    ObjectNode data = Json.object();
                    data.set("atRisk", s.get("atRisk"));
                    data.set("stuck", s.get("stuck"));
                    data.set("lag", s.get("lag"));
                    data.set("recovered", s.get("recovered"));
                    data.put("observing", observer.reader().reading());
                    JsonNode atRisk = s.get("atRisk");
                    long unpricedCount = atRisk.get("unpriced").asLong();
                    String unpriced = unpricedCount > 0 ? " Also " + count(unpricedCount, "dead letter") + " the catalog cannot price, value unknown." : "";
                    String line = atRisk.get("orders").asLong() > 0
                            ? usd(atRisk.get("value").asDouble()) + " at risk: " + count(atRisk.get("orders").asLong(), "order") + ", "
                                    + count(atRisk.get("customers").asLong(), "customer") + " waiting (stuck " + usd(s.get("stuck").get("value").asDouble())
                                    + ", in lag " + usd(s.get("lag").get("value").asDouble()) + ")." + unpriced
                            : !unpriced.isEmpty() ? "No priced money held up." + unpriced : "Nothing held up: $0.00 at risk.";
                    String nr = notReady.apply(null);
                    return answer(nr == null ? line : line + " " + nr, data);
                }));

        tools.add(new Tool("stuck_events", "Stuck events",
                "The dead letters still stuck, newest last, each with its value, customer, order and the error that stopped it. Filter by the district that dead-lettered it.",
                schema(new Param("district", "string", "Only dead letters from this district, such as payments", false, null, null),
                        new Param("limit", "integer", "At most this many, the newest (default 100)", false, 1.0, 3000.0)),
                readOnly("Stuck events"), args -> {
                    String district = text(args, "district");
                    Double limit = number(args, "limit");
                    if (args != null && args.containsKey("limit") && (limit == null || limit != Math.rint(limit) || limit < 1 || limit > 3000)) {
                        return failure("limit must be a whole number from 1 to 3000");
                    }
                    int n = limit == null ? 100 : limit.intValue();
                    var all = observer.reader().stuck().stream().filter(s -> district == null || district.isEmpty() || s.district().equals(district)).toList();
                    var shown = all.subList(Math.max(0, all.size() - n), all.size());
                    double total = all.stream().mapToDouble(s -> s.priced().valueOrZero().doubleValue()).sum();
                    ObjectNode data = Json.object();
                    data.putObject("total").put("events", all.size()).put("value", Math.round(total * 100) / 100.0);
                    data.put("shown", shown.size());
                    ArrayNode events = data.putArray("events");
                    shown.forEach(s -> events.add(Messages.stuck(s)));
                    String at = district == null || district.isEmpty() ? "" : " at " + district;
                    String line = all.isEmpty() ? "Nothing stuck" + at + "."
                            : count(all.size(), "event") + " stuck" + at + ", worth " + usd(total) + "; showing the newest " + shown.size() + ".";
                    return answer(line, data);
                }));

        tools.add(new Tool("last_episode", "Last recovery",
                "The latest stuck spell that was fully recovered: the money and events it held, and how long from the first event stuck to the last one recovered.",
                schema(), readOnly("Last recovery"), args -> {
                    JsonNode e = observer.reader().statsJson().get("lastEpisode");
                    if (e == null || e.isNull()) {
                        ObjectNode data = Json.object();
                        data.putNull("lastEpisode");
                        return answer("No recovery yet since the observer started.", data);
                    }
                    ObjectNode data = Json.object();
                    data.set("lastEpisode", e);
                    return answer("Recovered " + usd(e.get("value").asDouble()) + ": " + count(e.get("events").asLong(), "order") + ", stuck "
                            + String.format(Locale.US, "%.1f", e.get("tookMs").asDouble() / 1000) + " s.", data);
                }));

        Catalog catalog = observer.catalog();
        Semantics semantics = catalog.semantics();
        if (semantics != null) {
            Function<String, List<Impact.Reader>> readers = catalog.readers();
            tools.add(new Tool("intents", "Declared intents",
                    "What each consumer cares about, in the catalog's terms, such as changes(Order.total): the event versions each intent matches and why, the topics they come from, what a consumer reads from each, and who reads by it.",
                    schema(), readOnly("Declared intents"), args -> {
                        List<RoutingTable> tables = semantics.compiled();
                        String line = tables.isEmpty() ? "The catalog declares no intents."
                                : String.join("\n\n", tables.stream().map(t -> Describe.table(t, readers.apply(t.intent()))).toList());
                        ObjectNode data = Json.object();
                        ArrayNode is = data.putArray("intents");
                        tables.forEach(t -> is.add(Describe.tableJson(t, readers.apply(t.intent()))));
                        return answer(line, data);
                    }));

            tools.add(new Tool("explain_intent", "Explain an intent",
                    "Compiles a predicate against the catalog and shows what it would match, without declaring it. The language: changes(Entity.attribute), emits(EventType), about(Entity), combined with and, or, not and parentheses.",
                    schema(new Param("predicate", "string", "Such as changes(Order.total) or emits(PaymentAuthorized) or emits(StockReserved)", true, null, null),
                            new Param("by", "string", "The entity to key by, when the predicate leaves it ambiguous", false, null, null)),
                    readOnly("Explain an intent"), args -> {
                        String predicate = text(args, "predicate");
                        if (predicate == null) {
                            return failure("explain_intent needs a predicate, such as changes(Order.total)");
                        }
                        Result<RoutingTable> table = semantics.explain(predicate, text(args, "by"));
                        if (!table.isOk()) {
                            return failure(String.join("\n", table.errors()));
                        }
                        return answer(Describe.table(table.value().named("matches"), null), Describe.tableJson(table.value().named("unnamed"), null));
                    }));

            tools.add(new Tool("impact", "Impact of an event version",
                    "What one event version reaches, worked out from the catalog before any event is produced: which intents it newly matches, which consumers read by them, and who must change first because the fields they read have moved. For the demo, impact of OrderPlaced@v2 says what break_schema is about to do.",
                    schema(new Param("event", "string", "An event version in the catalog, such as OrderPlaced@v2", true, null, null)),
                    readOnly("Impact of an event version"), args -> {
                        String event = text(args, "event");
                        Result<EventRef> ref = EventRef.parse(event == null ? "" : event);
                        if (!ref.isOk()) {
                            return failure(ref.error());
                        }
                        Result<Impact> impact = Impact.of(ref.value(), semantics, readers);
                        if (!impact.isOk()) {
                            return failure(impact.error());
                        }
                        return answer(impact.value().headline() + "\n\n" + impact.value().report(), Describe.impactJson(impact.value()));
                    }));
        }

        if (semantics != null) {
            tools.add(new Tool("tail_intent", "Tail an intent",
                    "The latest events the router delivered on an intent's topic (intent.<name> to a reader through the proxy), oldest first: the entity each is keyed by, the event version, where it came from, and whether it arrived late.",
                    schema(new Param("intent", "string", "A declared intent, such as order-total-changes", true, null, null),
                            new Param("limit", "integer", "At most this many (default 10)", false, 1.0, 200.0)),
                    readOnly("Tail an intent"), args -> {
                        String intent = text(args, "intent");
                        if (intent == null) {
                            return failure("tail_intent needs an intent");
                        }
                        Double limit = number(args, "limit");
                        ArrayNode events = Tail.of(observer.bootstrap(), intent, limit == null ? 10 : limit.intValue());
                        ObjectNode data = Json.object();
                        data.put("intent", intent);
                        data.set("events", events);
                        String line = events.isEmpty() ? "Nothing on intent." + intent + " yet: is the router running, and does anything produce what it matches?"
                                : count(events.size(), "event") + " on intent." + intent + ", oldest first.";
                        return answer(line, data);
                    }));
        }
        if (changes != null) {
            tools.add(new Tool("change_status", "Change status",
                    "The proposed catalog changes, or one of them: who must change and why, each migration's branch or pull request and whether it has merged, and whether the schema gate still refuses each new event version.",
                    schema(new Param("id", "string", "A change, such as rev2; all of them when left out", false, null, null)),
                    readOnly("Change status"), args -> {
                        try {
                            String id = text(args, "id");
                            if (id == null || id.isBlank()) {
                                ArrayNode all = changes.list();
                                ObjectNode data = Json.object();
                                data.set("changes", all);
                                return answer(all.isEmpty() ? "No change has been proposed." : count(all.size(), "change") + " proposed.", data);
                            }
                            ObjectNode s = changes.status(id);
                            return answer(s.get("id").asText() + ": " + s.get("status").asText() + ". " + s.path("headline").asText(""), s);
                        } catch (rif.contracts.ContractsException e) {
                            return failure(e.getMessage());
                        }
                    }));
        }
        if (changes != null && !observer.watching()) {
            tools.add(new Tool("declare_intent", "Declare an intent",
                    "Declares an intent on the router, which materialises it as a topic of its own (intent.<name> through the proxy), keyed by entity, ordered per entity, from the events that already exist. Check it first with explain_intent.",
                    schema(new Param("name", "string", "Lowercase letters, digits and hyphens, such as payment-decisions", true, null, null),
                            new Param("predicate", "string", "Such as changes(Order.payment)", true, null, null),
                            new Param("by", "string", "The entity to key by, when the predicate leaves it ambiguous", false, null, null)),
                    writes("Declare an intent", true), args -> {
                        String name = text(args, "name");
                        String predicate = text(args, "predicate");
                        if (name == null || predicate == null) {
                            return failure("declare_intent needs a name and a predicate");
                        }
                        Changes.Router.Reply r = changes.declare(name, predicate, text(args, "by"));
                        JsonNode body = r.json();
                        if (r.status() != 200) {
                            return failure((body.has("errors") ? body.get("errors").toString() : body.path("error").asText("the router answered " + r.status())) + " (" + r.status() + ")");
                        }
                        return answer("Declared " + name + ": readers subscribe to intent." + name + " through the proxy.", body);
                    }));
            tools.add(new Tool("propose_change", "Propose a catalog change",
                    "Proposes the catalog this server reads as the router's next revision: works out the impact, checks each new version against the schema registry, and opens a migration (a branch with the regenerated binding, or a pull request) for each consumer that must change. The gate refuses the new versions until approve_change.",
                    schema(), writes("Propose a catalog change", false), args -> {
                        try {
                            ObjectNode p = changes.propose(observer.catalog());
                            return p.get("refused").asBoolean() ? failure(p.get("text").asText()) : answer(p.get("text").asText(), p);
                        } catch (rif.contracts.ContractsException e) {
                            return failure(e.getMessage());
                        }
                    }));
            tools.add(new Tool("approve_change", "Approve a catalog change",
                    "Approves a proposed change once every consumer's migration has merged: registers the new event versions, so the gate lets them through, and activates the revision on the router. Refused while any migration is open.",
                    schema(new Param("id", "string", "The change, such as rev2", true, null, null)), writes("Approve a catalog change", true), args -> {
                        String id = text(args, "id");
                        if (id == null) {
                            return failure("approve_change needs the change's id");
                        }
                        try {
                            ObjectNode a = changes.approve(id);
                            return a.get("approved").asBoolean() ? answer(a.get("text").asText(), a) : failure(a.get("text").asText());
                        } catch (rif.contracts.ContractsException e) {
                            return failure(e.getMessage());
                        }
                    }));
        }

        // The simulator's controls: only when there is a simulator to control.
        SimulatorClient sim = observer.simulator();
        if (sim != null) {
            tools.add(new Tool("simulator_state", "Simulator state",
                    "The demo system: order traffic rate, which OrderPlaced version the orders team ships (2 is the breaking one), whether payments is patched, each service's counts, and the last redrive.",
                    schema(), readOnly("Simulator state"), args -> {
                        SimulatorClient.Reply res = sim.call("GET", "/state");
                        JsonNode s = res.json();
                        if (res.status() != 200) {
                            return failure(s.has("error") ? s.get("error").asText() : "the simulator answered " + res.status());
                        }
                        int v = s.get("orderVersion").asInt();
                        String line = "OrderPlaced v" + v + (v > 1 ? " (broken for payments unless patched)" : "") + ", payments "
                                + (s.get("patched").asBoolean() ? "patched" : "strict") + ", " + Json.number(s.get("rate")) + " orders/s"
                                + (s.get("redrive").get("running").asBoolean() ? ", redrive running" : "") + ".";
                        return answer(line, s);
                    }));

            tools.add(control(sim, "break_schema", "break", "Ship OrderPlaced v2",
                    "The orders team ships OrderPlaced v2, which writes total as { amount, currency }. Payments cannot read it and dead-letters every new order; inventory keeps working.",
                    "Orders now ship OrderPlaced v2. Unpatched payments dead-letters every one."));
            tools.add(control(sim, "restore_schema", "restore", "Back to OrderPlaced v1", "The orders team goes back to OrderPlaced v1.", "Orders ship OrderPlaced v1 again."));
            tools.add(control(sim, "patch_payments", "patch", "Patch payments",
                    "Payments learns to read both OrderPlaced shapes. New orders flow again; the dead letters stay stuck until a redrive.",
                    "Payments reads v1 and v2. Dead letters already stuck need a redrive."));
            tools.add(control(sim, "unpatch_payments", "unpatch", "Unpatch payments", "Payments goes back to reading only OrderPlaced v1.", "Payments reads only v1 again."));
            tools.add(control(sim, "redrive", "redrive", "Redrive the payments DLQ",
                    "One pass over payments.dlq with the current payments code, up to its end when the pass starts. It stops a partition at the first record that still fails, without moving its offset. Fails with 409 while one is running.",
                    "Redrive started. Watch value_at_risk drain and read last_episode when it reaches $0."));

            tools.add(new Tool("rehearse", "Rehearse a build on the past",
                    "Runs the events an intent delivered over the last minutes through a build of the consumer that reads by it, beside the build running now: the candidate's output goes to a branch topic no reader sees, and the answer is the difference. For the demo: intent order-total-changes, build patched (payments' regenerated binding) or strict (OrderPlaced v1 only).",
                    schema(new Param("intent", "string", "Such as order-total-changes", true, null, null),
                            new Param("build", "string", "patched or strict", true, null, null),
                            new Param("minutes", "number", "How far back, from 1 to 1440 (default 10)", false, 1.0, 1440.0)),
                    writes("Rehearse a build on the past", false), args -> {
                        String intent = text(args, "intent");
                        String build = text(args, "build");
                        Double minutes = number(args, "minutes");
                        if (intent == null || build == null) {
                            return failure("rehearse needs an intent and a build");
                        }
                        SimulatorClient.Reply res = sim.call("POST", "/rehearse?intent=" + java.net.URLEncoder.encode(intent, java.nio.charset.StandardCharsets.UTF_8)
                                + "&build=" + java.net.URLEncoder.encode(build, java.nio.charset.StandardCharsets.UTF_8) + "&minutes=" + (minutes == null ? 10 : minutes),
                                java.time.Duration.ofSeconds(60));
                        JsonNode body = res.json();
                        if (res.status() != 200) {
                            return failure((body.has("error") ? body.get("error").asText() : "the simulator answered " + res.status()) + " (" + res.status() + ")");
                        }
                        return answer(body.get("text").asText(), body);
                    }));

            tools.add(new Tool("set_rate", "Set order traffic", "Orders placed per second, 0 to 500.",
                    schema(new Param("perSecond", "number", "Orders a second", true, 0.0, 500.0)), writes("Set order traffic", true), args -> {
                        Double r = number(args, "perSecond");
                        if (r == null || r < 0 || r > 500) {
                            return failure("perSecond must be a number from 0 to 500");
                        }
                        String q = r == Math.rint(r) ? String.valueOf(r.longValue()) : String.valueOf(r);
                        SimulatorClient.Reply res = sim.call("POST", "/rate?perSecond=" + q);
                        JsonNode body = res.json();
                        if (res.status() != 200) {
                            return failure((body.has("error") ? body.get("error").asText() : "the simulator answered " + res.status()) + " (" + res.status() + ")");
                        }
                        return answer("Order traffic set to " + q + "/s.", body);
                    }));
        }
        return tools;
    }

    private static Tool control(SimulatorClient sim, String name, String action, String title, String description, String line) {
        return new Tool(name, title, description, schema(), writes(title, !action.equals("redrive")), args -> {
            SimulatorClient.Reply res = sim.call("POST", "/" + action);
            JsonNode body = res.json();
            if (res.status() != 200) {
                return failure((body.has("error") ? body.get("error").asText() : "the simulator answered " + res.status()) + " (" + res.status() + ")");
            }
            return answer(line, body);
        });
    }

    static McpSchema.Resource flowResource(Observer observer) {
        return new McpSchema.Resource("rif://flow", "flow", "The " + observer.catalog().flow() + " flow",
                "Districts, topics, which district produces each topic and which read it, the event types on each, and the dead-letter queues.",
                "application/json", null, null, null, null);
    }

    static McpSchema.ReadResourceResult readFlow(Observer observer, String uri) {
        return new McpSchema.ReadResourceResult(List.of(new McpSchema.TextResourceContents(uri, "application/json", Json.indented(observer.flow()), null)), null);
    }

    static McpSchema.ServerCapabilities capabilities() {
        return McpSchema.ServerCapabilities.builder().tools(false).resources(false, false).build();
    }

    // ---- the two servers --------------------------------------------------------------------------------------------

    /** The server over stdio: stdout is the protocol. */
    public static McpSyncServer stdio(Observer observer, Changes changes) {
        return McpServer.sync(new StdioServerTransportProvider(JSON))
                .serverInfo(NAME, VERSION)
                .instructions(instructions(observer))
                .capabilities(capabilities())
                .jsonMapper(JSON)
                .tools(tools(observer, changes).stream().map(t -> new McpServerFeatures.SyncToolSpecification(t.spec(), (ex, req) -> t.handler().apply(req.arguments()))).toList())
                .resources(new McpServerFeatures.SyncResourceSpecification(flowResource(observer), (ex, req) -> readFlow(observer, req.uri())))
                .requestTimeout(Duration.ofSeconds(30))
                .build();
    }

    /** The stateless server behind Streamable HTTP. */
    public static McpStatelessSyncServer stateless(Observer observer, Changes changes, McpStatelessServerTransport transport) {
        return McpServer.sync(transport)
                .serverInfo(NAME, VERSION)
                .instructions(instructions(observer))
                .capabilities(capabilities())
                .jsonMapper(JSON)
                .tools(tools(observer, changes).stream().map(t -> new McpStatelessServerFeatures.SyncToolSpecification(t.spec(), (ctx, req) -> t.handler().apply(req.arguments()))).toList())
                .resources(new McpStatelessServerFeatures.SyncResourceSpecification(flowResource(observer), (ctx, req) -> readFlow(observer, req.uri())))
                .build();
    }
}
