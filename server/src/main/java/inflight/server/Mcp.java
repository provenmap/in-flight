package inflight.server;

import java.text.NumberFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiFunction;
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
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.server.transport.StdioServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpStatelessServerTransport;
import inflight.catalog.Catalog;
import inflight.catalog.Describe;
import inflight.catalog.EventRef;
import inflight.catalog.Impact;
import inflight.catalog.Json;
import inflight.catalog.Result;
import inflight.catalog.RoutingTable;
import inflight.catalog.Semantics;
import inflight.kafka.Profile;
import inflight.kafka.Secrets;
import inflight.observer.Messages;
import inflight.observer.Observer;
import inflight.observer.SimulatorClient;
import inflight.recovery.Redrive;

/**
 * The MCP server: the easiest way to set the tool up, and everything the console shows, on the same engine and the same
 * actions, never by driving the console. An agent can take someone from nothing to a watched flow in one conversation:
 * status, connect, discover, sample, draft, validate, activate. The same tools are served over stdio and over
 * Streamable HTTP; each reads the engine's observer when it is called, so they follow it as it is reconfigured.
 *
 * <p>Secrets never go through a tool call: {@code connect} takes references to them. Reading is the default; tools that
 * change config say so, and the one that writes to the cluster, {@code redrive}, is destructive, opt-in per profile, and
 * dry-runs unless told otherwise.
 */
public final class Mcp {

    public static final String NAME = "in-flight";
    public static final String VERSION = "0.3.0";

    public static final McpJsonMapper JSON = new JacksonMcpJsonMapper(Json.MAPPER);

    /**
     * Asks the person something, through the client, when the client can: a question and its answers, the answer chosen
     * or null. Null itself when the client cannot ask (or over stateless HTTP).
     */
    public interface Asker {
        String ask(String question, List<String> answers);
    }

    /** One tool: what it is, what it takes, and what it does with its arguments (and an asker, which may be null). */
    public record Tool(String name, String title, String description, Map<String, Object> inputSchema, McpSchema.ToolAnnotations annotations,
                       BiFunction<Map<String, Object>, Asker, McpSchema.CallToolResult> asking) {

        public Tool(String name, String title, String description, Map<String, Object> inputSchema, McpSchema.ToolAnnotations annotations,
                    Function<Map<String, Object>, McpSchema.CallToolResult> handler) {
            this(name, title, description, inputSchema, annotations, (args, asker) -> handler.apply(args));
        }

        public Function<Map<String, Object>, McpSchema.CallToolResult> handler() {
            return args -> asking.apply(args, null);
        }

        McpSchema.Tool spec() {
            return new McpSchema.Tool(name, title, description, inputSchema, null, annotations, null, null);
        }
    }

    private static final NumberFormat COUNT = NumberFormat.getIntegerInstance(Locale.US);

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

    static McpSchema.CallToolResult of(Setup.Answer a) {
        return a.ok() ? answer(a.text(), a.data()) : failure(a.text());
    }

    // ---- input schemas ----------------------------------------------------------------------------------------------

    record Param(String name, String type, String description, boolean required, Double min, Double max, Map<String, Object> spec) {
        Param(String name, String type, String description, boolean required, Double min, Double max) {
            this(name, type, description, required, min, max, null);
        }
    }

    static Param object(String name, String description, boolean required, Map<String, Object> spec) {
        return new Param(name, "object", description, required, null, null, spec);
    }

    static Map<String, Object> schema(Param... params) {
        Map<String, Object> props = new LinkedHashMap<>();
        List<String> required = new ArrayList<>();
        for (Param p : params) {
            Map<String, Object> prop = new LinkedHashMap<>();
            if (p.spec() != null) {
                prop.putAll(p.spec());
            } else {
                prop.put("type", p.type());
            }
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

    static Map<String, Object> stringMap() {
        return Map.of("type", "object", "additionalProperties", Map.of("type", "string"));
    }

    static McpSchema.ToolAnnotations readOnly(String title) {
        return new McpSchema.ToolAnnotations(title, true, false, true, false, null);
    }

    /** Changes this tool's own config, nothing on the cluster. */
    static McpSchema.ToolAnnotations writes(String title, boolean idempotent) {
        return new McpSchema.ToolAnnotations(title, false, false, idempotent, false, null);
    }

    /** Writes to the cluster: destructive, and only where the profile allows it. */
    static McpSchema.ToolAnnotations destructive(String title) {
        return new McpSchema.ToolAnnotations(title, false, true, false, true, null);
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

    static boolean flag(Map<String, Object> args, String key, boolean otherwise) {
        Object v = args == null ? null : args.get(key);
        return v instanceof Boolean b ? b : otherwise;
    }

    static JsonNode node(Map<String, Object> args, String key) {
        Object v = args == null ? null : args.get(key);
        return v == null ? null : Json.tree(v);
    }

    // ---- the tools --------------------------------------------------------------------------------------------------

    public static String instructions(Engine engine) {
        return switch (engine.mode()) {
            case TRY -> "Sample data: an order-to-cash flow simulated on a local Kafka, labelled as such. value_at_risk is the headline: money held up "
                    + "in dead-letter queues (stuck) and in consumer lag. break_schema ships OrderPlaced v2, which payments cannot read, so orders "
                    + "pile up stuck; patch_payments makes payments read both shapes; redrive replays the dead letters; last_episode reports the "
                    + "recovery. impact of OrderPlaced@v2 says, before anything is produced, who that change breaks.";
            case WATCH -> "One flow, live from Kafka, watched from a catalog file: there are no controls. value_at_risk is the headline: money held up "
                    + "in dead-letter queues (stuck) and in consumer lag. stuck_events says why each one failed, grouped by cause.";
            case APP -> "The money moving through your Kafka. To set it up, follow the set-up-a-flow prompt: status, then connect (secrets by "
                    + "reference only: env:NAME, file:/path or keychain:service, never the secret itself), discover, sample_topic, draft_catalog, "
                    + "validate_catalog, edit_catalog to answer its questions, and activate. Once a flow is active, value_at_risk is the headline, "
                    + "stuck_events says what is stuck and why, and redrive replays a dead-letter queue (a dry run first; a real run only where the "
                    + "profile allows writes).";
        };
    }

    /** Tools on an observer someone else started: for tests and the old stdio entry. */
    public static List<Tool> tools(Observer observer) {
        return tools(observer, null);
    }

    public static List<Tool> tools(Observer observer, Changes changes) {
        Engine e = Engine.around(observer);
        return tools(e, new Setup(e), changes);
    }

    public static List<Tool> tools(Engine engine, Setup setup, Changes changes) {
        List<Tool> tools = new ArrayList<>();
        setupTools(engine, setup, tools);
        watchTools(engine, setup, tools);
        meaningTools(engine, changes, tools);
        if (changes != null && engine.mode() != Engine.Mode.WATCH) {
            tryChangeTools(engine, changes, tools);
        }
        if (engine.mode() == Engine.Mode.TRY) {
            Observer o = engine.observer();
            SimulatorClient sim = o == null ? null : o.simulator();
            if (sim != null) {
                simulatorTools(sim, tools);
            }
        }
        return tools;
    }

    /** The observer, or a failure saying how to get one. */
    private static Object watched(Engine engine) {
        Observer o = engine.observer();
        if (o == null) {
            return failure("Nothing is being watched yet. " + engine.status().get("next").asText());
        }
        return o;
    }

    private static void setupTools(Engine engine, Setup setup, List<Tool> tools) {
        tools.add(new Tool("status", "Status",
                "Where things stand: what is connected, which flow is active, whether it is being read, which of its topics do not exist yet, the saved profiles, flows and drafts, and what to do next. Start here.",
                schema(), readOnly("Status"), args -> {
                    ObjectNode s = engine.status();
                    String line = switch (s.get("stage").asText()) {
                        case "connect" -> "Not connected to a cluster. " + s.get("next").asText();
                        case "discover" -> "Connected as " + s.path("profile").path("name").asText() + " to " + s.path("profile").path("bootstrap").asText()
                                + ", no flow active. " + s.get("next").asText();
                        default -> "Watching " + s.path("flow").asText() + (s.has("bootstrap") ? " on " + s.get("bootstrap").asText() : "")
                                + (s.get("observing").asBoolean() ? "" : " (connecting)") + "."
                                + (s.get("missingTopics").isEmpty() ? "" : " Waiting for topics: " + String.join(", ", Api.list(s.get("missingTopics"))) + ".")
                                + (s.has("problem") ? " Problem: " + s.get("problem").asText() : "");
                    };
                    return answer(line, s);
                }));
        if (engine.mode() != Engine.Mode.APP) {
            return; // the sample data and watch of one file are fixed: nothing to set up
        }

        Map<String, Object> registry = Map.of("type", "object", "properties", Map.of(
                "url", Map.of("type", "string", "description", "Such as http://registry:8081, or Apicurio's http://registry:8080/apis/ccompat/v7"),
                "username", Map.of("type", "string", "description", "Plain, or a reference"),
                "password", Map.of("type", "string", "description", "A reference: env:NAME, file:/path or keychain:service[/account]")),
                "required", List.of("url"), "additionalProperties", false);
        tools.add(new Tool("connect", "Connect to a cluster",
                "Tests a connection to a Kafka cluster (and its schema registry), and saves it as a profile, by default the one in use. Secrets are "
                        + "references, never values: env:NAME, file:/path or keychain:service[/account]; a password typed into a chat would stay in its "
                        + "transcript. Auth presets: " + String.join("; ", Profile.PRESETS.stream().map(p -> p + " (" + Profile.PRESET_HELP.get(p) + ")").toList())
                        + ". Set test to only test it.",
                schema(new Param("name", "string", "The profile's name, such as staging", true, null, null),
                        new Param("bootstrap", "string", "Bootstrap servers, such as kafka-1:9092,kafka-2:9092", true, null, null),
                        new Param("auth", "string", "One of " + String.join(", ", Profile.PRESETS) + " (default none)", false, null, null),
                        object("secrets", "By role: username (plain or a reference), password, keystorePassword, keyPassword, truststorePassword "
                                + "(references only)", false, stringMap()),
                        object("settings", "Further Java client settings that are not secret, such as ssl.truststore.location", false, stringMap()),
                        object("registry", "The schema registry, for Avro and Protobuf", false, registry),
                        new Param("allowWrites", "boolean", "Whether redrive may write to this cluster (default false)", false, null, null),
                        new Param("use", "boolean", "Make it the profile in use (default true)", false, null, null),
                        new Param("test", "boolean", "Only test it; save nothing (default false)", false, null, null)),
                writes("Connect to a cluster", true), args -> {
                    ObjectNode p = Json.object();
                    p.put("name", text(args, "name")).put("bootstrap", text(args, "bootstrap")).put("auth", text(args, "auth") == null ? "none" : text(args, "auth"));
                    for (String k : List.of("secrets", "settings", "registry")) {
                        JsonNode v = node(args, k);
                        if (v != null) {
                            p.set(k, v);
                        }
                    }
                    p.put("allowWrites", flag(args, "allowWrites", false));
                    Profile profile = Workspace.profile(p);
                    // A secret given as a value is refused before anything is tried, and not echoed back.
                    for (Map.Entry<String, String> s : profile.secrets().entrySet()) {
                        if (!s.getKey().equals("username") && !Secrets.isReference(s.getValue())) {
                            return failure("secrets." + s.getKey() + " looks like a secret itself. Give a reference instead (" + Secrets.FORMS
                                    + "), and keep the secret out of this conversation: set it in the environment, a file or the keychain.");
                        }
                    }
                    if (profile.registry() != null && profile.registry().password() != null && !Secrets.isReference(profile.registry().password())) {
                        return failure("registry.password looks like a secret itself. Give a reference instead (" + Secrets.FORMS + ").");
                    }
                    try {
                        Engine.Tested t = flag(args, "test", false) ? Engine.test(profile) : engine.connect(profile, flag(args, "use", true));
                        return t.ok() ? answer(t.text(), t.data()) : failure(t.text());
                    } catch (IllegalStateException e) {
                        return failure(e.getMessage());
                    }
                }));

        tools.add(new Tool("discover", "Discover the cluster",
                "What the cluster in use shows: its topics and how many records each holds, which look like a flow, a dead-letter queue (and why: "
                        + "its name, or Kafka Connect's, Spring's or this tool's headers on its records) or internal; its consumer groups and what each "
                        + "reads; which group seems to write each dead-letter queue; and the registry's subjects. Reads only.",
                schema(), readOnly("Discover the cluster"), args -> of(setup.discover())));

        tools.add(new Tool("sample_topic", "Sample a topic",
                "The field shapes of a topic's latest records: each field's path, its types, how often it is present, and an example, decoded "
                        + "through the registry when the records are Avro. Example values are masked (digits as 9, letters as x) unless reveal is set. "
                        + "Reads without a consumer group, so nobody's lag changes.",
                schema(new Param("topic", "string", "The topic", true, null, null),
                        new Param("n", "integer", "How many records, from 1 to 1000 (default 50)", false, 1.0, 1000.0),
                        new Param("reveal", "boolean", "Show example values as they are (default false)", false, null, null)),
                readOnly("Sample a topic"), args -> {
                    Double n = number(args, "n");
                    return of(setup.sample(text(args, "topic"), n == null ? 50 : n.intValue(), flag(args, "reveal", false)));
                }));

        tools.add(new Tool("draft_catalog", "Draft a catalog",
                "Drafts a catalog for a flow from what the cluster shows: its events and topics, which field is each event's value (and whether in "
                        + "minor units), its order and its customer, the teams, and which group reads what and dead-letters where. Every guess comes with "
                        + "a confidence and a question; where the client can ask, the least sure are asked directly, else they come back for you to ask. "
                        + "Saved as the flow's draft. Answer with edit_catalog, check with validate_catalog, then activate.",
                schema(new Param("flow", "string", "The flow's name: lowercase letters, digits and hyphens, such as checkout", true, null, null),
                        new Param("topics", "array", "Only these topics (default: every topic that looks like a flow and has records)", false, null, null,
                                Map.of("type", "array", "items", Map.of("type", "string")))),
                writes("Draft a catalog", true), (args, asker) -> {
                    String flow = text(args, "flow");
                    List<String> topics = new ArrayList<>();
                    JsonNode t = node(args, "topics");
                    if (t != null) {
                        t.forEach(x -> topics.add(x.asText()));
                    }
                    Setup.Answer a = setup.draft(flow, topics);
                    if (!a.ok() || asker == null) {
                        return of(a);
                    }
                    return of(askOpenQuestions(setup, flow, a, asker));
                }));

        tools.add(new Tool("validate_catalog", "Validate a catalog",
                "Checks a catalog, then dry-runs it on records sampled now: for each event, how many of its topic's records it prices, what they "
                        + "would be worth, and the reasons the rest are not. Give a catalog, or a flow to check its draft (or its catalog when it has no draft).",
                schema(new Param("flow", "string", "A flow: its draft, else its catalog", false, null, null),
                        object("catalog", "A catalog as JSON, instead of a flow's", false, Map.of("type", "object"))),
                readOnly("Validate a catalog"), args -> of(setup.validate(text(args, "flow"), node(args, "catalog")))));

        tools.add(new Tool("edit_catalog", "Edit a catalog",
                "Applies a JSON Patch (RFC 6902) to a flow's draft, or to the active catalog with target active, and checks the result; or answers "
                        + "one of the draft's questions with one of its answers (guess and option, as draft_catalog listed them). An active catalog "
                        + "edited is switched to at once.",
                schema(new Param("flow", "string", "The flow", true, null, null),
                        new Param("target", "string", "draft (default) or active", false, null, null),
                        new Param("patch", "array", "JSON Patch operations, such as [{\"op\":\"replace\",\"path\":\"/events/0/value\",\"value\":\"total_cents\"}]",
                                false, null, null, Map.of("type", "array", "items", Map.of("type", "object"))),
                        new Param("guess", "string", "A question's id, from the draft", false, null, null),
                        new Param("option", "string", "One of that question's answers, by its label", false, null, null)),
                writes("Edit a catalog", false), args -> of(setup.edit(text(args, "flow"), text(args, "target"), node(args, "patch"), text(args, "guess"),
                        text(args, "option")))));

        tools.add(new Tool("activate", "Activate a flow",
                "Saves a catalog as a flow (a plain file under the workspace's flows, to keep in a repository) and switches the running observer "
                        + "to it, no restart: the console and these tools show it at once. Without a catalog, activates the flow's draft.",
                schema(new Param("flow", "string", "The flow", true, null, null),
                        object("catalog", "The catalog, instead of the draft", false, Map.of("type", "object"))),
                writes("Activate a flow", true), args -> of(setup.activate(text(args, "flow"), node(args, "catalog")))));
    }

    /** Asks the draft's least sure questions through the client, and applies the answers. */
    private static Setup.Answer askOpenQuestions(Setup setup, String flow, Setup.Answer drafted, Asker asker) {
        int asked = 0;
        int answered = 0;
        for (JsonNode g : drafted.data().path("guesses")) {
            if (asked >= 5 || g.path("confidence").asDouble() >= 0.9 || g.path("options").size() < 2) {
                continue;
            }
            List<String> labels = new ArrayList<>();
            g.path("options").forEach(o -> labels.add(o.path("label").asText()));
            asked++;
            String chosen = asker.ask(g.path("question").asText(), labels);
            if (chosen == null) {
                break;
            }
            Setup.Answer e = setup.edit(flow, "draft", null, g.path("id").asText(), chosen);
            if (e.ok()) {
                answered++;
            }
        }
        if (asked == 0) {
            return drafted;
        }
        Setup.Answer now = setup.getDraft(flow);
        return Setup.Answer.ok(drafted.text() + " Asked " + asked + ", " + answered + " answered; " + now.data().path("guesses").size() + " left.", now.data());
    }

    private static void watchTools(Engine engine, Setup setup, List<Tool> tools) {
        tools.add(new Tool("value_at_risk", "Value at risk",
                "The headline: the value, cases and parties held up right now (money, orders and customers unless the catalog counts something else: its 'measure' says what, in which unit, and what it calls a case and a party), split into stuck (dead-lettered, not yet recovered) and consumer lag, by district and by consumer group. Lag the observer did not see the events of is valued from each topic's recent average and marked as an estimate.",
                schema(), readOnly("Value at risk"), args -> {
                    if (!(watched(engine) instanceof Observer observer)) {
                        return (McpSchema.CallToolResult) watched(engine);
                    }
                    inflight.catalog.Display shown = observer.catalog().display();
                    ObjectNode s = observer.reader().statsJson();
                    ObjectNode data = Json.object();
                    data.set("measure", shown.json());
                    data.set("atRisk", s.get("atRisk"));
                    data.set("stuck", s.get("stuck"));
                    data.set("lag", s.get("lag"));
                    data.set("recovered", s.get("recovered"));
                    data.put("observing", observer.reader().reading());
                    JsonNode atRisk = s.get("atRisk");
                    long unpricedCount = atRisk.get("unpriced").asLong();
                    String unpriced = unpricedCount > 0 ? " Also " + count(unpricedCount, "dead letter") + " the catalog cannot price, value unknown." : "";
                    double estimated = s.get("lag").path("estimated").asDouble(0);
                    String est = estimated > 0 ? " Of the lag, " + shown.amount(estimated) + " is estimated from recent averages." : "";
                    String line = atRisk.get("orders").asLong() > 0 || estimated > 0
                            ? shown.amount(atRisk.get("value").asDouble()) + " at risk: " + shown.cases(atRisk.get("orders").asLong()) + ", "
                                    + shown.parties(atRisk.get("customers").asLong()) + " waiting (stuck " + shown.amount(s.get("stuck").get("value").asDouble())
                                    + ", in lag " + shown.amount(s.get("lag").get("value").asDouble()) + ")." + est + unpriced
                            : !unpriced.isEmpty() ? "Nothing priced is held up." + unpriced : "Nothing held up: " + shown.amount(0) + " at risk.";
                    String nr = observer.reader().reading() ? null : "The observer is still connecting to Kafka; its figures will start at zero.";
                    return answer(nr == null ? line : line + " " + nr, data);
                }));

        tools.add(new Tool("stuck_events", "Stuck events",
                "The dead letters still stuck, grouped by the error that stopped them (one row per cause, with what varies taken out), then the events themselves, newest last, each with its value, customer, order and error. Filter by the district that dead-lettered it.",
                schema(new Param("district", "string", "Only dead letters from this district, such as payments", false, null, null),
                        new Param("limit", "integer", "At most this many events, the newest (default 100)", false, 1.0, 3000.0)),
                readOnly("Stuck events"), args -> {
                    if (!(watched(engine) instanceof Observer observer)) {
                        return (McpSchema.CallToolResult) watched(engine);
                    }
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
                    data.set("causes", Causes.of(all, observer.catalog().display()).get("causes"));
                    data.put("shown", shown.size());
                    ArrayNode events = data.putArray("events");
                    shown.forEach(s -> events.add(Messages.stuck(s)));
                    String at = district == null || district.isEmpty() ? "" : " at " + district;
                    inflight.catalog.Display worth = observer.catalog().display();
                    StringBuilder line = new StringBuilder(all.isEmpty() ? "Nothing stuck" + at + "."
                            : count(all.size(), "event") + " stuck" + at + ", worth " + worth.amount(total) + "; showing the newest " + shown.size() + ".");
                    data.get("causes").forEach(c -> line.append("\n  ").append(count(c.get("events").asLong(), "event")).append(", ")
                            .append(worth.amount(c.get("value").asDouble())).append(": ").append(c.get("cause").asText()));
                    return answer(line.toString(), data);
                }));

        tools.add(new Tool("last_episode", "Last recovery",
                "The latest stuck spell that was fully recovered: the value and events it held, and how long from the first event stuck to the last one recovered. Kept across restarts.",
                schema(), readOnly("Last recovery"), args -> {
                    if (!(watched(engine) instanceof Observer observer)) {
                        return (McpSchema.CallToolResult) watched(engine);
                    }
                    JsonNode e = observer.reader().statsJson().get("lastEpisode");
                    if (e == null || e.isNull()) {
                        ObjectNode data = Json.object();
                        data.putNull("lastEpisode");
                        return answer("No recovery yet.", data);
                    }
                    ObjectNode data = Json.object();
                    data.set("lastEpisode", e);
                    inflight.catalog.Display shown = observer.catalog().display();
                    return answer("Recovered " + shown.amount(e.get("value").asDouble()) + ": " + shown.cases(e.get("events").asLong())
                            + ", stuck " + String.format(Locale.US, "%.1f", e.get("tookMs").asDouble() / 1000) + " s.", data);
                }));

        if (engine.mode() == Engine.Mode.APP) {
            tools.add(new Tool("redrive", "Redrive a dead-letter queue",
                    "Replays dead letters from a queue of the active flow to the topic each came from, as they were, with a redriveof header naming "
                            + "the dead letter it recovers. Filter by error text, order, or failure time. A dry run (the default) says what a run would "
                            + "write, and writes nothing; a real run writes to the cluster, only where the profile allows writes, rate-limited, and runs "
                            + "in the background: call again with status to see it end. Fix what failed first, or the letters come straight back.",
                    schema(new Param("dlq", "string", "The dead-letter queue, such as payments.dlq", true, null, null),
                            new Param("dryRun", "boolean", "Only say what it would do (default true)", false, null, null),
                            new Param("error", "string", "Only dead letters whose error contains this", false, null, null),
                            new Param("order", "string", "Only this order's dead letters", false, null, null),
                            new Param("from", "string", "Only those that failed at or after this ISO time", false, null, null),
                            new Param("to", "string", "Only those that failed at or before this ISO time", false, null, null),
                            new Param("perSecond", "number", "Records written a second, 1 to 5000 (default 50)", false, 1.0, 5000.0),
                            new Param("limit", "integer", "At most this many (default all)", false, 1.0, null),
                            new Param("status", "boolean", "Only report the last redrive of this queue", false, null, null)),
                    destructive("Redrive a dead-letter queue"), args -> {
                        String dlq = text(args, "dlq");
                        if (dlq == null) {
                            return failure("redrive needs a dlq");
                        }
                        if (flag(args, "status", false)) {
                            return of(setup.redriveStatus(dlq, 0));
                        }
                        Redrive.Filter f;
                        try {
                            f = Setup.filter(text(args, "error"), text(args, "order"), text(args, "from"), text(args, "to"));
                        } catch (java.time.format.DateTimeParseException e) {
                            return failure("from and to are ISO times, such as 2026-11-13T09:00:00Z");
                        }
                        Double rate = number(args, "perSecond");
                        Double limit = number(args, "limit");
                        return of(setup.redrive(dlq, f, flag(args, "dryRun", true), rate == null ? 50 : rate, limit == null ? 0 : limit.intValue(), 25_000));
                    }));
        }
    }

    private static void meaningTools(Engine engine, Changes changes, List<Tool> tools) {
        tools.add(new Tool("intents", "Declared intents",
                "What each consumer cares about, in the catalog's terms, such as changes(Order.total): the event versions each intent matches and why, the topics they come from, what a consumer reads from each, and who reads by it.",
                schema(), readOnly("Declared intents"), args -> withMeaning(engine, (c, semantics) -> {
                    List<RoutingTable> tables = semantics.compiled();
                    String line = tables.isEmpty() ? "The catalog declares no intents."
                            : String.join("\n\n", tables.stream().map(t -> Describe.table(t, c.readers().apply(t.intent()))).toList());
                    ObjectNode data = Json.object();
                    ArrayNode is = data.putArray("intents");
                    tables.forEach(t -> is.add(Describe.tableJson(t, c.readers().apply(t.intent()))));
                    return answer(line, data);
                })));

        tools.add(new Tool("explain_intent", "Explain an intent",
                "Compiles a predicate against the catalog and shows what it would match, without declaring it. The language: changes(Entity.attribute), emits(EventType), about(Entity), combined with and, or, not and parentheses.",
                schema(new Param("predicate", "string", "Such as changes(Order.total) or emits(PaymentAuthorized) or emits(StockReserved)", true, null, null),
                        new Param("by", "string", "The entity to key by, when the predicate leaves it ambiguous", false, null, null)),
                readOnly("Explain an intent"), args -> withMeaning(engine, (c, semantics) -> {
                    String predicate = text(args, "predicate");
                    if (predicate == null) {
                        return failure("explain_intent needs a predicate, such as changes(Order.total)");
                    }
                    Result<RoutingTable> table = semantics.explain(predicate, text(args, "by"));
                    if (!table.isOk()) {
                        return failure(String.join("\n", table.errors()));
                    }
                    return answer(Describe.table(table.value().named("matches"), null), Describe.tableJson(table.value().named("unnamed"), null));
                })));

        tools.add(new Tool("impact", "Impact of an event version",
                "What one event version reaches, worked out from the catalog before any event is produced: which intents it newly matches, which consumers read by them, and who must change first because the fields they read have moved. Exits non-zero in CI (in-flight impact) when someone must.",
                schema(new Param("event", "string", "An event version in the catalog, such as OrderPlaced@v2", true, null, null)),
                readOnly("Impact of an event version"), args -> withMeaning(engine, (c, semantics) -> {
                    String event = text(args, "event");
                    Result<EventRef> ref = EventRef.parse(event == null ? "" : event);
                    if (!ref.isOk()) {
                        return failure(ref.error());
                    }
                    Result<Impact> impact = Impact.of(ref.value(), semantics, c.readers());
                    if (!impact.isOk()) {
                        return failure(impact.error());
                    }
                    return answer(impact.value().headline() + "\n\n" + impact.value().report(), Describe.impactJson(impact.value()));
                })));

        tools.add(new Tool("tail_intent", "Tail an intent",
                "The latest events the router delivered on an intent's topic (intent.<name> to a reader through the proxy), oldest first: the entity each is keyed by, the event version, where it came from, and whether it arrived late.",
                schema(new Param("intent", "string", "A declared intent, such as order-total-changes", true, null, null),
                        new Param("limit", "integer", "At most this many (default 10)", false, 1.0, 200.0)),
                readOnly("Tail an intent"), args -> withMeaning(engine, (c, semantics) -> {
                    String intent = text(args, "intent");
                    if (intent == null) {
                        return failure("tail_intent needs an intent");
                    }
                    Double limit = number(args, "limit");
                    ArrayNode events = Tail.of(engine.observer().bootstrap(), intent, limit == null ? 10 : limit.intValue());
                    ObjectNode data = Json.object();
                    data.put("intent", intent);
                    data.set("events", events);
                    String line = events.isEmpty() ? "Nothing on intent." + intent + " yet: is the router running, and does anything produce what it matches?"
                            : count(events.size(), "event") + " on intent." + intent + ", oldest first.";
                    return answer(line, data);
                })));

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
                        } catch (inflight.contracts.ContractsException e) {
                            return failure(e.getMessage());
                        }
                    }));
        }
    }

    private interface WithMeaning {
        McpSchema.CallToolResult apply(Catalog c, Semantics s);
    }

    private static McpSchema.CallToolResult withMeaning(Engine engine, WithMeaning f) {
        if (!(watched(engine) instanceof Observer o)) {
            return (McpSchema.CallToolResult) watched(engine);
        }
        Catalog c = o.catalog();
        if (c.semantics() == null) {
            return failure(Describe.NO_SEMANTICS);
        }
        return f.apply(c, c.semantics());
    }

    private static void tryChangeTools(Engine engine, Changes changes, List<Tool> tools) {
        if (engine.mode() == Engine.Mode.TRY) {
            declareIntent(changes, tools);
        }
        changeTools(engine, changes, tools);
    }

    private static void declareIntent(Changes changes, List<Tool> tools) {
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
    }

    private static void changeTools(Engine engine, Changes changes, List<Tool> tools) {
        tools.add(new Tool("propose_change", "Propose a catalog change",
                "Proposes the catalog this server reads as the router's next revision: works out the impact, checks each new version against the schema registry, and opens a migration (a branch with the regenerated binding, or a pull request) for each consumer that must change. The gate refuses the new versions until approve_change.",
                schema(), writes("Propose a catalog change", false), args -> {
                    if (engine.observer() == null) {
                        return failure("Nothing is being watched yet: activate a flow first.");
                    }
                    try {
                        ObjectNode p = changes.propose(engine.observer().catalog());
                        return p.get("refused").asBoolean() ? failure(p.get("text").asText()) : answer(p.get("text").asText(), p);
                    } catch (inflight.contracts.ContractsException e) {
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
                    } catch (inflight.contracts.ContractsException e) {
                        return failure(e.getMessage());
                    }
                }));
    }

    private static void simulatorTools(SimulatorClient sim, List<Tool> tools) {
        tools.add(new Tool("simulator_state", "Simulator state",
                "The sample data's system: order traffic rate, which OrderPlaced version the orders team ships (2 is the breaking one), whether payments is patched, each service's counts, and the last redrive.",
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
                "Runs the events an intent delivered over the last minutes through a build of the consumer that reads by it, beside the build running now: the candidate's output goes to a branch topic no reader sees, and the answer is the difference. For the sample data: intent order-total-changes, build patched (payments' regenerated binding) or strict (OrderPlaced v1 only).",
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

    // ---- resources and prompts ----------------------------------------------------------------------------------------

    static McpSchema.Resource flowResource() {
        return new McpSchema.Resource("in-flight://flow", "flow", "The flow being watched",
                "Districts, topics, which district produces each topic and which read it, the event types on each, and the dead-letter queues.",
                "application/json", null, null, null, null);
    }

    static McpSchema.ReadResourceResult readFlow(Engine engine, String uri) {
        Observer o = engine.observer();
        String text = o == null ? Json.indented(Json.object().putNull("flow").put("next", engine.status().get("next").asText())) : Json.indented(o.flow());
        return new McpSchema.ReadResourceResult(List.of(new McpSchema.TextResourceContents(uri, "application/json", text, null)), null);
    }

    static final String SETUP_PROMPT = "set-up-a-flow";

    static McpSchema.Prompt setupPrompt() {
        return new McpSchema.Prompt(SETUP_PROMPT, "Set up a flow", "Walks through connecting to a Kafka cluster and watching one of its flows, step by step.",
                List.of(new McpSchema.PromptArgument("flow", "What to call the flow, such as checkout", false),
                        new McpSchema.PromptArgument("cluster", "Which cluster, in your words, such as \"our staging Kafka\"", false)));
    }

    static McpSchema.GetPromptResult setupMessages(Map<String, Object> args) {
        String flow = args == null || args.get("flow") == null ? "the flow" : String.valueOf(args.get("flow"));
        String cluster = args == null || args.get("cluster") == null ? "the cluster" : String.valueOf(args.get("cluster"));
        String text = """
                Set up in-flight to watch %s on %s, with its tools, in this order. Stop and ask me whenever a step needs something only I know.

                1. status: see what is already set up. If a profile is in use and %s is already active, say so and stop.
                2. connect: ask me for the bootstrap servers, which auth preset fits (none, ssl, sasl-plain for Confluent Cloud, scram-sha-256, scram-sha-512, mtls, msk-iam), and where each secret is. Never ask me to type a password: ask for a reference (env:NAME, file:/path or keychain:service) and, if I have not stored it yet, tell me how. Give the registry if the topics are Avro. Use test true first if unsure.
                3. discover: summarise the topics that look like a flow, the consumer groups, and the dead-letter queues, and ask me which topics belong to %s if it is not obvious.
                4. sample_topic on one or two of them, to show me the fields (values stay masked).
                5. draft_catalog with those topics. Then ask me each of its open questions that you cannot settle from the samples, least sure first, and apply my answers with edit_catalog (guess and option).
                6. validate_catalog: show me what each event would be worth on the sampled records. If anything does not price, fix it with edit_catalog and validate again.
                7. activate, then value_at_risk, and tell me where to look in the console (it is served on http://localhost:8088).
                """.formatted(flow, cluster, flow, flow);
        return new McpSchema.GetPromptResult("Set up a flow", List.of(new McpSchema.PromptMessage(McpSchema.Role.USER, new McpSchema.TextContent(null, text, null))));
    }

    static McpSchema.ServerCapabilities capabilities() {
        return McpSchema.ServerCapabilities.builder().tools(false).resources(false, false).prompts(false).build();
    }

    // ---- the two servers --------------------------------------------------------------------------------------------

    /** Asks through an exchange whose client declared elicitation; null otherwise. */
    static Asker asker(McpSyncServerExchange ex) {
        if (ex == null || ex.getClientCapabilities() == null || ex.getClientCapabilities().elicitation() == null) {
            return null;
        }
        return (question, answers) -> {
            Map<String, Object> answer = new LinkedHashMap<>();
            answer.put("type", "string");
            answer.put("title", "Answer");
            answer.put("enum", answers);
            Map<String, Object> schema = Map.of("type", "object", "properties", Map.of("answer", answer), "required", List.of("answer"));
            try {
                McpSchema.ElicitResult r = ex.createElicitation(McpSchema.ElicitFormRequest.builder(question, schema).build());
                if (r.action() != McpSchema.ElicitResult.Action.ACCEPT || r.content() == null) {
                    return null;
                }
                Object a = r.content().get("answer");
                return a == null ? null : a.toString();
            } catch (RuntimeException e) {
                return null;
            }
        };
    }

    /** The server over stdio: stdout is the protocol. */
    public static McpSyncServer stdio(Engine engine, Setup setup, Changes changes) {
        return McpServer.sync(new StdioServerTransportProvider(JSON))
                .serverInfo(NAME, VERSION)
                .instructions(instructions(engine))
                .capabilities(capabilities())
                .jsonMapper(JSON)
                .tools(tools(engine, setup, changes).stream().map(t -> new McpServerFeatures.SyncToolSpecification(t.spec(),
                        (ex, req) -> t.asking().apply(req.arguments(), asker(ex)))).toList())
                .resources(new McpServerFeatures.SyncResourceSpecification(flowResource(), (ex, req) -> readFlow(engine, req.uri())))
                .prompts(new McpServerFeatures.SyncPromptSpecification(setupPrompt(), (ex, req) -> setupMessages(req.arguments())))
                .requestTimeout(Duration.ofSeconds(120))
                .build();
    }

    /** The stateless server behind Streamable HTTP: it cannot ask, so open questions come back in results. */
    public static McpStatelessSyncServer stateless(Engine engine, Setup setup, Changes changes, McpStatelessServerTransport transport) {
        return McpServer.sync(transport)
                .serverInfo(NAME, VERSION)
                .instructions(instructions(engine))
                .capabilities(capabilities())
                .jsonMapper(JSON)
                .tools(tools(engine, setup, changes).stream().map(t -> new McpStatelessServerFeatures.SyncToolSpecification(t.spec(),
                        (ctx, req) -> t.asking().apply(req.arguments(), null))).toList())
                .resources(new McpStatelessServerFeatures.SyncResourceSpecification(flowResource(), (ctx, req) -> readFlow(engine, req.uri())))
                .prompts(new McpStatelessServerFeatures.SyncPromptSpecification(setupPrompt(), (ctx, req) -> setupMessages(req.arguments())))
                .build();
    }

    /** On an observer someone else started: for tests. */
    public static McpStatelessSyncServer stateless(Observer observer, Changes changes, McpStatelessServerTransport transport) {
        Engine e = Engine.around(observer);
        return stateless(e, new Setup(e), changes, transport);
    }
}
