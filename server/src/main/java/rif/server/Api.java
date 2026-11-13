package rif.server;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.javalin.config.RoutesConfig;
import io.javalin.http.Context;
import rif.catalog.Catalog;
import rif.catalog.Describe;
import rif.catalog.EventRef;
import rif.catalog.Impact;
import rif.catalog.Json;
import rif.catalog.Result;
import rif.catalog.RoutingTable;
import rif.contracts.ContractsException;
import rif.kafka.Profile;
import rif.observer.Observer;
import rif.recovery.Redrive;

/**
 * The console's API beyond the stream: setup, recovery, what the catalog means, the intents over it, and changes to it.
 * Every MCP action is here too, so nothing is agent-only.
 *
 * <pre>
 *   GET    /api/status                   what is connected, which flow is active, what is missing, what to do next
 *   GET    /api/presets                  the auth presets and what each is for
 *   POST   /api/connect                  {profile, use?}: test a connection and save it as a profile (secrets by reference)
 *   POST   /api/test                     {profile}: test it, save nothing
 *   POST   /api/profiles/{name}/use      make a saved profile the active one
 *   DELETE /api/profiles/{name}          forget a profile
 *   GET    /api/discover                 topics, groups, dead-letter queues, registry subjects
 *   GET    /api/topics/{topic}/sample    ?n=50&amp;reveal=false: the field shapes of its latest records
 *   POST   /api/flows/{flow}/draft       {topics?}: draft a catalog from the cluster, with a question for every guess
 *   GET    /api/flows/{flow}/draft       the draft, its questions and its last dry run
 *   POST   /api/flows/{flow}/validate    {catalog?}: check it and dry-run it on sampled records
 *   PATCH  /api/flows/{flow}             {patch | guess+option, target?}: edit the draft, or the active catalog
 *   POST   /api/flows/{flow}/activate    {catalog?}: save it and switch the observer to it
 *   GET    /api/flows/{flow}             the flow's catalog file
 *   GET    /api/stuck/causes             what is stuck, grouped by the error that stopped it
 *   POST   /api/redrive                  {dlq, dryRun, error?, order?, from?, to?, perSecond?, limit?}
 *   GET    /api/redrive/{dlq}            the last redrive of a queue
 *   POST   /api/redrive/{dlq}/stop       stop it
 *   PUT    /api/alerts                   {webhook, threshold}: post when money at risk crosses it
 *   GET    /api/catalog                  the catalog the observer reads, as JSON
 *   GET    /api/intents                  each declared intent: matches, why, what its readers take, live state from the router
 *   POST   /api/intents/explain          {predicate, by?}: what a predicate would match, or {errors} saying where it is wrong
 *   PUT    /api/intents/{name}           {predicate, by?}: declare it on the router                          (try only)
 *   GET    /api/intents/{name}/tail      the latest events on its topic
 *   GET    /api/impact?event=Type@vN     what one event version reaches, before it is produced
 *   GET    /api/changes                  every proposed change and its state
 *   POST   /api/changes                  propose the catalog the observer reads as the router's next revision
 *   GET    /api/changes/{id}             one change: who must change, their bindings as diffs, merged or not, the gate
 *   POST   /api/changes/{id}/merge       the sample data's teams merge their migrations                         (try only)
 *   POST   /api/changes/{id}/approve     once every migration merged (its pull request, read with gh): register, activate
 * </pre>
 */
final class Api {

    private final Engine engine;
    private final Setup setup;
    private final Changes changes;

    Api(Engine engine, Setup setup, Changes changes) {
        this.engine = engine;
        this.setup = setup;
        this.changes = changes;
    }

    void register(RoutesConfig r) {
        r.get("/api/status", ctx -> json(ctx, 200, engine.status()));
        r.get("/api/presets", ctx -> {
            ObjectNode o = Json.object();
            ArrayNode ps = o.putArray("presets");
            Profile.PRESETS.forEach(p -> ps.addObject().put("id", p).put("help", Profile.PRESET_HELP.get(p)));
            o.put("secretForms", rif.kafka.Secrets.FORMS);
            json(ctx, 200, o);
        });
        r.post("/api/test", ctx -> reconfig(ctx, () -> {
            Engine.Tested t = Engine.test(Workspace.profile(body(ctx).path("profile")));
            answer(ctx, new Setup.Answer(t.ok(), t.text(), t.data()));
        }));
        r.post("/api/connect", ctx -> reconfig(ctx, () -> {
            JsonNode b = body(ctx);
            Engine.Tested t = engine.connect(Workspace.profile(b.path("profile")), b.path("use").asBoolean(true));
            answer(ctx, new Setup.Answer(t.ok(), t.text(), t.data()));
        }));
        r.post("/api/profiles/{name}/use", ctx -> reconfig(ctx, () -> {
            engine.use(ctx.pathParam("name"));
            json(ctx, 200, engine.status());
        }));
        r.delete("/api/profiles/{name}", ctx -> reconfig(ctx, () -> {
            Workspace ws = engine.workspace();
            ws.save(ws.load().withoutProfile(ctx.pathParam("name")));
            json(ctx, 200, engine.status());
        }));
        r.get("/api/discover", ctx -> answer(ctx, setup.discover()));
        r.get("/api/topics/{topic}/sample", ctx -> answer(ctx, setup.sample(ctx.pathParam("topic"),
                ctx.queryParamAsClass("n", Integer.class).getOrDefault(50), "true".equals(ctx.queryParam("reveal")))));
        r.post("/api/flows/{flow}/draft", ctx -> reconfig(ctx, () -> {
            List<String> topics = new ArrayList<>();
            body(ctx).path("topics").forEach(t -> topics.add(t.asText()));
            answer(ctx, setup.draft(ctx.pathParam("flow"), topics));
        }));
        r.get("/api/flows/{flow}/draft", ctx -> answer(ctx, setup.getDraft(ctx.pathParam("flow")), 404));
        r.post("/api/flows/{flow}/validate", ctx -> {
            JsonNode c = body(ctx).get("catalog");
            answer(ctx, setup.validate(ctx.pathParam("flow"), c == null || c.isNull() ? null : c), 200);
        });
        r.patch("/api/flows/{flow}", ctx -> reconfig(ctx, () -> {
            JsonNode b = body(ctx);
            answer(ctx, setup.edit(ctx.pathParam("flow"), b.path("target").asText("draft"), b.get("patch"), b.path("guess").asText(null),
                    b.path("option").asText(null)), 200);
        }));
        r.post("/api/flows/{flow}/activate", ctx -> reconfig(ctx, () -> {
            JsonNode c = body(ctx).get("catalog");
            answer(ctx, setup.activate(ctx.pathParam("flow"), c == null || c.isNull() ? null : c));
        }));
        r.get("/api/flows/{flow}", ctx -> {
            JsonNode f = engine.workspace() == null ? null : engine.workspace().flowJson(ctx.pathParam("flow"));
            if (f == null) {
                error(ctx, 404, "there is no flow " + ctx.pathParam("flow"));
            } else {
                json(ctx, 200, f);
            }
        });
        r.get("/api/stuck/causes", ctx -> withObserver(ctx, o -> json(ctx, 200, Causes.of(o))));
        r.post("/api/redrive", ctx -> {
            JsonNode b = body(ctx);
            Redrive.Filter f;
            try {
                f = Setup.filter(b.path("error").asText(null), b.path("order").asText(null), b.path("from").asText(null), b.path("to").asText(null));
            } catch (java.time.format.DateTimeParseException e) {
                error(ctx, 400, "from and to are ISO times, such as 2026-11-13T09:00:00Z");
                return;
            }
            answer(ctx, setup.redrive(b.path("dlq").asText(""), f, b.path("dryRun").asBoolean(true), b.path("perSecond").asDouble(50),
                    b.path("limit").asInt(0), 0), 409);
        });
        r.get("/api/redrive/{dlq}", ctx -> answer(ctx, setup.redriveStatus(ctx.pathParam("dlq"), 0)));
        r.post("/api/redrive/{dlq}/stop", ctx -> answer(ctx, setup.stopRedrive(ctx.pathParam("dlq")), 409));
        r.put("/api/alerts", ctx -> reconfig(ctx, () -> {
            JsonNode b = body(ctx);
            String hook = b.path("webhook").asText(null);
            if (hook != null && !hook.isBlank() && !hook.matches("https?://.+")) {
                error(ctx, 400, "webhook: an http or https address");
                return;
            }
            Workspace ws = engine.workspace();
            ws.save(ws.load().withAlerts(hook == null || hook.isBlank() ? Workspace.Alerts.OFF : new Workspace.Alerts(hook, b.path("threshold").asDouble(0))));
            json(ctx, 200, Json.object().put("webhook", hook).put("threshold", b.path("threshold").asDouble(0)));
        }));

        r.get("/api/catalog", ctx -> withObserver(ctx, o -> json(ctx, 200, o.catalog().toJson())));
        r.get("/api/intents", ctx -> withObserver(ctx, o -> intents(ctx, o)));
        r.post("/api/intents/explain", ctx -> withObserver(ctx, o -> explain(ctx, o)));
        r.put("/api/intents/{name}", ctx -> writes(ctx, () -> declare(ctx)));
        r.get("/api/intents/{name}/tail", ctx -> withObserver(ctx, o -> {
            int n = Math.max(1, Math.min(200, ctx.queryParamAsClass("n", Integer.class).getOrDefault(20)));
            ObjectNode out = Json.object();
            out.set("events", Tail.of(o.bootstrap(), ctx.pathParam("name"), n));
            json(ctx, 200, out);
        }));
        r.get("/api/impact", ctx -> withObserver(ctx, o -> impact(ctx, o)));
        r.get("/api/changes", ctx -> guarded(ctx, () -> json(ctx, 200, Json.object().set("changes", changes.list()))));
        r.post("/api/changes", ctx -> withObserver(ctx, o -> guarded(ctx, () -> json(ctx, 200, changes.propose(o.catalog())))));
        r.get("/api/changes/{id}", ctx -> guarded(ctx, () -> json(ctx, 200, changes.status(ctx.pathParam("id")))));
        r.post("/api/changes/{id}/merge", ctx -> writes(ctx, () -> guarded(ctx, () -> json(ctx, 200, changes.merge(ctx.pathParam("id"))))));
        r.post("/api/changes/{id}/approve", ctx -> guarded(ctx, () -> json(ctx, 200, changes.approve(ctx.pathParam("id")))));
    }

    private void intents(Context ctx, Observer o) {
        Catalog c = o.catalog();
        ObjectNode out = Json.object();
        ArrayNode is = out.putArray("intents");
        if (c.semantics() != null) {
            for (RoutingTable t : c.semantics().compiled()) {
                ObjectNode x = Describe.tableJson(t, c.readers().apply(t.intent()));
                ArrayNode readers = x.putArray("readerDistricts");
                c.consumersOf(t.intent()).forEach(k -> readers.add(k.district()));
                is.add(x);
            }
        }
        JsonNode router = engine.mode() == Engine.Mode.TRY ? changes.routerIntents() : null;
        if (router == null) {
            out.putNull("router");
        } else {
            out.set("router", router);
        }
        out.put("meaning", c.semantics() != null);
        json(ctx, 200, out);
    }

    private void explain(Context ctx, Observer o) {
        Catalog c = o.catalog();
        if (c.semantics() == null) {
            error(ctx, 400, Describe.NO_SEMANTICS);
            return;
        }
        JsonNode b = body(ctx);
        String predicate = b.path("predicate").asText("");
        String by = b.path("by").asText(null);
        Result<RoutingTable> t = c.semantics().explain(predicate, by == null || by.isBlank() ? null : by);
        if (!t.isOk()) {
            // A predicate being typed is not a failed request: what is wrong with it is the answer.
            ObjectNode out = Json.object();
            ArrayNode errors = out.putArray("errors");
            t.errors().forEach(errors::add);
            json(ctx, 200, out);
            return;
        }
        json(ctx, 200, Describe.tableJson(t.value().named("unnamed"), null));
    }

    private void declare(Context ctx) {
        JsonNode b = body(ctx);
        Changes.Router.Reply r = changes.declare(ctx.pathParam("name"), b.path("predicate").asText(""), b.path("by").asText(null));
        ctx.status(r.status()).contentType("application/json").result(r.body());
    }

    private void impact(Context ctx, Observer o) {
        Catalog c = o.catalog();
        Result<EventRef> ref = EventRef.parse(String.valueOf(ctx.queryParam("event")));
        if (!ref.isOk()) {
            error(ctx, 400, ref.error());
            return;
        }
        if (c.semantics() == null) {
            error(ctx, 400, Describe.NO_SEMANTICS);
            return;
        }
        Result<Impact> impact = Impact.of(ref.value(), c.semantics(), c.readers());
        if (!impact.isOk()) {
            error(ctx, 400, impact.error());
            return;
        }
        ObjectNode out = Describe.impactJson(impact.value());
        out.put("headline", impact.value().headline());
        out.put("report", impact.value().report());
        json(ctx, 200, out);
    }

    // ---- guards -----------------------------------------------------------------------------------------------------

    private interface Body {
        void run();
    }

    private void withObserver(Context ctx, java.util.function.Consumer<Observer> then) {
        Observer o = engine.observer();
        if (o == null) {
            error(ctx, 409, "nothing is being watched yet: connect to a cluster and activate a flow");
            return;
        }
        then.accept(o);
    }

    /** Only the sample data's controls: the router, contracts and the demo's merges belong to {@code try}. */
    private void writes(Context ctx, Body then) {
        if (engine.mode() != Engine.Mode.TRY) {
            error(ctx, 403, "this changes the sample data's router and contracts: it runs only under revenue-in-flight try");
            return;
        }
        then.run();
    }

    /** Setup changes config: refused by fixed engines, with what to do instead. */
    private void reconfig(Context ctx, Body then) {
        try {
            then.run();
        } catch (IllegalStateException | IllegalArgumentException e) {
            error(ctx, 409, e.getMessage());
        }
    }

    private void guarded(Context ctx, Body then) {
        try {
            then.run();
        } catch (ContractsException e) {
            error(ctx, 409, e.getMessage());
        }
    }

    private static JsonNode body(Context ctx) {
        String b = ctx.body();
        return b == null || b.isBlank() ? Json.object() : Json.parse(b).orElse(Json.object());
    }

    private static void answer(Context ctx, Setup.Answer a) {
        answer(ctx, a, 400);
    }

    private static void answer(Context ctx, Setup.Answer a, int failStatus) {
        ObjectNode o = a.data().deepCopy();
        o.put("text", a.text());
        o.put("ok", a.ok());
        json(ctx, a.ok() ? 200 : failStatus, o);
    }

    static void json(Context ctx, int status, JsonNode body) {
        ctx.status(status).contentType("application/json").result(Json.write(body));
    }

    static void error(Context ctx, int status, String msg) {
        json(ctx, status, Json.object().put("error", msg));
    }

    static List<String> list(JsonNode a) {
        return a == null ? List.of() : java.util.stream.StreamSupport.stream(a.spliterator(), false).map(JsonNode::asText).toList();
    }
}
