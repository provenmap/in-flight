package rif.server;

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
import rif.observer.Observer;

/**
 * The console's views beyond the flow: what the catalog means, the intents over it, and changes to it.
 *
 * <pre>
 *   GET  /api/catalog                   the catalog the observer reads, as JSON
 *   GET  /api/intents                   each declared intent: matches, why, what its readers take, live state from the router
 *   POST /api/intents/explain           {predicate, by?}: what a predicate would match, or {errors} saying where it is wrong
 *   PUT  /api/intents/{name}            {predicate, by?}: declare it on the router
 *   GET  /api/intents/{name}/tail       the latest events on its topic
 *   GET  /api/impact?event=Type@vN      what one event version reaches, before it is produced
 *   GET  /api/changes                   every proposed change and its state
 *   POST /api/changes                   propose the catalog the observer reads as the router's next revision
 *   GET  /api/changes/{id}              one change: who must change, their bindings as diffs, merged or not, the gate
 *   POST /api/changes/{id}/merge        demo only: the consumers' teams merge their migrations
 *   POST /api/changes/{id}/approve      once every migration merged: register the new versions, activate on the router
 * </pre>
 *
 * In watch mode nothing here writes: only the GETs and explain answer.
 */
final class Api {

    private final Observer observer;
    private final Changes changes;

    Api(Observer observer, Changes changes) {
        this.observer = observer;
        this.changes = changes;
    }

    void register(RoutesConfig r) {
        r.get("/api/catalog", ctx -> json(ctx, 200, observer.catalog().toJson()));
        r.get("/api/intents", this::intents);
        r.post("/api/intents/explain", this::explain);
        r.put("/api/intents/{name}", ctx -> writes(ctx, () -> declare(ctx)));
        r.get("/api/intents/{name}/tail", ctx -> {
            int n = Math.max(1, Math.min(200, ctx.queryParamAsClass("n", Integer.class).getOrDefault(20)));
            ObjectNode o = Json.object();
            o.set("events", Tail.of(observer.bootstrap(), ctx.pathParam("name"), n));
            json(ctx, 200, o);
        });
        r.get("/api/impact", this::impact);
        r.get("/api/changes", ctx -> guarded(ctx, () -> json(ctx, 200, Json.object().set("changes", changes.list()))));
        r.post("/api/changes", ctx -> writes(ctx, () -> guarded(ctx, () -> json(ctx, 200, changes.propose(observer.catalog())))));
        r.get("/api/changes/{id}", ctx -> guarded(ctx, () -> json(ctx, 200, changes.status(ctx.pathParam("id")))));
        r.post("/api/changes/{id}/merge", ctx -> writes(ctx, () -> guarded(ctx, () -> json(ctx, 200, changes.merge(ctx.pathParam("id"))))));
        r.post("/api/changes/{id}/approve", ctx -> writes(ctx, () -> guarded(ctx, () -> json(ctx, 200, changes.approve(ctx.pathParam("id"))))));
    }

    private void intents(Context ctx) {
        Catalog c = observer.catalog();
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
        JsonNode router = changes.routerIntents();
        if (router == null) {
            out.putNull("router");
        } else {
            out.set("router", router);
        }
        out.put("meaning", c.semantics() != null);
        json(ctx, 200, out);
    }

    private void explain(Context ctx) {
        Catalog c = observer.catalog();
        if (c.semantics() == null) {
            error(ctx, 400, Describe.NO_SEMANTICS);
            return;
        }
        JsonNode body = Json.parse(ctx.body()).orElse(Json.object());
        String predicate = body.path("predicate").asText("");
        String by = body.path("by").asText(null);
        Result<RoutingTable> t = c.semantics().explain(predicate, by == null || by.isBlank() ? null : by);
        if (!t.isOk()) {
            // A predicate being typed is not a failed request: what is wrong with it is the answer.
            ObjectNode o = Json.object();
            ArrayNode errors = o.putArray("errors");
            t.errors().forEach(errors::add);
            json(ctx, 200, o);
            return;
        }
        json(ctx, 200, Describe.tableJson(t.value().named("unnamed"), null));
    }

    private void declare(Context ctx) {
        JsonNode body = Json.parse(ctx.body()).orElse(Json.object());
        Changes.Router.Reply r = changes.declare(ctx.pathParam("name"), body.path("predicate").asText(""), body.path("by").asText(null));
        ctx.status(r.status()).contentType("application/json").result(r.body());
    }

    private void impact(Context ctx) {
        Catalog c = observer.catalog();
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
        ObjectNode o = Describe.impactJson(impact.value());
        o.put("headline", impact.value().headline());
        o.put("report", impact.value().report());
        json(ctx, 200, o);
    }

    private void writes(Context ctx, Runnable then) {
        if (observer.watching()) {
            error(ctx, 403, "this observer only watches: it changes nothing");
            return;
        }
        then.run();
    }

    private void guarded(Context ctx, Runnable then) {
        try {
            then.run();
        } catch (ContractsException e) {
            error(ctx, 409, e.getMessage());
        }
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
