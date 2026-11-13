package rif.router;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import rif.catalog.Catalog;
import rif.catalog.Describe;
import rif.catalog.Intent;
import rif.catalog.Json;
import rif.catalog.Result;
import rif.catalog.RoutingTable;

/**
 * The control API: read and change the catalog and the intents. JSON in, JSON out.
 *
 * <ul>
 * <li>{@code GET /health}: the Streams state, overall and per intent, and the active revision.</li>
 * <li>{@code GET /stats}: the counters, in total and per intent.</li>
 * <li>{@code GET /catalog}, {@code PUT /catalog}: the active catalog; a new one is checked before it is activated.</li>
 * <li>{@code GET /intents}, {@code GET /intents/{name}}, {@code PUT /intents/{name}}: the compiled intents, and declaring
 * one at runtime with {@code {"predicate": ..., "by": ...}}.</li>
 * </ul>
 */
public final class ControlApi implements AutoCloseable {

    private final RouterService service;
    private final HttpServer server;

    /** Binds the port now; port 0 picks a free one, which {@link #port()} then tells. */
    public ControlApi(RouterService service, int port) {
        this.service = service;
        try {
            this.server = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot bind the control API to port " + port, e);
        }
        server.createContext("/", ex -> {
            try {
                route(ex);
            } catch (RuntimeException e) {
                reply(ex, 500, Json.object().put("error", e.toString()));
            }
        });
    }

    public void start() {
        server.start();
    }

    public int port() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private void route(HttpExchange ex) throws IOException {
        String path = ex.getRequestURI().getPath().replaceAll("/+$", "");
        List<String> parts = path.isEmpty() ? List.of() : Arrays.asList(path.substring(1).split("/"));
        String method = ex.getRequestMethod();
        String first = parts.isEmpty() ? "" : parts.get(0);

        if (method.equals("GET") && parts.equals(List.of("health"))) {
            ObjectNode o = Json.object().put("streams", service.streamsState()).put("catalogRevision", service.catalog().map(Catalog::revision).orElse(0));
            ObjectNode intents = o.putObject("intents");
            service.streamsStates().forEach(intents::put);
            reply(ex, 200, o);
        } else if (method.equals("GET") && parts.equals(List.of("stats"))) {
            reply(ex, 200, service.stats().snapshot());
        } else if (method.equals("GET") && parts.equals(List.of("catalog"))) {
            Optional<Catalog> c = service.catalog();
            if (c.isPresent()) {
                reply(ex, 200, c.get().toJson());
            } else {
                reply(ex, 404, Json.object().put("error", "no catalog is active"));
            }
        } else if (method.equals("PUT") && parts.equals(List.of("catalog"))) {
            Result<Catalog> res = Catalog.parse(body(ex)).flatMap(service::activateCatalog);
            if (res.isOk()) {
                ObjectNode o = Json.object().put("active", res.value().revision());
                o.set("intents", tablesJson());
                reply(ex, 200, o);
            } else {
                reply(ex, 400, errors(res.errors()));
            }
        } else if (method.equals("GET") && parts.equals(List.of("intents"))) {
            reply(ex, 200, tablesJson());
        } else if (method.equals("GET") && parts.size() == 2 && first.equals("intents")) {
            Optional<RoutingTable> t = service.routingTable(parts.get(1));
            if (t.isPresent()) {
                reply(ex, 200, tableJson(t.get()));
            } else {
                reply(ex, 404, Json.object().put("error", "no intent '" + parts.get(1) + "'"));
            }
        } else if (method.equals("PUT") && parts.size() == 2 && first.equals("intents")) {
            Result<RoutingTable> res = readIntent(parts.get(1), body(ex)).flatMap(service::declareIntent);
            if (res.isOk()) {
                reply(ex, 200, tableJson(res.value()));
            } else {
                reply(ex, 400, errors(res.errors()));
            }
        } else {
            reply(ex, 404, Json.object().put("error", "no route for " + method + " " + path));
        }
    }

    /** The body names the intent or leaves the name to the path; if it names one, it must be the path's. */
    static Result<Intent> readIntent(String name, String body) {
        return Json.parse(body).flatMap(j -> {
            if (!j.isObject()) {
                return Result.err("an intent is a JSON object with a 'predicate'");
            }
            ObjectNode o = ((ObjectNode) j).deepCopy();
            if (!o.has("name")) {
                o.put("name", name);
            }
            JsonNode named = o.get("name");
            if (!named.isTextual() || !named.asText().equals(name)) {
                return Result.err("body names '" + named.asText() + "', path names '" + name + "'");
            }
            return Registry.readIntent(o);
        });
    }

    private ArrayNode tablesJson() {
        ArrayNode out = Json.array();
        service.routingTables().stream().sorted(java.util.Comparator.comparing(RoutingTable::intent)).forEach(t -> out.add(tableJson(t)));
        return out;
    }

    private ObjectNode tableJson(RoutingTable t) {
        List<rif.catalog.Impact.Reader> readers = service.catalog().map(c -> c.readers().apply(t.intent())).orElse(List.of());
        ObjectNode o = Describe.tableJson(t, readers);
        o.put("revision", t.revision());
        o.put("topic", t.topic());
        o.put("declaredBy", service.declaredByCatalog(t.intent()) ? "catalog" : "runtime");
        IntentApplication app = service.applications().get(t.intent());
        if (app != null) {
            o.put("applicationId", app.applicationId());
        }
        return o;
    }

    private static ObjectNode errors(List<String> errs) {
        ObjectNode o = Json.object();
        ArrayNode a = o.putArray("errors");
        errs.forEach(a::add);
        return o;
    }

    private static String body(HttpExchange ex) throws IOException {
        return new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    }

    private static void reply(HttpExchange ex, int status, JsonNode json) throws IOException {
        byte[] bytes = Json.pretty(json).getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, bytes.length);
        ex.getResponseBody().write(bytes);
        ex.close();
    }
}
