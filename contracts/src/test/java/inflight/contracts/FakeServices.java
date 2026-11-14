package inflight.contracts;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import inflight.catalog.Catalog;
import inflight.catalog.Json;
import inflight.catalog.Model;

/**
 * The router's catalog endpoint and the part of Apicurio Registry v3 contracts uses, on one local port. The registry
 * keeps artifacts and versions in memory and, when told to, finds every new version incompatible.
 */
final class FakeServices implements AutoCloseable {

    private static final String ARTIFACTS = "/apis/registry/v3/groups/intent-bus/artifacts";

    private final HttpServer server;
    volatile JsonNode active;
    final List<JsonNode> activated = new CopyOnWriteArrayList<>();
    final Set<String> artifacts = ConcurrentHashMap.newKeySet();
    final Set<String> versions = ConcurrentHashMap.newKeySet();
    final List<String> requests = new CopyOnWriteArrayList<>();
    volatile String incompatible;

    FakeServices(JsonNode active) throws IOException {
        this.active = active;
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/catalog", this::router);
        server.createContext(ARTIFACTS, this::registry);
        server.start();
    }

    String routerUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    String apicurioUrl() {
        return routerUrl() + "/apis/registry/v3";
    }

    /** Registers every event version of a catalog, as `contracts register` would have. */
    FakeServices registered(Catalog c) {
        for (Model.Event e : c.semantics().model().events()) {
            artifacts.add(e.ref().name());
            versions.add(e.ref().name() + "/" + e.ref().version());
        }
        return this;
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private void router(HttpExchange x) throws IOException {
        requests.add(x.getRequestMethod() + " " + x.getRequestURI().getPath());
        switch (x.getRequestMethod()) {
            case "GET" -> respond(x, 200, Json.write(active));
            case "PUT" -> {
                JsonNode body = Json.parse(new String(x.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)).value();
                activated.add(body);
                active = body;
                respond(x, 200, "{}");
            }
            default -> respond(x, 405, "{}");
        }
    }

    private void registry(HttpExchange x) throws IOException {
        String method = x.getRequestMethod();
        String rest = x.getRequestURI().getPath().substring(ARTIFACTS.length());
        boolean dry = "dryRun=true".equals(x.getRequestURI().getQuery());
        requests.add(method + " " + x.getRequestURI().getPath() + (dry ? "?dryRun=true" : ""));
        String[] parts = rest.isEmpty() ? new String[0] : rest.substring(1).split("/");
        JsonNode body = method.equals("POST") ? Json.parse(new String(x.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)).value() : null;

        if (method.equals("GET") && parts.length == 3 && parts[1].equals("versions")) {
            respond(x, versions.contains(parts[0] + "/" + parts[2]) ? 200 : 404, "{}");
        } else if (method.equals("GET") && parts.length == 1) {
            respond(x, artifacts.contains(parts[0]) ? 200 : 404, "{}");
        } else if (method.equals("POST") && parts.length == 0) {
            String name = body.get("artifactId").asText();
            if (refuse(x)) {
                return;
            }
            if (!dry) {
                artifacts.add(name);
                versions.add(name + "/" + body.at("/firstVersion/version").asText());
            }
            respond(x, 200, "{}");
        } else if (method.equals("POST") && parts.length == 2 && parts[1].equals("versions")) {
            if (refuse(x)) {
                return;
            }
            if (!dry) {
                versions.add(parts[0] + "/" + body.get("version").asText());
            }
            respond(x, 200, "{}");
        } else if (method.equals("POST") && parts.length == 2 && parts[1].equals("rules")) {
            x.sendResponseHeaders(204, -1);
            x.close();
        } else {
            respond(x, 404, "{\"title\": \"no such route\"}");
        }
    }

    private boolean refuse(HttpExchange x) throws IOException {
        if (incompatible == null) {
            return false;
        }
        respond(x, 409, Json.write(Json.object().set("causes", Json.array().add(Json.object().put("description", incompatible)))));
        return true;
    }

    private static void respond(HttpExchange x, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        x.getResponseHeaders().add("Content-Type", "application/json");
        x.sendResponseHeaders(status, bytes.length);
        try (var out = x.getResponseBody()) {
            out.write(bytes);
        }
    }
}
