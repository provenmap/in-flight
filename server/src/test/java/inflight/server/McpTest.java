package inflight.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.javalin.Javalin;
import inflight.catalog.Catalog;
import inflight.catalog.Json;
import inflight.observer.Observer;

/** Ported from src/mcp/server.test.ts: the tools on an observer that never starts, and MCP over HTTP's guard. */
class McpTest {

    @BeforeAll
    static void letTestsSetTheHostHeader() {
        System.setProperty("jdk.httpclient.allowRestrictedHeaders", "host");
    }

    /** An observer that is never started: no Kafka, an empty ledger, and a simulator nobody runs. */
    static Observer observer() {
        return new Observer(new Observer.Options("localhost:1", "http://localhost:1", Catalog.defaultCatalog()));
    }

    private final List<Javalin> apps = new ArrayList<>();
    private final HttpClient http = HttpClient.newHttpClient();

    @AfterEach
    void stop() {
        apps.forEach(Javalin::stop);
    }

    int app(String bindHost, String token) {
        McpHttp transport = new McpHttp(bindHost, token);
        Mcp.stateless(observer(), null, transport);
        Javalin a = Javalin.create(c -> {
            c.startup.showJavalinBanner = false;
            c.routes.post("/mcp", transport::post);
            c.routes.get("/mcp", transport::other);
        }).start("localhost", 0);
        apps.add(a);
        return a.port();
    }

    static final String INIT = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-06-18\",\"capabilities\":{},\"clientInfo\":{\"name\":\"t\",\"version\":\"1\"}}}";

    HttpResponse<String> post(int port, String body, Map<String, String> headers) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/mcp"))
                .header("Content-Type", "application/json").header("Accept", "application/json, text/event-stream")
                .POST(HttpRequest.BodyPublishers.ofString(body));
        headers.forEach(b::header);
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    /** Calls a tool on a local server: its text, its structured content, and whether it is an error. */
    record Called(String text, JsonNode data, boolean isError) {}

    int local;

    Called call(String name, ObjectNode args) throws Exception {
        if (local == 0) {
            local = app("localhost", null);
        }
        ObjectNode req = Json.object().put("jsonrpc", "2.0").put("id", 2).put("method", "tools/call");
        req.putObject("params").put("name", name).set("arguments", args);
        JsonNode res = Json.parse(post(local, Json.write(req), Map.of("Host", "localhost")).body()).value();
        JsonNode result = res.get("result");
        StringBuilder text = new StringBuilder();
        result.get("content").forEach(c -> text.append(text.isEmpty() ? "" : "\n").append(c.get("text").asText()));
        return new Called(text.toString(), result.get("structuredContent"), result.path("isError").asBoolean(false));
    }

    Called call(String name) throws Exception {
        return call(name, Json.object());
    }

    @Test
    void anEmptyLedgerIsZeroAtRiskAndSaysItIsNotObservingYet() throws Exception {
        Called r = call("value_at_risk");
        assertTrue(r.text().startsWith("Nothing held up: $0.00 at risk. The observer is still connecting"), r.text());
        assertEquals(0, r.data().get("atRisk").get("value").asDouble());
        assertEquals(0, r.data().get("atRisk").get("orders").asInt());
        assertFalse(r.data().get("observing").asBoolean());
        assertEquals("No recovery yet.\n\n{\n  \"lastEpisode\": null\n}", call("last_episode").text());
    }

    @Test
    void controlsFailWithThe502WhenTheSimulatorIsDown() throws Exception {
        for (String name : List.of("break_schema", "redrive", "simulator_state")) {
            Called r = call(name);
            assertTrue(r.isError(), name);
            assertTrue(r.text().contains("the simulator is not reachable at http://localhost:1"), r.text());
        }
    }

    @Test
    void intentsExplainIntentAndImpactAnswerFromTheCatalogAlone() throws Exception {
        Called intents = call("intents");
        assertTrue(intents.text().contains("order-total-changes: changes(Order.total)"));
        List<String> seen = new ArrayList<>();
        intents.data().get("intents").forEach(i -> seen.add(i.get("intent").asText() + " " + i.get("consumers")));
        assertEquals(List.of("order-total-changes [\"payments-service\"]", "orders-placed [\"inventory-service\"]",
                "ready-to-ship [\"shipping-service\"]", "order-dispatched [\"customers\"]"), seen);

        Called explained = call("explain_intent", Json.object().put("predicate", "changes(Order.payment)"));
        assertEquals("Order", explained.data().get("keyedBy").asText());
        assertEquals("[\"payments.authorized\",\"payments.declined\"]", explained.data().get("topics").toString());
        Called wrong = call("explain_intent", Json.object().put("predicate", "changes(Order.totl)"));
        assertTrue(wrong.isError());
        assertEquals("Order has no attribute 'totl' (has: total, status, payment, stock)", wrong.text());

        Called impact = call("impact", Json.object().put("event", "OrderPlaced@v2"));
        assertTrue(impact.text().startsWith("payments-service must change before this ships, or its events pile up in payments.dlq."), impact.text());
        JsonNode must = impact.data().get("mustChangeFirst").get(0);
        assertEquals(List.of("payments-service", "OrderPlaced@v2", "new-reads", "payments.dlq"),
                List.of(must.get("consumer").asText(), must.get("event").asText(), must.get("because").asText(), must.get("deadLettersTo").asText()));
        assertEquals("[\"order-dispatched\",\"ready-to-ship\"]", impact.data().get("unaffected").toString());
        assertTrue(call("impact", Json.object().put("event", "OrderPlaced v2")).isError());
    }

    @Test
    void toolInputsAreValidated() throws Exception {
        assertTrue(call("set_rate", Json.object().put("perSecond", -1)).isError());
        assertTrue(call("stuck_events", Json.object().put("limit", 0)).isError());
    }

    // ---- MCP over HTTP ----

    @Test
    void onLocalhostItAnswersRequestsToLocalhostWithoutAToken() throws Exception {
        HttpResponse<String> res = post(app("localhost", null), INIT, Map.of("Host", "localhost:8088"));
        assertEquals(200, res.statusCode());
        assertEquals("in-flight", Json.parse(res.body()).value().get("result").get("serverInfo").get("name").asText());
    }

    @Test
    void onLocalhostItRefusesOtherHostsAndOriginsWhichStopsDnsRebinding() throws Exception {
        int port = app("localhost", null);
        assertEquals(403, post(port, INIT, Map.of("Host", "evil.example:8088")).statusCode());
        assertEquals(403, post(port, INIT, Map.of("Host", "localhost:8088", "Origin", "https://evil.example")).statusCode());
        assertEquals(200, post(port, INIT, Map.of("Host", "localhost:8088", "Origin", "http://localhost:5173")).statusCode());
    }

    @Test
    void beyondLocalhostItIsOffWithoutATokenAndNeedsTheTokenOnceOneIsSet() throws Exception {
        assertEquals(403, post(app("0.0.0.0", null), INIT, Map.of("Host", "demo.example")).statusCode());
        int port = app("0.0.0.0", "s3cret");
        assertEquals(401, post(port, INIT, Map.of("Host", "demo.example")).statusCode());
        assertEquals(401, post(port, INIT, Map.of("Host", "demo.example", "Authorization", "Bearer nope")).statusCode());
        assertEquals(200, post(port, INIT, Map.of("Host", "demo.example", "Authorization", "Bearer s3cret")).statusCode());
    }
}
