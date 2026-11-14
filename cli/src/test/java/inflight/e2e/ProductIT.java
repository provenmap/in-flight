package inflight.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import inflight.catalog.Json;
import inflight.server.Server;
import inflight.server.Workspace;

/**
 * The product, end to end, as PRODUCT.md's phases 1 to 4 say it is done: an agent, over MCP, takes a running instance
 * from no config to a watched flow of a sample application it has never seen (Avro on one topic), then to another
 * cluster and back without a restart; the application's consumer breaks, its dead letters are priced and grouped by
 * cause, it is fixed, its queue is redriven (refused until the profile allows writes, dry run first); and a restart in
 * the middle keeps the episode. The simulator is not involved.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ProductIT {

    static E2e.OwnKafka kafka;
    static E2e.OwnKafka other;
    static GenericContainer<?> apicurio;
    static String registry;
    static SampleApp app;
    static Server server;
    static int port = 38188;

    @TempDir
    static Path home;

    @SuppressWarnings("resource")
    @BeforeAll
    static void start() throws Exception {
        kafka = E2e.OwnKafka.start();
        other = E2e.OwnKafka.start();
        apicurio = new GenericContainer<>(DockerImageName.parse("apicurio/apicurio-registry:3.3.3")).withExposedPorts(8080)
                .waitingFor(Wait.forHttp("/apis/registry/v3/system/info").forPort(8080).withStartupTimeout(Duration.ofMinutes(2)));
        apicurio.start();
        registry = "http://" + apicurio.getHost() + ":" + apicurio.getMappedPort(8080);
        app = new SampleApp(kafka.bootstrap, registry + "/apis/ccompat/v7");
        for (int i = 0; i < 30; i++) {
            app.order(1000 + i * 100);
        }
        E2e.until(() -> app.fulfilled.get() >= 30, 30_000, () -> "fulfilled " + app.fulfilled.get());
        server = Server.start(Server.Options.fromEnv().withPort(port).app(new Workspace(home)));
    }

    @AfterAll
    static void stop() throws Exception {
        if (server != null) {
            server.close();
        }
        if (app != null) {
            app.close();
        }
        for (AutoCloseable c : new AutoCloseable[] {kafka, other}) {
            if (c != null) {
                c.close();
            }
        }
        if (apicurio != null) {
            apicurio.stop();
        }
    }

    record Called(String text, JsonNode data, boolean isError) {}

    static Called mcp(String tool, Map<String, Object> args) throws Exception {
        ObjectNode req = Json.object().put("jsonrpc", "2.0").put("id", 1).put("method", "tools/call");
        req.putObject("params").put("name", tool).set("arguments", Json.tree(args));
        HttpResponse<String> r = E2e.HTTP.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/mcp"))
                .header("Content-Type", "application/json").header("Accept", "application/json, text/event-stream")
                .timeout(Duration.ofSeconds(120)).POST(HttpRequest.BodyPublishers.ofString(Json.write(req))).build(), HttpResponse.BodyHandlers.ofString());
        JsonNode result = Json.parse(r.body()).value().get("result");
        StringBuilder text = new StringBuilder();
        result.get("content").forEach(c -> text.append(c.get("text").asText()));
        return new Called(text.toString(), result.get("structuredContent"), result.path("isError").asBoolean(false));
    }

    static JsonNode status() {
        return E2e.http("GET", "http://localhost:" + port + "/api/status").body();
    }

    static Map<String, Object> profile(String name, String bootstrap, boolean writes) {
        return Map.of("name", name, "bootstrap", bootstrap, "auth", "none", "registry", Map.of("url", registry), "allowWrites", writes);
    }

    @Test
    @Order(1)
    void anAgentConnectsDiscoversAndSamplesAvro() throws Exception {
        assertEquals("connect", status().get("stage").asText());
        Called c = mcp("connect", profile("staging", kafka.bootstrap, false));
        assertFalse(c.isError(), c.text());
        assertTrue(c.text().contains("The registry at " + registry + " answers."), c.text());
        assertEquals("discover", status().get("stage").asText());

        Called d = mcp("discover", Map.of());
        assertFalse(d.isError(), d.text());
        List<String> flow = new ArrayList<>();
        d.data().get("topics").forEach(t -> {
            if (t.get("kind").asText().equals("flow")) {
                flow.add(t.get("name").asText());
            }
        });
        assertEquals(List.of(SampleApp.ORDERS, SampleApp.PAYMENTS), flow);
        JsonNode dlq = d.data().get("dlqs").get(0);
        assertEquals(SampleApp.DLT, dlq.get("topic").asText());
        assertEquals(SampleApp.FULFILMENT, dlq.get("owner").asText());

        Called s = mcp("sample_topic", Map.of("topic", SampleApp.PAYMENTS, "n", 20));
        assertFalse(s.isError(), s.text());
        assertEquals(20, s.data().get("formats").get("avro").asInt(), s.text());
        List<String> fields = new ArrayList<>();
        s.data().get("fields").forEach(f -> fields.add(f.get("path").asText()));
        assertEquals(List.of("order_id", "customer_id", "amount", "currency"), fields);
        assertTrue(s.data().get("fields").get(0).get("example").asText().matches("xxx-9+"), "masked");
    }

    @Test
    @Order(2)
    void itDraftsValidatesAndActivatesWithEveryValueRight() throws Exception {
        Called d = mcp("draft_catalog", Map.of("flow", "shop"));
        assertFalse(d.isError(), d.text());
        JsonNode events = d.data().get("catalog").get("events");
        JsonNode orders = events.get(0);
        assertEquals("total_cents", orders.get("value").asText());
        assertTrue(orders.get("minorUnits").asBoolean());
        assertEquals("order_id", orders.get("order").asText());
        JsonNode payments = events.get(1);
        assertEquals("amount", payments.get("value").asText());
        assertFalse(payments.path("minorUnits").asBoolean(false));
        assertTrue(d.data().get("guesses").size() > 0);

        Called v = mcp("validate_catalog", Map.of("flow", "shop"));
        assertFalse(v.isError(), v.text());
        // 30 orders of 10.00 to 39.00: the same money on both topics, cents and Avro decimals alike.
        for (JsonNode e : v.data().get("events")) {
            assertEquals(30, e.get("priced").asInt(), v.text());
            assertEquals(0, e.get("min").decimalValue().compareTo(new java.math.BigDecimal("10.00")), v.text());
            assertEquals(0, e.get("max").decimalValue().compareTo(new java.math.BigDecimal("39.00")), v.text());
        }

        Called a = mcp("activate", Map.of("flow", "shop"));
        assertFalse(a.isError(), a.text());
        E2e.until(() -> status().get("observing").asBoolean(), 30_000, () -> String.valueOf(status()));
        assertEquals("shop", status().get("flow").asText());
    }

    @Test
    @Order(3)
    void itMovesToAnotherClusterAndBackWithoutARestart() throws Exception {
        Called c = mcp("connect", profile("other", other.bootstrap, false));
        assertFalse(c.isError(), c.text());
        E2e.until(() -> status().path("bootstrap").asText().equals(other.bootstrap) && status().get("missingTopics").size() == 3, 30_000,
                () -> String.valueOf(status()));
        Called s = mcp("status", Map.of());
        assertTrue(s.text().contains("Waiting for topics"), s.text());
        server.engine().use("staging");
        E2e.until(() -> status().path("bootstrap").asText().equals(kafka.bootstrap) && status().get("observing").asBoolean()
                && status().get("missingTopics").isEmpty(), 30_000, () -> String.valueOf(status()));
    }

    @Test
    @Order(4)
    void aBrokenConsumersDeadLettersArePricedAndGroupedByCause() throws Exception {
        try (E2e.Viewer v = new E2e.Viewer("http://localhost:" + port)) {
            v.until(x -> x.first() != null, 20_000);
            app.broken.set(true);
            for (int i = 0; i < 10; i++) {
                app.order(2500); // 25.00 each
            }
            E2e.until(() -> app.deadLettered.get() >= 10, 30_000, () -> "dead-lettered " + app.deadLettered.get());
            v.until(x -> x.stats() != null && x.stats().get("stuck").get("events").asInt() == 10, 30_000);
        }
        Called r = mcp("value_at_risk", Map.of());
        assertEquals(0, r.data().get("stuck").get("value").decimalValue().compareTo(new java.math.BigDecimal("250")), r.text());
        Called s = mcp("stuck_events", Map.of());
        assertEquals(1, s.data().get("causes").size(), s.text());
        assertTrue(s.data().get("causes").get(0).get("cause").asText().startsWith("Warehouse"), s.text());
    }

    @Test
    @Order(5)
    void aRestartInTheMiddleKeepsTheEpisode() throws Exception {
        server.close();
        server = Server.start(Server.Options.fromEnv().withPort(port).app(new Workspace(home)));
        E2e.until(() -> status().get("observing").asBoolean(), 30_000, () -> String.valueOf(status()));
        E2e.until(() -> {
            try {
                return mcp("value_at_risk", Map.of()).data().get("stuck").get("events").asInt() == 10;
            } catch (Exception e) {
                return false;
            }
        }, 30_000, () -> "stuck after restart");
    }

    @Test
    @Order(6)
    void itIsFixedAndItsQueueRedrivenDryRunFirstAndOnlyWhereAllowed() throws Exception {
        app.broken.set(false);
        Called dry = mcp("redrive", Map.of("dlq", SampleApp.DLT));
        assertFalse(dry.isError(), dry.text());
        assertEquals(10, dry.data().get("matched").asInt(), dry.text());
        assertEquals(0, dry.data().get("written").asInt());
        assertEquals(0, dry.data().get("value").decimalValue().compareTo(new java.math.BigDecimal("250")), dry.text());
        Called refused = mcp("redrive", Map.of("dlq", SampleApp.DLT, "dryRun", false));
        assertTrue(refused.isError());
        assertTrue(refused.text().contains("does not allow writes"), refused.text());

        assertFalse(mcp("connect", profile("staging", kafka.bootstrap, true)).isError());
        E2e.until(() -> status().get("observing").asBoolean(), 30_000, () -> String.valueOf(status()));
        Called run = mcp("redrive", Map.of("dlq", SampleApp.DLT, "dryRun", false));
        assertFalse(run.isError(), run.text());
        E2e.until(() -> {
            try {
                return mcp("value_at_risk", Map.of()).data().get("stuck").get("events").asInt() == 0;
            } catch (Exception e) {
                return false;
            }
        }, 60_000, () -> "still stuck");
        Called last = mcp("last_episode", Map.of());
        assertEquals(10, last.data().get("lastEpisode").get("events").asInt(), last.text());
        assertEquals(0, last.data().get("lastEpisode").get("value").decimalValue().compareTo(new java.math.BigDecimal("250")), last.text());
        // Run again: nothing left to write.
        Called again = mcp("redrive", Map.of("dlq", SampleApp.DLT));
        assertEquals(0, again.data().get("matched").asInt(), again.text());
    }
}
