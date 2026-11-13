package rif.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static rif.e2e.E2e.sleep;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.client.transport.ServerParameters;
import io.modelcontextprotocol.client.transport.StdioClientTransport;
import io.modelcontextprotocol.spec.McpClientTransport;
import io.modelcontextprotocol.spec.McpSchema;
import rif.catalog.Json;
import rif.server.Mcp;
import rif.server.Server;
import rif.simulator.Simulator;

/**
 * The MCP server end to end, ported from test/e2e/mcp.test.ts, over stdio (the built jar, as {@code claude mcp add} runs
 * it) and over HTTP: an agent breaks the schema, sees money at risk, patches, redrives, and reads it back to $0 with the
 * last episode matching what was stuck.
 */
class McpIT {

    static final int SIM_PORT = 48090;

    static E2e.OwnKafka kafka;

    @org.junit.jupiter.api.BeforeAll
    static void kafka() {
        kafka = E2e.OwnKafka.start();
    }

    @org.junit.jupiter.api.AfterAll
    static void noKafka() {
        if (kafka != null) {
            kafka.close();
        }
    }

    Simulator sim;
    Server server;
    McpSyncClient client;

    @AfterEach
    void stop() throws Exception {
        if (client != null) {
            client.closeGracefully();
        }
        if (server != null) {
            server.close();
        }
        if (sim != null) {
            E2e.control(sim.url(), "restore");
            E2e.control(sim.url(), "unpatch");
            sim.close();
        }
    }

    void start(Supplier<McpClientTransport> transport, boolean http) {
        String bootstrap = kafka.bootstrap;
        sim = Simulator.start(Simulator.Options.fromEnv().withBootstrap(bootstrap).withPort(SIM_PORT));
        E2e.settle(sim.url());
        if (http) {
            Server.Options o = Server.Options.fromEnv();
            server = Server.start(o.withPort(48088).withObserver(o.observer().withBootstrap(bootstrap).withSimulator(sim.url())));
        }
        client = McpClient.sync(transport.get()).requestTimeout(Duration.ofSeconds(60)).initializationTimeout(Duration.ofSeconds(60)).build();
        client.initialize();
    }

    static McpClientTransport stdio() {
        Path jar = Path.of("cli/target/revenue-in-flight.jar");
        assertTrue(Files.exists(jar), "build the jar first: ./mvnw package");
        ServerParameters params = ServerParameters.builder(Path.of(System.getProperty("java.home"), "bin", "java").toString())
                .args("-jar", jar.toString(), "mcp")
                .env(Map.of("BOOTSTRAP_SERVERS", kafka.bootstrap, "SIMULATOR_URL", "http://localhost:" + SIM_PORT))
                .build();
        return new StdioClientTransport(params, Mcp.JSON);
    }

    static McpClientTransport http() {
        return HttpClientStreamableHttpTransport.builder("http://localhost:48088").endpoint("/mcp").jsonMapper(Mcp.JSON).build();
    }

    record Called(JsonNode data, String text, boolean isError) {}

    Called call(String name, Map<String, Object> args) {
        McpSchema.CallToolResult res = client.callTool(new McpSchema.CallToolRequest(name, args));
        StringBuilder text = new StringBuilder();
        res.content().forEach(c -> text.append(text.isEmpty() ? "" : "\n").append(((McpSchema.TextContent) c).text()));
        JsonNode data = res.structuredContent() == null ? Json.object() : Json.tree(res.structuredContent());
        return new Called(data, text.toString(), Boolean.TRUE.equals(res.isError()));
    }

    Called call(String name) {
        return call(name, Map.of());
    }

    Called until(Supplier<Called> f, Predicate<Called> ok) {
        long end = System.currentTimeMillis() + 60_000;
        while (true) {
            Called c = f.get();
            if (ok.test(c)) {
                return c;
            }
            if (System.currentTimeMillis() > end) {
                throw new AssertionError("timed out; last " + c.data());
            }
            sleep(500);
        }
    }

    @Test
    void overStdio() {
        start(McpIT::stdio, false);
        toolsAndTheFlowResource();
        breakSeeMoneyAtRiskPatchRedriveBackToZero();
        aBadRateIsRefused();
    }

    @Test
    void overHttp() {
        start(McpIT::http, true);
        toolsAndTheFlowResource();
        breakSeeMoneyAtRiskPatchRedriveBackToZero();
        aBadRateIsRefused();
    }

    void toolsAndTheFlowResource() {
        List<String> tools = client.listTools().tools().stream().map(McpSchema.Tool::name).sorted().toList();
        assertEquals(List.of("approve_change", "break_schema", "change_status", "declare_intent", "explain_intent", "impact", "intents", "last_episode",
                "patch_payments", "propose_change", "redrive", "rehearse", "restore_schema", "set_rate", "simulator_state", "status", "stuck_events", "tail_intent",
                "unpatch_payments", "value_at_risk"), tools);
        McpSchema.ReadResourceResult flow = client.readResource(new McpSchema.ReadResourceRequest("rif://flow"));
        JsonNode f = Json.parse(((McpSchema.TextResourceContents) flow.contents().get(0)).text()).value();
        assertEquals(List.of("orders", "payments", "inventory", "shipping", "customers"), f.get("districts").findValuesAsText("id"));
        assertEquals(List.of("payments.dlq", "inventory.dlq", "shipping.dlq"), f.get("dlqs").findValuesAsText("dlq"));
    }

    void breakSeeMoneyAtRiskPatchRedriveBackToZero() {
        until(() -> call("value_at_risk"), r -> r.data().get("observing").asBoolean() && r.data().get("stuck").get("events").asLong() == 0);
        assertEquals(1, call("simulator_state").data().get("orderVersion").asInt());

        assertEquals(20, call("set_rate", Map.of("perSecond", 20)).data().get("rate").asInt());
        Called broke = call("break_schema");
        assertFalse(broke.isError());
        assertEquals(2, broke.data().get("orderVersion").asInt());
        sleep(4000);

        Called jammed = until(() -> call("value_at_risk"), r -> r.data().get("stuck").get("events").asLong() > 20);
        assertTrue(jammed.data().get("atRisk").get("value").asDouble() > 0);
        assertTrue(jammed.data().get("stuck").get("byDistrict").get("payments").asDouble() > 0);
        assertTrue(jammed.text().matches("(?s)^\\$[\\d,.]+ at risk: [\\d,]+ orders.*"), jammed.text());

        Called stuck = call("stuck_events", Map.of("district", "payments", "limit", 5));
        assertEquals(5, stuck.data().get("shown").asInt());
        JsonNode e0 = stuck.data().get("events").get(0);
        assertEquals(List.of("payments.dlq", "OrderPlaced@v2", "'total' is an object, expected a decimal string"),
                List.of(e0.get("dlq").asText(), e0.get("event").asText(), e0.get("error").asText()));
        assertEquals(0, call("stuck_events", Map.of("district", "inventory")).data().get("total").get("events").asLong());

        assertTrue(call("patch_payments").data().get("patched").asBoolean());
        sleep(1500);
        JsonNode before = call("stuck_events", Map.of("limit", 3000)).data().get("total");
        assertFalse(call("redrive").isError());
        Called again = call("redrive");
        assertTrue(again.isError());
        assertTrue(again.text().contains("a redrive is already running (409)"), again.text());

        call("set_rate", Map.of("perSecond", 8));
        Called drained = until(() -> call("value_at_risk"),
                r -> r.data().get("stuck").get("events").asLong() == 0 && r.data().get("atRisk").get("value").asDouble() == 0);
        assertTrue(drained.text().startsWith("Nothing held up: $0.00 at risk.\n"), drained.text());
        JsonNode episode = call("last_episode").data().get("lastEpisode");
        assertEquals(before.get("events").asLong(), episode.get("events").asLong());
        assertEquals(before.get("value").asDouble(), episode.get("value").asDouble(), 0.01);
        assertTrue(episode.get("tookMs").asLong() > 4000);
        // The observer can see the last redriven event a moment before the simulator marks its pass done.
        JsonNode redrive = until(() -> call("simulator_state"), r -> !r.data().get("redrive").get("running").asBoolean()).data().get("redrive");
        assertEquals(List.of("false", String.valueOf(before.get("events").asLong()), "0", "true"),
                List.of(redrive.get("running").asText(), redrive.get("redriven").asText(), redrive.get("remaining").asText(), String.valueOf(redrive.get("error").isNull())));

        call("restore_schema");
        call("unpatch_payments");
    }

    void aBadRateIsRefused() {
        assertTrue(call("set_rate", Map.of("perSecond", 900)).isError());
    }
}
