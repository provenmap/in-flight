package rif.e2e;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static rif.e2e.E2e.control;
import static rif.e2e.E2e.sleep;
import static rif.e2e.E2e.state;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import com.fasterxml.jackson.databind.JsonNode;

import rif.kafka.Kafka;
import rif.router.Router;
import rif.server.Server;
import rif.simulator.Simulator;

/**
 * Phase 4: subscribe by intent. The router materialises each declared intent as a topic, the services read them through
 * the proxy as {@code intent.<name>} (payments reads {@code intent.order-total-changes}), the observer counts lag on
 * them, and the incident demo still passes: break, jam, patch, redrive, back to $0.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class IntentsIT {

    static Stack stack;
    static Router router;
    static Simulator sim;
    static Server server;

    @BeforeAll
    static void start() throws Exception {
        stack = Stack.start();
        String bootstrap = stack.bootstrap();
        Path state = Files.createTempDirectory("rif-router");
        Map<String, String> env = new HashMap<>();
        env.put("BOOTSTRAP_SERVERS", bootstrap);
        env.put("STATE_DIR", state.toString());
        env.put("ROUTER_PORT", "28085");
        router = Router.start(Router.Config.fromEnv(env).withSeedCatalog(Path.of("catalog/order-to-cash.json")));
        sim = Simulator.start(Simulator.Options.fromEnv().withBootstrap(bootstrap).withPort(28091).withIntentsVia(stack.proxy()));
        Server.Options o = Server.Options.fromEnv();
        server = Server.start(o.withPort(28089).withObserver(o.observer().withBootstrap(bootstrap).withSimulator(sim.url())));
    }

    @AfterAll
    static void stop() throws Exception {
        if (server != null) {
            server.close();
        }
        if (sim != null) {
            sim.close();
        }
        if (router != null) {
            router.close();
        }
        if (stack != null) {
            stack.close();
        }
    }

    @Test
    @Order(1)
    void eachServiceReadsByItsIntentThroughTheProxyAndTheFlowRuns() {
        JsonNode services = state(sim.url()).get("services");
        assertEquals("[\"intent.order-total-changes\"]", services.get("payments").get("reads").toString());
        assertEquals("[\"intent.orders-placed\"]", services.get("inventory").get("reads").toString());
        assertEquals("[\"intent.ready-to-ship\"]", services.get("shipping").get("reads").toString());
        E2e.until(() -> {
            JsonNode s = state(sim.url()).get("services");
            return s.get("payments").get("processed").asLong() > 10 && s.get("shipping").get("processed").asLong() > 10;
        }, 90_000, () -> state(sim.url()).toString());
        JsonNode health = E2e.http("GET", "http://localhost:28085/health").body();
        assertTrue(health.toString().contains("order-total-changes"), health.toString());
    }

    @Test
    @Order(2)
    void theIntentTopicCarriesTheOriginalEventWithItsProvenance() {
        try (KafkaConsumer<byte[], byte[]> c = Kafka.consumer(stack.bootstrap(), null)) {
            c.subscribe(List.of("__intent.order-total-changes"));
            ConsumerRecord<byte[], byte[]> got = null;
            long end = System.currentTimeMillis() + 30_000;
            while (got == null && System.currentTimeMillis() < end) {
                for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofMillis(500))) {
                    got = r;
                    break;
                }
            }
            assertTrue(got != null, "nothing on the intent topic");
            Map<String, String> h = Kafka.headers(got.headers());
            assertEquals("order-total-changes", h.get("ib-intent"));
            assertEquals("orders.placed", h.get("ib-source-topic"));
            String body = new String(got.value(), UTF_8);
            assertTrue(body.contains("\"type\":\"OrderPlaced\""), body);
            assertEquals(new String(got.key(), UTF_8), h.get("ib-entity"));
        }
    }

    @Test
    @Order(3)
    void theObserverCountsLagOnTheIntentTopic() {
        try (E2e.Viewer v = new E2e.Viewer(server.url())) {
            v.until(x -> x.stats() != null && x.stats().get("intents").has("order-total-changes"), 30_000);
            JsonNode snap = v.first();
            assertTrue(snap.get("flow").get("intents").toString().contains("\"intent\":\"order-total-changes\""));
            assertEquals("__intent.order-total-changes", v.stats().get("intents").get("order-total-changes").get("topic").asText());
        }
    }

    @Test
    @Order(5)
    void anIntentDeclaredAtRuntimeSeesTheEventsThatAlreadyExist() throws Exception {
        // intent-bus ran every intent in one Streams application, so a new one started from that application's offsets.
        var put = java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://localhost:28085/intents/payment-decisions"))
                .header("Content-Type", "application/json")
                .PUT(java.net.http.HttpRequest.BodyPublishers.ofString("{\"predicate\":\"changes(Order.payment)\"}")).build();
        var res = E2e.HTTP.send(put, java.net.http.HttpResponse.BodyHandlers.ofString());
        assertEquals(200, res.statusCode(), res.body());
        Instant declared = Instant.now();
        try (KafkaConsumer<byte[], byte[]> c = Kafka.consumer(stack.bootstrap(), null)) {
            E2e.until(() -> Kafka.listTopics(Kafka.admin(stack.bootstrap())).contains("__intent.payment-decisions"), 60_000, () -> "no topic");
            c.subscribe(List.of("__intent.payment-decisions"));
            long older = 0;
            long end = System.currentTimeMillis() + 60_000;
            while (older == 0 && System.currentTimeMillis() < end) {
                for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofMillis(500))) {
                    if (Instant.ofEpochMilli(r.timestamp()).isBefore(declared.minusSeconds(2))) {
                        older++;
                    }
                }
            }
            assertTrue(older > 0, "no event from before the intent was declared reached it");
        }
    }

    @Test
    @Order(6)
    void rehearsingPaymentsPatchedBuildOnThePastShowsItWouldHaveChargedWhatTheStrictOneDeadLettered() {
        // The incident (order 4) put OrderPlaced v2 on intent.order-total-changes; payments now runs strict again.
        E2e.Reply r = E2e.http("POST", sim.url() + "/rehearse?intent=order-total-changes&build=patched&minutes=30");
        assertEquals(200, r.status(), r.body().toString());
        JsonNode report = r.body();
        assertEquals("strict", report.get("baseline").asText());
        assertTrue(report.get("events").asInt() > 10, report.toString());
        assertTrue(report.get("baselineCounts").path("dead-lettered").asInt() > 0, report.toString());
        assertEquals(0, report.get("candidateCounts").path("dead-lettered").asInt(0), report.toString());
        assertEquals(report.get("baselineCounts").path("dead-lettered").asInt(), report.get("differ").asInt());
        JsonNode d = report.get("differences").get(0);
        assertEquals("OrderPlaced@v2", d.get("event").asText());
        assertEquals("dead-lettered", d.get("baseline").get("result").asText());
        assertTrue(Kafka.listTopics(Kafka.admin(stack.bootstrap())).contains(report.get("branchTopic").asText()));
        assertEquals(409, E2e.http("POST", sim.url() + "/rehearse?intent=orders-placed&build=patched&minutes=30").status());
    }

    @Test
    @Order(4)
    void theIncidentDemoStillPassesReadingByIntent() {
        try (E2e.Viewer v = new E2e.Viewer(server.url())) {
            v.until(x -> x.stats() != null);
            control(sim.url(), "rate?perSecond=20");
            control(sim.url(), "break");
            sleep(4000);
            control(sim.url(), "restore");
            // The router holds each event for its grace window, so dead letters arrive a little later than they did.
            v.until(x -> x.stats().get("stuck").get("events").asLong() > 10 && x.stats().get("stuck").get("events").asLong() == x.of("dead").size()
                    && x.stats().get("sim").get("services").get("payments").get("deadLettered").asLong() == x.of("dead").size(), 60_000);
            sleep(3000);
            List<JsonNode> dead = v.of("dead");
            assertEquals(dead.size(), v.stats().get("stuck").get("events").asLong());
            assertEquals("OrderPlaced@v2", dead.get(0).get("event").asText());
            assertTrue(v.stats().get("atRisk").get("value").asDouble() > 0);

            control(sim.url(), "patch");
            control(sim.url(), "redrive");
            v.until(x -> x.stats().get("stuck").get("events").asLong() == 0 && !x.stats().get("lastEpisode").isNull(), 90_000);
            assertEquals(dead.size(), v.stats().get("lastEpisode").get("events").asLong());
            control(sim.url(), "rate?perSecond=8");
            v.until(x -> x.stats().get("atRisk").get("value").asDouble() == 0, 60_000);
            control(sim.url(), "unpatch");
        }
    }
}
