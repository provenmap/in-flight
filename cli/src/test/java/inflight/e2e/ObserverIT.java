package inflight.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static inflight.e2e.E2e.control;
import static inflight.e2e.E2e.sleep;
import static inflight.e2e.E2e.state;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import com.fasterxml.jackson.databind.JsonNode;

import inflight.kafka.Kafka;
import inflight.server.Server;
import inflight.simulator.Simulator;

/**
 * Milestone 3, ported from test/e2e/observer.test.ts: money at risk as a viewer of the WebSocket sees it. A healthy flow
 * reads $0, a jam counts every stuck order, patch and redrive recover them all into one episode, a restarted observer
 * counts them as recovered, and at 500 orders a second the observer keeps up.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ObserverIT {

    static E2e.OwnKafka kafka;
    static Simulator sim;
    static Server server;
    static String bootstrap;

    static Server startServer() {
        Server.Options o = Server.Options.fromEnv();
        return Server.start(o.withPort(38088).withObserver(o.observer().withBootstrap(bootstrap).withSimulator("http://localhost:38090")));
    }

    @BeforeAll
    static void start() {
        kafka = E2e.OwnKafka.start();
        bootstrap = kafka.bootstrap;
        sim = Simulator.start(Simulator.Options.fromEnv().withBootstrap(bootstrap).withPort(38090));
        E2e.settle(sim.url());
        server = startServer();
    }

    @AfterAll
    static void stop() throws Exception {
        if (server != null) {
            server.close();
        }
        if (sim != null) {
            sim.close();
        }
        if (kafka != null) {
            kafka.close();
        }
    }

    @Test
    @Order(1)
    void aNewViewerGetsASnapshotFirstWithTheFlow() {
        try (E2e.Viewer v = new E2e.Viewer(server.url())) {
            v.until(x -> x.size() > 0);
            JsonNode snap = v.first();
            assertEquals("snapshot", snap.get("type").asText());
            assertEquals("[\"orders\",\"payments\",\"inventory\",\"shipping\",\"customers\"]",
                    Json(snap.get("flow").get("districts").findValuesAsText("id")));
            JsonNode placed = null;
            for (JsonNode t : snap.get("flow").get("topics")) {
                if (t.get("topic").asText().equals("orders.placed")) {
                    placed = t;
                }
            }
            assertEquals("[\"payments\",\"inventory\"]", placed.get("to").toString());
        }
    }

    static String Json(List<String> xs) {
        return inflight.catalog.Json.write(xs);
    }

    @Test
    @Order(2)
    void aHealthyFlowReadsZeroAtRiskNothingStuck() {
        control(sim.url(), "rate?perSecond=8");
        try (E2e.Viewer v = new E2e.Viewer(server.url())) {
            v.until(x -> x.of("stats").size() >= 12 && x.of("event").size() > 20);
            List<JsonNode> stats = v.of("stats").subList(2, v.of("stats").size());
            assertTrue(stats.stream().allMatch(s -> s.get("stuck").get("events").asLong() == 0));
            // Lag is bounded by the polled end, so a flowing pipeline mostly reads exactly $0.
            assertTrue(stats.stream().filter(s -> s.get("atRisk").get("value").asDouble() == 0).count() >= stats.size() / 2.0);
            v.until(x -> x.of("event").stream().anyMatch(e -> e.get("topic").asText().equals("shipping.dispatched")));
            JsonNode dispatched = v.of("event").stream().filter(e -> e.get("topic").asText().equals("shipping.dispatched")).findFirst().orElseThrow();
            assertEquals("[\"customers\"]", dispatched.get("to").toString());
        }
    }

    @Test
    @Order(3)
    void aJamCountsEveryStuckOrderPatchAndRedriveRecoverThemAllInOneEpisode() {
        try (E2e.Viewer v = new E2e.Viewer(server.url())) {
            v.until(x -> x.stats() != null);
            control(sim.url(), "rate?perSecond=20");
            control(sim.url(), "break");
            sleep(4000);
            control(sim.url(), "restore");
            v.until(x -> x.stats().get("stuck").get("events").asLong() > 0 && x.stats().get("stuck").get("events").asLong() == x.of("dead").size()
                    && x.stats().get("sim").get("services").get("payments").get("processed").asLong() > 0);
            sleep(1500);
            JsonNode jammed = v.stats();
            List<JsonNode> dead = v.of("dead");
            assertTrue(dead.size() > 20, "dead " + dead.size());
            assertEquals(dead.size(), jammed.get("stuck").get("events").asLong());
            double deadValue = dead.stream().mapToDouble(d -> d.get("value").asDouble()).sum();
            assertEquals(deadValue, jammed.get("stuck").get("byDistrict").get("payments").asDouble(), 0.01);
            assertTrue(jammed.get("atRisk").get("value").asDouble() >= jammed.get("stuck").get("value").asDouble() - 0.005);
            JsonNode d0 = dead.get(0);
            assertEquals(List.of("payments.dlq", "payments", "OrderPlaced@v2", "'total' is an object, expected a decimal string"),
                    List.of(d0.get("dlq").asText(), d0.get("district").asText(), d0.get("event").asText(), d0.get("error").asText()));

            control(sim.url(), "patch");
            control(sim.url(), "redrive");
            v.until(x -> x.of("recovered").size() == dead.size() && x.stats().get("stuck").get("events").asLong() == 0
                    && !x.stats().get("lastEpisode").isNull(), 60_000);
            JsonNode done = v.stats();
            assertEquals(0, done.get("stuck").get("value").asDouble());
            assertEquals(dead.size(), done.get("lastEpisode").get("events").asLong());
            assertEquals(jammed.get("stuck").get("value").asDouble(), done.get("lastEpisode").get("value").asDouble(), 0.01);
            Set<String> recovered = new HashSet<>();
            v.of("recovered").forEach(r -> recovered.add(r.get("id").asText()));
            Set<String> deadIds = new HashSet<>();
            dead.forEach(d -> deadIds.add(d.get("id").asText()));
            assertEquals(deadIds, recovered);
            assertTrue(v.of("recovered").stream().allMatch(r -> r.get("afterMs").asLong() > 0));
            assertEquals(dead.size(), v.of("event").stream().filter(e -> e.get("redrive").asBoolean()).count());
            control(sim.url(), "rate?perSecond=8");
            v.until(x -> x.stats().get("atRisk").get("value").asDouble() == 0);
            control(sim.url(), "unpatch");
        }
    }

    @Test
    @Order(4)
    void aRestartedObserverCountsEveryDeadLetterAsRecoveredAndNothingAsStuck() throws Exception {
        server.close();
        server = startServer();
        long all;
        try (Admin admin = Kafka.admin(bootstrap)) {
            all = Kafka.ends(admin, Kafka.partitions(admin, List.of("payments.dlq"))).values().stream().mapToLong(Long::longValue).sum();
        }
        try (E2e.Viewer v = new E2e.Viewer(server.url())) {
            v.until(x -> x.size() > 0);
            v.until(x -> (x.stats() != null ? x.stats() : x.first().get("stats")).get("recovered").get("events").asLong() >= all);
            JsonNode s = v.stats() != null ? v.stats() : v.first().get("stats");
            assertEquals(0, s.get("stuck").get("events").asLong());
            assertEquals(all, s.get("recovered").get("events").asLong());
            assertEquals(0, v.first().get("stuck").size());
            assertEquals(0, v.of("dead").size());
            assertTrue(v.of("recovered").stream().allMatch(r -> r.get("afterMs").isNull()));
        }
    }

    @Test
    @Order(5)
    void atFiveHundredOrdersASecondTheObserverKeepsUp() {
        try (E2e.Viewer v = new E2e.Viewer(server.url())) {
            v.until(x -> x.stats() != null);
            long placedBefore = state(sim.url()).get("placed").asLong();
            control(sim.url(), "rate?perSecond=500");
            int from = v.size();
            sleep(5000);
            control(sim.url(), "rate?perSecond=0");
            long placed = state(sim.url()).get("placed").asLong() - placedBefore;
            sleep(1500);
            List<JsonNode> after = v.since(from);
            long seen = after.stream().filter(m -> m.get("type").asText().equals("event") && m.get("topic").asText().equals("orders.placed")).count();
            long statsFrames = after.stream().filter(m -> m.get("type").asText().equals("stats")).count();
            System.out.printf("500/s for 5 s: %d orders placed, %d seen by the viewer, %d stats messages%n", placed, seen, statsFrames);
            assertTrue(placed > 2000, "placed " + placed);
            assertTrue(seen >= placed * 0.95, "seen " + seen + " of " + placed);
            assertTrue(statsFrames >= 10);
            // Payments charges one order at a time, so the flood is real lag, valued from what the observer saw.
            assertTrue(v.of("stats").stream().anyMatch(s -> s.get("lag").get("byGroup").get("payments-service").get("events").asLong() > 500
                    && s.get("lag").get("byGroup").get("payments-service").get("value").asDouble() > 0));

            // Leave the pipeline drained for whoever runs next.
            for (int i = 0; i < 150; i++) {
                JsonNode s = state(sim.url());
                JsonNode p = s.get("services").get("payments");
                if (p.get("processed").asLong() + p.get("deadLettered").asLong() >= s.get("placed").asLong()) {
                    break;
                }
                sleep(1000);
            }
            control(sim.url(), "rate?perSecond=8");
        }
    }
}
