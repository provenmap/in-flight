package inflight.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static inflight.e2e.E2e.control;
import static inflight.e2e.E2e.sleep;
import static inflight.e2e.E2e.state;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import com.fasterxml.jackson.databind.JsonNode;

import inflight.cli.Demo;

/**
 * Phase 5: contracts in the loop. One schema change, OrderPlaced v2, shipped two ways on the whole stack. Around the
 * catalog it jams payments and has to be recovered; through the catalog the gate holds it until payments' regenerated
 * binding merges and the change is approved, and then it ships with nothing stuck.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TwoWaysIT {

    static Stack stack;
    static Demo demo;
    static String sim;
    static String app;

    @BeforeAll
    static void start() throws Exception {
        Path dir = Files.createTempDirectory("in-flight-demo");
        stack = Stack.start();
        demo = Demo.start(new Demo.Options(dir, stack.bootstrap(), stack.proxy(), stack.registry(), 28185, 28190, 28188, "localhost"));
        sim = demo.simulator().url();
        app = demo.server().url();
        E2e.until(() -> state(sim).get("services").get("payments").get("processed").asLong() > 5, 90_000, () -> state(sim).toString());
    }

    @AfterAll
    static void stop() throws Exception {
        if (demo != null) {
            demo.close();
        }
        if (stack != null) {
            stack.close();
        }
    }

    static JsonNode stats(E2e.Viewer v) {
        return v.stats();
    }

    @Test
    @Order(1)
    void aroundTheCatalogTheChangeJamsPaymentsAndHasToBeRecovered() {
        try (E2e.Viewer v = new E2e.Viewer(app)) {
            v.until(x -> x.stats() != null);
            control(sim, "rate?perSecond=20");
            control(sim, "break");
            sleep(4000);
            control(sim, "restore");
            v.until(x -> x.stats().get("stuck").get("events").asLong() > 10 && x.stats().get("stuck").get("events").asLong() == x.of("dead").size()
                    && x.stats().get("sim").get("services").get("payments").get("deadLettered").asLong() == x.of("dead").size(), 60_000);
            JsonNode dead = v.of("dead").get(0);
            assertEquals("OrderPlaced@v2", dead.get("event").asText());
            // The two facts the catalog adds: the intent payments reads by, and that its impact predicted this.
            assertEquals("order-total-changes", dead.get("intent").asText());
            assertEquals("predicted", dead.get("prediction").asText());
            long jammed = v.stats().get("stuck").get("events").asLong();

            control(sim, "patch");
            control(sim, "redrive");
            v.until(x -> x.stats().get("stuck").get("events").asLong() == 0 && !x.stats().get("lastEpisode").isNull(), 90_000);
            assertEquals(jammed, v.stats().get("lastEpisode").get("events").asLong());
            control(sim, "unpatch");
            control(sim, "rate?perSecond=8");
        }
    }

    @Test
    @Order(2)
    void throughTheCatalogTheGateHoldsTheChangeUntilPaymentsMergesThenItShipsWithNothingStuck() {
        // Propose: payments must change, inventory need not; payments' binding is regenerated on a branch.
        E2e.Reply proposed = E2e.http("POST", app + "/api/changes");
        assertEquals(200, proposed.status(), proposed.body().toString());
        assertTrue(!proposed.body().get("refused").asBoolean(), proposed.body().toString());
        JsonNode change = proposed.body().get("change");
        String id = change.get("id").asText();
        assertEquals("pending", change.get("status").asText());
        assertEquals(List.of("payments-service"), change.get("migrations").findValuesAsText("consumer"));
        assertTrue(change.get("migrations").get(0).get("diff").asText().contains("OrderPlacedV2"), "the migration's diff is the regenerated binding");
        assertEquals("refused", change.get("gate").get(0).get("gate").asText());

        // The orders team ships v2 through the gate: refused, so v2 never reaches anyone and nothing gets stuck.
        try (E2e.Viewer v = new E2e.Viewer(app)) {
            v.until(x -> x.stats() != null);
            long stuckBefore = v.stats().get("stuck").get("events").asLong();
            long v2Before = state(sim).get("placedV2").asLong();
            long refusedBefore = state(sim).get("refusedByGate").asLong();
            assertEquals(200, control(sim, "gate?on=true").status());
            control(sim, "rate?perSecond=20");
            control(sim, "break");
            sleep(5000);
            JsonNode s = state(sim);
            assertTrue(s.get("refusedByGate").asLong() - refusedBefore > 10, s.toString());
            assertEquals(v2Before, s.get("placedV2").asLong(), s.toString());
            assertEquals(stuckBefore, v.stats().get("stuck").get("events").asLong());

            // Approval is refused while payments' migration is open.
            E2e.Reply early = E2e.http("POST", app + "/api/changes/" + id + "/approve");
            assertTrue(early.body().toString().contains("not merged") || !early.body().path("approved").asBoolean(true), early.body().toString());

            // Payments merges its migration and deploys; the change is approved; v2 is registered and flows.
            assertEquals(200, E2e.http("POST", app + "/api/changes/" + id + "/merge").status());
            E2e.Reply approved = E2e.http("POST", app + "/api/changes/" + id + "/approve");
            assertEquals(200, approved.status(), approved.body().toString());
            assertTrue(approved.body().get("approved").asBoolean(), approved.body().toString());
            assertEquals("approved", approved.body().get("change").get("status").asText());

            E2e.until(() -> state(sim).get("placedV2").asLong() - v2Before > 20, 60_000, () -> state(sim).toString());
            sleep(5000);
            JsonNode after = state(sim);
            assertEquals(stuckBefore, v.stats().get("stuck").get("events").asLong(), "nothing got stuck");
            assertTrue(after.get("services").get("payments").get("processed").asLong() > 0);
            control(sim, "restore");
            control(sim, "gate?on=false");
            control(sim, "rate?perSecond=8");
        }
    }
}
