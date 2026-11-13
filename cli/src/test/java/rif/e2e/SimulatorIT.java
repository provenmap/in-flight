package rif.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static rif.e2e.E2e.control;
import static rif.e2e.E2e.sleep;
import static rif.e2e.E2e.state;

import java.util.Map;
import java.util.TreeMap;

import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import com.fasterxml.jackson.databind.JsonNode;

import rif.kafka.Kafka;
import rif.simulator.Simulator;

/**
 * Milestone 1, ported from test/e2e/simulator.test.ts: the simulator jams on OrderPlaced v2, a redrive before the patch
 * moves nothing, and patch plus redrive recovers every dead letter.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SimulatorIT {

    static E2e.OwnKafka kafka;
    static Simulator sim;
    static Admin admin;

    static Map<String, Long> redriveOffsets() {
        return new TreeMap<>(Kafka.committed(admin, "payments-service-redrive"));
    }

    @BeforeAll
    static void start() {
        kafka = E2e.OwnKafka.start();
        String bootstrap = kafka.bootstrap;
        sim = Simulator.start(Simulator.Options.fromEnv().withBootstrap(bootstrap).withPort(28090).withRate(20));
        admin = Kafka.admin(bootstrap);
        E2e.settle(sim.url());
    }

    @AfterAll
    static void stop() throws Exception {
        if (sim != null) {
            sim.close();
        }
        if (admin != null) {
            admin.close();
        }
        if (kafka != null) {
            kafka.close();
        }
    }

    @Test
    @Order(1)
    void breakingTheSchemaJamsPaymentsAndOnlyPayments() {
        JsonNode before = state(sim.url()).get("services");
        control(sim.url(), "break");
        sleep(4000);
        control(sim.url(), "restore");
        sleep(1500);
        JsonNode after = state(sim.url()).get("services");
        assertTrue(after.get("payments").get("deadLettered").asLong() - before.get("payments").get("deadLettered").asLong() > 20, after.toString());
        assertEquals(before.get("inventory").get("deadLettered").asLong(), after.get("inventory").get("deadLettered").asLong());
        assertTrue(after.get("inventory").get("processed").asLong() - before.get("inventory").get("processed").asLong() > 40, after.toString());
    }

    @Test
    @Order(2)
    void aRedriveBeforeThePatchStopsAtTheFirstUnreadableRecordAndMovesNoOffsets() {
        Map<String, Long> offsets = redriveOffsets();
        assertEquals(200, control(sim.url(), "redrive").status());
        assertEquals(409, control(sim.url(), "redrive").status());
        JsonNode r = E2e.waitForRedrive(sim.url());
        assertEquals(0, r.get("redriven").asLong());
        assertTrue(r.get("remaining").asLong() > 20, r.toString());
        assertTrue(r.get("error").asText().contains("'total' is an object, expected a decimal string"), r.toString());
        assertEquals(offsets, redriveOffsets());
    }

    @Test
    @Order(3)
    void patchThenRedriveEveryDeadLetterIsRedrivenAndTheGroupIsAtTheEndOfTheQueue() {
        control(sim.url(), "patch");
        assertEquals(200, control(sim.url(), "redrive").status());
        JsonNode r = E2e.waitForRedrive(sim.url());
        assertEquals(0, r.get("remaining").asLong());
        assertTrue(r.get("error").isNull(), r.toString());
        assertTrue(r.get("redriven").asLong() > 20, r.toString());
        Map<String, Long> ends = new TreeMap<>(Kafka.ends(admin, Kafka.partitions(admin, java.util.List.of("payments.dlq"))));
        assertEquals(ends, redriveOffsets());
        control(sim.url(), "unpatch");
    }

    @Test
    @Order(4)
    void theControlApiRefusesWhatItDoesNotKnow() {
        assertEquals(405, E2e.http("GET", sim.url() + "/break").status());
        assertEquals(404, E2e.http("GET", sim.url() + "/nope").status());
        assertEquals("no such endpoint /nope", E2e.http("GET", sim.url() + "/nope").body().get("error").asText());
        assertEquals(400, E2e.http("POST", sim.url() + "/rate?perSecond=900").status());
        assertEquals(20, control(sim.url(), "rate?perSecond=20").body().get("rate").asInt());
    }
}
