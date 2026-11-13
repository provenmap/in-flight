package rif.e2e;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import com.fasterxml.jackson.databind.JsonNode;

import rif.catalog.Catalog;
import rif.kafka.Kafka;
import rif.server.Server;

/**
 * Watch mode, ported from test/e2e/watch.test.ts: a flow of its own on topics of its own, written the way other producers
 * write, with no simulator. Plain JSON events, Kafka Connect's dead-letter headers, a recovery with no redrive marker,
 * and a dead letter no version in the catalog can price, recovered when its redrive group commits past it.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class WatchIT {

    static final String RUN = "w" + Long.toString(System.currentTimeMillis(), 36);

    static String t(String name) {
        return RUN + "." + name;
    }

    static E2e.OwnKafka kafka;
    static Server server;
    static KafkaProducer<String, byte[]> writer;
    static E2e.Viewer viewer;
    static String bootstrap;

    static JsonNode stats() {
        JsonNode s = viewer.stats();
        return s == null ? rif.catalog.Json.object() : s;
    }

    static long stuck(String field) {
        return stats().path("stuck").path(field).asLong(-1);
    }

    @BeforeAll
    static void start() {
        kafka = E2e.OwnKafka.start();
        bootstrap = kafka.bootstrap;
        Catalog catalog = Catalog.parse("""
                {"flow": "checkout", "currency": "USD",
                 "districts": [{"id": "cart", "name": "Cart"}, {"id": "billing", "name": "Billing"}, {"id": "buyers", "name": "Buyers", "sink": true}],
                 "events": [
                   {"type": "CartCheckedOut", "version": 1, "topic": "%1$s.carts", "district": "cart", "value": "price", "customer": "buyer", "order": "cartId"},
                   {"type": "InvoicePaid", "version": 1, "topic": "%1$s.invoices", "district": "billing", "value": "paid", "customer": "buyer", "order": "cartId"}],
                 "consumers": [
                   {"group": "%1$s-billing", "district": "billing", "reads": ["%1$s.carts"], "dlq": "%1$s.billing.dlq", "redrive": "%1$s-billing-redrive"},
                   {"district": "buyers", "reads": ["%1$s.invoices"]}]}
                """.formatted(RUN)).value();
        try (Admin admin = Kafka.admin(bootstrap)) {
            Kafka.ensureTopics(admin, List.of(t("carts"), t("invoices"), t("billing.dlq")), 1);
        }
        writer = Kafka.producer(bootstrap);
        Server.Options o = Server.Options.fromEnv();
        server = Server.start(o.withPort(58088).withObserver(o.observer().withBootstrap(bootstrap).withCatalog(catalog).withSimulator(null)));
        viewer = new E2e.Viewer(server.url());
        viewer.until(v -> !v.of("snapshot").isEmpty());
    }

    @AfterAll
    static void stop() throws Exception {
        if (viewer != null) {
            viewer.close();
        }
        if (server != null) {
            server.close();
        }
        if (writer != null) {
            writer.close();
        }
        try (Admin admin = Kafka.admin(bootstrap)) {
            Kafka.deleteTopics(admin, List.of(t("carts"), t("invoices"), t("billing.dlq")));
        } catch (RuntimeException ignored) {
            // best effort
        }
        if (kafka != null) {
            kafka.close();
        }
    }

    static void send(String topic, String key, String value, Map<String, String> headers) {
        RecordHeaders hs = new RecordHeaders();
        headers.forEach((k, v) -> hs.add(k, v.getBytes(UTF_8)));
        writer.send(new ProducerRecord<>(topic, null, key, value.getBytes(UTF_8), hs));
        writer.flush();
    }

    @Test
    @Order(1)
    void theSnapshotSaysItWatchesAndTheControlsAreNotThere() {
        JsonNode snap = viewer.of("snapshot").get(0);
        assertEquals("watch", snap.get("mode").asText());
        assertEquals("checkout", snap.get("flow").get("name").asText());
        assertTrue(snap.get("stats").get("sim").isNull());
        assertEquals(404, E2e.http("GET", server.url() + "/api/sim/state").status());
    }

    @Test
    @Order(2)
    void plainJsonFlowsAKafkaConnectDeadLetterIsStuckAndTheDistrictProducingForThatOrderRecoversIt() {
        send(t("carts"), "k1", "{\"cartId\":\"k1\",\"buyer\":\"b-k1\",\"price\":\"25.00\"}", Map.of());
        send(t("billing.dlq"), "k2", "{\"cartId\":\"k2\",\"buyer\":\"b-k2\",\"price\":\"40.00\"}",
                Map.of("__connect.errors.topic", t("carts"), "__connect.errors.exception.message", "Unknown currency"));
        viewer.until(v -> v.of("event").stream().anyMatch(m -> m.get("orderId").asText().equals("k1")) && stuck("events") == 1);
        JsonNode dead = viewer.of("dead").get(0);
        assertEquals(List.of("CartCheckedOut@v1", "billing", "Unknown currency", "true"),
                List.of(dead.get("event").asText(), dead.get("district").asText(), dead.get("error").asText(), dead.get("priced").asText()));
        assertEquals(40, dead.get("value").asDouble());
        assertEquals(40, stats().get("atRisk").get("value").asDouble());

        send(t("invoices"), "k2", "{\"cartId\":\"k2\",\"buyer\":\"b-k2\",\"paid\":\"40.00\"}", Map.of());
        viewer.until(v -> stuck("events") == 0);
        assertTrue(viewer.of("recovered").stream().anyMatch(m -> m.get("id").asText().equals(dead.get("id").asText())));
        assertEquals(40, stats().get("lastEpisode").get("value").asDouble());
    }

    @Test
    @Order(3)
    void aDeadLetterNothingInTheCatalogCanPriceIsStuckWithItsValueUnknownUntilTheRedriveGroupPassesIt() {
        send(t("billing.dlq"), "k3", "not json at all", Map.of("__connect.errors.exception.message", "Bad bytes"));
        viewer.until(v -> stuck("unpriced") == 1);
        assertEquals(0, stats().get("atRisk").get("value").asDouble());
        assertEquals(1, stats().get("atRisk").get("unpriced").asLong());
        List<JsonNode> deads = viewer.of("dead");
        JsonNode dead = deads.get(deads.size() - 1);
        assertEquals(List.of("an unreadable record", "false"), List.of(dead.get("event").asText(), dead.get("priced").asText()));

        // A redrive tool of one's own, as far as the observer can tell: its group commits past the dead letter.
        String id = dead.get("id").asText();
        long offset = Long.parseLong(id.substring(id.lastIndexOf(':') + 1));
        try (KafkaConsumer<byte[], byte[]> redrive = Kafka.consumer(bootstrap, RUN + "-billing-redrive")) {
            redrive.commitSync(Map.of(new TopicPartition(t("billing.dlq"), 0), new OffsetAndMetadata(offset + 1)));
        }
        viewer.until(v -> stuck("events") == 0);
        assertTrue(viewer.of("recovered").stream().anyMatch(m -> m.get("id").asText().equals(id)));
    }
}
