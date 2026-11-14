package inflight.router;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.Test;

import inflight.catalog.Intent;
import inflight.catalog.Json;

/** Configuration from the environment, and the small encodings the router keeps: registry intents and routed events. */
class RouterConfigTest {

    @Test
    void defaults() {
        Router.Config c = Router.Config.fromEnv(Map.of());
        assertEquals("localhost:9092", c.bootstrapServers());
        assertEquals("in-flight-router", c.applicationId());
        assertEquals(8085, c.port());
        assertEquals(Duration.ofMillis(1500), c.grace());
        assertEquals(Duration.ofMillis(250), c.tick());
        assertEquals(Path.of(System.getProperty("java.io.tmpdir"), "in-flight-router"), c.stateDir());
        assertEquals("at_least_once", c.processingGuarantee());
        assertEquals(3, c.partitions());
        assertEquals(1, c.replication());
        assertNull(c.seedCatalog());
    }

    @Test
    void fromTheEnvironment() {
        Router.Config c = Router.Config.fromEnv(Map.of("APPLICATION_ID", "r", "ROUTER_PORT", "9000", "PROCESSING_GUARANTEE", "exactly_once_v2",
                "STATE_DIR", "/var/r", "SEED_CATALOG", "catalog/order-to-cash.json", "CONTROL_PORT", "1"));
        assertEquals(9000, c.port());
        assertEquals("r-orders", c.streams().applicationIdFor("orders"));
        assertEquals(Path.of("/var/r/orders"), c.streams().stateDirFor("orders"));
        assertEquals("exactly_once_v2", c.streams().propertiesFor("orders").get("processing.guarantee"));
        assertEquals(Path.of("catalog/order-to-cash.json"), c.seedCatalog());
    }

    @Test
    void anIntentRoundTripsThroughTheRegistryEncoding() {
        Intent i = Intent.parse("ready", "emits(PaymentAuthorized) or emits(StockReserved)", "Order").value();
        assertEquals(i, Json.parse(Registry.intentJson(i)).flatMap(Registry::readIntent).value());
    }

    @Test
    void aRoutedEventRoundTrips() {
        Routed r = new Routed("i", 3, "o-1", 1_790_000_000_000L, 7, "e", "orders.placed", 2, 99, "{\"a\":1}");
        assertEquals(r, Routed.fromJson(r.toJson()));
        assertEquals(r, Routed.SERDE.deserializer().deserialize("t", Routed.SERDE.serializer().serialize("t", r)));
    }

    @Test
    void releaseOrderKeysSortByEventTimeThenEntityThenSequence() {
        Routed a = new Routed("i", 1, "o-1", 5, 2, "x", "t", 0, 0, "");
        Routed b = new Routed("i", 1, "o-1", 5, 10, "a", "t", 0, 0, "");
        Routed c = new Routed("i", 1, "o-1", 40, 0, "a", "t", 0, 0, "");
        assertEquals(-1, Integer.signum(OrderingProcessor.bufferKey(a).compareTo(OrderingProcessor.bufferKey(b))));
        assertEquals(-1, Integer.signum(OrderingProcessor.bufferKey(b).compareTo(OrderingProcessor.bufferKey(c))));
        assertEquals(-1, Integer.signum(OrderingProcessor.bufferKey(b).compareTo(OrderingProcessor.upTo(5))));
        assertEquals(1, Integer.signum(OrderingProcessor.bufferKey(c).compareTo(OrderingProcessor.upTo(39))));
    }
}
