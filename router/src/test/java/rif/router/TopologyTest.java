package rif.router;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;

import rif.catalog.Catalog;
import rif.catalog.Catalog.EventSpec;
import rif.catalog.EventRef;
import rif.catalog.Intent;
import rif.catalog.IntentCompiler;
import rif.catalog.Json;
import rif.catalog.RoutingTable;
import rif.router.RouterStats.Counter;

/** The topology, driven without Kafka: ordering, late marking, provenance, and what is skipped. Ported from TopologySuite. */
class TopologyTest {

    static final Catalog REV1 = Catalog.load(Path.of("catalog/src/test/resources/intent-bus/rev1.json")).value();
    static final Instant T0 = Instant.parse("2026-10-30T12:00:00Z");
    static final Duration GRACE = Duration.ofSeconds(2);

    static RoutingTable table(String name, String predicate) {
        return IntentCompiler.compile(Intent.parse(name, predicate).value(), REV1.semantics().model(), REV1.revision()).value();
    }

    final RoutingTable totals = table("order-total-changes", "changes(Order.total)");
    final RoutingTable statuses = table("order-status", "changes(Order.status)");

    static ObjectNode obj(String... kv) {
        ObjectNode o = Json.object();
        for (int i = 0; i < kv.length; i += 2) {
            o.put(kv[i], kv[i + 1]);
        }
        return o;
    }

    static String event(String type, String id, Instant at, ObjectNode data, Long seq) {
        ObjectNode o = Json.object();
        o.put("specversion", "1.0").put("id", id).put("source", "/test").put("type", type).put("schemaversion", 1)
                .put("time", at.toString()).put("datacontenttype", "application/json");
        o.set("data", data);
        if (seq != null) {
            o.put("sequence", seq.toString());
        }
        return Json.write(o);
    }

    static String event(String type, String id, Instant at, ObjectNode data) {
        return event(type, id, at, data, null);
    }

    /** A driver over one topology, with its own counters. */
    static final class Driver implements AutoCloseable {
        final RouterStats stats = new RouterStats();
        final TopologyTestDriver driver;

        Driver(List<RoutingTable> tables, Set<EventRef> known) {
            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
            try {
                props.put(StreamsConfig.STATE_DIR_CONFIG, Files.createTempDirectory("rif-router-test").toString());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            driver = new TopologyTestDriver(IntentTopology.build(tables, new IntentTopology.Ordering(GRACE, Duration.ofMillis(100)), known, stats), props, T0);
        }

        Driver(RoutingTable... tables) {
            this(List.of(tables), Set.of());
        }

        TestInputTopic<String, String> input(String topic) {
            return driver.createInputTopic(topic, new StringSerializer(), new StringSerializer());
        }

        TestOutputTopic<String, String> output(String intent) {
            return driver.createOutputTopic(Intent.topicFor(intent), new StringDeserializer(), new StringDeserializer());
        }

        void advance(Duration d) {
            driver.advanceWallClockTime(d);
        }

        @Override
        public void close() {
            driver.close();
        }
    }

    static String typeOf(String v) {
        return Json.parse(v).value().get("type").asText();
    }

    static String idOf(String v) {
        return Json.parse(v).value().get("id").asText();
    }

    static String header(TestRecord<String, String> r, String key) {
        Header h = r.headers().lastHeader(key);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    @Test
    void threeTypesFromThreeTopicsArriveInEventTimeOrderPerOrderThroughOneIntent() {
        try (Driver f = new Driver(totals)) {
            // Arrival order differs from event time: the refund arrives first.
            f.input("billing.refunds").pipeInput("o-1", event("RefundIssued", "e3", T0.plusMillis(300), obj("refundId", "r-1", "orderId", "o-1", "amount", "10.00", "issuedAt", T0.toString())));
            f.input("orders.placed").pipeInput("o-1", event("OrderPlaced", "e1", T0.plusMillis(100), obj("orderId", "o-1", "customerId", "c-1", "total", "100.00", "currency", "EUR", "placedAt", T0.toString())));
            f.input("orders.amended").pipeInput("o-1", event("OrderAmended", "e2", T0.plusMillis(200), obj("orderId", "o-1", "newTotal", "120.00", "amendedAt", T0.toString())));

            TestOutputTopic<String, String> out = f.output("order-total-changes");
            assertTrue(out.isEmpty(), "nothing is released inside the grace window");
            f.advance(Duration.ofSeconds(3));

            List<TestRecord<String, String>> records = out.readRecordsToList();
            assertEquals(List.of("OrderPlaced", "OrderAmended", "RefundIssued"), records.stream().map(r -> typeOf(r.value())).toList());
            assertTrue(records.stream().allMatch(r -> r.key().equals("o-1")));
            TestRecord<String, String> first = records.get(0);
            assertEquals("orders.placed", header(first, IntentHeaders.SOURCE_TOPIC));
            assertEquals("1", header(first, IntentHeaders.CATALOG_REVISION));
            assertEquals("order-total-changes", header(first, IntentHeaders.INTENT));
            assertEquals("o-1", header(first, IntentHeaders.ENTITY));
            assertEquals("0", header(first, IntentHeaders.SOURCE_PARTITION));
            assertEquals("0", header(first, IntentHeaders.SOURCE_OFFSET));
            assertNull(header(first, IntentHeaders.LATE));
            assertEquals(T0.plusMillis(100).toEpochMilli(), first.timestamp().longValue());
        }
    }

    @Test
    void theRouterReadsOnlyTheTopicsSomeIntentNeeds() {
        String topo = IntentTopology.build(List.of(totals), new IntentTopology.Ordering(GRACE, Duration.ofMillis(100)), Set.of(), new RouterStats())
                .describe().toString();
        assertTrue(topo.contains("orders.placed") && !topo.contains("orders.shipped"), topo);
    }

    @Test
    void thePayloadIsTheProducersCloudEventUnchanged() {
        try (Driver f = new Driver(totals)) {
            String e = event("OrderPlaced", "e1", T0, obj("orderId", "o-9", "customerId", "c", "total", "1.00", "currency", "EUR", "placedAt", T0.toString()));
            f.input("orders.placed").pipeInput("whatever", e);
            f.advance(Duration.ofSeconds(3));
            TestRecord<String, String> r = f.output("order-total-changes").readRecord();
            assertEquals(e, r.value());
            assertEquals("o-9", r.key(), "re-keyed by the Order key, not the source key");
        }
    }

    @Test
    void tiesInEventTimeAreBrokenByTheProducersSequence() {
        try (Driver f = new Driver(totals)) {
            f.input("orders.amended").pipeInput("o-1", event("OrderAmended", "b", T0, obj("orderId", "o-1", "newTotal", "2", "amendedAt", T0.toString()), 2L));
            f.input("orders.amended").pipeInput("o-1", event("OrderAmended", "a", T0, obj("orderId", "o-1", "newTotal", "1", "amendedAt", T0.toString()), 1L));
            f.advance(Duration.ofSeconds(3));
            assertEquals(List.of("a", "b"), f.output("order-total-changes").readValuesToList().stream().map(TopologyTest::idOf).toList());
        }
    }

    @Test
    void anEventOlderThanOneAlreadyDeliveredForItsEntityIsDeliveredAtOnceMarkedLate() {
        try (Driver f = new Driver(totals)) {
            f.input("orders.amended").pipeInput("o-1", event("OrderAmended", "new", T0.plusSeconds(1), obj("orderId", "o-1", "newTotal", "2", "amendedAt", T0.toString())));
            f.advance(Duration.ofSeconds(4));
            TestOutputTopic<String, String> out = f.output("order-total-changes");
            assertEquals(1, out.readRecordsToList().size());

            f.input("billing.refunds").pipeInput("o-1", event("RefundIssued", "old", T0, obj("refundId", "r", "orderId", "o-1", "amount", "1", "issuedAt", T0.toString())));
            TestRecord<String, String> late = out.readRecord();
            assertEquals("old", idOf(late.value()));
            assertEquals("true", header(late, IntentHeaders.LATE));
            assertEquals(1L, f.stats.get(Counter.LATE));
            assertEquals(1L, f.stats.get(Counter.LATE, "order-total-changes"));
        }
    }

    @Test
    void otherEntitiesAreNotHeldBackByALateEntity() {
        try (Driver f = new Driver(totals)) {
            f.input("orders.amended").pipeInput("o-1", event("OrderAmended", "x", T0.plusSeconds(1), obj("orderId", "o-1", "newTotal", "2", "amendedAt", T0.toString())));
            f.advance(Duration.ofSeconds(4));
            f.input("orders.amended").pipeInput("o-2", event("OrderAmended", "y", T0, obj("orderId", "o-2", "newTotal", "2", "amendedAt", T0.toString())));
            f.advance(Duration.ofSeconds(1));
            TestOutputTopic<String, String> out = f.output("order-total-changes");
            out.readRecord();
            TestRecord<String, String> r = out.readRecord();
            assertEquals("o-2", r.key());
            assertNull(header(r, IntentHeaders.LATE), "o-2 has no watermark yet, so it is released normally");
        }
    }

    @Test
    void oneSourceEventFeedsEveryIntentItMatches() {
        try (Driver f = new Driver(totals, statuses)) {
            f.input("orders.placed").pipeInput("o-1", event("OrderPlaced", "e1", T0, obj("orderId", "o-1", "customerId", "c", "total", "1.00", "currency", "EUR", "placedAt", T0.toString())));
            f.input("orders.shipped").pipeInput("o-1", event("OrderShipped", "e2", T0.plusMillis(5), obj("orderId", "o-1", "carrier", "dhl", "shippedAt", T0.toString())));
            f.advance(Duration.ofSeconds(3));
            assertEquals(List.of("OrderPlaced"), f.output("order-total-changes").readValuesToList().stream().map(TopologyTest::typeOf).toList());
            assertEquals(List.of("OrderPlaced", "OrderShipped"), f.output("order-status").readValuesToList().stream().map(TopologyTest::typeOf).toList());
            assertEquals(Map.of("order-total-changes", 1L, "order-status", 2L),
                    Map.of("order-total-changes", f.stats.get(Counter.DELIVERED, "order-total-changes"), "order-status", f.stats.get(Counter.DELIVERED, "order-status")));
        }
    }

    @Test
    void unknownVersionsMissingKeysAndJunkAreCountedAndSkipped() {
        try (Driver f = new Driver(totals)) {
            TestInputTopic<String, String> in = f.input("orders.placed");
            in.pipeInput("k", event("OrderPlaced", "v2", T0, obj("orderId", "o-1")).replace("\"schemaversion\":1", "\"schemaversion\":2"));
            in.pipeInput("k", event("OrderPlaced", "nokey", T0, obj("customerId", "c")));
            in.pipeInput("k", "not json");
            f.advance(Duration.ofSeconds(3));
            assertTrue(f.output("order-total-changes").isEmpty());
            assertEquals(List.of(1L, 1L, 1L), List.of(f.stats.get(Counter.UNROUTED), f.stats.get(Counter.UNKEYED), f.stats.get(Counter.UNPARSEABLE)));
            assertEquals(1L, f.stats.get(Counter.UNPARSEABLE, "order-total-changes"));
        }
    }

    @Test
    void anUnknownVersionOfAKnownTypeWasProducedAroundTheGateCountedApartAndRoutedLikeItsTypeSoItIsNeverLost() {
        Set<EventRef> known = REV1.events().stream().map(EventSpec::ref).collect(Collectors.toSet());
        try (Driver f = new Driver(List.of(totals), known)) {
            String e = event("OrderPlaced", "e9", T0, obj("orderId", "o-1")).replace("\"schemaversion\":1", "\"schemaversion\":7");
            f.input("orders.placed").pipeInput("o-1", e);
            f.input("orders.placed").pipeInput("o-1", e.replace("e9", "e10"));
            f.input("orders.placed").pipeInput("o-1", event("OrderLost", "e11", T0, obj("orderId", "o-1")));
            f.advance(Duration.ofSeconds(3));
            // Its readers meet it, and fail loudly if they cannot read it, rather than never hearing of the order.
            assertEquals(2, f.output("order-total-changes").readValuesToList().size());
            assertEquals(List.of(3L, 0L), List.of(f.stats.get(Counter.UNCATALOGUED), f.stats.get(Counter.UNROUTED)));
            assertTrue(!f.stats.firstUncatalogued("OrderPlaced@v7"), "logged once: the first sighting is already recorded");
        }
    }

    @Test
    void theRevisionAndKnownVersionsMoveOnWithoutARestart() {
        java.util.concurrent.atomic.AtomicReference<IntentTopology.Routing> routing =
                new java.util.concurrent.atomic.AtomicReference<>(new IntentTopology.Routing(List.of(totals), Set.of()));
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        try {
            props.put(StreamsConfig.STATE_DIR_CONFIG, Files.createTempDirectory("rif-router-test").toString());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        RouterStats stats = new RouterStats();
        try (TopologyTestDriver driver = new TopologyTestDriver(
                IntentTopology.build(routing::get, new IntentTopology.Ordering(GRACE, Duration.ofMillis(100)), stats), props, T0)) {
            TestInputTopic<String, String> in = driver.createInputTopic("orders.amended", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> out = driver.createOutputTopic(Intent.topicFor("order-total-changes"), new StringDeserializer(), new StringDeserializer());
            in.pipeInput("o-1", event("OrderAmended", "a", T0, obj("orderId", "o-1", "newTotal", "1", "amendedAt", T0.toString())));
            routing.set(new IntentTopology.Routing(List.of(new RoutingTable(totals.intent(), totals.predicate(), totals.entity(), 2, totals.routes(),
                    totals.sourceTopics())), Set.of()));
            in.pipeInput("o-1", event("OrderAmended", "b", T0.plusMillis(1), obj("orderId", "o-1", "newTotal", "2", "amendedAt", T0.toString())));
            driver.advanceWallClockTime(Duration.ofSeconds(3));
            assertEquals(List.of("1", "2"), out.readRecordsToList().stream().map(r -> header(r, IntentHeaders.CATALOG_REVISION)).toList());
        }
    }
}
