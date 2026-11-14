package inflight.proxy;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kafka.common.compress.Compression;
import io.kroxylicious.kafka.common.message.ProduceRequestData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.record.internal.MemoryRecords;
import io.kroxylicious.kafka.common.record.internal.SimpleRecord;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.testing.filter.context.MockFilterContext;

class SchemaGateFilterTest {

    /** A registry that knows a fixed set of type versions, written {@code Type@v}. */
    static final class Known implements SchemaLookup {
        final Set<String> known;
        final boolean down;
        final List<String> asked = new ArrayList<>();

        Known(Set<String> known, boolean down) {
            this.known = known;
            this.down = down;
        }

        Known(Set<String> known) {
            this(known, false);
        }

        @Override
        public CompletionStage<Boolean> isRegistered(String type, int version) {
            asked.add(type + "@" + version);
            return down ? CompletableFuture.failedFuture(new IllegalStateException("connection refused"))
                    : CompletableFuture.completedFuture(known.contains(type + "@" + version));
        }
    }

    static String cloudEvent(String type, int version) {
        return "{\"specversion\":\"1.0\",\"id\":\"x\",\"source\":\"/s\",\"type\":\"" + type + "\",\"schemaversion\":" + version
                + ",\"time\":\"2026-10-30T12:00:00Z\",\"data\":{}}";
    }

    static ProduceRequestData produce(String topic, String... values) {
        ProduceRequestData req = new ProduceRequestData().setAcks((short) 1);
        ProduceRequestData.TopicProduceData td = new ProduceRequestData.TopicProduceData().setName(topic);
        SimpleRecord[] records = new SimpleRecord[values.length];
        for (int i = 0; i < values.length; i++) {
            records[i] = new SimpleRecord(values[i].getBytes(UTF_8));
        }
        td.partitionData().add(new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(MemoryRecords.withRecords(Compression.NONE, records)));
        req.topicData().add(td);
        return req;
    }

    static RequestFilterResult run(SchemaLookup lookup, ProduceRequestData req) throws Exception {
        RequestHeaderData hdr = new RequestHeaderData().setRequestApiKey(ApiKeys.PRODUCE.id).setRequestApiVersion((short) 12);
        return new SchemaGateFilter(lookup, null).onProduceRequest((short) 12, hdr, req, MockFilterContext.builder(hdr, req).build())
                .toCompletableFuture().get();
    }

    @Test
    void registeredVersionsPass() throws Exception {
        assertFalse(run(new Known(Set.of("OrderPlaced@1")), produce("orders.placed", cloudEvent("OrderPlaced", 1))).shortCircuitResponse());
    }

    @Test
    void anUnregisteredVersionAnywhereInTheRequestRefusesTheWholeRequest() throws Exception {
        Known lookup = new Known(Set.of("OrderPlaced@1"));
        assertTrue(run(lookup, produce("billing.invoices", cloudEvent("OrderPlaced", 1), cloudEvent("InvoiceAdjusted", 1))).shortCircuitResponse());
        assertEquals(Set.of("OrderPlaced@1", "InvoiceAdjusted@1"), Set.copyOf(lookup.asked), "each distinct version is asked once");
        assertEquals(2, lookup.asked.size());
    }

    @Test
    void nonCloudEventValuesAndInternalTopicsAreNotGated() throws Exception {
        Known lookup = new Known(Set.of());
        assertFalse(run(lookup, produce("logs", "plain text", "{\"no\":\"type\"}")).shortCircuitResponse());
        assertFalse(run(lookup, produce("_intent-bus.registry", cloudEvent("Anything", 9))).shortCircuitResponse());
        assertEquals(List.of(), lookup.asked);
    }

    @Test
    void schemaversionAsAStringIsReadToo() {
        String v = cloudEvent("OrderPlaced", 2).replace("\"schemaversion\":2", "\"schemaversion\":\"2\"");
        assertEquals("[EventVersion[type=OrderPlaced, version=2]]", SchemaGateFilter.eventVersions(produce("t", v)).toString());
    }

    @Test
    void theGateFailsClosedWhenTheRegistryIsUnreachable() throws Exception {
        assertTrue(run(new Known(Set.of("OrderPlaced@1"), true), produce("orders.placed", cloudEvent("OrderPlaced", 1))).shortCircuitResponse());
    }

    @Test
    void reportOnlyRefusesNothingAndSaysWhatItWouldHave() throws Exception {
        RequestHeaderData hdr = new RequestHeaderData().setRequestApiKey(ApiKeys.PRODUCE.id).setRequestApiVersion((short) 12);
        for (boolean down : new boolean[] {false, true}) {
            ProduceRequestData req = produce("orders.placed", cloudEvent("OrderPlaced", 2));
            SchemaGateFilter f = new SchemaGateFilter(new Known(Set.of("OrderPlaced@1"), down), null, true);
            RequestFilterResult r = f.onProduceRequest((short) 12, hdr, req, MockFilterContext.builder(hdr, req).build()).toCompletableFuture().get();
            assertFalse(r.shortCircuitResponse());
            assertEquals(1, f.reported.size());
            assertTrue(f.reported.get(0).contains(down ? "unreachable" : "OrderPlaced v2 not registered"), f.reported.get(0));
        }
        SchemaGateConfig cfg = new ObjectMapper().readValue("{\"reportOnly\":true}", SchemaGateConfig.class);
        assertTrue(cfg.reportOnly());
    }

    @Test
    @SuppressWarnings("rawtypes")
    void theGateFactoryIsDiscoverableAndReadsItsConfig() throws Exception {
        List<String> all = ServiceLoader.load(FilterFactory.class).stream().map(p -> p.type().getName()).toList();
        assertTrue(all.contains("inflight.proxy.SchemaGateFilterFactory"), all.toString());
        SchemaGateConfig cfg = new ObjectMapper().readValue("{\"registryUrl\":\"http://r:8080/apis/registry/v3/\"}", SchemaGateConfig.class);
        assertEquals(new SchemaGateConfig("http://r:8080/apis/registry/v3", "intent-bus", 1000L), cfg);
    }
}
