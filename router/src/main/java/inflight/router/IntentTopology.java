package inflight.router;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import inflight.catalog.EventRef;
import inflight.catalog.Intent;
import inflight.catalog.RoutingTable;

/**
 * The router's topology: sources, then route, then repartition by (intent, entity), then order, then intent topics.
 *
 * <p>The router runs one of these per intent, each in its own Streams application. Processor, repartition and store
 * names are fixed, and the application id is the intent's, so a restart with a new catalog keeps the intent's state.
 * The topology itself works over any number of routing tables; the tests use that to feed two intents at once.
 */
public final class IntentTopology {

    public static final String BUFFER_STORE = "ib-ordering-buffer";
    public static final String WATERMARK_STORE = "ib-ordering-watermark";
    public static final String REPARTITION = "by-entity";

    private IntentTopology() {}

    /**
     * How long an event waits for earlier ones by the wall clock before it is released, and how often the buffer is
     * checked.
     */
    public record Ordering(Duration grace, Duration tick) {}

    /**
     * What the route step consults for each record: the routing tables, and every event version the active catalog
     * knows. An event outside {@code known} can only come from a producer that bypassed the schema gate; it is counted
     * and logged once, never routed. An empty {@code known} turns that check off.
     */
    public record Routing(List<RoutingTable> tables, Set<EventRef> known) {
        public Routing {
            tables = List.copyOf(tables);
            known = Set.copyOf(known);
        }

        /** Every topic some table reads, sorted. */
        public List<String> sourceTopics() {
            Set<String> out = new TreeSet<>();
            tables.forEach(t -> out.addAll(t.sourceTopics()));
            return List.copyOf(out);
        }
    }

    public static Topology build(List<RoutingTable> tables, Ordering ordering, Set<EventRef> known, RouterStats stats) {
        Routing fixed = new Routing(tables, known);
        return build(() -> fixed, ordering, stats);
    }

    /**
     * Builds the topology over the source topics {@code routing} names now. The route step reads {@code routing} for
     * every record, so its revision and known versions can move on without a restart, as long as the topics stay the
     * same.
     */
    public static Topology build(Supplier<Routing> routing, Ordering ordering, RouterStats stats) {
        List<String> sources = routing.get().sourceTopics();
        if (sources.isEmpty()) {
            throw new IllegalArgumentException("no intent routes any topic; nothing to build");
        }
        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(storeOf(BUFFER_STORE));
        builder.addStateStore(storeOf(WATERMARK_STORE));

        builder.stream(sources, Consumed.with(Serdes.String(), Serdes.String()).withName("sources"))
                .process(() -> new RouteProcessor(routing, stats), Named.as("route"))
                .repartition(Repartitioned.<String, Routed>as(REPARTITION).withKeySerde(Serdes.String()).withValueSerde(Routed.SERDE))
                .process(() -> new OrderingProcessor(ordering, stats), Named.as("order"), BUFFER_STORE, WATERMARK_STORE)
                .to(INTENT_TOPIC, Produced.with(Serdes.String(), Serdes.String()).withName("intents"));
        return builder.build();
    }

    private static StoreBuilder<KeyValueStore<String, String>> storeOf(String name) {
        return Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(name), Serdes.String(), Serdes.String());
    }

    /** The sink topic comes from the record's intent header. */
    private static final TopicNameExtractor<String, String> INTENT_TOPIC = (key, value, ctx) -> {
        Header h = ctx.headers().lastHeader(IntentHeaders.INTENT);
        return Intent.topicFor(new String(h.value(), StandardCharsets.UTF_8));
    };
}
