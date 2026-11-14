package inflight.router;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import inflight.catalog.EventRef;
import inflight.catalog.Result;
import inflight.catalog.RoutingTable;
import inflight.router.RouterStats.Counter;

/**
 * Looks every record up in every routing table and emits one copy per matching intent, keyed by intent and entity.
 *
 * <p>A version the active catalog does not know was produced around the schema gate. intent-bus skipped it, which made
 * the event vanish for everyone reading by intent: an order lost without a trace. Here, a version of a type an intent
 * already routes follows its type's newest route there (the same key path), so its readers meet it and fail loudly, into
 * their dead-letter queues, where it can be seen and recovered. It is still counted as uncatalogued and logged once. A
 * type the catalog has never declared stays outside every intent.
 */
final class RouteProcessor implements Processor<String, String, String, Routed> {

    private static final Logger log = LoggerFactory.getLogger(RouteProcessor.class);

    private final Supplier<IntentTopology.Routing> routing;
    private final RouterStats stats;
    private ProcessorContext<String, Routed> ctx;

    RouteProcessor(Supplier<IntentTopology.Routing> routing, RouterStats stats) {
        this.routing = routing;
        this.stats = stats;
    }

    @Override
    public void init(ProcessorContext<String, Routed> context) {
        this.ctx = context;
    }

    @Override
    public void process(Record<String, String> record) {
        RecordMetadata meta = ctx.recordMetadata().orElseThrow();
        IntentTopology.Routing now = routing.get();
        List<String> readers = now.tables().stream().filter(t -> t.sourceTopics().contains(meta.topic())).map(RoutingTable::intent).toList();

        Result<CloudEventView> parsed = CloudEventView.parse(record.value());
        if (!parsed.isOk()) {
            stats.countSource(Counter.UNPARSEABLE, readers);
            log.warn("skipping {}-{}@{}: {}", meta.topic(), meta.partition(), meta.offset(), parsed.error());
            return;
        }
        CloudEventView ev = parsed.value();
        EventRef ref = new EventRef(ev.eventType(), ev.schemaVersion());

        boolean uncatalogued = !now.known().isEmpty() && !now.known().contains(ref);
        if (uncatalogued) {
            stats.countSource(Counter.UNCATALOGUED, readers);
            if (stats.firstUncatalogued(ref.show())) {
                log.warn("{} v{} on {} is not in the active catalog: it was produced around the schema gate. It goes to the intents "
                        + "that route its type, where readers that cannot read it dead-letter it", ev.eventType(), ev.schemaVersion(), meta.topic());
            }
        }
        boolean matched = false;
        for (RoutingTable table : now.tables()) {
            Optional<RoutingTable.Route> route = uncatalogued ? newestOfType(table, ev.eventType()) : table.lookup(ev.eventType(), ev.schemaVersion());
            if (route.isEmpty()) {
                continue;
            }
            matched = true;
            String entityId = route.get().keyPath().read(ev.data()).map(CloudEventView::keyText).orElse(null);
            if (entityId == null) {
                stats.count(Counter.UNKEYED, table.intent());
                log.warn("{} {}: no {} key at '{}'", ref.show(), ev.id(), table.entity(), route.get().keyPath().show());
                continue;
            }
            stats.count(Counter.ROUTED, table.intent());
            Routed routed = new Routed(table.intent(), table.revision(), entityId, ev.time().toEpochMilli(),
                    ev.sequence() == null ? 0L : ev.sequence(), ev.id(), meta.topic(), meta.partition(), meta.offset(), record.value());
            ctx.forward(new Record<>(Routed.compositeKey(table.intent(), entityId), routed, record.timestamp()));
        }
        if (!matched && !uncatalogued) {
            stats.countSource(Counter.UNROUTED, readers);
        }
    }

    /** The route of the newest version of a type that a table routes, if it routes any. */
    static Optional<RoutingTable.Route> newestOfType(RoutingTable table, String type) {
        return table.routes().stream().filter(r -> r.event().name().equals(type))
                .max(java.util.Comparator.comparingInt(r -> r.event().version()));
    }
}
