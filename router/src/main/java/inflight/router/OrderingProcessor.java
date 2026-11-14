package inflight.router;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import inflight.router.RouterStats.Counter;

/**
 * Releases each (intent, entity)'s events in event-time order, then sequence, then event id.
 *
 * <p>Events wait in a store sorted by that order until they are older than the grace window by the wall clock. An event
 * older than one already released for its entity is released at once and marked late: it is never dropped.
 */
final class OrderingProcessor implements Processor<String, Routed, String, String> {

    static final String LOWEST = "0";

    private final IntentTopology.Ordering ordering;
    private final RouterStats stats;
    private ProcessorContext<String, String> ctx;
    private KeyValueStore<String, String> buffer;
    private KeyValueStore<String, String> watermarks;

    OrderingProcessor(IntentTopology.Ordering ordering, RouterStats stats) {
        this.ordering = ordering;
        this.stats = stats;
    }

    @Override
    public void init(ProcessorContext<String, String> context) {
        ctx = context;
        buffer = context.getStateStore(IntentTopology.BUFFER_STORE);
        watermarks = context.getStateStore(IntentTopology.WATERMARK_STORE);
        context.schedule(ordering.tick(), PunctuationType.WALL_CLOCK_TIME, this::release);
    }

    @Override
    public void process(Record<String, Routed> record) {
        Routed r = record.value();
        String wm = watermarks.get(record.key());
        if (wm != null && r.eventTimeMs() < Long.parseLong(wm)) {
            emit(r, true);
        } else {
            buffer.put(bufferKey(r), r.toJson());
        }
    }

    private void release(long now) {
        long cutoff = now - ordering.grace().toMillis();
        List<KeyValue<String, String>> ready = new ArrayList<>();
        try (KeyValueIterator<String, String> it = buffer.range(LOWEST, upTo(cutoff))) {
            it.forEachRemaining(ready::add);
        }
        for (KeyValue<String, String> kv : ready) {
            Routed r = Routed.fromJson(kv.value);
            emit(r, false);
            String composite = r.compositeKey();
            String prev = watermarks.get(composite);
            if (prev == null || r.eventTimeMs() > Long.parseLong(prev)) {
                watermarks.put(composite, Long.toString(r.eventTimeMs()));
            }
            buffer.delete(kv.key);
        }
    }

    private void emit(Routed r, boolean late) {
        RecordHeaders headers = new RecordHeaders();
        add(headers, IntentHeaders.INTENT, r.intent());
        add(headers, IntentHeaders.CATALOG_REVISION, Integer.toString(r.catalogRevision()));
        add(headers, IntentHeaders.ENTITY, r.entityId());
        add(headers, IntentHeaders.SOURCE_TOPIC, r.sourceTopic());
        add(headers, IntentHeaders.SOURCE_PARTITION, Integer.toString(r.sourcePartition()));
        add(headers, IntentHeaders.SOURCE_OFFSET, Long.toString(r.sourceOffset()));
        if (late) {
            add(headers, IntentHeaders.LATE, "true");
            stats.count(Counter.LATE, r.intent());
        }
        stats.count(Counter.DELIVERED, r.intent());
        ctx.forward(new Record<>(r.entityId(), r.payload(), r.eventTimeMs(), headers));
    }

    private static void add(RecordHeaders headers, String key, String value) {
        headers.add(key, value.getBytes(StandardCharsets.UTF_8));
    }

    /** Sortable: zero-padded event time first, so a range scan yields release order. */
    static String bufferKey(Routed r) {
        return String.format("%019d|%s|%019d|%s", r.eventTimeMs(), r.compositeKey(), r.sequence(), r.eventId());
    }

    /** Inclusive upper bound for a range scan: the bare next millisecond sorts after every key at or before the cutoff. */
    static String upTo(long cutoffMs) {
        return String.format("%019d", Math.max(cutoffMs + 1, 0L));
    }
}
