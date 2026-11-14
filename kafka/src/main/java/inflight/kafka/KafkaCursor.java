package inflight.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import inflight.broker.Commit;
import inflight.broker.Cursor;
import inflight.broker.Header;
import inflight.broker.Message;
import inflight.broker.Shard;

/**
 * A consumer that reads by assignment. Observing, it runs under a group id nobody else uses and never commits, so
 * nobody's lag or commits change; consuming, it runs under the given group and commits only when told to.
 */
final class KafkaCursor implements Cursor {

    private final KafkaConsumer<byte[], byte[]> consumer;
    private final boolean canCommit;
    private final Set<TopicPartition> assigned = new LinkedHashSet<>();

    KafkaCursor(KafkaConsumer<byte[], byte[]> consumer, boolean canCommit) {
        this.consumer = consumer;
        this.canCommit = canCommit;
    }

    @Override
    public void seek(Map<Shard, Long> from) {
        if (from.isEmpty()) {
            return;
        }
        from.keySet().forEach(s -> assigned.add(KafkaBroker.tp(s)));
        consumer.assign(assigned);
        from.forEach((s, o) -> consumer.seek(KafkaBroker.tp(s), o));
    }

    @Override
    public List<Message> poll(Duration timeout) {
        try {
            List<Message> out = new ArrayList<>();
            for (ConsumerRecord<byte[], byte[]> r : consumer.poll(timeout)) {
                List<Header> hs = new ArrayList<>();
                r.headers().forEach(h -> hs.add(new Header(h.key(), h.value())));
                out.add(new Message(new Shard(r.topic(), r.partition()), r.offset(), r.timestamp(), r.key(), r.value(), hs));
            }
            return out;
        } catch (WakeupException e) {
            throw new Woken();
        }
    }

    @Override
    public long position(Shard shard) {
        return consumer.position(KafkaBroker.tp(shard));
    }

    @Override
    public void commit(Map<Shard, Commit> commits) {
        if (!canCommit) {
            throw new IllegalStateException("an observing cursor never commits");
        }
        if (commits.isEmpty()) {
            return;
        }
        Map<TopicPartition, OffsetAndMetadata> out = new LinkedHashMap<>();
        commits.forEach((s, c) -> out.put(KafkaBroker.tp(s), new OffsetAndMetadata(c.offset(), c.note() == null ? "" : c.note())));
        consumer.commitSync(out);
    }

    @Override
    public void wakeup() {
        consumer.wakeup();
    }

    @Override
    public void close() {
        consumer.close();
    }
}
