package inflight.kafka;

import java.time.Duration;
import java.util.List;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;

import inflight.broker.Header;
import inflight.broker.Writer;

/** A producer with acks from all replicas, one message at a time. */
final class KafkaWriter implements Writer {

    private final KafkaProducer<byte[], byte[]> producer;

    KafkaWriter(KafkaProducer<byte[], byte[]> producer) {
        this.producer = producer;
    }

    @Override
    public void send(String stream, byte[] key, byte[] value, List<Header> headers) {
        RecordHeaders hs = new RecordHeaders();
        headers.forEach(h -> hs.add(h.key(), h.value()));
        try {
            producer.send(new ProducerRecord<>(stream, null, key, value, hs)).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (Exception e) {
            throw new IllegalStateException(e.getCause() != null ? e.getCause().getMessage() : e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        producer.close(Duration.ofSeconds(10));
    }
}
