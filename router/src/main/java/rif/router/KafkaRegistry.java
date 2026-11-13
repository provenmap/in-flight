package rif.router;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import rif.catalog.Catalog;
import rif.catalog.Intent;
import rif.catalog.Json;
import rif.catalog.Result;

/**
 * A compacted topic: key {@code catalog} holds the active catalog as JSON, key {@code intent/<name>} each intent
 * declared at runtime. A forgotten intent is a tombstone. Loading reads the topic from the start to its end.
 */
public final class KafkaRegistry implements Registry {

    public static final String TOPIC = "_rif.registry";
    static final String CATALOG_KEY = "catalog";
    static final String INTENT_PREFIX = "intent/";

    private final String bootstrap;
    private final String topic;
    private final KafkaProducer<String, String> producer;

    public KafkaRegistry(String bootstrap, String topic) {
        this.bootstrap = bootstrap;
        this.topic = topic;
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        this.producer = new KafkaProducer<>(p, new StringSerializer(), new StringSerializer());
    }

    @Override
    public State load() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p, new StringDeserializer(), new StringDeserializer())) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream().map(i -> new TopicPartition(topic, i.partition())).toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
            Map<String, String> latest = new LinkedHashMap<>();
            while (partitions.stream().anyMatch(tp -> consumer.position(tp) < end.get(tp))) {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
                    if (r.value() == null) {
                        latest.remove(r.key());
                    } else {
                        latest.put(r.key(), r.value());
                    }
                }
            }
            Catalog catalog = null;
            if (latest.containsKey(CATALOG_KEY)) {
                Result<Catalog> c = Catalog.parse(latest.get(CATALOG_KEY));
                if (!c.isOk()) {
                    throw new IllegalStateException("the registry's catalog does not read: " + c.error());
                }
                catalog = c.value();
            }
            Map<String, Intent> intents = new LinkedHashMap<>();
            for (Map.Entry<String, String> e : latest.entrySet()) {
                if (e.getKey().startsWith(INTENT_PREFIX)) {
                    Result<Intent> i = Json.parse(e.getValue()).flatMap(Registry::readIntent);
                    if (!i.isOk()) {
                        throw new IllegalStateException("the registry's " + e.getKey() + " does not read: " + i.error());
                    }
                    intents.put(i.value().name(), i.value());
                }
            }
            return new State(catalog, intents);
        }
    }

    @Override
    public void saveCatalog(Catalog catalog) {
        put(CATALOG_KEY, Json.write(catalog.toJson()));
    }

    @Override
    public void saveIntent(Intent intent) {
        put(INTENT_PREFIX + intent.name(), Registry.intentJson(intent));
    }

    @Override
    public void forgetIntent(String name) {
        put(INTENT_PREFIX + name, null);
    }

    private void put(String key, String value) {
        try {
            producer.send(new ProducerRecord<>(topic, key, value)).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted writing " + key + " to " + topic, e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("cannot write " + key + " to " + topic + ": " + e.getCause().getMessage(), e.getCause());
        }
    }

    @Override
    public void close() {
        producer.close();
    }
}
