package rif.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Kafka client settings, and the few admin calls every part shares: topics, a group's commits, the end of each
 * partition, and whether a group has members.
 */
public final class Kafka {

    /** Partitions for every topic this app creates. */
    public static final int PARTITIONS = 3;

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private Kafka() {}

    /**
     * Connection settings from the environment, for a cluster that needs more than an address: {@code RIF_KAFKA_} and
     * then the Java client property in capitals with dots as underscores, so {@code RIF_KAFKA_SECURITY_PROTOCOL=SASL_SSL}
     * sets {@code security.protocol}.
     */
    public static Map<String, String> settingsFrom(Map<String, String> env) {
        Map<String, String> out = new HashMap<>();
        env.forEach((k, v) -> {
            if (k.startsWith("RIF_KAFKA_") && k.length() > 10 && v != null) {
                out.put(k.substring(10).toLowerCase().replace('_', '.'), v);
            }
        });
        return out;
    }

    /** {@code BOOTSTRAP_SERVERS}, or the local Compose Kafka. */
    public static String bootstrap() {
        String b = System.getenv("BOOTSTRAP_SERVERS");
        return b == null || b.isBlank() ? "localhost:9092" : b;
    }

    private static Properties connection(String bootstrap) {
        Properties p = new Properties();
        p.putAll(settingsFrom(System.getenv()));
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        return p;
    }

    /**
     * A consumer of bytes. With a group it can subscribe, commit and read its group's commits; without one it only
     * reads by assignment, under a group id nobody else uses, so it commits nothing and shows up in nobody's lag.
     */
    public static KafkaConsumer<byte[], byte[]> consumer(String bootstrap, String group) {
        Properties p = connection(bootstrap);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, group != null ? group : "rif-reader-" + UUID.randomUUID());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        return new KafkaConsumer<>(p, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    /** A producer with acks from all replicas; {@code flush} waits until everything sent is acknowledged. */
    public static KafkaProducer<String, byte[]> producer(String bootstrap) {
        Properties p = connection(bootstrap);
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        return new KafkaProducer<>(p, new StringSerializer(), new ByteArraySerializer());
    }

    /**
     * A producer for writing through the proxy's schema gate: a refused request fails at once rather than being retried,
     * and nothing waits to batch, so one refused order does not take others down with it.
     */
    public static KafkaProducer<String, byte[]> gatedProducer(String proxy) {
        Properties p = connection(proxy);
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        p.put(ProducerConfig.RETRIES_CONFIG, "0");
        p.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        return new KafkaProducer<>(p, new StringSerializer(), new ByteArraySerializer());
    }

    /** A producer and consumer of text, for registries and tests. */
    public static KafkaProducer<String, String> textProducer(String bootstrap) {
        Properties p = connection(bootstrap);
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(p, new StringSerializer(), new StringSerializer());
    }

    public static KafkaConsumer<String, String> textConsumer(String bootstrap, String group) {
        Properties p = connection(bootstrap);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, group != null ? group : "rif-reader-" + UUID.randomUUID());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(p, new StringDeserializer(), new StringDeserializer());
    }

    public static Admin admin(String bootstrap) {
        Properties p = connection(bootstrap);
        p.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        p.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");
        return Admin.create(p);
    }

    /** {@code topic:partition}, the key the observer's maps use. */
    public static String key(String topic, int partition) {
        return topic + ":" + partition;
    }

    public static String key(TopicPartition tp) {
        return key(tp.topic(), tp.partition());
    }

    /** The topic part of a {@link #key}. */
    public static String topicOf(String key) {
        return key.substring(0, key.lastIndexOf(':'));
    }

    /** Headers as text, the last value given for each key winning. */
    public static Map<String, String> headers(Headers headers) {
        Map<String, String> out = new HashMap<>();
        for (Header h : headers) {
            out.put(h.key(), h.value() == null ? "" : new String(h.value(), java.nio.charset.StandardCharsets.UTF_8));
        }
        return out;
    }

    // ---- admin ----------------------------------------------------------------------------------------------------

    public static Set<String> listTopics(Admin admin) {
        return get(admin.listTopics().names());
    }

    /** Creates whichever of these topics do not exist yet. */
    public static void ensureTopics(Admin admin, Collection<String> topics, int partitions) {
        Set<String> existing = listTopics(admin);
        List<NewTopic> missing = topics.stream().distinct().filter(t -> !existing.contains(t)).map(t -> new NewTopic(t, partitions, (short) 1)).toList();
        if (missing.isEmpty()) {
            return;
        }
        try {
            admin.createTopics(missing).all().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new IllegalStateException("cannot create topics " + missing + ": " + e.getCause().getMessage(), e.getCause());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (TimeoutException e) {
            throw new IllegalStateException("timed out creating topics " + missing, e);
        }
    }

    /** A group's committed offsets, keyed by {@code topic:partition}. A group with no commits has none. */
    public static Map<String, Long> committed(Admin admin, String group) {
        try {
            Map<TopicPartition, OffsetAndMetadata> got = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            Map<String, Long> out = new HashMap<>();
            got.forEach((tp, om) -> {
                if (om != null && om.offset() >= 0) {
                    out.put(key(tp), om.offset());
                }
            });
            return out;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof GroupIdNotFoundException) {
                return Map.of();
            }
            throw new IllegalStateException("cannot read the commits of group " + group + ": " + e.getCause().getMessage(), e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (TimeoutException e) {
            throw new IllegalStateException("timed out reading the commits of group " + group, e);
        }
    }

    /** Every partition of these topics, in order. */
    public static List<TopicPartition> partitions(Admin admin, Collection<String> topics) {
        if (topics.isEmpty()) {
            return List.of();
        }
        Map<String, TopicDescription> ds = get(admin.describeTopics(topics).allTopicNames());
        List<TopicPartition> out = new ArrayList<>();
        ds.values().forEach(d -> d.partitions().forEach(p -> out.add(new TopicPartition(d.name(), p.partition()))));
        out.sort(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition));
        return out;
    }

    /** The end of each partition, the offset the next record will get, keyed by {@code topic:partition}. */
    public static Map<String, Long> ends(Admin admin, Collection<TopicPartition> tps) {
        if (tps.isEmpty()) {
            return Map.of();
        }
        Map<TopicPartition, OffsetSpec> req = tps.stream().collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> res = get(admin.listOffsets(req).all());
        Map<String, Long> out = new HashMap<>();
        res.forEach((tp, info) -> out.put(key(tp), info.offset()));
        return out;
    }

    /** How many members a group has: none for a group that only commits by assignment, or that does not exist. */
    public static int members(Admin admin, String group) {
        try {
            Map<String, ConsumerGroupDescription> d = admin.describeConsumerGroups(List.of(group)).all().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            ConsumerGroupDescription g = d.get(group);
            return g == null ? 0 : g.members().size();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof GroupIdNotFoundException) {
                return 0;
            }
            throw new IllegalStateException("cannot describe group " + group + ": " + e.getCause().getMessage(), e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (TimeoutException e) {
            throw new IllegalStateException("timed out describing group " + group, e);
        }
    }

    public static void deleteTopics(Admin admin, Collection<String> topics) {
        Set<String> existing = new HashSet<>(listTopics(admin));
        List<String> present = topics.stream().filter(existing::contains).toList();
        if (!present.isEmpty()) {
            get(admin.deleteTopics(present).all());
        }
    }

    private static <T> T get(org.apache.kafka.common.KafkaFuture<T> f) {
        try {
            return f.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause().getMessage(), e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (TimeoutException e) {
            throw new IllegalStateException("Kafka did not answer in " + TIMEOUT.toSeconds() + " s", e);
        }
    }
}
