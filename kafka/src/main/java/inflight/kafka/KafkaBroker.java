package inflight.kafka;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;

import inflight.broker.Broker;
import inflight.broker.Capability;
import inflight.broker.ClusterInfo;
import inflight.broker.Commit;
import inflight.broker.Cursor;
import inflight.broker.Group;
import inflight.broker.Shard;
import inflight.broker.Writer;

/** A session with one Kafka cluster: an admin client, opened lazily, and consumers and producers made on demand. */
final class KafkaBroker implements Broker {

    private static final long TIMEOUT_S = 15;

    private final String bootstrap;
    private Admin admin;

    KafkaBroker(String bootstrap) {
        this.bootstrap = bootstrap;
    }

    private synchronized Admin admin() {
        if (admin == null) {
            admin = Kafka.admin(bootstrap);
        }
        return admin;
    }

    static TopicPartition tp(Shard s) {
        return new TopicPartition(s.stream(), s.index());
    }

    static Shard shard(TopicPartition tp) {
        return new Shard(tp.topic(), tp.partition());
    }

    @Override
    public Set<Capability> capabilities() {
        return EnumSet.allOf(Capability.class);
    }

    @Override
    public ClusterInfo describe() {
        try {
            var d = admin().describeCluster();
            return new ClusterInfo(d.clusterId().get(TIMEOUT_S, TimeUnit.SECONDS), d.nodes().get(TIMEOUT_S, TimeUnit.SECONDS).size(), streams().size());
        } catch (Exception e) {
            throw failure("cannot describe the cluster", e);
        }
    }

    @Override
    public Set<String> streams() {
        return Kafka.listTopics(admin());
    }

    @Override
    public List<Shard> shards(Collection<String> streams) {
        Set<String> existing = streams();
        List<String> present = streams.stream().distinct().filter(existing::contains).toList();
        return Kafka.partitions(admin(), present).stream().map(KafkaBroker::shard).toList();
    }

    @Override
    public Map<Shard, Long> starts(Collection<Shard> shards) {
        return offsets(shards, OffsetSpec.earliest());
    }

    @Override
    public Map<Shard, Long> ends(Collection<Shard> shards) {
        return offsets(shards, OffsetSpec.latest());
    }

    @Override
    public Map<Shard, Long> offsetsAt(Collection<Shard> shards, Instant at) {
        Map<Shard, Long> found = offsets(shards, OffsetSpec.forTimestamp(at.toEpochMilli()));
        Map<Shard, Long> ends = ends(shards);
        Map<Shard, Long> out = new LinkedHashMap<>();
        for (Shard s : shards) {
            Long o = found.get(s);
            out.put(s, o == null || o < 0 ? ends.get(s) : o);
        }
        return out;
    }

    private Map<Shard, Long> offsets(Collection<Shard> shards, OffsetSpec spec) {
        if (shards.isEmpty()) {
            return Map.of();
        }
        Map<TopicPartition, OffsetSpec> req = new LinkedHashMap<>();
        shards.forEach(s -> req.put(tp(s), spec));
        try {
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> got = admin().listOffsets(req).all().get(TIMEOUT_S, TimeUnit.SECONDS);
            Map<Shard, Long> out = new LinkedHashMap<>();
            got.forEach((tp, i) -> out.put(shard(tp), i.offset()));
            return out;
        } catch (Exception e) {
            throw failure("cannot read the topics' offsets", e);
        }
    }

    @Override
    public List<Group> groups() {
        Collection<GroupListing> listed;
        try {
            listed = admin().listGroups(ListGroupsOptions.forConsumerGroups()).all().get(TIMEOUT_S, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw failure("cannot list consumer groups", e);
        }
        List<String> ids = listed.stream().map(GroupListing::groupId).filter(g -> !g.startsWith(Kafka.OBSERVER_GROUP_PREFIX)).sorted().toList();
        Map<String, ConsumerGroupDescription> described = new TreeMap<>();
        if (!ids.isEmpty()) {
            try {
                described.putAll(admin().describeConsumerGroups(ids).all().get(TIMEOUT_S, TimeUnit.SECONDS));
            } catch (Exception e) {
                // Described or not, a group's commits still say what it reads.
            }
        }
        List<Group> out = new ArrayList<>();
        for (String id : ids) {
            ConsumerGroupDescription d = described.get(id);
            Set<String> assigned = new java.util.TreeSet<>();
            if (d != null) {
                d.members().forEach(m -> m.assignment().topicPartitions().forEach(tp -> assigned.add(tp.topic())));
            }
            String state = d == null || d.groupState() == null ? "unknown" : d.groupState().toString().toLowerCase(Locale.ROOT);
            out.add(new Group(id, state, d == null ? 0 : d.members().size(), assigned));
        }
        return out;
    }

    @Override
    public Map<Shard, Commit> committed(String group) {
        try {
            Map<TopicPartition, OffsetAndMetadata> got = admin().listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                    .get(TIMEOUT_S, TimeUnit.SECONDS);
            Map<Shard, Commit> out = new HashMap<>();
            got.forEach((tp, om) -> {
                if (om != null && om.offset() >= 0) {
                    out.put(shard(tp), new Commit(om.offset(), om.metadata() == null || om.metadata().isEmpty() ? null : om.metadata()));
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

    @Override
    public int members(String group) {
        return Kafka.members(admin(), group);
    }

    @Override
    public Cursor observe() {
        return new KafkaCursor(Kafka.consumer(bootstrap, null), false);
    }

    @Override
    public Cursor consume(String group) {
        if (group == null || group.isBlank()) {
            throw new IllegalArgumentException("a consuming cursor needs a group");
        }
        return new KafkaCursor(Kafka.consumer(bootstrap, group), true);
    }

    @Override
    public Writer writer() {
        return new KafkaWriter(Kafka.bytesProducer(bootstrap));
    }

    @Override
    public synchronized void close() {
        if (admin != null) {
            admin.close();
            admin = null;
        }
    }

    private static IllegalStateException failure(String what, Exception e) {
        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        Throwable c = e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
        return new IllegalStateException(what + ": " + c.getMessage(), c);
    }
}
