package inflight.discovery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import inflight.broker.Broker;
import inflight.broker.Capability;
import inflight.broker.Commit;
import inflight.broker.Endpoint;
import inflight.broker.Shard;
import inflight.catalog.Json;
import inflight.catalog.Result;
import inflight.codec.SchemaRegistry;

/**
 * What a broker shows, read through its adapter: its topics and how much is on each, its consumer groups and what each
 * reads, which topics look like dead-letter queues and why, and the registry's subjects. Nothing is written.
 */
public final class Discovery {

    /** A topic: partitions, records on it now (end minus start), and what it seems to be. */
    public record Topic(String name, int partitions, long records, String kind) {}

    /** A consumer group: its state, members, the topics it reads (by commits and assignments), and its lag. */
    public record Group(String group, String state, int members, List<String> reads, long lag) {}

    /**
     * A dead-letter queue candidate, the reasons it is one, and what it seems to belong to: the topics its letters say
     * they came from, and the group that seems to write it.
     */
    public record DeadLetterQueue(String topic, List<String> reasons, List<String> sources, String owner) {}

    /** All of it. {@code registry} is null when there is none; {@code registryProblem} says why it could not be read. */
    public record Found(String bootstrap, List<Topic> topics, List<Group> groups, List<DeadLetterQueue> dlqs, List<String> subjects,
                        String registryProblem) {

        public List<Topic> flowTopics() {
            return topics.stream().filter(t -> t.kind().equals("flow")).toList();
        }

        public Topic topic(String name) {
            return topics.stream().filter(t -> t.name().equals(name)).findFirst().orElse(null);
        }

        public List<Group> readersOf(String topic) {
            return groups.stream().filter(g -> g.reads().contains(topic)).toList();
        }
    }

    /** Names a dead-letter queue goes by: {@code .dlq}, {@code -dlt}, {@code .DLT}, {@code dlq-}, {@code dead-letter}. */
    static final Pattern DLQ_NAME = Pattern.compile("(?i)(^|[._-])(dlq|dlt|dead[._-]?letters?)([._-]|$)");
    static final Pattern RETRY_NAME = Pattern.compile("(?i)([._-])retry([._-]?\\d*)$");

    private Discovery() {}

    /** What kind of topic a name says it is: internal, a dead-letter queue, a retry topic, or part of a flow. */
    static String kindOf(String topic) {
        String t = topic.toLowerCase(Locale.ROOT);
        if (t.startsWith("__") || t.equals("_schemas") || t.startsWith("_confluent") || t.startsWith("connect-") && (t.endsWith("-configs")
                || t.endsWith("-offsets") || t.endsWith("-status")) || t.endsWith("-changelog") || t.endsWith("-repartition")
                || t.startsWith("in-flight-") || t.startsWith("_") || t.startsWith("intent.") || t.startsWith("kafka-ui")) {
            return "internal";
        }
        if (DLQ_NAME.matcher(topic).find()) {
            return "dlq";
        }
        if (RETRY_NAME.matcher(topic).find()) {
            return "retry";
        }
        return "flow";
    }

    /** The name a dead-letter queue would have without its marker: {@code payments.dlq} is {@code payments}. */
    static String stem(String dlq) {
        return DLQ_NAME.matcher(dlq).replaceAll("$1").replaceAll("[._-]+$", "").replaceAll("^[._-]+", "");
    }

    public static Found discover(Endpoint endpoint, SchemaRegistry registry, Sampler sampler) {
        try (Broker broker = endpoint.open()) {
            Set<String> names = broker.streams();
            List<Shard> parts = broker.shards(names);
            Map<String, Long> ends = Shard.keyed(broker.ends(parts));
            Map<String, Long> starts = Shard.keyed(broker.starts(parts));
            Map<String, Integer> partitions = new TreeMap<>();
            Map<String, Long> records = new TreeMap<>();
            parts.forEach(tp -> {
                partitions.merge(tp.stream(), 1, Integer::sum);
                String k = tp.key();
                records.merge(tp.stream(), Math.max(0, ends.getOrDefault(k, 0L) - starts.getOrDefault(k, 0L)), Long::sum);
            });
            List<Topic> topics = new ArrayList<>();
            partitions.forEach((name, n) -> topics.add(new Topic(name, n, records.getOrDefault(name, 0L), kindOf(name))));

            List<Group> groups = groups(broker, ends);
            // Sampling reads records, so only where reading them takes nothing from anyone.
            List<DeadLetterQueue> dlqs = dlqs(topics, groups, broker.capabilities().contains(Capability.OBSERVE) ? sampler : null);
            // A topic whose letters carry dead-letter headers is one, whatever its name.
            Set<String> byHeaders = new LinkedHashSet<>();
            dlqs.forEach(q -> byHeaders.add(q.topic()));
            List<Topic> kinded = topics.stream().map(t -> byHeaders.contains(t.name()) && !t.kind().equals("dlq") ? new Topic(t.name(), t.partitions(), t.records(), "dlq") : t).toList();

            List<String> subjects = List.of();
            String problem = null;
            if (registry != null) {
                Result<List<String>> s = registry.subjects();
                if (s.isOk()) {
                    subjects = s.value();
                } else {
                    problem = s.error();
                }
            }
            return new Found(endpoint.address(), kinded, groups, dlqs, subjects, problem);
        }
    }

    private static List<Group> groups(Broker broker, Map<String, Long> ends) {
        List<Group> out = new ArrayList<>();
        for (inflight.broker.Group g : broker.groups()) {
            Map<String, Long> committed;
            try {
                committed = Commit.offsets(broker.committed(g.id()));
            } catch (RuntimeException e) {
                committed = Map.of();
            }
            Set<String> reads = new java.util.TreeSet<>(g.assigned());
            committed.keySet().forEach(k -> reads.add(Shard.streamOf(k)));
            long lag = 0;
            if (broker.capabilities().contains(Capability.GROUP_LAG)) {
                for (Map.Entry<String, Long> c : committed.entrySet()) {
                    lag += Math.max(0, ends.getOrDefault(c.getKey(), c.getValue()) - c.getValue());
                }
            }
            out.add(new Group(g.id(), g.state(), g.members(), List.copyOf(reads), lag));
        }
        return out;
    }

    /**
     * Dead-letter queue candidates: by name, then by the headers on their letters (Kafka Connect's, Spring's, this
     * tool's), and who seems to write them: the group whose name matches the queue's stem, else one reading the topic
     * the letters say they came from.
     */
    private static List<DeadLetterQueue> dlqs(List<Topic> topics, List<Group> groups, Sampler sampler) {
        List<DeadLetterQueue> out = new ArrayList<>();
        for (Topic t : topics) {
            if (t.kind().equals("internal")) {
                continue;
            }
            List<String> reasons = new ArrayList<>();
            if (t.kind().equals("dlq")) {
                reasons.add("its name marks it as a dead-letter queue");
            }
            Set<String> sources = new LinkedHashSet<>();
            if (sampler != null && t.records() > 0 && (t.kind().equals("dlq") || t.kind().equals("flow"))) {
                Sampler.Headers h = sampler.headers(t.name(), 20);
                if (!h.conventions().isEmpty()) {
                    reasons.add("its records carry " + String.join(" and ", h.conventions()) + " dead-letter headers");
                }
                sources.addAll(h.sourceTopics());
            }
            if (reasons.isEmpty()) {
                continue;
            }
            out.add(new DeadLetterQueue(t.name(), reasons, List.copyOf(sources), owner(t.name(), List.copyOf(sources), groups)));
        }
        return out;
    }

    static String owner(String dlq, List<String> sources, List<Group> groups) {
        String stem = stem(dlq).toLowerCase(Locale.ROOT);
        if (!stem.isEmpty()) {
            for (Group g : groups) {
                String name = g.group().toLowerCase(Locale.ROOT);
                if (name.equals(stem) || name.startsWith(stem + "-") || name.startsWith(stem + ".") || name.startsWith(stem + "_")
                        || stem.startsWith(name)) {
                    return g.group();
                }
            }
        }
        for (String s : sources) {
            List<Group> readers = groups.stream().filter(g -> g.reads().contains(s) && !g.reads().contains(dlq)).toList();
            if (readers.size() == 1) {
                return readers.get(0).group();
            }
        }
        return null;
    }

    public static ObjectNode json(Found f) {
        ObjectNode o = Json.object();
        o.put("bootstrap", f.bootstrap());
        ArrayNode ts = o.putArray("topics");
        f.topics().forEach(t -> ts.addObject().put("name", t.name()).put("partitions", t.partitions()).put("records", t.records()).put("kind", t.kind()));
        ArrayNode gs = o.putArray("groups");
        f.groups().forEach(g -> {
            ObjectNode x = gs.addObject().put("group", g.group()).put("state", g.state()).put("members", g.members()).put("lag", g.lag());
            ArrayNode r = x.putArray("reads");
            g.reads().forEach(r::add);
        });
        ArrayNode qs = o.putArray("dlqs");
        f.dlqs().forEach(q -> {
            ObjectNode x = qs.addObject().put("topic", q.topic());
            ArrayNode why = x.putArray("reasons");
            q.reasons().forEach(why::add);
            ArrayNode src = x.putArray("sources");
            q.sources().forEach(src::add);
            if (q.owner() == null) {
                x.putNull("owner");
            } else {
                x.put("owner", q.owner());
            }
        });
        ArrayNode ss = o.putArray("subjects");
        f.subjects().forEach(ss::add);
        if (f.registryProblem() != null) {
            o.put("registryProblem", f.registryProblem());
        }
        return o;
    }

    /** One line for a person: how much was found. */
    public static String summary(Found f) {
        long flow = f.topics().stream().filter(t -> t.kind().equals("flow")).count();
        long internal = f.topics().stream().filter(t -> t.kind().equals("internal")).count();
        return f.topics().size() + " topics (" + flow + " that look like a flow, " + f.dlqs().size() + " dead-letter queues, " + internal
                + " internal), " + f.groups().size() + " consumer groups" + (f.subjects().isEmpty() ? "" : ", " + f.subjects().size() + " registry subjects")
                + (f.registryProblem() != null ? "; the registry could not be read: " + f.registryProblem() : "") + ".";
    }
}
