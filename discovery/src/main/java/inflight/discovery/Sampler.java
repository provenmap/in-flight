package inflight.discovery;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import inflight.broker.Broker;
import inflight.broker.Cursor;
import inflight.broker.Endpoint;
import inflight.broker.Message;
import inflight.broker.Shard;
import inflight.catalog.CloudEvent;
import inflight.catalog.Json;
import inflight.catalog.Records;
import inflight.catalog.Result;
import inflight.codec.Decoders;

/**
 * Reads a topic's recent records through an observing cursor, so nobody's lag, commits or messages change: from the end of each
 * partition back, spread across partitions, bounded by count, bytes and time, so a very busy topic is sampled as
 * cheaply as a quiet one. What comes back is the records' shapes, and the records themselves for a dry run.
 */
public final class Sampler {

    /** Never more than this many records in one sample. */
    public static final int MAX = 1000;
    /** Never more than this many bytes of values in one sample. */
    static final long MAX_BYTES = 16L * 1024 * 1024;
    static final Duration TIME = Duration.ofSeconds(8);

    /** One record as sampled: where it was, its headers, its format, and what it says (or why it could not be read). */
    public record Sampled(int partition, long offset, long timestamp, Map<String, String> headers, String format, Result<Records.Decoded> decoded) {}

    /** The headers a topic's records carry, the dead-letter conventions they match, and the topics they say they came from. */
    public record Headers(Set<String> names, List<String> conventions, List<String> sourceTopics) {}

    private final Endpoint endpoint;
    private final Decoders decoders;

    public Sampler(Endpoint endpoint, Decoders decoders) {
        this.endpoint = endpoint;
        this.decoders = decoders == null ? Decoders.plain() : decoders;
    }

    public Decoders decoders() {
        return decoders;
    }

    /** Up to {@code n} of the topic's latest records, newest last within each partition. */
    public List<Sampled> sample(String topic, int n) {
        int want = Math.max(1, Math.min(MAX, n));
        List<Sampled> out = new ArrayList<>();
        try (Broker broker = endpoint.open(); Cursor c = broker.observe()) {
            if (!broker.streams().contains(topic)) {
                throw new IllegalArgumentException("there is no topic " + topic);
            }
            List<Shard> parts = broker.shards(List.of(topic));
            Map<Shard, Long> low = broker.starts(parts);
            Map<Shard, Long> high = broker.ends(parts);
            List<Shard> live = parts.stream().filter(tp -> high.get(tp) > low.get(tp)).toList();
            if (live.isEmpty()) {
                return out;
            }
            // An even share per partition, from each one's end back: recent records, all partitions represented.
            int share = Math.max(1, (int) Math.ceil(want / (double) live.size()));
            Map<Shard, Long> stopAt = new LinkedHashMap<>();
            Map<Shard, Long> from = new LinkedHashMap<>();
            for (Shard tp : live) {
                from.put(tp, Math.max(low.get(tp), high.get(tp) - share));
                stopAt.put(tp, high.get(tp));
            }
            c.seek(from);
            long bytes = 0;
            long deadline = System.currentTimeMillis() + TIME.toMillis();
            Set<Shard> done = new LinkedHashSet<>();
            while (done.size() < live.size() && out.size() < want && bytes < MAX_BYTES && System.currentTimeMillis() < deadline) {
                for (Message r : c.poll(Duration.ofMillis(200))) {
                    Shard tp = r.shard();
                    if (r.offset() >= stopAt.get(tp)) {
                        done.add(tp);
                        continue;
                    }
                    if (out.size() >= want) {
                        break;
                    }
                    byte[] v = r.valueOrEmpty();
                    bytes += v.length;
                    Map<String, String> hs = r.textHeaders();
                    Decoders.Read read = decoders.read(v, hs);
                    out.add(new Sampled(r.index(), r.offset(), r.timestamp(), hs, read.format(), read.decoded()));
                    if (r.offset() + 1 >= stopAt.get(tp)) {
                        done.add(tp);
                    }
                }
                for (Shard tp : live) {
                    if (c.position(tp) >= stopAt.get(tp)) {
                        done.add(tp);
                    }
                }
            }
        }
        return out;
    }

    /** The headers on a topic's latest records, and what they say about it as a dead-letter queue. */
    public Headers headers(String topic, int n) {
        Set<String> names = new LinkedHashSet<>();
        Set<String> sources = new LinkedHashSet<>();
        for (Sampled s : sample(topic, n)) {
            names.addAll(s.headers().keySet());
            for (String h : List.of(CloudEvent.DeadLetter.SOURCE_TOPIC, "__connect.errors.topic", "kafka_dlt-original-topic")) {
                String v = s.headers().get(h);
                if (v != null && !v.isBlank()) {
                    sources.add(v);
                }
            }
        }
        List<String> conventions = new ArrayList<>();
        if (names.stream().anyMatch(h -> h.startsWith("__connect.errors."))) {
            conventions.add("Kafka Connect's");
        }
        if (names.stream().anyMatch(h -> h.startsWith("kafka_dlt-"))) {
            conventions.add("Spring Kafka's");
        }
        if (names.contains(CloudEvent.DeadLetter.ERROR)) {
            conventions.add("in-flight's");
        }
        return new Headers(names, conventions, List.copyOf(sources));
    }

    // ---- shapes ----------------------------------------------------------------------------------------------------

    /**
     * One field across the sample: its path, the JSON types seen, how often it is present, and an example (masked
     * unless revealed). {@code numericText} is true when it is text that always reads as a number, as money often is.
     */
    public record Field(String path, Set<String> types, double presence, String example, boolean numericText, boolean wholeNumbers, int count) {}

    /** A topic's shape: formats seen, the CloudEvent types and versions, the fields, and the records that could not be read. */
    public record Shape(String topic, int sampled, Map<String, Integer> formats, Map<String, Integer> eventTypes, List<Field> fields,
                        int unreadable, List<String> problems, Set<String> headers) {}

    public static Shape shape(String topic, List<Sampled> records, boolean reveal) {
        Map<String, Integer> formats = new TreeMap<>();
        Map<String, Integer> types = new TreeMap<>();
        Map<String, Acc> fields = new LinkedHashMap<>();
        Set<String> headers = new LinkedHashSet<>();
        Set<String> problems = new LinkedHashSet<>();
        int readable = 0;
        int unreadable = 0;
        for (Sampled s : records) {
            headers.addAll(s.headers().keySet());
            String fmt = s.format();
            if (!s.decoded().isOk()) {
                unreadable++;
                if (problems.size() < 5) {
                    problems.add(s.decoded().error());
                }
                formats.merge(fmt, 1, Integer::sum);
                continue;
            }
            Records.Decoded d = s.decoded().value();
            if (d.type() != null) {
                fmt = "cloudevents" + (fmt.equals("json") ? "" : "+" + fmt);
                types.merge(d.version() == null ? d.type() : d.type() + "@v" + d.version(), 1, Integer::sum);
            }
            formats.merge(fmt, 1, Integer::sum);
            readable++;
            Set<String> seenHere = new LinkedHashSet<>();
            walk("", d.data(), fields, seenHere, reveal);
        }
        int total = readable;
        List<Field> out = new ArrayList<>();
        fields.forEach((path, a) -> out.add(new Field(path, a.types, total == 0 ? 0 : a.count / (double) total, a.example, a.numericText && a.texts > 0,
                a.whole && a.numbers > 0, a.count)));
        return new Shape(topic, records.size(), formats, types, out, unreadable, List.copyOf(problems), headers);
    }

    private static final class Acc {
        final Set<String> types = new LinkedHashSet<>();
        int count;
        int texts;
        int numbers;
        boolean numericText = true;
        boolean whole = true;
        String example;
    }

    private static void walk(String prefix, JsonNode node, Map<String, Acc> fields, Set<String> seen, boolean reveal) {
        for (Iterator<Map.Entry<String, JsonNode>> it = node.properties().iterator(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> e = it.next();
            String path = prefix.isEmpty() ? e.getKey() : prefix + "." + e.getKey();
            JsonNode v = e.getValue();
            Acc a = fields.computeIfAbsent(path, k -> new Acc());
            if (seen.add(path)) {
                a.count++;
            }
            a.types.add(typeOf(v));
            if (v.isTextual()) {
                a.texts++;
                if (inflight.catalog.Pricing.money(v) == null) {
                    a.numericText = false;
                }
            } else if (v.isNumber()) {
                a.numbers++;
                if (!v.isIntegralNumber() && v.decimalValue().stripTrailingZeros().scale() > 0) {
                    a.whole = false;
                }
            }
            if (a.example == null && !v.isNull() && !v.isContainerNode()) {
                a.example = reveal ? v.asText() : mask(v);
            }
            if (v.isObject()) {
                walk(path, v, fields, seen, reveal);
            }
        }
    }

    static String typeOf(JsonNode v) {
        if (v.isTextual()) {
            return "string";
        }
        if (v.isIntegralNumber()) {
            return "integer";
        }
        if (v.isNumber()) {
            return "number";
        }
        if (v.isBoolean()) {
            return "boolean";
        }
        if (v.isObject()) {
            return "object";
        }
        if (v.isArray()) {
            return "array";
        }
        return "null";
    }

    /**
     * An example with its content hidden and its shape kept: digits become 9, letters x or X, so {@code ord-81723}
     * reads {@code xxx-99999} and {@code 129.95} reads {@code 999.99}. Enough to tell an id from an amount, nothing more.
     */
    static String mask(JsonNode v) {
        if (v.isBoolean()) {
            return v.asText();
        }
        String s = v.asText();
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < s.length() && i < 40; i++) {
            char ch = s.charAt(i);
            b.append(Character.isDigit(ch) ? '9' : Character.isUpperCase(ch) ? 'X' : Character.isLetter(ch) ? 'x' : ch);
        }
        if (s.length() > 40) {
            b.append('…');
        }
        return b.toString();
    }

    public static ObjectNode json(Shape s) {
        ObjectNode o = Json.object();
        o.put("topic", s.topic());
        o.put("sampled", s.sampled());
        o.put("unreadable", s.unreadable());
        ObjectNode f = o.putObject("formats");
        s.formats().forEach(f::put);
        ObjectNode t = o.putObject("eventTypes");
        s.eventTypes().forEach(t::put);
        ArrayNode fs = o.putArray("fields");
        for (Field x : s.fields()) {
            ObjectNode n = fs.addObject().put("path", x.path());
            ArrayNode ty = n.putArray("types");
            x.types().forEach(ty::add);
            n.put("presence", Math.round(x.presence() * 1000) / 1000.0);
            if (x.example() != null) {
                n.put("example", x.example());
            }
            if (x.numericText()) {
                n.put("numericText", true);
            }
        }
        ArrayNode p = o.putArray("problems");
        s.problems().forEach(p::add);
        ArrayNode h = o.putArray("headers");
        s.headers().forEach(h::add);
        return o;
    }
}
