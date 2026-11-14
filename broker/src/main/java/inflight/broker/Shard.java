package inflight.broker;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One ordered part of a stream: a Kafka partition, a Pulsar partition, a Kinesis shard. Offsets are counted per shard.
 *
 * @param stream what the catalog calls a topic: the name events are written to and read from
 * @param index the shard's number within its stream, from 0
 */
public record Shard(String stream, int index) implements Comparable<Shard> {

    private static final Comparator<Shard> ORDER = Comparator.comparing(Shard::stream).thenComparingInt(Shard::index);

    public Shard {
        if (stream == null || stream.isEmpty()) {
            throw new IllegalArgumentException("a shard needs its stream's name");
        }
    }

    /** {@code stream:index}, the key the observer's maps and saved state use. */
    public String key() {
        return key(stream, index);
    }

    public static String key(String stream, int index) {
        return stream + ":" + index;
    }

    /** The stream part of a {@link #key}. */
    public static String streamOf(String key) {
        return key.substring(0, key.lastIndexOf(':'));
    }

    /** The index part of a {@link #key}. */
    public static int indexOf(String key) {
        return Integer.parseInt(key.substring(key.lastIndexOf(':') + 1));
    }

    /** The same map, keyed by {@link #key()}. */
    public static <V> Map<String, V> keyed(Map<Shard, V> m) {
        Map<String, V> out = new LinkedHashMap<>();
        m.forEach((s, v) -> out.put(s.key(), v));
        return out;
    }

    @Override
    public int compareTo(Shard o) {
        return ORDER.compare(this, o);
    }

    @Override
    public String toString() {
        return key();
    }
}
