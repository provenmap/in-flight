package inflight.broker;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Where a group has got to on one shard: everything below {@code offset} is dealt with. {@code note} is text the group
 * left with its commit (Kafka's commit metadata); redrive uses it to remember records it wrote past a filtered gap.
 * Null when the broker keeps none.
 */
public record Commit(long offset, String note) {

    public static Commit at(long offset) {
        return new Commit(offset, null);
    }

    /** Only the offsets, keyed by {@link Shard#key()}. */
    public static Map<String, Long> offsets(Map<Shard, Commit> commits) {
        Map<String, Long> out = new LinkedHashMap<>();
        commits.forEach((s, c) -> out.put(s.key(), c.offset()));
        return out;
    }
}
