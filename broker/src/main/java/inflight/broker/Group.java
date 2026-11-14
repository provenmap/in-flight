package inflight.broker;

import java.util.Set;

/**
 * A consumer group (Kafka), subscription (Pulsar) or application (Kinesis): something that reads streams and
 * remembers how far it got.
 *
 * @param state what the broker says about it, in lower case, such as {@code stable} or {@code empty}; {@code unknown}
 *     when it says nothing
 * @param members how many readers are in it now
 * @param assigned the streams its members are reading now; the ones it has committed on come from
 *     {@link Broker#committed(String)}
 */
public record Group(String id, String state, int members, Set<String> assigned) {

    public Group {
        state = state == null ? "unknown" : state;
        assigned = assigned == null ? Set.of() : Set.copyOf(assigned);
    }
}
