package inflight.broker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * One message as read from a shard. Nothing here is decoded: the codec module turns {@code value} and the headers into
 * an event.
 *
 * @param offset its place in the shard: see {@link Broker} for what offsets must mean
 * @param timestamp when the broker says it was written, in epoch milliseconds
 * @param key the message key, or null
 * @param value the payload, or null for a tombstone
 * @param headers every header, in order, repeats included
 */
public record Message(Shard shard, long offset, long timestamp, byte[] key, byte[] value, List<Header> headers) {

    public Message {
        headers = headers == null ? List.of() : List.copyOf(headers);
    }

    public String stream() {
        return shard.stream();
    }

    public int index() {
        return shard.index();
    }

    /** The value, or no bytes for a tombstone. */
    public byte[] valueOrEmpty() {
        return value == null ? new byte[0] : value;
    }

    /** Headers as text, the last value given for each key winning. */
    public Map<String, String> textHeaders() {
        Map<String, String> out = new HashMap<>();
        for (Header h : headers) {
            out.put(h.key(), h.text());
        }
        return out;
    }

    /** {@code stream:index:offset}: an id for a message that has none of its own. */
    public String place() {
        return shard.key() + ":" + offset;
    }
}
