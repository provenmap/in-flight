package inflight.broker;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A session with one broker: what the observer, discovery, redrive, the intent tail and rehearse ask of it. Opened by
 * {@link BrokerAdapter#open(String)}, used by one part of the tool at a time, closed when that part is done.
 *
 * <h2>Offsets</h2>
 * Every position is a {@code long} offset within a {@link Shard}, and the tool relies on three things about them:
 * <ol>
 *   <li>they only grow: a later message on a shard has a larger offset;</li>
 *   <li>the end of a shard ({@link #ends}) is the offset the next message written will get;</li>
 *   <li>with {@link Capability#GROUP_LAG}, they are dense: {@code end - committed} is how many messages are behind.</li>
 * </ol>
 * A broker whose positions do not fit (Kinesis sequence numbers, which are 128-bit and sparse) cannot be adapted
 * faithfully yet: the observer's ledger and its saved state count in offsets. That is a known limit of this interface.
 *
 * <h2>Failures</h2>
 * A call the broker does not answer, or refuses, throws {@link IllegalStateException} with the broker's reason, which
 * the console shows as it is. A stream that does not exist is not a failure where a method says what happens instead.
 */
public interface Broker extends AutoCloseable {

    /** What this broker can do; the same set as its adapter's. */
    Set<Capability> capabilities();

    /** The cluster, for a connection test. */
    ClusterInfo describe();

    /** Every stream's name, internal ones included: discovery decides what each is. */
    Set<String> streams();

    /** Every shard of these streams, sorted by stream then index. Streams that do not exist have none. */
    List<Shard> shards(Collection<String> streams);

    /** The offset of the oldest message still kept on each shard. */
    Map<Shard, Long> starts(Collection<Shard> shards);

    /** The offset the next message written to each shard will get. */
    Map<Shard, Long> ends(Collection<Shard> shards);

    /**
     * The offset of the first message on each shard written at or after {@code at}; a shard with none has its
     * {@link #ends end}.
     */
    Map<Shard, Long> offsetsAt(Collection<Shard> shards, Instant at);

    /** Every group that reads from this broker, except the ones this tool's own observing cursors make. */
    List<Group> groups();

    /** A group's committed position on each shard it has committed on; empty for a group with none, or none at all. */
    Map<Shard, Commit> committed(String group);

    /** How many readers a group has now: none for a group that does not exist. */
    int members(String group);

    /** A cursor with no side effects, as {@link Capability#OBSERVE} describes. */
    Cursor observe();

    /** A cursor that reads under {@code group} and can commit for it. Needs {@link Capability#REDRIVE}. */
    Cursor consume(String group);

    /** Writes messages. Needs {@link Capability#REDRIVE}. */
    Writer writer();

    @Override
    void close();
}
