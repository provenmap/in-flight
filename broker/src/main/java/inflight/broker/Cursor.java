package inflight.broker;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Reads messages from chosen shards, from chosen offsets. One thread uses a cursor, except for {@link #wakeup()}.
 *
 * <p>A cursor from {@link Broker#observe()} must have no side effects at all (see {@link Capability#OBSERVE}). One from
 * {@link Broker#consume(String)} reads under a group and may {@link #commit} for it.
 */
public interface Cursor extends AutoCloseable {

    /** Thrown by {@link #poll} on the thread that was polling, after another thread called {@link #wakeup()}. */
    final class Woken extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public Woken() {
            super("woken");
        }
    }

    /**
     * Reads these shards from these offsets from now on, adding them to what the cursor reads already. Shards already
     * being read and not named here keep their places.
     */
    void seek(Map<Shard, Long> from);

    /** The next messages, waiting up to {@code timeout} for some; empty when there are none yet. */
    List<Message> poll(Duration timeout);

    /** The offset of the next message this cursor will return from {@code shard}. */
    long position(Shard shard);

    /** Records how far the group has got. Only for a cursor from {@link Broker#consume(String)}. */
    void commit(Map<Shard, Commit> commits);

    /** Makes a {@link #poll} in progress, or the next one, throw {@link Woken}. Safe from any thread. */
    void wakeup();

    @Override
    void close();
}
