package inflight.broker;

import java.util.List;

/** Writes messages, for redrive. Only from an adapter with {@link Capability#REDRIVE}. */
public interface Writer extends AutoCloseable {

    /** Writes one message and returns once the broker has it durably; fails with the broker's reason otherwise. */
    void send(String stream, byte[] key, byte[] value, List<Header> headers);

    @Override
    void close();
}
