package rif.router;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.streams.CloseOptions;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import rif.catalog.RoutingTable;

/**
 * One intent's Streams application: its own application id, state directory, topology and, when running, its own
 * {@link KafkaStreams}. One instance is one run: a restart replaces it with a new instance of the next generation,
 * under the same application id, so committed offsets and state carry over.
 *
 * <p>A catalog change that leaves the intent's routes as they were does not restart it; the running topology picks up
 * the new revision and the new set of known event versions through {@link #routing()}.
 */
public final class IntentApplication {

    /**
     * Streams leaves its group by default only through the session timeout. On a restart the new instance would wait for
     * the old member, so it leaves explicitly.
     */
    private static final CloseOptions LEAVE = CloseOptions.groupMembershipOperation(CloseOptions.GroupMembershipOperation.LEAVE_GROUP)
            .withTimeout(Duration.ofSeconds(30));

    private final String intent;
    private final String applicationId;
    private final Path stateDir;
    private final int generation;
    private final AtomicReference<IntentTopology.Routing> routing;
    private final Topology topology;
    private final KafkaStreams streams;
    private final String failure;

    IntentApplication(String intent, String applicationId, Path stateDir, int generation, AtomicReference<IntentTopology.Routing> routing,
                      Topology topology, KafkaStreams streams, String failure) {
        this.intent = intent;
        this.applicationId = applicationId;
        this.stateDir = stateDir;
        this.generation = generation;
        this.routing = routing;
        this.topology = topology;
        this.streams = streams;
        this.failure = failure;
    }

    public String intent() {
        return intent;
    }

    public String applicationId() {
        return applicationId;
    }

    public Path stateDir() {
        return stateDir;
    }

    /** 1 for the first run of this intent in this router, one more for each restart. */
    public int generation() {
        return generation;
    }

    /** The routing table the application routes by now. */
    public RoutingTable table() {
        return routing.get().tables().get(0);
    }

    public IntentTopology.Routing routing() {
        return routing.get();
    }

    public Topology topology() {
        return topology;
    }

    /**
     * The Streams state; {@code ERROR} when the application could not be started, and {@code NOT_RUNNING} when the router
     * was built not to run streams.
     */
    public String state() {
        if (failure != null) {
            return "ERROR";
        }
        return streams == null ? "NOT_RUNNING" : streams.state().toString();
    }

    /** Why the application could not be started, or null. */
    public String failure() {
        return failure;
    }

    void update(IntentTopology.Routing next) {
        routing.set(next);
    }

    void close() {
        if (streams != null) {
            streams.close(LEAVE);
        }
    }
}
