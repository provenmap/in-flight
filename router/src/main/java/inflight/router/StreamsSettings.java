package inflight.router;

import java.nio.file.Path;
import java.util.Properties;

import org.apache.kafka.streams.StreamsConfig;

/**
 * What every intent's Streams application shares, and how each gets its own name and state from it.
 *
 * <p>Each intent runs as its own application, {@code <applicationId>-<intent>}, with state under
 * {@code <stateDir>/<intent>}. A new intent has no committed offsets, so it reads its source topics from the earliest
 * offset and sees events produced before it was declared. Restarting one intent leaves the others alone.
 *
 * @param guarantee {@code at_least_once}, or {@code exactly_once_v2} for exactly-once into the intent topics
 */
public record StreamsSettings(String bootstrap, String applicationId, Path stateDir, String guarantee, int replication) {

    public String applicationIdFor(String intent) {
        return applicationId + "-" + intent;
    }

    public Path stateDirFor(String intent) {
        return stateDir.resolve(intent);
    }

    public Properties propertiesFor(String intent) {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationIdFor(intent));
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(StreamsConfig.STATE_DIR_CONFIG, stateDirFor(intent).toString());
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, guarantee);
        p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, replication);
        p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
        p.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "earliest");
        return p;
    }
}
