package inflight.router;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;

/**
 * Creates the topics the router needs: the registry, the source topics the catalog names, and one per intent. Topics
 * that exist are left as they are.
 */
public final class TopicManager implements AutoCloseable {

    private final Admin admin;
    private final int partitions;
    private final short replication;

    public TopicManager(String bootstrap, int partitions, short replication) {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        this.admin = Admin.create(p);
        this.partitions = partitions;
        this.replication = replication;
    }

    public void ensure(Collection<String> topics) {
        ensure(topics, false);
    }

    /** Creates whichever of these topics are missing. A compacted topic gets one partition. */
    public void ensure(Collection<String> topics, boolean compacted) {
        try {
            Set<String> existing = admin.listTopics().names().get();
            List<String> missing = topics.stream().distinct().filter(t -> !existing.contains(t)).toList();
            if (missing.isEmpty()) {
                return;
            }
            Map<String, String> configs = compacted ? Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT) : Map.of();
            int parts = compacted ? 1 : partitions;
            List<NewTopic> news = missing.stream().map(t -> new NewTopic(t, parts, replication).configs(configs)).toList();
            try {
                admin.createTopics(news).all().get();
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof TopicExistsException)) {
                    throw e;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted creating topics", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("cannot create topics " + topics + ": " + e.getCause().getMessage(), e.getCause());
        }
    }

    @Override
    public void close() {
        admin.close();
    }
}
