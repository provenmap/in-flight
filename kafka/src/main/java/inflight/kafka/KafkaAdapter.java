package inflight.kafka;

import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;

import inflight.broker.Broker;
import inflight.broker.BrokerAdapter;
import inflight.broker.Capability;
import inflight.broker.ClusterInfo;
import inflight.broker.Profile;

/**
 * Kafka, and everything that speaks its protocol: Redpanda, WarpStream, Amazon MSK, Confluent Cloud, Azure Event Hubs'
 * Kafka endpoint. Registered in {@code META-INF/services/inflight.broker.BrokerAdapter}.
 */
public final class KafkaAdapter implements BrokerAdapter {

    public static final String KIND = "kafka";

    @Override
    public String kind() {
        return KIND;
    }

    @Override
    public String displayName() {
        return "Kafka";
    }

    @Override
    public String addressHelp() {
        return "the cluster's bootstrap servers, such as kafka-1:9092,kafka-2:9092";
    }

    @Override
    public String defaultAddress() {
        return "localhost:9092";
    }

    @Override
    public Set<Capability> capabilities() {
        return EnumSet.allOf(Capability.class);
    }

    @Override
    public List<Preset> presets() {
        return KafkaAuth.PRESETS;
    }

    @Override
    public Set<String> secretRoles() {
        return KafkaAuth.SECRET_ROLES;
    }

    @Override
    public List<String> problems(Profile p) {
        return KafkaAuth.problems(p);
    }

    @Override
    public void configure(Profile p) {
        Kafka.settings(p.address(), KafkaAuth.clientSettings(p));
    }

    @Override
    public ClusterInfo describe(String address, Duration timeout) {
        try (Admin admin = Kafka.quickAdmin(address, timeout)) {
            DescribeClusterResult d = admin.describeCluster();
            long s = Math.max(1, timeout.toSeconds() + 2);
            String id = d.clusterId().get(s, TimeUnit.SECONDS);
            int nodes = d.nodes().get(s, TimeUnit.SECONDS).size();
            int topics = Kafka.listTopics(admin).size();
            return new ClusterInfo(id, nodes, topics);
        } catch (java.util.concurrent.ExecutionException e) {
            throw new IllegalStateException(e.getCause() != null ? e.getCause().getMessage() : e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (java.util.concurrent.TimeoutException e) {
            throw new IllegalStateException("no answer in " + timeout.toSeconds() + " s", e);
        } catch (RuntimeException e) {
            throw new IllegalStateException(e.getCause() != null ? e.getCause().getMessage() : e.getMessage(), e);
        }
    }

    @Override
    public Broker open(String address) {
        return new KafkaBroker(address);
    }
}
