package rif.observer;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.LockSupport;

import org.apache.kafka.clients.admin.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

import rif.catalog.Catalog;
import rif.kafka.Kafka;

/**
 * Polls, once a second, what the reader cannot see in the records themselves: the service groups' committed offsets,
 * the end of every partition they read, the redrive groups' committed offsets, and the simulator's knobs, if there is a
 * simulator.
 */
public final class Poller {

    private static final Logger log = LoggerFactory.getLogger(Poller.class);

    /**
     * What the poller last saw: each service group's committed offsets, the redrive groups' commits by queue, each
     * partition's end, and the simulator's state (null when there is none, or it is down).
     */
    public record Polled(Map<String, Map<String, Long>> committed, Map<String, Map<String, Long>> redriven, Map<String, Long> ends, JsonNode sim) {}

    private final String bootstrap;
    private final Catalog catalog;
    private final SimulatorClient simulator;
    private volatile Polled latest;
    private volatile boolean running = true;
    private Thread thread;

    public Poller(String bootstrap, Catalog catalog, SimulatorClient simulator) {
        this.bootstrap = bootstrap;
        this.catalog = catalog;
        this.simulator = simulator;
    }

    public Polled latest() {
        return latest;
    }

    public void start() {
        thread = Thread.ofPlatform().daemon().name("poller").start(this::run);
    }

    private void run() {
        try (Admin admin = Kafka.admin(bootstrap)) {
            while (running) {
                try {
                    Map<String, Map<String, Long>> committed = new LinkedHashMap<>();
                    for (Catalog.Consumer s : catalog.services()) {
                        committed.put(s.group(), Kafka.committed(admin, s.group()));
                    }
                    Set<String> topics = new LinkedHashSet<>();
                    catalog.services().forEach(s -> topics.addAll(s.reads()));
                    Set<String> existing = Kafka.listTopics(admin);
                    topics.retainAll(existing);
                    catalog.intentTopicsByGroup().values().stream().filter(existing::contains).forEach(topics::add);
                    Map<String, Long> ends = Kafka.ends(admin, Kafka.partitions(admin, topics));
                    Map<String, Map<String, Long>> redriven = new LinkedHashMap<>();
                    for (String q : catalog.dlqs()) {
                        String g = catalog.redriveGroupOf(q);
                        if (g != null) {
                            redriven.put(q, Kafka.committed(admin, g));
                        }
                    }
                    JsonNode sim = simulator == null ? null : simulator.state(Duration.ofSeconds(1));
                    latest = new Polled(committed, redriven, ends, sim);
                } catch (RuntimeException e) {
                    if (running) {
                        log.warn("poller: {}", e.getMessage());
                    }
                }
                sleep(1000);
            }
        } catch (RuntimeException e) {
            log.error("poller: {}", e.getMessage(), e);
        }
    }

    private void sleep(long ms) {
        long until = System.currentTimeMillis() + ms;
        while (running && System.currentTimeMillis() < until) {
            LockSupport.parkNanos(50_000_000L);
        }
    }

    public void stop() throws InterruptedException {
        running = false;
        if (thread != null) {
            thread.join(15_000);
        }
    }

    static List<String> sortedKeys(Map<String, ?> m) {
        return m.keySet().stream().sorted().toList();
    }
}
