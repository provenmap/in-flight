package rif.router;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rif.catalog.Catalog;
import rif.catalog.Result;

/**
 * The router as one thing to start and stop: the registry topic, one Streams application per intent, and the control
 * API. {@link #start(Config)} returns once everything is up; {@link #close()} stops it all.
 */
public final class Router implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Router.class);

    /**
     * Everything the router is configured with.
     *
     * @param seedCatalog a catalog file to activate when the registry holds none; null for none
     */
    public record Config(String bootstrapServers, String applicationId, int port, Duration grace, Duration tick, Path stateDir,
                         String processingGuarantee, int partitions, int replication, Path seedCatalog) {

        /**
         * Reads BOOTSTRAP_SERVERS (localhost:9092), APPLICATION_ID (rif-router), ROUTER_PORT (8085), GRACE_MS (1500),
         * TICK_MS (250), STATE_DIR (rif-router in the temporary directory), PROCESSING_GUARANTEE (at_least_once),
         * PARTITIONS (3), REPLICATION (1) and SEED_CATALOG (none).
         */
        public static Config fromEnv(Map<String, String> env) {
            String seed = env.get("SEED_CATALOG");
            return new Config(
                    env.getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092"),
                    env.getOrDefault("APPLICATION_ID", "rif-router"),
                    Integer.parseInt(env.getOrDefault("ROUTER_PORT", "8085")),
                    Duration.ofMillis(Long.parseLong(env.getOrDefault("GRACE_MS", "1500"))),
                    Duration.ofMillis(Long.parseLong(env.getOrDefault("TICK_MS", "250"))),
                    Path.of(env.getOrDefault("STATE_DIR", Path.of(System.getProperty("java.io.tmpdir"), "rif-router").toString())),
                    env.getOrDefault("PROCESSING_GUARANTEE", "at_least_once"),
                    Integer.parseInt(env.getOrDefault("PARTITIONS", "3")),
                    Integer.parseInt(env.getOrDefault("REPLICATION", "1")),
                    seed == null || seed.isBlank() ? null : Path.of(seed));
        }

        public Config withSeedCatalog(Path file) {
            return new Config(bootstrapServers, applicationId, port, grace, tick, stateDir, processingGuarantee, partitions, replication, file);
        }

        public Config withPort(int p) {
            return new Config(bootstrapServers, applicationId, p, grace, tick, stateDir, processingGuarantee, partitions, replication, seedCatalog);
        }

        public StreamsSettings streams() {
            return new StreamsSettings(bootstrapServers, applicationId, stateDir, processingGuarantee, replication);
        }

        public IntentTopology.Ordering ordering() {
            return new IntentTopology.Ordering(grace, tick);
        }
    }

    private final RouterService service;
    private final ControlApi api;

    private Router(RouterService service, ControlApi api) {
        this.service = service;
        this.api = api;
    }

    /** Creates the registry topic, loads the registry, starts each intent's application and the control API. */
    public static Router start(Config config) {
        TopicManager topics = new TopicManager(config.bootstrapServers(), config.partitions(), (short) config.replication());
        KafkaRegistry registry = null;
        RouterService service = null;
        try {
            topics.ensure(List.of(KafkaRegistry.TOPIC), true);
            registry = new KafkaRegistry(config.bootstrapServers(), KafkaRegistry.TOPIC);
            service = new RouterService(registry, topics, config.streams(), config.ordering(), true);
            if (service.catalog().isEmpty() && config.seedCatalog() != null) {
                Result<Catalog> seeded = Catalog.load(config.seedCatalog()).flatMap(service::activateCatalog);
                if (!seeded.isOk()) {
                    throw new IllegalStateException("seed catalog " + config.seedCatalog() + " rejected: " + seeded.error());
                }
                log.info("seeded catalog revision {} from {}", seeded.value().revision(), config.seedCatalog());
            } else {
                service.start();
            }
            ControlApi api = new ControlApi(service, config.port());
            api.start();
            log.info("router control API on http://localhost:{}", api.port());
            return new Router(service, api);
        } catch (RuntimeException e) {
            if (service != null) {
                service.close();
            } else {
                if (registry != null) {
                    registry.close();
                }
                topics.close();
            }
            throw e;
        }
    }

    public RouterService service() {
        return service;
    }

    /** The control API's port, which differs from the configured one when that was 0. */
    public int port() {
        return api.port();
    }

    @Override
    public void close() {
        api.close();
        service.close();
    }
}
