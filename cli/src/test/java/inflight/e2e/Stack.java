package inflight.e2e;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * The whole stack the Compose file runs, in containers for a test: Kafka (reachable as kafka:19092 inside), the schema
 * registry, and Kroxylicious with this repository's filters on fixed host ports, since the proxy advertises them.
 */
final class Stack implements AutoCloseable {

    static final int PROXY_PORT = 29192;

    private final Network net;
    private final KafkaContainer kafka;
    private final GenericContainer<?> apicurio;
    private final GenericContainer<?> proxy;

    private Stack(Network net, KafkaContainer kafka, GenericContainer<?> apicurio, GenericContainer<?> proxy) {
        this.net = net;
        this.kafka = kafka;
        this.apicurio = apicurio;
        this.proxy = proxy;
    }

    /** A whole stack for one suite, stopped after it: the proxy's ports are fixed, so suites take turns. */
    @SuppressWarnings("resource")
    static Stack start() {
        Path jar = Path.of("stack/in-flight-proxy-filter.jar");
        if (!Files.exists(jar)) {
            throw new IllegalStateException("build first: ./mvnw package puts the filter at " + jar);
        }
        Network net = Network.newNetwork();
        KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.4.0")).withNetwork(net).withListener("kafka:19092");
        kafka.start();
        GenericContainer<?> apicurio = new GenericContainer<>(DockerImageName.parse("apicurio/apicurio-registry:3.3.3"))
                .withNetwork(net).withNetworkAliases("apicurio").withExposedPorts(8080)
                .waitingFor(Wait.forHttp("/apis/registry/v3/system/info").forPort(8080).withStartupTimeout(Duration.ofMinutes(2)));
        apicurio.start();
        String config = """
                clusterDefinitions:
                  - name: kafka
                    bootstrapServers: kafka:19092
                virtualClusters:
                  - name: intents
                    target:
                      cluster: kafka
                    gateways:
                      - name: plain
                        portIdentifiesNode:
                          bootstrapAddress: localhost:%d
                filterDefinitions:
                  - name: schema-gate
                    type: inflight.proxy.SchemaGateFilterFactory
                    config:
                      registryUrl: http://apicurio:8080/apis/registry/v3
                      group: intent-bus
                      negativeCacheMs: 500
                  - name: intent-alias
                    type: inflight.proxy.IntentAliasFilterFactory
                defaultFilters:
                  - schema-gate
                  - intent-alias
                """.formatted(PROXY_PORT);
        @SuppressWarnings({"deprecation", "resource"})
        FixedHostPortGenericContainer<?> proxy = new FixedHostPortGenericContainer<>("quay.io/kroxylicious/proxy:0.25.1");
        proxy.withNetwork(net)
                .withCopyToContainer(org.testcontainers.images.builder.Transferable.of(config), "/opt/in-flight/proxy.yaml")
                .withCopyFileToContainer(MountableFile.forHostPath(jar), "/opt/in-flight/in-flight-proxy-filter.jar")
                .withEnv("KROXYLICIOUS_CLASSPATH", "/opt/in-flight/in-flight-proxy-filter.jar")
                .withCommand("--config", "/opt/in-flight/proxy.yaml");
        for (int i = 0; i < 4; i++) {
            proxy.withFixedExposedPort(PROXY_PORT + i, PROXY_PORT + i);
        }
        proxy.waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)));
        proxy.start();
        return new Stack(net, kafka, apicurio, proxy);
    }

    String bootstrap() {
        return kafka.getBootstrapServers();
    }

    String proxy() {
        return "localhost:" + PROXY_PORT;
    }

    String registry() {
        return "http://localhost:" + apicurio.getMappedPort(8080) + "/apis/registry/v3";
    }

    @Override
    public void close() {
        proxy.stop();
        apicurio.stop();
        kafka.stop();
        net.close();
    }
}
