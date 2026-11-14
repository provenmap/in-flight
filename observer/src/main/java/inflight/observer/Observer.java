package inflight.observer;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import inflight.catalog.Catalog;
import inflight.kafka.Decoders;
import inflight.kafka.Kafka;

/**
 * The observer: a reader that prices every event into the ledger, a poller for offsets and the simulator, and the
 * simulator's control API. The console and the MCP server both work from one of these. Without a simulator it watches:
 * the same figures, from whatever produces the catalog's topics, and no controls.
 */
public final class Observer implements AutoCloseable {

    /**
     * Where Kafka is, where the simulator is (null to watch a flow with no simulator), the catalog, how records are
     * decoded, and where the ledger keeps its state between runs (null to keep none).
     */
    public record Options(String bootstrap, String simulatorUrl, Catalog catalog, Decoders decoders, java.nio.file.Path stateFile) {
        public Options(String bootstrap, String simulatorUrl, Catalog catalog) {
            this(bootstrap, simulatorUrl, catalog, Decoders.plain(), null);
        }

        public Options {
            if (decoders == null) {
                decoders = Decoders.plain();
            }
        }

        public static Options fromEnv() {
            return new Options(Kafka.bootstrap(), SimulatorClient.urlFromEnv(), Catalog.defaultCatalog());
        }

        public Options withSimulator(String url) {
            return new Options(bootstrap, url, catalog, decoders, stateFile);
        }

        public Options withBootstrap(String b) {
            return new Options(b, simulatorUrl, catalog, decoders, stateFile);
        }

        public Options withCatalog(Catalog c) {
            return new Options(bootstrap, simulatorUrl, c, decoders, stateFile);
        }

        public Options withDecoders(Decoders d) {
            return new Options(bootstrap, simulatorUrl, catalog, d, stateFile);
        }

        public Options withStateFile(java.nio.file.Path f) {
            return new Options(bootstrap, simulatorUrl, catalog, decoders, f);
        }
    }

    private final String bootstrap;
    private final Catalog catalog;
    private final SimulatorClient simulator;
    private final Poller poller;
    private final Reader reader;
    private final List<Consumer<ArrayNode>> listeners = new CopyOnWriteArrayList<>();

    public Observer(Options opts) {
        this.bootstrap = opts.bootstrap();
        this.catalog = opts.catalog();
        this.simulator = opts.simulatorUrl() == null ? null : new SimulatorClient(opts.simulatorUrl());
        this.poller = new Poller(bootstrap, catalog, simulator);
        this.reader = new Reader(bootstrap, catalog, frame -> listeners.forEach(l -> l.accept(frame)), poller, simulator == null, opts.decoders(), opts.stateFile());
    }

    public Observer start() {
        poller.start();
        reader.start();
        return this;
    }

    /** Every frame the reader publishes from now on; the returned runnable stops them. */
    public Runnable onFrame(Consumer<ArrayNode> listener) {
        listeners.add(listener);
        return () -> listeners.remove(listener);
    }

    public String bootstrap() {
        return bootstrap;
    }

    public Catalog catalog() {
        return catalog;
    }

    /** The simulator, or null when this observer only watches. */
    public SimulatorClient simulator() {
        return simulator;
    }

    public Reader reader() {
        return reader;
    }

    public ObjectNode flow() {
        return Messages.flow(catalog);
    }

    public boolean watching() {
        return simulator == null;
    }

    @Override
    public void close() throws InterruptedException {
        reader.stop();
        poller.stop();
    }
}
