package rif.observer;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import rif.catalog.Catalog;
import rif.kafka.Kafka;

/**
 * The observer: a reader that prices every event into the ledger, a poller for offsets and the simulator, and the
 * simulator's control API. The console and the MCP server both work from one of these. Without a simulator it watches:
 * the same figures, from whatever produces the catalog's topics, and no controls.
 */
public final class Observer implements AutoCloseable {

    /** Where Kafka is, where the simulator is (null to watch a flow with no simulator), and the catalog. */
    public record Options(String bootstrap, String simulatorUrl, Catalog catalog) {
        public static Options fromEnv() {
            return new Options(Kafka.bootstrap(), SimulatorClient.urlFromEnv(), Catalog.defaultCatalog());
        }

        public Options withSimulator(String url) {
            return new Options(bootstrap, url, catalog);
        }

        public Options withBootstrap(String b) {
            return new Options(b, simulatorUrl, catalog);
        }

        public Options withCatalog(Catalog c) {
            return new Options(bootstrap, simulatorUrl, c);
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
        this.reader = new Reader(bootstrap, catalog, frame -> listeners.forEach(l -> l.accept(frame)), poller, simulator == null);
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
