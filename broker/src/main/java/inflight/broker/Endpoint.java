package inflight.broker;

/**
 * Which broker, and where: what the observer, discovery and redrive are handed instead of a client. Credentials are
 * not here; the adapter keeps them by address once a profile is {@link BrokerAdapter#configure configured}.
 */
public record Endpoint(String kind, String address) {

    public Endpoint {
        kind = Brokers.normal(kind);
    }

    /** An address on a broker of the default kind. */
    public static Endpoint of(String address) {
        return new Endpoint(Brokers.DEFAULT_KIND, address);
    }

    /**
     * From the environment: {@code INFLIGHT_BROKER} for the kind (default {@value Brokers#DEFAULT_KIND}), and
     * {@code BOOTSTRAP_SERVERS} for the address, else the adapter's default.
     */
    public static Endpoint fromEnv() {
        String kind = Brokers.normal(System.getenv("INFLIGHT_BROKER"));
        String address = System.getenv("BOOTSTRAP_SERVERS");
        if (address == null || address.isBlank()) {
            address = Brokers.adapter(kind).defaultAddress();
        }
        return new Endpoint(kind, address);
    }

    public BrokerAdapter adapter() {
        return Brokers.adapter(kind);
    }

    /** A session with this broker. */
    public Broker open() {
        return Brokers.open(this);
    }

    /** "Kafka at localhost:9092", for messages. */
    public String show() {
        String name = Brokers.has(kind) ? adapter().displayName() : kind;
        return name + " at " + address;
    }

    @Override
    public String toString() {
        return address;
    }
}
