package inflight.broker;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;

/** The broker adapters on the class path, by kind. */
public final class Brokers {

    /** The kind a profile or endpoint that names none is. */
    public static final String DEFAULT_KIND = "kafka";

    private static volatile Map<String, BrokerAdapter> adapters;

    private Brokers() {}

    private static Map<String, BrokerAdapter> adapters() {
        Map<String, BrokerAdapter> a = adapters;
        if (a == null) {
            synchronized (Brokers.class) {
                if (adapters == null) {
                    Map<String, BrokerAdapter> m = new LinkedHashMap<>();
                    for (BrokerAdapter x : ServiceLoader.load(BrokerAdapter.class, Brokers.class.getClassLoader())) {
                        BrokerAdapter before = m.putIfAbsent(x.kind().toLowerCase(Locale.ROOT), x);
                        if (before != null) {
                            throw new IllegalStateException("two broker adapters call themselves " + x.kind() + ": "
                                    + before.getClass().getName() + " and " + x.getClass().getName());
                        }
                    }
                    adapters = Map.copyOf(m);
                }
                a = adapters;
            }
        }
        return a;
    }

    /** The kinds there are adapters for, sorted. */
    public static List<String> kinds() {
        return adapters().keySet().stream().sorted().toList();
    }

    /** Whether there is an adapter for {@code kind}. */
    public static boolean has(String kind) {
        return adapters().containsKey(normal(kind));
    }

    /** The adapter for {@code kind} (the default kind when null), or a failure naming the kinds there are. */
    public static BrokerAdapter adapter(String kind) {
        BrokerAdapter a = adapters().get(normal(kind));
        if (a == null) {
            List<String> ks = kinds();
            throw new IllegalStateException("there is no adapter for " + normal(kind) + " brokers on the class path"
                    + (ks.isEmpty() ? "" : ": this build has " + String.join(", ", ks)));
        }
        return a;
    }

    /** A session with the broker an endpoint names. */
    public static Broker open(Endpoint e) {
        return adapter(e.kind()).open(e.address());
    }

    static String normal(String kind) {
        return kind == null || kind.isBlank() ? DEFAULT_KIND : kind.toLowerCase(Locale.ROOT);
    }
}
