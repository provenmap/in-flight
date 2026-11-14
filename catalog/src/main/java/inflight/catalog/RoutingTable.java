package inflight.catalog;

import java.util.List;
import java.util.Optional;

/**
 * An intent compiled against one catalog revision: exactly which event versions it means, from which topics, keyed by
 * which entity. The router reads nothing else at runtime.
 */
public record RoutingTable(String intent, String predicate, String entity, int revision, List<Route> routes, List<String> sourceTopics) {

    /** One thing a consumer of an intent takes from an event: the entity's key, or what feeds a watched attribute. */
    public record Read(String what, String source, String type) {}

    /** How one event type version reaches an intent: why it matches, and what a consumer reads from it. */
    public record Route(EventRef event, String topic, FieldPath keyPath, List<String> reasons, List<Read> reads) {
        public Route {
            reasons = List.copyOf(reasons);
            reads = List.copyOf(reads);
        }
    }

    public RoutingTable {
        routes = List.copyOf(routes);
        sourceTopics = List.copyOf(sourceTopics);
    }

    public String topic() {
        return Intent.topicFor(intent);
    }

    public Optional<Route> lookup(String name, int version) {
        return routes.stream().filter(r -> r.event().name().equals(name) && r.event().version() == version).findFirst();
    }

    public RoutingTable named(String name) {
        return new RoutingTable(name, predicate, entity, revision, routes, sourceTopics);
    }
}
