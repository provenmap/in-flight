package rif.catalog;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * The flow: its districts, every event type version, who reads what, what the events mean, and the intents declared
 * over them. One file describes it; everything else reads it.
 *
 * @param flow the flow's name
 * @param revision the catalog's revision, which contracts uses to tell a proposal from the active catalog
 * @param currency the currency values are in, or null when the catalog prices nothing
 * @param roles what is in flight, who waits on it and what it is worth, in the catalog's own terms; may be null
 * @param semantics what the events mean and the intents declared over them; null when the catalog says nothing about meaning
 */
public record Catalog(String flow, int revision, String currency, List<District> districts, List<EventSpec> events, List<Consumer> consumers,
                      DeadLetterHeaders deadLetterHeaders, Roles roles, Semantics semantics, Map<String, java.math.BigDecimal> rates) {

    /** A catalog with no exchange rates: every event is in the catalog's one currency. */
    public Catalog(String flow, int revision, String currency, List<District> districts, List<EventSpec> events, List<Consumer> consumers,
                   DeadLetterHeaders deadLetterHeaders, Roles roles, Semantics semantics) {
        this(flow, revision, currency, districts, events, consumers, deadLetterHeaders, roles, semantics, Map.of());
    }

    /** A place on the map: a team that produces or reads events. A sink, such as the customers, only reads. */
    public record District(String id, String name, boolean sink) {}

    /**
     * An event type version, its topic and district, and where its value, customer and order are declared to be. Each
     * path may be null: with roles declared, the order and the customer come from the event's keys and the value from
     * its effect on the measure, so a path is only an override.
     */
    public record EventSpec(EventRef ref, String topic, String district, FieldPath value, FieldPath customer, FieldPath order, Amount amount) {
        public EventSpec(EventRef ref, String topic, String district, FieldPath value, FieldPath customer, FieldPath order) {
            this(ref, topic, district, value, customer, order, Amount.PLAIN);
        }

        public EventSpec {
            if (amount == null) {
                amount = Amount.PLAIN;
            }
        }
    }

    /**
     * How an event's value is written: in major units (dollars) or minor ones (cents, {@code total_cents}), and in which
     * currency, fixed for the event or read from a field of it. Null currency and field: the catalog's currency.
     */
    public record Amount(boolean minorUnits, String currency, FieldPath currencyField) {
        public static final Amount PLAIN = new Amount(false, null, null);

        public boolean isPlain() {
            return !minorUnits && currency == null && currencyField == null;
        }
    }

    /**
     * A reader of topics. A service has a consumer group and a dead-letter queue; a sink has neither. A service that
     * can redrive its dead-letter queue names the consumer group that does it: that group's committed offsets say which
     * dead letters have been dealt with, which is how the observer knows after a restart.
     *
     * @param intent the declared intent this consumer reads by, or null
     * @param name the group, or the district for a sink
     */
    public record Consumer(String group, String district, List<String> reads, String dlq, String redrive, String intent, String name) {
        public Consumer {
            reads = List.copyOf(reads);
        }

        public boolean isService() {
            return group != null;
        }
    }

    /**
     * The headers a flow's services put on a dead letter, when they are not one of the conventions the observer knows:
     * this app's own, Kafka Connect's and Spring Kafka's. Each may be null.
     */
    public record DeadLetterHeaders(String error, String failedAt, String sourceTopic) {
        public static final DeadLetterHeaders NONE = new DeadLetterHeaders(null, null, null);

        boolean isEmpty() {
            return error == null && failedAt == null && sourceTopic == null;
        }
    }

    /**
     * The three roles that turn events into money, or any other measure: the case in flight (an entity, such as Order),
     * the party waiting on it (Customer), and the measure it is worth (an attribute of the case, such as Order.total).
     * An event's case and party come from its keys; its worth from its own effect on the measure, else from what has
     * been seen for its case. Any of the three may be null.
     */
    public record Roles(String caseEntity, String party, AttrRef measure) {}

    public Catalog {
        districts = List.copyOf(districts);
        events = List.copyOf(events);
        consumers = List.copyOf(consumers);
        if (deadLetterHeaders == null) {
            deadLetterHeaders = DeadLetterHeaders.NONE;
        }
        rates = rates == null ? Map.of() : Map.copyOf(rates);
    }

    // ---- what it says ------------------------------------------------------------------------------------------------

    public Optional<EventSpec> find(EventRef ref) {
        return events.stream().filter(e -> e.ref().equals(ref)).findFirst();
    }

    public Optional<District> district(String id) {
        return districts.stream().filter(d -> d.id().equals(id)).findFirst();
    }

    /** Every flow topic, in the order the catalog first names it. */
    public List<String> topics() {
        return List.copyOf(new LinkedHashSet<>(events.stream().map(EventSpec::topic).toList()));
    }

    public List<String> dlqs() {
        return consumers.stream().map(Consumer::dlq).filter(q -> q != null).toList();
    }

    /** The consumers with a group: the services. */
    public List<Consumer> services() {
        return consumers.stream().filter(Consumer::isService).toList();
    }

    public Optional<Consumer> consumer(String group) {
        return consumers.stream().filter(k -> group.equals(k.group())).findFirst();
    }

    public List<Consumer> readersOf(String topic) {
        return consumers.stream().filter(k -> k.reads().contains(topic)).toList();
    }

    /** The consumers that read by this intent. */
    public List<Consumer> consumersOf(String intent) {
        return consumers.stream().filter(k -> intent.equals(k.intent())).toList();
    }

    /** The consumers of an intent with the queue their failures land in, as impact analysis needs them. */
    public Function<String, List<Impact.Reader>> readers() {
        return intent -> consumersOf(intent).stream().map(k -> new Impact.Reader(k.name(), k.dlq())).toList();
    }

    /** The consumer group that redrives this dead-letter queue, or null. */
    public String redriveGroupOf(String dlq) {
        return ownerOfDlq(dlq).map(Consumer::redrive).orElse(null);
    }

    /** The service that dead-letters into this queue. */
    public Optional<Consumer> ownerOfDlq(String dlq) {
        return consumers.stream().filter(k -> dlq.equals(k.dlq())).findFirst();
    }

    /** The district that produces onto this topic, or null. One topic belongs to one district. */
    public String districtOf(String topic) {
        return events.stream().filter(e -> e.topic().equals(topic)).map(EventSpec::district).findFirst().orElse(null);
    }

    /** The meaning of an event version, when the catalog gives it one. */
    public Optional<Model.Event> meaning(EventRef ref) {
        return semantics == null ? Optional.empty() : semantics.model().find(ref);
    }

    /** Where an event carries its order (the case): the declared path, else the case's key. Null when neither. */
    public FieldPath orderPath(EventSpec e) {
        if (e.order() != null) {
            return e.order();
        }
        return roles == null || roles.caseEntity() == null ? null : meaning(e.ref()).map(m -> m.keys().get(roles.caseEntity())).orElse(null);
    }

    /** Where an event carries its customer (the party): the declared path, else the party's key. Null when neither. */
    public FieldPath customerPath(EventSpec e) {
        if (e.customer() != null) {
            return e.customer();
        }
        return roles == null || roles.party() == null ? null : meaning(e.ref()).map(m -> m.keys().get(roles.party())).orElse(null);
    }

    /** The event's effects on the measure, in the order declared. */
    public List<Effect> measureEffects(EventSpec e) {
        if (roles == null || roles.measure() == null) {
            return List.of();
        }
        return meaning(e.ref()).map(m -> m.affects(roles.measure())).orElse(List.of());
    }

    /** True when the catalog can put a value on its events at all. */
    public boolean prices() {
        return (roles != null && roles.measure() != null) || events.stream().anyMatch(e -> e.value() != null);
    }

    /** The same catalog at another revision. */
    public Catalog withRevision(int r) {
        com.fasterxml.jackson.databind.node.ObjectNode json = (com.fasterxml.jackson.databind.node.ObjectNode) toJson();
        json.put("revision", r);
        return fromJson(json).value();
    }

    /**
     * The catalog as it was before one event version was declared: the event taken out, with its meaning. Fails when
     * something else depends on it, such as an intent that would match nothing without it.
     */
    public Result<Catalog> withoutEvent(EventRef ref) {
        com.fasterxml.jackson.databind.node.ObjectNode json = (com.fasterxml.jackson.databind.node.ObjectNode) toJson();
        com.fasterxml.jackson.databind.node.ArrayNode kept = Json.array();
        json.get("events").forEach(e -> {
            if (!(e.get("type").asText().equals(ref.name()) && e.get("version").asInt() == ref.version())) {
                kept.add(e);
            }
        });
        json.set("events", kept);
        return fromJson(json);
    }

    /** The topic an intent is materialised on, for each service that reads by intent: {@code __intent.<name>}. */
    public java.util.Map<String, String> intentTopicsByGroup() {
        java.util.Map<String, String> out = new java.util.LinkedHashMap<>();
        services().forEach(s -> {
            if (s.intent() != null) {
                out.put(s.group(), Intent.topicFor(s.intent()));
            }
        });
        return out;
    }

    public Catalog withSemantics(Semantics s) {
        return new Catalog(flow, revision, currency, districts, events, consumers, deadLetterHeaders, roles, s, rates);
    }

    // ---- reading ---------------------------------------------------------------------------------------------------

    public static Result<Catalog> load(Path file) {
        try {
            return parse(Files.readString(file));
        } catch (IOException e) {
            return Result.err("cannot read " + file + ": " + e.getMessage());
        }
    }

    public static Result<Catalog> parse(String text) {
        Result<com.fasterxml.jackson.databind.JsonNode> json = Json.parse(text);
        return json.isOk() ? CatalogReader.read(json.value()) : Result.err(json.errors());
    }

    public static Result<Catalog> fromJson(com.fasterxml.jackson.databind.JsonNode json) {
        return CatalogReader.read(json);
    }

    public com.fasterxml.jackson.databind.JsonNode toJson() {
        return CatalogWriter.write(this);
    }

    // ---- the shipped catalog -----------------------------------------------------------------------------------------

    /** The catalog this build ships with, inside the jar. */
    public static final String SHIPPED = "/rif/catalog/order-to-cash.json";

    private static volatile Catalog shipped;

    /**
     * {@code RIF_CATALOG} when it is set, else the shipped order-to-cash flow. The file is read once.
     */
    public static Catalog defaultCatalog() {
        Catalog c = shipped;
        if (c == null) {
            String file = System.getenv("RIF_CATALOG");
            Result<Catalog> res = file != null && !file.isEmpty() ? load(Path.of(file)) : parse(shippedText());
            if (!res.isOk()) {
                throw new IllegalStateException((file != null ? file : "the shipped catalog") + ": " + res.error());
            }
            shipped = c = res.value();
        }
        return c;
    }

    public static String shippedText() {
        try (InputStream in = Catalog.class.getResourceAsStream(SHIPPED)) {
            if (in == null) {
                throw new IllegalStateException("the shipped catalog is missing from the build: " + SHIPPED);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // ---- checks ----------------------------------------------------------------------------------------------------

    /** Every reference resolves, and no event type version, consumer group or dead-letter queue is declared twice. */
    public static List<String> validate(Catalog c) {
        Set<String> ids = new LinkedHashSet<>();
        List<String> out = new ArrayList<>();
        for (String d : dupes(c.districts().stream().map(District::id).toList())) {
            out.add("district '" + d + "' is declared twice");
        }
        c.districts().forEach(d -> ids.add(d.id()));
        Set<String> topics = new LinkedHashSet<>(c.topics());

        if (c.revision() < 1) {
            out.add("catalog: 'revision' must be at least 1, was " + c.revision());
        }
        for (EventSpec e : c.events()) {
            String where = e.ref().show();
            if (!e.ref().name().matches("[A-Z][A-Za-z0-9]*")) {
                out.add(where + ": type name must be a capitalised identifier");
            }
            if (e.ref().version() < 1) {
                out.add(where + ": version must be at least 1");
            }
            if (!e.topic().matches("[A-Za-z0-9][A-Za-z0-9._-]*") || e.topic().startsWith("__") || e.topic().startsWith("intent.")) {
                out.add(where + ": '" + e.topic() + "' cannot be a source topic (reserved or invalid name)");
            }
            if (!ids.contains(e.district())) {
                out.add(where + ": unknown district '" + e.district() + "'");
            }
            if (c.district(e.district()).map(District::sink).orElse(false)) {
                out.add(where + ": a sink cannot produce events");
            }
        }
        for (String r : dupes(c.events().stream().map(e -> e.ref().show()).toList())) {
            out.add(r + " is declared twice");
        }
        for (String t : c.topics()) {
            long producers = c.events().stream().filter(e -> e.topic().equals(t)).map(EventSpec::district).distinct().count();
            if (producers > 1) {
                out.add("topic '" + t + "' is produced by more than one district");
            }
        }

        for (Consumer k : c.consumers()) {
            String who = "consumer " + k.name();
            if (!ids.contains(k.district())) {
                out.add(who + ": unknown district '" + k.district() + "'");
            }
            if (k.reads().isEmpty()) {
                out.add(who + ": reads nothing");
            }
            for (String t : k.reads()) {
                if (!topics.contains(t)) {
                    out.add(who + ": reads unknown topic '" + t + "'");
                }
            }
            if (k.dlq() != null && k.group() == null) {
                out.add(who + ": a dead-letter queue needs a consumer group");
            }
            if (k.dlq() != null && topics.contains(k.dlq())) {
                out.add(who + ": dead-letter queue '" + k.dlq() + "' is also an event topic");
            }
            if (k.redrive() != null && k.dlq() == null) {
                out.add(who + ": a redrive group needs a dead-letter queue");
            }
        }
        List<String> groups = new ArrayList<>();
        for (Consumer k : c.consumers()) {
            if (k.group() != null) {
                groups.add(k.group());
            }
            if (k.redrive() != null) {
                groups.add(k.redrive());
            }
        }
        for (String g : dupes(groups)) {
            out.add("consumer group '" + g + "' is declared twice");
        }
        for (String q : dupes(c.dlqs())) {
            out.add("dead-letter queue '" + q + "' is declared twice");
        }

        out.addAll(validateRoles(c));
        return out;
    }

    private static List<String> validateRoles(Catalog c) {
        List<String> out = new ArrayList<>();
        Roles r = c.roles();
        Map<String, Map<String, FieldType>> entities = c.semantics() == null ? Map.of() : c.semantics().model().entities();
        if (r != null) {
            if (r.caseEntity() != null && !entities.containsKey(r.caseEntity())) {
                out.add("roles: the case '" + r.caseEntity() + "' is not an entity");
            }
            if (r.party() != null && !entities.containsKey(r.party())) {
                out.add("roles: the party '" + r.party() + "' is not an entity");
            }
            if (r.measure() != null) {
                FieldType t = c.semantics() == null ? null : c.semantics().model().attributeType(r.measure());
                if (t == null) {
                    out.add("roles: the measure " + r.measure().show() + " is not an attribute");
                } else if (!t.isNumeric()) {
                    out.add("roles: the measure " + r.measure().show() + " must be numeric, is " + t.show());
                }
                if (r.caseEntity() == null || !r.caseEntity().equals(r.measure().entity())) {
                    out.add("roles: the measure " + r.measure().show() + " must be an attribute of the case" + (r.caseEntity() == null ? "" : " " + r.caseEntity()));
                }
            }
        }
        if (c.prices() && c.currency() == null) {
            out.add("catalog: 'currency' must be a string");
        }
        for (EventSpec e : c.events()) {
            boolean anyPath = e.value() != null || e.customer() != null || e.order() != null;
            if (r == null) {
                if (anyPath && (e.value() == null || e.customer() == null || e.order() == null)) {
                    out.add(e.ref().show() + ": declares some of 'value', 'customer' and 'order' but not all; without roles an event declares all three or none");
                }
                continue;
            }
            if (r.caseEntity() != null && c.orderPath(e) == null) {
                out.add(e.ref().show() + ": no order: declare 'order', or a key for " + r.caseEntity());
            }
            if (r.party() != null && c.customerPath(e) == null) {
                out.add(e.ref().show() + ": no customer: declare 'customer', or a key for " + r.party());
            }
        }
        return out;
    }

    static <A> List<A> dupes(List<A> as) {
        Map<A, Integer> seen = new LinkedHashMap<>();
        as.forEach(a -> seen.merge(a, 1, Integer::sum));
        return seen.entrySet().stream().filter(e -> e.getValue() > 1).map(Map.Entry::getKey).toList();
    }
}
