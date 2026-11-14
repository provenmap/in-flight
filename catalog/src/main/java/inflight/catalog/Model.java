package inflight.catalog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;

/** Entities with typed attributes, and every event type version that says what it means. */
public record Model(Map<String, Map<String, FieldType>> entities, List<Event> events) {

    /**
     * What one event type version means: its typed fields, the entities it is about (each with the field that carries
     * its key, such as Order to {@code orderId}), and what it does to them.
     */
    public record Event(EventRef ref, String topic, String district, Map<String, FieldType> fields, Map<String, FieldPath> keys,
                        List<Effect> effects) {
        public Event {
            fields = Collections.unmodifiableMap(new LinkedHashMap<>(fields));
            keys = Collections.unmodifiableMap(new LinkedHashMap<>(keys));
            effects = List.copyOf(effects);
        }

        public List<Effect> affects(AttrRef attr) {
            return effects.stream().filter(e -> e.attribute().equals(attr)).toList();
        }
    }

    /** One event as the catalog file declares it, before its meaning is checked. */
    public record RawEvent(EventRef ref, String topic, String district, JsonNode fields, JsonNode keys, JsonNode effects) {
        boolean saysNothing() {
            return fields == null && keys == null && effects == null;
        }
    }

    public Model {
        Map<String, Map<String, FieldType>> copy = new LinkedHashMap<>();
        entities.forEach((k, v) -> copy.put(k, Collections.unmodifiableMap(new LinkedHashMap<>(v))));
        entities = Collections.unmodifiableMap(copy);
        events = List.copyOf(events);
    }

    public Optional<Event> find(EventRef ref) {
        return events.stream().filter(e -> e.ref().equals(ref)).findFirst();
    }

    public List<Event> versionsOf(String name) {
        return events.stream().filter(e -> e.ref().name().equals(name)).sorted(Comparator.comparingInt(e -> e.ref().version())).toList();
    }

    public FieldType attributeType(AttrRef attr) {
        Map<String, FieldType> attrs = entities.get(attr.entity());
        return attrs == null ? null : attrs.get(attr.attribute());
    }

    /** The same model with one event version taken out: the catalog as it was before that version. */
    public Model without(EventRef ref) {
        return new Model(entities, events.stream().filter(e -> !e.ref().equals(ref)).toList());
    }

    /** A model and every reason it is not sound. */
    public record Parsed(Model model, List<String> errors) {}

    private static final Pattern TYPE_NAME = Pattern.compile("^[A-Z][A-Za-z0-9]*$");

    /**
     * Reads and checks the catalog's meaning. An event with no {@code fields}, {@code keys} or {@code effects} stays outside
     * it: it is still priced and watched, but no intent can match it. Every failure names what is wrong; nothing is guessed.
     */
    public static Parsed parse(JsonNode rawEntities, List<RawEvent> rawEvents) {
        List<String> errors = new ArrayList<>();
        Map<String, Map<String, FieldType>> entities = new LinkedHashMap<>();

        if (rawEntities != null && !rawEntities.isObject()) {
            errors.add("'entities' must be an object of entity to attributes");
        }
        if (Json.isObject(rawEntities)) {
            for (Iterator<Map.Entry<String, JsonNode>> it = rawEntities.fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> e = it.next();
                String entity = e.getKey();
                if (!TYPE_NAME.matcher(entity).matches()) {
                    errors.add("entity '" + entity + "' must be a capitalised identifier");
                }
                Result<Map<String, FieldType>> fields = FieldType.parseFields(e.getValue());
                if (!fields.isOk()) {
                    errors.add("entity " + entity + ": " + fields.error());
                    continue;
                }
                fields.value().forEach((attr, t) -> {
                    if (t instanceof FieldType.Obj) {
                        errors.add(entity + "." + attr + " must be a scalar type, was " + t.show());
                    }
                });
                entities.put(entity, fields.value());
            }
        }

        List<Event> events = new ArrayList<>();
        for (RawEvent raw : rawEvents) {
            if (raw.saysNothing()) {
                continue;
            }
            String where = raw.ref().show();
            Result<Map<String, FieldType>> fields = FieldType.parseFields(raw.fields() == null ? Json.object() : raw.fields());
            if (!fields.isOk()) {
                errors.add(where + ": " + fields.error());
                continue;
            }
            Map<String, FieldPath> keys = new LinkedHashMap<>();
            List<Effect> effects = new ArrayList<>();

            if (raw.keys() != null && !raw.keys().isObject()) {
                errors.add(where + ": 'keys' must be an object of entity to field path");
            }
            if (Json.isObject(raw.keys())) {
                for (Iterator<Map.Entry<String, JsonNode>> it = raw.keys().fields(); it.hasNext(); ) {
                    Map.Entry<String, JsonNode> k = it.next();
                    String entity = k.getKey();
                    Result<FieldPath> path = k.getValue().isTextual() ? FieldPath.parse(k.getValue().asText()) : null;
                    if (path == null || !path.isOk()) {
                        errors.add(where + ": the key for " + entity + " must be a dotted field path");
                        continue;
                    }
                    if (!entities.containsKey(entity)) {
                        errors.add(where + ": key for unknown entity '" + entity + "'");
                    }
                    FieldPath p = path.value();
                    FieldType t = FieldType.at(fields.value(), p);
                    if (t == null) {
                        errors.add(where + ": key path '" + p.show() + "' for " + entity + " is not a field");
                    } else if (t.optional()) {
                        errors.add(where + ": key path '" + p.show() + "' for " + entity + " is optional");
                    } else if (!(t.is(FieldType.Kind.STRING) || t.is(FieldType.Kind.INT) || t.is(FieldType.Kind.LONG))) {
                        errors.add(where + ": key path '" + p.show() + "' for " + entity + " must be string, int or long, was " + t.show());
                    }
                    keys.put(entity, p);
                }
            }
            if (keys.isEmpty()) {
                errors.add(where + ": declares no entity key, so no intent could key it");
            }

            if (raw.effects() != null && !raw.effects().isArray()) {
                errors.add(where + ": 'effects' must be an array");
            }
            if (raw.effects() != null && raw.effects().isArray()) {
                for (JsonNode rawEffect : raw.effects()) {
                    Result<Effect> parsed = parseEffect(rawEffect);
                    if (!parsed.isOk()) {
                        errors.add(where + ": " + parsed.error());
                        continue;
                    }
                    Effect effect = parsed.value();
                    String label = where + ": effect '" + effect.show() + "'";
                    Map<String, FieldType> attrs = entities.get(effect.attribute().entity());
                    FieldType attrType = attrs == null ? null : attrs.get(effect.attribute().attribute());
                    if (attrType == null) {
                        errors.add(label + ": unknown attribute " + effect.attribute().show());
                        continue;
                    }
                    String entity = effect.attribute().entity();
                    if (!keys.containsKey(entity)) {
                        errors.add(label + ": the event carries no " + entity + " key, so it cannot say which " + entity + " it changes");
                    }
                    if (effect.op() != Effect.Op.ASSIGN && !attrType.isNumeric()) {
                        errors.add(label + ": " + effect.op().show() + " needs a numeric attribute, " + effect.attribute().show() + " is " + attrType.show());
                    }
                    switch (effect.source()) {
                        case Effect.FromField f -> {
                            FieldType t = FieldType.at(fields.value(), f.field());
                            if (t == null) {
                                errors.add(label + ": '" + f.field().show() + "' is not a field");
                            } else if (!FieldType.assignable(t, attrType)) {
                                errors.add(label + ": '" + f.field().show() + "' is " + t.show() + ", " + effect.attribute().show() + " is " + attrType.show());
                            }
                        }
                        case Effect.Const c -> {
                            if (effect.op() != Effect.Op.ASSIGN) {
                                errors.add(label + ": only assign takes a constant");
                            }
                            if (!constFits(c.value(), attrType)) {
                                errors.add(label + ": constant " + Json.write(c.value()) + " does not fit " + attrType.show());
                            }
                        }
                    }
                    effects.add(effect);
                }
            }
            events.add(new Event(raw.ref(), raw.topic(), raw.district(), fields.value(), keys, effects));
        }
        return new Parsed(new Model(entities, events), errors);
    }

    private static final Set<String> OPS = Set.of("assign", "add", "subtract");

    private static Result<Effect> parseEffect(JsonNode raw) {
        if (!Json.isObject(raw)) {
            return Result.err("an effect must be an object");
        }
        JsonNode op = raw.get("op");
        JsonNode attribute = raw.get("attribute");
        JsonNode from = raw.get("from");
        JsonNode value = raw.get("value");
        if (op == null || !op.isTextual() || !OPS.contains(op.asText())) {
            return Result.err("an effect's 'op' must be one of assign, add, subtract");
        }
        String[] parts = attribute != null && attribute.isTextual() ? attribute.asText().split("\\.", -1) : new String[0];
        if (parts.length != 2 || parts[0].isEmpty() || parts[1].isEmpty()) {
            return Result.err("an effect's 'attribute' must be Entity.attribute, was " + (attribute == null ? "null" : Json.write(attribute)));
        }
        AttrRef ref = new AttrRef(parts[0], parts[1]);
        if (from != null && value != null) {
            return Result.err("effect on " + ref.show() + ": give 'from' or 'value', not both");
        }
        if (from != null && from.isTextual()) {
            Result<FieldPath> path = FieldPath.parse(from.asText());
            return path.isOk() ? Result.ok(new Effect(Effect.Op.of(op.asText()), ref, new Effect.FromField(path.value())))
                    : Result.err("effect on " + ref.show() + ": " + path.error());
        }
        if (value != null) {
            return Result.ok(new Effect(Effect.Op.of(op.asText()), ref, new Effect.Const(value)));
        }
        return Result.err("effect on " + ref.show() + ": needs 'from' (a field path) or 'value' (a constant)");
    }

    private static boolean constFits(JsonNode value, FieldType t) {
        if (!(t instanceof FieldType.Scalar s)) {
            return false;
        }
        return switch (s.kind()) {
            case STRING, TIMESTAMP -> value.isTextual();
            case DECIMAL -> {
                if (!value.isTextual() || value.asText().isBlank()) {
                    yield false;
                }
                try {
                    new java.math.BigDecimal(value.asText().trim());
                    yield true;
                } catch (NumberFormatException e) {
                    yield false;
                }
            }
            case INT, LONG -> value.isIntegralNumber() || (value.isNumber() && value.decimalValue().stripTrailingZeros().scale() <= 0);
            case BOOLEAN -> value.isBoolean();
        };
    }
}
