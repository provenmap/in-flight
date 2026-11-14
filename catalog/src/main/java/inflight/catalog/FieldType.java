package inflight.catalog;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** The type of an event field or an entity attribute: a scalar, or an object of named fields. */
public sealed interface FieldType {

    enum Kind {
        STRING, INT, LONG, DECIMAL, BOOLEAN, TIMESTAMP;

        public String show() {
            return name().toLowerCase();
        }

        static Kind of(String text) {
            return Arrays.stream(values()).filter(k -> k.show().equals(text)).findFirst().orElse(null);
        }
    }

    record Scalar(Kind kind, boolean optional) implements FieldType {}

    /** Fields keep the order the catalog wrote them in. */
    record Obj(Map<String, FieldType> fields, boolean optional) implements FieldType {
        public Obj {
            fields = java.util.Collections.unmodifiableMap(new LinkedHashMap<>(fields));
        }
    }

    boolean optional();

    default FieldType withOptional(boolean optional) {
        return switch (this) {
            case Scalar s -> new Scalar(s.kind(), optional);
            case Obj o -> new Obj(o.fields(), optional);
        };
    }

    default boolean isNumeric() {
        return this instanceof Scalar s && (s.kind() == Kind.INT || s.kind() == Kind.LONG || s.kind() == Kind.DECIMAL);
    }

    default boolean is(Kind kind) {
        return this instanceof Scalar s && s.kind() == kind;
    }

    default String show() {
        String base = switch (this) {
            case Scalar s -> s.kind().show();
            case Obj o -> o.fields().entrySet().stream().map(e -> e.getKey() + ": " + e.getValue().show())
                    .collect(Collectors.joining(", ", "{ ", " }"));
        };
        return optional() ? base + "?" : base;
    }

    static boolean same(FieldType a, FieldType b) {
        return a.show().equals(b.show());
    }

    /** {@code "decimal"}, {@code "string?"}, or an object of named fields. */
    static Result<FieldType> parse(JsonNode json) {
        if (json != null && json.isTextual()) {
            String text = json.asText();
            boolean optional = text.endsWith("?");
            Kind kind = Kind.of(optional ? text.substring(0, text.length() - 1) : text);
            if (kind == null) {
                String known = Arrays.stream(Kind.values()).map(Kind::show).sorted().collect(Collectors.joining(", "));
                return Result.err("unknown field type '" + text + "' (known: " + known + ", or an object)");
            }
            return Result.ok(new Scalar(kind, optional));
        }
        if (Json.isObject(json)) {
            return parseFields(json).map(fs -> new Obj(fs, false));
        }
        return Result.err("a field type is a name such as \"decimal\" or an object of fields");
    }

    static Result<Map<String, FieldType>> parseFields(JsonNode json) {
        if (!Json.isObject(json)) {
            return Result.err("fields must be an object of name to type");
        }
        Map<String, FieldType> out = new LinkedHashMap<>();
        for (Iterator<Map.Entry<String, JsonNode>> it = json.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> e = it.next();
            Result<FieldType> t = parse(e.getValue());
            if (!t.isOk()) {
                return Result.err(e.getKey() + ": " + t.error());
            }
            out.put(e.getKey(), t.value());
        }
        return Result.ok(out);
    }

    /** The type at a path, optional when any step on the way is optional; null when the path is not a field. */
    static FieldType at(Map<String, FieldType> fields, FieldPath path) {
        Map<String, FieldType> at = fields;
        boolean optional = false;
        for (int i = 0; i < path.segments().size(); i++) {
            FieldType t = at.get(path.segments().get(i));
            if (t == null) {
                return null;
            }
            optional = optional || t.optional();
            if (i == path.segments().size() - 1) {
                return t.withOptional(optional);
            }
            if (!(t instanceof Obj o)) {
                return null;
            }
            at = o.fields();
        }
        return null;
    }

    /** A value of type {@code from} can feed an attribute of type {@code to}: the same type, or a numeric widening. */
    static boolean assignable(FieldType from, FieldType to) {
        if (!(from instanceof Scalar f) || !(to instanceof Scalar t)) {
            return false;
        }
        if (f.kind() == t.kind()) {
            return true;
        }
        if (f.kind() == Kind.INT) {
            return t.kind() == Kind.LONG || t.kind() == Kind.DECIMAL;
        }
        return f.kind() == Kind.LONG && t.kind() == Kind.DECIMAL;
    }

    /** The JSON a catalog writes for this type. */
    static JsonNode toJson(FieldType t) {
        return switch (t) {
            case Scalar s -> Json.tree(s.kind().show() + (s.optional() ? "?" : ""));
            case Obj o -> {
                ObjectNode out = Json.object();
                o.fields().forEach((k, v) -> out.set(k, toJson(v)));
                yield out;
            }
        };
    }

    static JsonNode fieldsJson(Map<String, FieldType> fields) {
        ObjectNode out = Json.object();
        fields.forEach((k, v) -> out.set(k, toJson(v)));
        return out;
    }
}
