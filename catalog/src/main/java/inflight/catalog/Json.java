package inflight.catalog;

import java.io.IOException;
import java.io.UncheckedIOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** One JSON mapper for the whole product. Objects keep their keys in the order they were written. */
public final class Json {

    public static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

    private static final ObjectMapper PRETTY = MAPPER.copy().enable(SerializationFeature.INDENT_OUTPUT);

    private Json() {}

    public static ObjectNode object() {
        return JsonNodeFactory.instance.objectNode();
    }

    public static ArrayNode array() {
        return JsonNodeFactory.instance.arrayNode();
    }

    /** Parses JSON text; a failure says what the parser saw. */
    public static Result<JsonNode> parse(String text) {
        try {
            JsonNode node = MAPPER.readTree(text);
            return node == null || node.isMissingNode() ? Result.err("not JSON: no content") : Result.ok(node);
        } catch (JsonProcessingException e) {
            return Result.err("not JSON: " + e.getOriginalMessage());
        }
    }

    public static Result<JsonNode> parse(byte[] bytes, int offset, int length) {
        try {
            JsonNode node = MAPPER.readTree(bytes, offset, length);
            return node == null || node.isMissingNode() ? Result.err("not JSON: no content") : Result.ok(node);
        } catch (IOException e) {
            String msg = e instanceof JsonProcessingException j ? j.getOriginalMessage() : e.getMessage();
            return Result.err("not JSON: " + msg);
        }
    }

    public static boolean isObject(JsonNode n) {
        return n != null && n.isObject();
    }

    /** The JSON text of a value, on one line, the way JavaScript's JSON.stringify writes it. */
    public static String write(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static String pretty(Object value) {
        try {
            return PRETTY.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Indented two spaces the way JavaScript's {@code JSON.stringify(value, null, 2)} writes it: {@code "key": value},
     * one array element per line, {@code []} and {@code {}} when empty, and whole numbers without a decimal point.
     */
    public static String indented(JsonNode node) {
        StringBuilder sb = new StringBuilder();
        indented(node, sb, "");
        return sb.toString();
    }

    private static void indented(JsonNode n, StringBuilder sb, String pad) {
        if (n.isObject()) {
            if (n.isEmpty()) {
                sb.append("{}");
                return;
            }
            sb.append("{\n");
            var it = n.fields();
            while (it.hasNext()) {
                var e = it.next();
                sb.append(pad).append("  ").append(write(e.getKey())).append(": ");
                indented(e.getValue(), sb, pad + "  ");
                sb.append(it.hasNext() ? ",\n" : "\n");
            }
            sb.append(pad).append('}');
        } else if (n.isArray()) {
            if (n.isEmpty()) {
                sb.append("[]");
                return;
            }
            sb.append("[\n");
            for (int i = 0; i < n.size(); i++) {
                sb.append(pad).append("  ");
                indented(n.get(i), sb, pad + "  ");
                sb.append(i < n.size() - 1 ? ",\n" : "\n");
            }
            sb.append(pad).append(']');
        } else if (n.isNumber()) {
            sb.append(number(n));
        } else {
            sb.append(write(n));
        }
    }

    /** A number as JavaScript prints it: {@code 100}, not {@code 100.0}. */
    public static String number(JsonNode n) {
        if (n.isIntegralNumber()) {
            return n.asText();
        }
        java.math.BigDecimal d = n.isBigDecimal() ? n.decimalValue() : new java.math.BigDecimal(Double.toString(n.doubleValue()));
        d = d.stripTrailingZeros();
        return d.scale() <= 0 ? d.toBigInteger().toString() : d.toPlainString();
    }

    /** The node as a plain Java value: maps, lists, strings, numbers, booleans and null. */
    public static Object plain(JsonNode node) {
        return MAPPER.convertValue(node, Object.class);
    }

    public static JsonNode tree(Object value) {
        return MAPPER.valueToTree(value);
    }

    /** A text field, or null when it is absent or not text. */
    public static String text(JsonNode obj, String key) {
        JsonNode v = obj == null ? null : obj.get(key);
        return v != null && v.isTextual() ? v.asText() : null;
    }
}
