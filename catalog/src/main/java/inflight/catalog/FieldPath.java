package inflight.catalog;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;

/** A dotted path into an event's data, such as {@code total.amount}. */
public record FieldPath(List<String> segments) {

    public FieldPath {
        segments = List.copyOf(segments);
    }

    public String show() {
        return String.join(".", segments);
    }

    @Override
    public String toString() {
        return show();
    }

    /** Reads this path out of a JSON object. Null counts as missing. */
    public Optional<JsonNode> read(JsonNode json) {
        JsonNode at = json;
        for (String name : segments) {
            at = at != null && at.isObject() ? at.get(name) : null;
            if (at == null || at.isNull()) {
                return Optional.empty();
            }
        }
        return Optional.ofNullable(at);
    }

    public static Result<FieldPath> parse(String text) {
        String[] parts = text.split("\\.", -1);
        boolean ok = !text.isEmpty() && Arrays.stream(parts).noneMatch(String::isEmpty);
        return ok ? Result.ok(new FieldPath(Arrays.asList(parts))) : Result.err("'" + text + "' is not a dotted field path");
    }

    public static FieldPath of(String text) {
        return parse(text).value();
    }
}
