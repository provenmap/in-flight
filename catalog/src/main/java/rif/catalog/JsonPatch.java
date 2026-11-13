package rif.catalog;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * JSON Patch (RFC 6902): add, remove, replace, move, copy and test, with JSON Pointer paths (RFC 6901). A patch
 * applies whole or not at all: the document is copied first and the copy returned.
 */
public final class JsonPatch {

    private JsonPatch() {}

    public static Result<JsonNode> apply(JsonNode doc, JsonNode patch) {
        if (patch == null || !patch.isArray()) {
            return Result.err("a patch is an array of operations, such as [{\"op\": \"replace\", \"path\": \"/currency\", \"value\": \"EUR\"}]");
        }
        JsonNode[] root = {doc.deepCopy()};
        for (int i = 0; i < patch.size(); i++) {
            JsonNode op = patch.get(i);
            String where = "operation " + i;
            if (!op.isObject() || !op.path("op").isTextual() || !op.path("path").isTextual()) {
                return Result.err(where + ": needs op and path");
            }
            String path = op.get("path").asText();
            Result<List<String>> tokens = pointer(path);
            if (!tokens.isOk()) {
                return Result.err(where + ": " + tokens.error());
            }
            String name = op.get("op").asText();
            String err = switch (name) {
                case "add" -> op.has("value") ? add(root, tokens.value(), op.get("value").deepCopy()) : "add needs a value";
                case "remove" -> remove(root, tokens.value()) == null ? "nothing at " + path : null;
                case "replace" -> {
                    if (!op.has("value")) {
                        yield "replace needs a value";
                    }
                    if (get(root[0], tokens.value()) == null) {
                        yield "nothing at " + path + " to replace";
                    }
                    remove(root, tokens.value());
                    yield add(root, tokens.value(), op.get("value").deepCopy());
                }
                case "move", "copy" -> {
                    if (!op.path("from").isTextual()) {
                        yield name + " needs from";
                    }
                    Result<List<String>> from = pointer(op.get("from").asText());
                    if (!from.isOk()) {
                        yield from.error();
                    }
                    JsonNode v = get(root[0], from.value());
                    if (v == null) {
                        yield "nothing at " + op.get("from").asText();
                    }
                    if (name.equals("move")) {
                        remove(root, from.value());
                    }
                    yield add(root, tokens.value(), v.deepCopy());
                }
                case "test" -> {
                    JsonNode v = get(root[0], tokens.value());
                    yield v != null && v.equals(op.get("value")) ? null : "test failed at " + path;
                }
                default -> "unknown op '" + name + "'";
            };
            if (err != null) {
                return Result.err(where + " (" + name + " " + path + "): " + err);
            }
        }
        return Result.ok(root[0]);
    }

    static Result<List<String>> pointer(String p) {
        if (p.isEmpty()) {
            return Result.ok(List.of());
        }
        if (!p.startsWith("/")) {
            return Result.err("a path is a JSON Pointer, starting with /");
        }
        List<String> out = new ArrayList<>();
        for (String t : p.substring(1).split("/", -1)) {
            out.add(t.replace("~1", "/").replace("~0", "~"));
        }
        return Result.ok(out);
    }

    static JsonNode get(JsonNode node, List<String> tokens) {
        JsonNode n = node;
        for (String t : tokens) {
            if (n == null) {
                return null;
            }
            if (n.isObject()) {
                n = n.get(t);
            } else if (n.isArray()) {
                int i = index(t, n.size(), false);
                n = i < 0 ? null : n.get(i);
            } else {
                return null;
            }
        }
        return n;
    }

    private static String add(JsonNode[] root, List<String> tokens, JsonNode value) {
        if (tokens.isEmpty()) {
            root[0] = value;
            return null;
        }
        JsonNode parent = get(root[0], tokens.subList(0, tokens.size() - 1));
        String last = tokens.get(tokens.size() - 1);
        if (parent instanceof ObjectNode o) {
            o.set(last, value);
            return null;
        }
        if (parent instanceof ArrayNode a) {
            int i = index(last, a.size(), true);
            if (i < 0) {
                return "index " + last + " is out of range";
            }
            a.insert(i, value);
            return null;
        }
        return "no object or array to add to";
    }

    private static JsonNode remove(JsonNode[] root, List<String> tokens) {
        if (tokens.isEmpty()) {
            return null;
        }
        JsonNode parent = get(root[0], tokens.subList(0, tokens.size() - 1));
        String last = tokens.get(tokens.size() - 1);
        if (parent instanceof ObjectNode o) {
            return o.remove(last);
        }
        if (parent instanceof ArrayNode a) {
            int i = index(last, a.size(), false);
            return i < 0 ? null : a.remove(i);
        }
        return null;
    }

    private static int index(String t, int size, boolean forAdd) {
        if (forAdd && t.equals("-")) {
            return size;
        }
        if (!t.matches("0|[1-9][0-9]*")) {
            return -1;
        }
        int i = Integer.parseInt(t);
        return i < size || forAdd && i == size ? i : -1;
    }
}
