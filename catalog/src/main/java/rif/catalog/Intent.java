package rif.catalog;

import java.util.regex.Pattern;

/** A declared intent: what a consumer cares about, and which entity its events are keyed by ({@code by} may be null). */
public record Intent(String name, Predicate predicate, String by) {

    private static final Pattern NAME = Pattern.compile("^[a-z0-9][a-z0-9-]{0,99}$");

    /** The real topic that materialises an intent. Clients read it as {@code intent.<name>} through the proxy. */
    public static String topicFor(String name) {
        return "__intent." + name;
    }

    /** The name a client uses through the proxy. */
    public static String aliasFor(String name) {
        return "intent." + name;
    }

    public String topic() {
        return topicFor(name);
    }

    public static Result<Intent> parse(String name, String predicate, String by) {
        if (!NAME.matcher(name).matches()) {
            return Result.err("intent name '" + name + "' must be lowercase letters, digits and hyphens");
        }
        Result<Predicate> p = IntentLanguage.parse(predicate);
        if (!p.isOk()) {
            return Result.err("intent '" + name + "': " + p.error());
        }
        return Result.ok(new Intent(name, p.value(), by == null || by.isEmpty() ? null : by));
    }

    public static Result<Intent> parse(String name, String predicate) {
        return parse(name, predicate, null);
    }
}
