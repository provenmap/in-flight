package inflight.catalog;

import java.util.HashMap;
import java.util.Map;

/**
 * The environment, by this product's names. Its variables start {@code INFLIGHT_}; before it was renamed from
 * revenue-in-flight they started {@code RIF_}, and a setup made then still works: an old name is read when the new
 * one is not set.
 */
public final class Env {

    private Env() {}

    /** A variable by its {@code INFLIGHT_} name, else its old {@code RIF_} one; null when neither is set. */
    public static String get(String name) {
        String v = System.getenv(name);
        if (v == null && name.startsWith("INFLIGHT_")) {
            v = System.getenv("RIF_" + name.substring("INFLIGHT_".length()));
        }
        return v;
    }

    /** The whole environment, with each old {@code RIF_} variable also under its {@code INFLIGHT_} name unless that is set. */
    public static Map<String, String> all() {
        return withOldNames(System.getenv());
    }

    static Map<String, String> withOldNames(Map<String, String> env) {
        Map<String, String> out = new HashMap<>(env);
        env.forEach((k, v) -> {
            if (k.startsWith("RIF_")) {
                out.putIfAbsent("INFLIGHT_" + k.substring(4), v);
            }
        });
        return out;
    }
}
