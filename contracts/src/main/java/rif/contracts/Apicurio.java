package rif.contracts;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import rif.catalog.EventRef;
import rif.catalog.Json;
import rif.catalog.Model;
import rif.catalog.Result;

/**
 * Apicurio Registry v3: one artifact per event type in one group, one version per catalog version. The schema gate in
 * the proxy reads the same group, so a version that is not registered here cannot be produced through the proxy: the
 * registry holds the approved shapes.
 *
 * <p>Artifacts carry no compatibility rule of their own. intent-bus gave each a BACKWARD rule, which refuses a breaking
 * change outright, however its readers are prepared. Compatibility is the catalog's to judge: its impact analysis knows
 * who reads which field, and a change is held until each reader that must change has merged its migration. A rule
 * someone sets on an artifact is still honoured: a version the registry refuses blocks the proposal.
 */
public final class Apicurio {

    /** The group the schema gate reads by default. */
    public static final String GROUP = "intent-bus";

    /** What the registry says about a version that is not yet registered, or that it already is. */
    public sealed interface Verdict {
        record Compatible() implements Verdict {}

        record Incompatible(String reason) implements Verdict {}

        record AlreadyRegistered() implements Verdict {}
    }

    private final String baseUrl;
    private final String group;

    public Apicurio(String baseUrl, String group) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.group = group;
    }

    public Apicurio(String baseUrl) {
        this(baseUrl, GROUP);
    }

    private String artifacts() {
        return baseUrl + "/groups/" + group + "/artifacts";
    }

    private String versions(String name) {
        return artifacts() + "/" + name + "/versions";
    }

    public boolean isRegistered(EventRef ref) {
        return Http.get(versions(ref.name()) + "/" + ref.version()).status() == 200;
    }

    private boolean artifactExists(String name) {
        return Http.get(artifacts() + "/" + name).status() == 200;
    }

    /** Checks a version against the registry's rules without registering it. */
    public Verdict check(Model.Event e) {
        if (isRegistered(e.ref())) {
            return new Verdict.AlreadyRegistered();
        }
        Http.Response r = artifactExists(e.ref().name())
                ? Http.post(versions(e.ref().name()) + "?dryRun=true", versionBody(e))
                : Http.post(artifacts() + "?dryRun=true", createBody(e));
        return r.ok() ? new Verdict.Compatible() : new Verdict.Incompatible("HTTP " + r.status() + ": " + problem(r));
    }

    /** Registers a version, creating the artifact and its compatibility rule on first use. */
    public Result<EventRef> register(Model.Event e) {
        EventRef ref = e.ref();
        if (isRegistered(ref)) {
            return Result.ok(ref);
        }
        if (artifactExists(ref.name())) {
            Http.Response r = Http.post(versions(ref.name()), versionBody(e));
            return r.ok() ? Result.ok(ref) : Result.err(ref.show() + ": HTTP " + r.status() + ": " + problem(r));
        }
        Http.Response r = Http.post(artifacts(), createBody(e));
        return r.ok() ? Result.ok(ref) : Result.err(ref.show() + ": HTTP " + r.status() + ": " + problem(r));
    }

    private static ObjectNode versionBody(Model.Event e) {
        ObjectNode o = Json.object();
        o.put("version", String.valueOf(e.ref().version()));
        o.putObject("content").put("content", Json.pretty(JsonSchemas.of(e))).put("contentType", "application/json");
        return o;
    }

    private static ObjectNode createBody(Model.Event e) {
        String name = e.ref().name();
        ObjectNode o = Json.object();
        o.put("artifactId", name);
        o.put("artifactType", "JSON");
        o.put("name", name);
        o.put("description", name + (e.district() == null ? "" : ", produced by " + e.district()) + " to " + e.topic());
        o.set("firstVersion", versionBody(e));
        return o;
    }

    /** The registry's own account of a failure: its causes, else its detail or title, else the body. */
    private static String problem(Http.Response r) {
        Result<JsonNode> parsed = r.json();
        if (!parsed.isOk() || !parsed.value().isObject()) {
            return r.body().length() > 300 ? r.body().substring(0, 300) : r.body();
        }
        JsonNode j = parsed.value();
        List<String> causes = new ArrayList<>();
        if (j.path("causes").isArray()) {
            for (JsonNode c : j.get("causes")) {
                causes.add(c.path("description").isTextual() ? c.get("description").asText() : c.toString());
            }
        }
        if (!causes.isEmpty()) {
            return String.join("; ", causes);
        }
        for (String key : List.of("detail", "title")) {
            if (j.path(key).isTextual()) {
                return j.get(key).asText();
            }
        }
        return r.body();
    }
}
