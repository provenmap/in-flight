package inflight.kafka;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.JsonNode;

import inflight.catalog.Json;
import inflight.catalog.Result;

/**
 * A schema registry, read only: schemas by id, and the subjects it holds. Confluent's API first, which Apicurio also
 * serves under {@code /apis/ccompat/v7}; then Apicurio's own, for its eight-byte global ids. Schemas are cached: an id
 * names one schema forever.
 */
public final class SchemaRegistry {

    /** A schema: its type (AVRO, PROTOBUF or JSON) and its text. */
    public record Schema(String type, String text) {}

    private final String url;
    private final String authorization;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final Map<String, Schema> cache = new ConcurrentHashMap<>();

    public SchemaRegistry(String url, String username, String password) {
        this.url = url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
        this.authorization = username == null || password == null ? null
                : "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
    }

    /** A profile's registry, its secrets resolved now; null when it has none. */
    public static SchemaRegistry of(Profile.Registry r) {
        if (r == null || r.url() == null) {
            return null;
        }
        String user = r.username() == null ? null : Secrets.resolveOrPlain(r.username());
        String pass = r.password() == null ? null : Secrets.resolve(r.password());
        return new SchemaRegistry(r.url(), user, pass);
    }

    public String url() {
        return url;
    }

    /** The schema a Confluent-framed record names by its four-byte id. */
    public Result<Schema> byId(int id) {
        return cached("id:" + id, () -> {
            Result<JsonNode> j = getJson("/schemas/ids/" + id);
            if (!j.isOk()) {
                Result<JsonNode> cc = getJson("/apis/ccompat/v7/schemas/ids/" + id);
                if (!cc.isOk()) {
                    return Result.err(j.errors());
                }
                j = cc;
            }
            JsonNode b = j.value();
            String text = b.path("schema").asText(null);
            if (text == null) {
                return Result.err("the registry's schema " + id + " has no text");
            }
            return Result.ok(new Schema(b.path("schemaType").asText("AVRO"), text));
        });
    }

    /** The schema an Apicurio-framed record names by its eight-byte global id. */
    public Result<Schema> byGlobalId(long id) {
        return cached("global:" + id, () -> {
            for (String path : List.of("/apis/registry/v3/ids/globalIds/" + id, "/apis/registry/v2/ids/globalIds/" + id)) {
                Result<String> text = get(path);
                if (text.isOk()) {
                    return Result.ok(new Schema(guessType(text.value()), text.value()));
                }
            }
            return Result.err("the registry has no schema with global id " + id);
        });
    }

    /** The subjects (Confluent) or artifacts (Apicurio) the registry holds. */
    public Result<List<String>> subjects() {
        for (String path : List.of("/subjects", "/apis/ccompat/v7/subjects")) {
            Result<JsonNode> j = getJson(path);
            if (j.isOk() && j.value().isArray()) {
                List<String> out = new ArrayList<>();
                j.value().forEach(s -> out.add(s.asText()));
                return Result.ok(out);
            }
        }
        return Result.err("the registry at " + url + " lists no subjects: is it a Confluent or Apicurio registry?");
    }

    /** Whether the registry answers at all: null when it does, else why not. */
    public String check() {
        Result<List<String>> s = subjects();
        return s.isOk() ? null : s.error();
    }

    static String guessType(String text) {
        String t = text.strip();
        if (t.startsWith("syntax") || t.contains("message ") && !t.startsWith("{")) {
            return "PROTOBUF";
        }
        if (t.startsWith("{") && (t.contains("\"$schema\"") || t.contains("\"properties\"")) && !t.contains("\"fields\"")) {
            return "JSON";
        }
        return "AVRO";
    }

    private interface Fetch {
        Result<Schema> get();
    }

    private Result<Schema> cached(String key, Fetch fetch) {
        Schema s = cache.get(key);
        if (s != null) {
            return Result.ok(s);
        }
        Result<Schema> got = fetch.get();
        if (got.isOk()) {
            cache.put(key, got.value());
        }
        return got;
    }

    private Result<JsonNode> getJson(String path) {
        Result<String> text = get(path);
        return text.isOk() ? Json.parse(text.value()) : Result.err(text.errors());
    }

    private Result<String> get(String path) {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url + path)).timeout(Duration.ofSeconds(10))
                .header("Accept", "application/vnd.schemaregistry.v1+json, application/json, */*");
        if (authorization != null) {
            b.header("Authorization", authorization);
        }
        try {
            HttpResponse<String> r = http.send(b.GET().build(), HttpResponse.BodyHandlers.ofString());
            if (r.statusCode() != 200) {
                return Result.err("the registry answered " + r.statusCode() + " for " + path);
            }
            return Result.ok(r.body());
        } catch (IOException e) {
            return Result.err("the registry at " + url + " cannot be reached: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Result.err("interrupted");
        }
    }
}
