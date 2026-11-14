package inflight.observer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import com.fasterxml.jackson.databind.JsonNode;

import inflight.catalog.Json;
import inflight.catalog.Result;

/** The simulator's control API, as the observer, the view and the MCP server reach it. */
public final class SimulatorClient {

    /** An answer from the simulator: its status and body, or a 502 when it could not be reached. */
    public record Reply(int status, String body) {
        public JsonNode json() {
            Result<JsonNode> j = Json.parse(body);
            return j.isOk() ? j.value() : Json.object().put("error", body);
        }
    }

    private final String url;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    public SimulatorClient(String url) {
        this.url = url;
    }

    public String url() {
        return url;
    }

    public Reply call(String method, String path, Duration timeout) {
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(url + path)).timeout(timeout)
                    .method(method, HttpRequest.BodyPublishers.noBody()).build();
            HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
            return new Reply(res.statusCode(), res.body());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return unreachable();
        } catch (Exception e) {
            return unreachable();
        }
    }

    public Reply call(String method, String path) {
        return call(method, path, Duration.ofSeconds(5));
    }

    private Reply unreachable() {
        return new Reply(502, Json.write(Json.object().put("error", "the simulator is not reachable at " + url)));
    }

    /** The knobs and counts, or null when the simulator is down. */
    public JsonNode state(Duration timeout) {
        Reply r = call("GET", "/state", timeout);
        if (r.status() != 200) {
            return null;
        }
        Result<JsonNode> j = Json.parse(r.body());
        return j.isOk() ? j.value() : null;
    }

    /** {@code SIMULATOR_URL}, where {@code none} means there is no simulator. */
    public static String urlFromEnv() {
        String url = System.getenv("SIMULATOR_URL");
        if (url == null) {
            url = "http://localhost:8090";
        }
        return url.equals("none") ? null : url;
    }
}
