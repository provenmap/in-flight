package inflight.router;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import com.fasterxml.jackson.databind.JsonNode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import inflight.catalog.Json;

/** The control API over HTTP, against a service whose streams do not run. */
class ControlApiTest {

    RouterService service;
    ControlApi api;
    final HttpClient http = HttpClient.newHttpClient();

    @BeforeEach
    void up() {
        service = new RouterService(new InMemoryRegistry(), null, new StreamsSettings("dummy:9092", "in-flight-router", Path.of("build-state"), "at_least_once", 1),
                new IntentTopology.Ordering(Duration.ofSeconds(1), Duration.ofMillis(100)), false);
        api = new ControlApi(service, 0);
        api.start();
    }

    @AfterEach
    void down() {
        api.close();
        service.close();
    }

    record Reply(int status, JsonNode body) {}

    Reply call(String method, String path, String body) throws IOException, InterruptedException {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://localhost:" + api.port() + path));
        b.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        HttpResponse<String> r = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
        return new Reply(r.statusCode(), Json.parse(r.body()).value());
    }

    @Test
    void catalogThenIntentsThenStats() throws Exception {
        assertEquals(404, call("GET", "/catalog", null).status());
        assertEquals(0, call("GET", "/health", null).body().get("catalogRevision").asInt());

        Reply put = call("PUT", "/catalog", Files.readString(Path.of("catalog/order-to-cash.json")));
        assertEquals(200, put.status(), put.body().toString());
        assertEquals(2, put.body().get("active").asInt(), "the shipped catalog is revision 2");
        assertEquals(4, put.body().get("intents").size());

        Reply declared = call("PUT", "/intents/payments", "{\"predicate\": \"emits(PaymentAuthorized) or emits(PaymentDeclined)\", \"by\": \"Order\"}");
        assertEquals(200, declared.status(), declared.body().toString());
        assertEquals("runtime", declared.body().get("declaredBy").asText());
        assertEquals("in-flight-router-payments", declared.body().get("applicationId").asText());
        assertEquals("__intent.payments", declared.body().get("topic").asText());

        Reply one = call("GET", "/intents/ready-to-ship", null);
        assertEquals("catalog", one.body().get("declaredBy").asText());
        assertEquals(2, one.body().get("revision").asInt());
        assertEquals(5, call("GET", "/intents", null).body().size());
        assertEquals(404, call("GET", "/intents/nope", null).status());

        Reply health = call("GET", "/health", null);
        assertEquals("NOT_RUNNING", health.body().get("streams").asText());
        assertEquals(5, health.body().get("intents").size());

        JsonNode stats = call("GET", "/stats", null).body();
        assertEquals(0, stats.get("routed").asLong());
        assertTrue(stats.has("intents"));

        assertEquals(2, call("GET", "/catalog", null).body().get("revision").asInt());
    }

    @Test
    void badRequestsSayWhy() throws Exception {
        Reply noCatalog = call("PUT", "/intents/x", "{\"predicate\": \"emits(OrderPlaced)\"}");
        assertEquals(400, noCatalog.status());
        assertEquals("no catalog is active yet; PUT /catalog first", noCatalog.body().get("errors").get(0).asText());

        Reply junk = call("PUT", "/catalog", "{not json");
        assertEquals(400, junk.status());
        assertTrue(junk.body().get("errors").get(0).asText().startsWith("not JSON"));

        call("PUT", "/catalog", Files.readString(Path.of("catalog/order-to-cash.json")));
        Reply mismatch = call("PUT", "/intents/x", "{\"name\": \"y\", \"predicate\": \"emits(OrderPlaced)\"}");
        assertEquals("body names 'y', path names 'x'", mismatch.body().get("errors").get(0).asText());

        assertEquals(404, call("DELETE", "/catalog", null).status());
    }
}
