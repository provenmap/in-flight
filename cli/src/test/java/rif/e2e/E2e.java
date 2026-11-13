package rif.e2e;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.JsonNode;

import rif.catalog.Json;

/**
 * What the end-to-end tests share: a Kafka of each suite's own (so no suite meets what another left behind), the
 * simulator's control API, and a viewer of the observer's WebSocket.
 */
final class E2e {

    private E2e() {}

    /** A Kafka in a container for one suite, stopped after it. */
    static final class OwnKafka implements AutoCloseable {
        private final KafkaContainer container;
        final String bootstrap;

        private OwnKafka(KafkaContainer container) {
            this.container = container;
            this.bootstrap = container.getBootstrapServers();
        }

        @SuppressWarnings("resource")
        static OwnKafka start() {
            KafkaContainer k = new KafkaContainer(DockerImageName.parse("apache/kafka:4.4.0"));
            k.start();
            return new OwnKafka(k);
        }

        @Override
        public void close() {
            container.stop();
        }
    }

    static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    record Reply(int status, JsonNode body) {}

    static Reply http(String method, String url) {
        try {
            HttpResponse<String> r = HTTP.send(HttpRequest.newBuilder(URI.create(url)).method(method, HttpRequest.BodyPublishers.noBody())
                    .timeout(Duration.ofSeconds(30)).build(), HttpResponse.BodyHandlers.ofString());
            var json = Json.parse(r.body());
            return new Reply(r.statusCode(), json.isOk() ? json.value() : Json.object().put("text", r.body()));
        } catch (Exception e) {
            throw new IllegalStateException(method + " " + url + ": " + e, e);
        }
    }

    static JsonNode state(String url) {
        return http("GET", url + "/state").body();
    }

    static Reply control(String url, String action) {
        return http("POST", url + "/" + action);
    }

    static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    static void until(BooleanSupplier ok, long ms, java.util.function.Supplier<String> last) {
        long end = System.currentTimeMillis() + ms;
        while (!ok.getAsBoolean()) {
            if (System.currentTimeMillis() > end) {
                throw new AssertionError("timed out; last " + last.get());
            }
            sleep(200);
        }
    }

    /** Leaves the simulator in a known state: v1, unpatched, nothing stuck from an earlier run, the services caught up. */
    static void settle(String url) {
        control(url, "restore");
        control(url, "patch");
        control(url, "redrive");
        for (int i = 0; i < 120 && state(url).get("redrive").get("running").asBoolean(); i++) {
            sleep(500);
        }
        control(url, "unpatch");
        sleep(1500);
    }

    static JsonNode waitForRedrive(String url) {
        for (int i = 0; i < 120; i++) {
            JsonNode r = state(url).get("redrive");
            if (!r.get("running").asBoolean()) {
                return r;
            }
            sleep(500);
        }
        throw new AssertionError("the redrive did not finish");
    }

    /** A viewer: every message it was sent, in order. */
    static final class Viewer implements WebSocket.Listener, AutoCloseable {
        final List<JsonNode> msgs = Collections.synchronizedList(new ArrayList<>());
        private final StringBuilder partial = new StringBuilder();
        private final WebSocket ws;

        Viewer(String httpUrl) {
            ws = HTTP.newWebSocketBuilder().buildAsync(URI.create(httpUrl.replace("http", "ws") + "/ws"), this).join();
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            partial.append(data);
            if (last) {
                Json.parse(partial.toString()).value().forEach(msgs::add);
                partial.setLength(0);
            }
            webSocket.request(1);
            return null;
        }

        List<JsonNode> of(String type) {
            synchronized (msgs) {
                return msgs.stream().filter(m -> m.get("type").asText().equals(type)).toList();
            }
        }

        List<JsonNode> since(int from) {
            synchronized (msgs) {
                return List.copyOf(msgs.subList(from, msgs.size()));
            }
        }

        int size() {
            return msgs.size();
        }

        JsonNode first() {
            return msgs.isEmpty() ? null : msgs.get(0);
        }

        JsonNode stats() {
            List<JsonNode> s = of("stats");
            return s.isEmpty() ? null : s.get(s.size() - 1);
        }

        void until(Predicate<Viewer> pred, long ms) {
            E2e.until(() -> pred.test(this), ms, () -> String.valueOf(stats()));
        }

        void until(Predicate<Viewer> pred) {
            until(pred, 30_000);
        }

        @Override
        public void close() {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
        }
    }
}
