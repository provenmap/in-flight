package inflight.server;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import inflight.catalog.Json;
import inflight.observer.Observer;

/**
 * A webhook when money at risk crosses the workspace's threshold, and again when it falls back below it. The body is
 * JSON with a {@code text} line, which Slack's and most chat tools' incoming webhooks show as it is.
 */
final class Alerts implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Alerts.class);

    private final Engine engine;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private volatile boolean running = true;
    private Thread thread;
    private boolean above;

    Alerts(Engine engine) {
        this.engine = engine;
    }

    void start() {
        thread = Thread.ofPlatform().daemon().name("alerts").start(this::run);
    }

    private void run() {
        while (running) {
            try {
                Thread.sleep(2000);
                check();
            } catch (InterruptedException e) {
                return;
            } catch (RuntimeException e) {
                log.warn("alerts: {}", e.getMessage());
            }
        }
    }

    void check() {
        if (engine.workspace() == null || engine.mode() != Engine.Mode.APP) {
            return;
        }
        Workspace.Alerts a = engine.workspace().load().alerts();
        Observer o = engine.observer();
        if (a.webhook() == null || o == null || !o.reader().reading()) {
            return;
        }
        JsonNode atRisk = o.reader().statsJson().get("atRisk");
        double value = atRisk.get("value").asDouble();
        boolean now = value >= a.threshold();
        if (now == above) {
            return;
        }
        above = now;
        inflight.catalog.Display shown = o.catalog().display();
        String flow = engine.running() == null ? o.catalog().flow() : engine.running().flow();
        String text = now
                ? flow + ": " + shown.amount(value) + " at risk (" + shown.cases(atRisk.get("orders").asLong()) + ", " + shown.parties(atRisk.get("customers").asLong())
                        + "), over the " + shown.amount(a.threshold()) + " threshold"
                : flow + ": back under the " + shown.amount(a.threshold()) + " threshold, " + shown.amount(value) + " at risk";
        ObjectNode body = Json.object().put("text", text).put("flow", flow).put("atRisk", value).put("threshold", a.threshold())
                .put("state", now ? "over" : "under").put("at", Instant.now().toString());
        post(a.webhook(), body);
    }

    private void post(String url, ObjectNode body) {
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(10)).header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(Json.write(body))).build();
            HttpResponse<Void> r = http.send(req, HttpResponse.BodyHandlers.discarding());
            if (r.statusCode() >= 300) {
                log.warn("alerts: the webhook answered {}", r.statusCode());
            }
        } catch (Exception e) {
            log.warn("alerts: the webhook could not be reached: {}", e.getMessage());
        }
    }

    @Override
    public void close() {
        running = false;
        if (thread != null) {
            thread.interrupt();
        }
    }
}
