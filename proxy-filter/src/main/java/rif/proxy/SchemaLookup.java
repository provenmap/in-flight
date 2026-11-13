package rif.proxy;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

/** Answers whether an event type version is registered. Shared by every connection's filter. */
public interface SchemaLookup {

    CompletionStage<Boolean> isRegistered(String eventType, int version);

    /** Apicurio v3: a version is registered when {@code GET .../artifacts/{type}/versions/{version}} is 200. */
    final class Apicurio implements SchemaLookup {
        private final SchemaGateConfig config;
        private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        private final ConcurrentHashMap<String, Boolean> registered = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, Long> absentUntil = new ConcurrentHashMap<>();

        public Apicurio(SchemaGateConfig config) {
            this.config = config;
        }

        @Override
        public CompletionStage<Boolean> isRegistered(String eventType, int version) {
            String key = eventType + "@" + version;
            if (registered.containsKey(key)) {
                return CompletableFuture.completedFuture(true);
            }
            Long until = absentUntil.get(key);
            if (until != null && until > System.currentTimeMillis()) {
                return CompletableFuture.completedFuture(false);
            }
            String url = config.registryUrl() + "/groups/" + enc(config.group()) + "/artifacts/" + enc(eventType) + "/versions/" + version;
            HttpRequest req = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(5)).GET().build();
            return client.sendAsync(req, HttpResponse.BodyHandlers.discarding()).thenApply(resp -> {
                boolean ok = resp.statusCode() == 200;
                if (ok) {
                    registered.put(key, true);
                    absentUntil.remove(key);
                }
                else if (resp.statusCode() == 404) {
                    absentUntil.put(key, System.currentTimeMillis() + config.negativeCacheMs());
                }
                else {
                    throw new IllegalStateException("registry answered HTTP " + resp.statusCode() + " for " + key);
                }
                return ok;
            });
        }

        private static String enc(String s) {
            return URLEncoder.encode(s, StandardCharsets.UTF_8);
        }
    }
}
