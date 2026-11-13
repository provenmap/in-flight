package rif.contracts;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import com.fasterxml.jackson.databind.JsonNode;

import rif.catalog.Json;
import rif.catalog.Result;

/** A small JSON-over-HTTP helper on the JDK client. A server that cannot be reached is a {@link ContractsException}. */
final class Http {

    private static final HttpClient CLIENT = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    private Http() {}

    /** A response's status and body. */
    record Response(int status, String body) {
        boolean ok() {
            return status / 100 == 2;
        }

        Result<JsonNode> json() {
            return Json.parse(body);
        }
    }

    static Response get(String url) {
        return send(request(url).GET());
    }

    static Response post(String url, JsonNode body) {
        return send(request(url).POST(HttpRequest.BodyPublishers.ofString(Json.write(body))));
    }

    static Response put(String url, JsonNode body) {
        return send(request(url).PUT(HttpRequest.BodyPublishers.ofString(Json.write(body))));
    }

    private static HttpRequest.Builder request(String url) {
        try {
            return HttpRequest.newBuilder(URI.create(url));
        } catch (IllegalArgumentException e) {
            throw new ContractsException("not a URL: " + url, e);
        }
    }

    private static Response send(HttpRequest.Builder b) {
        HttpRequest request = b.header("Content-Type", "application/json").timeout(Duration.ofSeconds(30)).build();
        try {
            HttpResponse<String> r = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(r.statusCode(), r.body());
        } catch (IOException e) {
            throw new ContractsException("cannot reach " + request.uri() + ": " + (e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage()), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ContractsException("interrupted calling " + request.uri(), e);
        }
    }
}
