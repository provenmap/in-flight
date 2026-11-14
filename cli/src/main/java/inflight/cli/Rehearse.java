package inflight.cli;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import inflight.broker.Broker;
import inflight.broker.Cursor;
import inflight.broker.Endpoint;
import inflight.broker.Message;
import inflight.broker.Profile;
import inflight.broker.Shard;
import inflight.catalog.Intent;
import inflight.catalog.Json;
import inflight.catalog.Records;
import inflight.catalog.Result;
import inflight.codec.Decoders;
import inflight.codec.SchemaRegistry;
import inflight.server.Workspace;

/**
 * Rehearses any consumer on the past: the last minutes of a topic (or an intent's) run through a build given as a
 * command or a container image, beside a baseline build, and the difference comes back. Nothing is written to Kafka;
 * the builds see records only on their standard input.
 *
 * <p>The protocol is one JSON line in and one JSON line out per record. In: {@code topic}, {@code partition},
 * {@code offset}, {@code key}, {@code headers}, and {@code value} (the record decoded to JSON, through the registry for
 * Avro; else its bytes in base64 as {@code valueBase64}). Out: anything; a {@code result} field, when there is one, is
 * what is counted ("ok", "dead-letter", "PaymentAuthorized"...). Two builds disagree on a record when their lines differ.
 */
final class Rehearse {

    private Rehearse() {}

    /** A build: a process fed one line per record, read one line per record. */
    static final class Build implements AutoCloseable {
        final String name;
        private final Process process;
        private final BufferedWriter in;
        private final BufferedReader out;

        Build(String name, List<String> command) throws IOException {
            this.name = name;
            this.process = new ProcessBuilder(command).redirectError(ProcessBuilder.Redirect.INHERIT).start();
            this.in = new BufferedWriter(new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8));
            this.out = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
        }

        /** Its answer to one record: the line it wrote, or what went wrong. */
        String apply(String line) {
            try {
                in.write(line);
                in.newLine();
                in.flush();
                String got = out.readLine();
                return got == null ? "{\"result\":\"exited\",\"exit\":" + exit() + "}" : got;
            } catch (IOException e) {
                return "{\"result\":\"failed\",\"error\":" + Json.write(Json.tree(e.getMessage())) + "}";
            }
        }

        private int exit() {
            try {
                return process.waitFor(2, TimeUnit.SECONDS) ? process.exitValue() : -1;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return -1;
            }
        }

        @Override
        public void close() {
            try {
                in.close();
            } catch (IOException ignored) {
                // it may have gone already
            }
            try {
                if (!process.waitFor(10, TimeUnit.SECONDS)) {
                    process.destroy();
                }
            } catch (InterruptedException e) {
                process.destroyForcibly();
                Thread.currentThread().interrupt();
            }
        }
    }

    static List<String> command(String shell, String image) {
        if (image != null) {
            return List.of("docker", "run", "--rm", "-i", image);
        }
        boolean windows = System.getProperty("os.name", "").toLowerCase(java.util.Locale.ROOT).contains("win");
        return windows ? List.of("cmd", "/c", shell) : List.of("sh", "-c", shell);
    }

    static int run(Workspace ws, String[] argv) throws Exception {
        Setups.Args a = Setups.Args.of(argv, "topic", "intent", "candidate", "candidate-image", "baseline", "baseline-image", "minutes", "limit", "bootstrap");
        String topic = a.one("topic") != null ? a.one("topic") : a.one("intent") != null ? Intent.topicFor(a.one("intent")) : null;
        if (topic == null || a.one("candidate") == null && a.one("candidate-image") == null) {
            throw new Main.Failure("rehearse needs --topic or --intent, and --candidate \"<command>\" or --candidate-image <image>");
        }
        Profile p = ws.load().active();
        Endpoint endpoint = a.one("bootstrap") != null ? Endpoint.of(a.one("bootstrap")) : p != null ? p.endpoint() : Endpoint.fromEnv();
        Decoders decoders = Decoders.plain();
        if (p != null && a.one("bootstrap") == null) {
            p.adapter().configure(p);
            SchemaRegistry r = SchemaRegistry.of(p.registry());
            decoders = r == null ? Decoders.plain() : new Decoders(r);
        }
        double minutes = a.one("minutes") == null ? 10 : Double.parseDouble(a.one("minutes"));
        int limit = a.one("limit") == null ? 10_000 : Integer.parseInt(a.one("limit"));
        List<Message> records = read(endpoint, topic, Instant.now().minusMillis((long) (minutes * 60_000)), limit);

        Map<String, Integer> candidateResults = new TreeMap<>();
        Map<String, Integer> baselineResults = new TreeMap<>();
        List<String> differences = new ArrayList<>();
        int disagree = 0;
        try (Build candidate = new Build("candidate", command(a.one("candidate"), a.one("candidate-image")));
             Build baseline = a.one("baseline") == null && a.one("baseline-image") == null ? null
                     : new Build("baseline", command(a.one("baseline"), a.one("baseline-image")))) {
            for (Message r : records) {
                String line = Json.write(line(r, decoders));
                String c = candidate.apply(line);
                candidateResults.merge(result(c), 1, Integer::sum);
                if (baseline != null) {
                    String b = baseline.apply(line);
                    baselineResults.merge(result(b), 1, Integer::sum);
                    if (!same(b, c)) {
                        disagree++;
                        if (differences.size() < 20) {
                            differences.add(r.place() + ": " + result(b) + " -> " + result(c));
                        }
                    }
                }
            }
        }
        StringBuilder text = new StringBuilder("Rehearsed " + records.size() + " records of " + topic + " from the last " + Json.number(Json.tree(minutes))
                + " minutes: candidate " + candidateResults);
        if (!baselineResults.isEmpty()) {
            text.append(", baseline ").append(baselineResults).append(". ")
                    .append(disagree == 0 ? "They agree on every record." : "They disagree on " + disagree + ":");
            differences.forEach(d -> text.append("\n  ").append(d));
        } else {
            text.append('.');
        }
        System.out.println(text);
        return disagree == 0 ? 0 : 1;
    }

    private static List<Message> read(Endpoint endpoint, String topic, Instant since, int limit) {
        List<Message> out = new ArrayList<>();
        try (Broker broker = endpoint.open(); Cursor c = broker.observe()) {
            if (!broker.streams().contains(topic)) {
                throw new Main.Failure("there is no topic " + topic);
            }
            List<Shard> parts = broker.shards(List.of(topic));
            Map<Shard, Long> from = broker.offsetsAt(parts, since);
            Map<Shard, Long> stop = new LinkedHashMap<>(broker.ends(parts));
            c.seek(from);
            long deadline = System.currentTimeMillis() + 30_000;
            while (out.size() < limit && System.currentTimeMillis() < deadline && parts.stream().anyMatch(tp -> c.position(tp) < stop.get(tp))) {
                for (Message r : c.poll(Duration.ofMillis(200))) {
                    if (r.offset() < stop.get(r.shard()) && out.size() < limit) {
                        out.add(r);
                    }
                }
            }
        }
        out.sort((x, y) -> Long.compare(x.timestamp(), y.timestamp()));
        return out;
    }

    static ObjectNode line(Message r, Decoders decoders) {
        ObjectNode o = Json.object().put("topic", r.stream()).put("partition", r.index()).put("offset", r.offset()).put("timestamp", r.timestamp());
        if (r.key() != null) {
            o.put("key", new String(r.key(), StandardCharsets.UTF_8));
        }
        Map<String, String> hs = r.textHeaders();
        ObjectNode h = o.putObject("headers");
        hs.forEach(h::put);
        byte[] v = r.valueOrEmpty();
        Result<JsonNode> json = Json.parse(v, 0, v.length);
        if (json.isOk()) {
            o.set("value", json.value());
        } else {
            Result<Records.Decoded> d = decoders.decode(v, hs);
            if (d.isOk()) {
                o.set("value", d.value().data());
            } else {
                o.put("valueBase64", Base64.getEncoder().encodeToString(v));
            }
        }
        return o;
    }

    static String result(String line) {
        Result<JsonNode> j = Json.parse(line);
        if (j.isOk() && j.value().isObject() && j.value().has("result")) {
            return j.value().get("result").asText();
        }
        return line.length() > 40 ? line.substring(0, 40) + "…" : line;
    }

    static boolean same(String a, String b) {
        Result<JsonNode> x = Json.parse(a);
        Result<JsonNode> y = Json.parse(b);
        return x.isOk() && y.isOk() ? x.value().equals(y.value()) : a.equals(b);
    }
}
