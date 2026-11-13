package rif.simulator;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import rif.catalog.CloudEvent;
import rif.catalog.Pricing;
import rif.catalog.Result;

/** Events as the demo's services write and read them, and the JVM hashes their decisions rest on. */
public final class Events {

    private Events() {}

    /** An event to publish, keyed by its order so one order's events stay in one partition. */
    public record Out(String topic, String key, CloudEvent event) {
        public ProducerRecord<String, byte[]> record() {
            return new ProducerRecord<>(topic, key, event.toJson().getBytes(StandardCharsets.UTF_8));
        }
    }

    /** Stamps events from one source with a fresh id and that source's own sequence. */
    public static final class Emitter {
        private final String source;
        private long seq;

        public Emitter(String source) {
            this.source = source;
        }

        public synchronized CloudEvent emit(String type, int version, Instant at, ObjectNode data, String redriveOf) {
            return new CloudEvent(UUID.randomUUID().toString(), source, type, version, at, ++seq, data, redriveOf);
        }

        public CloudEvent emit(String type, int version, Instant at, ObjectNode data) {
            return emit(type, version, at, data, null);
        }
    }

    // ---- reading fields the way a service would: by name, failing with the reason it would log ----

    public static Result<String> str(JsonNode data, String key) {
        Result<JsonNode> v = field(data, key);
        if (!v.isOk()) {
            return Result.err(v.errors());
        }
        return v.value().isTextual() ? Result.ok(v.value().asText()) : Result.err("'" + key + "' is " + kind(v.value()) + ", expected a string");
    }

    /** A decimal string, the way every amount in this flow was written before v2. */
    public static Result<BigDecimal> decimal(JsonNode data, String key) {
        Result<JsonNode> v = field(data, key);
        if (!v.isOk()) {
            return Result.err(v.errors());
        }
        if (!v.value().isTextual()) {
            return Result.err("'" + key + "' is " + kind(v.value()) + ", expected a decimal string");
        }
        BigDecimal m = Pricing.money(v.value());
        return m != null ? Result.ok(m) : Result.err("'" + key + "' is '" + v.value().asText() + "', not a decimal");
    }

    public static Result<JsonNode> field(JsonNode data, String key) {
        JsonNode v = data != null && data.isObject() ? data.get(key) : null;
        return v == null || v.isNull() ? Result.err("no '" + key + "'") : Result.ok(v);
    }

    public static String kind(JsonNode v) {
        if (v.isArray()) {
            return "an array";
        }
        if (v.isNull()) {
            return "null";
        }
        if (v.isObject()) {
            return "an object";
        }
        if (v.isTextual()) {
            return "a string";
        }
        if (v.isNumber()) {
            return "a number";
        }
        return "a boolean";
    }

    /** Amounts on the wire: two decimal places, half up, as a string. */
    public static String formatMoney(BigDecimal b) {
        return b.setScale(2, RoundingMode.HALF_UP).toPlainString();
    }

    // ---- the JVM's hashes, so a decision is the same in every build, and for dead letters any build wrote ----

    /** Scala's {@code MurmurHash3.stringHash}. */
    public static int scalaStringHash(String s) {
        int h = 0xf7ca7fd2;
        int i = 0;
        for (; i + 1 < s.length(); i += 2) {
            h = mix(h, (s.charAt(i) << 16) + s.charAt(i + 1));
        }
        if (i < s.length()) {
            h = mixLast(h, s.charAt(i));
        }
        return avalanche(h ^ s.length());
    }

    private static int mixLast(int h, int k) {
        k *= 0xcc9e2d51;
        k = Integer.rotateLeft(k, 15);
        k *= 0x1b873593;
        return h ^ k;
    }

    private static int mix(int h, int k) {
        h = Integer.rotateLeft(mixLast(h, k), 13);
        return h * 5 + 0xe6546b64;
    }

    private static int avalanche(int h) {
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        h ^= h >>> 13;
        h *= 0xc2b2ae35;
        return h ^ (h >>> 16);
    }

    /** Java's {@code String.hashCode}. */
    public static int javaHash(String s) {
        return s.hashCode();
    }
}
