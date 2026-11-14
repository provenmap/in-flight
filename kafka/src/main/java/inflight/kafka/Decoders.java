package inflight.kafka;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import inflight.catalog.Json;
import inflight.catalog.Records;
import inflight.catalog.Result;

/**
 * Turns a record's bytes into what it says, whatever wrote it. A small interface, one {@link Decoder} per format, tried
 * in order: Avro and Protobuf through a schema registry (Confluent's wire format, a zero byte and a four-byte id, and
 * Apicurio's, an eight-byte global id in the frame or in headers), then JSON and CloudEvents, structured or binary.
 */
public final class Decoders {

    /** One format. {@code accepts} looks only at the framing; {@code decode} does the work. */
    public interface Decoder {
        String name();

        boolean accepts(byte[] value, Map<String, String> headers);

        Result<Records.Decoded> decode(byte[] value, Map<String, String> headers);
    }

    /** What a record turned out to be, and what it says. */
    public record Read(String format, Result<Records.Decoded> decoded) {}

    private final List<Decoder> chain;

    public Decoders(SchemaRegistry registry) {
        List<Decoder> c = new ArrayList<>();
        if (registry != null) {
            c.add(new Registered(registry));
        }
        c.add(new JsonDecoder());
        this.chain = List.copyOf(c);
    }

    /** JSON and CloudEvents only: no registry. */
    public static Decoders plain() {
        return new Decoders(null);
    }

    public Result<Records.Decoded> decode(byte[] value, Map<String, String> headers) {
        return read(value, headers).decoded();
    }

    public Read read(byte[] value, Map<String, String> headers) {
        byte[] v = value == null ? new byte[0] : value;
        for (Decoder d : chain) {
            if (d.accepts(v, headers)) {
                return new Read(d instanceof Registered reg ? reg.lastFormat(v, headers) : d.name(), d.decode(v, headers));
            }
        }
        boolean framed = v.length > 5 && v[0] == 0;
        return new Read("unknown", Result.err(framed
                ? "the record is framed for a schema registry (a zero byte and a schema id), and no registry is configured to read it"
                : "the record is not JSON, and not framed for a schema registry"));
    }

    // ---- JSON and CloudEvents --------------------------------------------------------------------------------------

    static final class JsonDecoder implements Decoder {
        @Override
        public String name() {
            return "json";
        }

        @Override
        public boolean accepts(byte[] value, Map<String, String> headers) {
            if (value.length > 5 && value[0] == 0) {
                // A JSON Schema serializer's frame: JSON after the five bytes.
                return value[5] == '{' || value[5] == '[';
            }
            for (byte b : value) {
                if (!Character.isWhitespace(b)) {
                    return b == '{' || b == '[';
                }
            }
            return false;
        }

        @Override
        public Result<Records.Decoded> decode(byte[] value, Map<String, String> headers) {
            return Records.decode(value, headers);
        }
    }

    // ---- through a registry ----------------------------------------------------------------------------------------

    static final class Registered implements Decoder {
        private final SchemaRegistry registry;
        private final Map<String, org.apache.avro.Schema> avro = new ConcurrentHashMap<>();

        Registered(SchemaRegistry registry) {
            this.registry = registry;
        }

        @Override
        public String name() {
            return "registry";
        }

        @Override
        public boolean accepts(byte[] value, Map<String, String> headers) {
            return headers.containsKey("apicurio.value.globalId") || value.length > 5 && value[0] == 0 && value[5] != '{' && value[5] != '[';
        }

        String lastFormat(byte[] value, Map<String, String> headers) {
            Located l = locate(value, headers);
            return l.schema().isOk() ? l.schema().value().type().toLowerCase(java.util.Locale.ROOT) : "registry";
        }

        /** Where the schema is named and the payload starts: a four-byte id, an eight-byte global id, or a header. */
        private record Located(Result<SchemaRegistry.Schema> schema, int offset, String key) {}

        private Located locate(byte[] value, Map<String, String> headers) {
            String hid = headers.get("apicurio.value.globalId");
            if (hid != null) {
                byte[] raw = hid.getBytes(java.nio.charset.StandardCharsets.ISO_8859_1);
                long id = raw.length == 8 ? ByteBuffer.wrap(raw).getLong() : parseLong(hid);
                return new Located(registry.byGlobalId(id), 0, "g" + id);
            }
            int id4 = ByteBuffer.wrap(value, 1, 4).getInt();
            Result<SchemaRegistry.Schema> s = registry.byId(id4);
            if (s.isOk()) {
                return new Located(s, 5, "c" + id4);
            }
            if (value.length > 9) {
                long id8 = ByteBuffer.wrap(value, 1, 8).getLong();
                Result<SchemaRegistry.Schema> g = registry.byGlobalId(id8);
                if (g.isOk()) {
                    return new Located(g, 9, "g" + id8);
                }
            }
            return new Located(s, 5, "c" + id4);
        }

        private static long parseLong(String s) {
            try {
                return Long.parseLong(s.trim());
            } catch (NumberFormatException e) {
                return -1;
            }
        }

        @Override
        public Result<Records.Decoded> decode(byte[] value, Map<String, String> headers) {
            Located l = locate(value, headers);
            if (!l.schema().isOk()) {
                return Result.err(l.schema().errors());
            }
            SchemaRegistry.Schema schema = l.schema().value();
            ObjectNode data;
            switch (schema.type()) {
                case "AVRO" -> {
                    Result<ObjectNode> r = avro(l.key(), schema.text(), value, l.offset());
                    if (!r.isOk()) {
                        return Result.err(r.errors());
                    }
                    data = r.value();
                }
                case "JSON" -> {
                    return Records.decode(java.util.Arrays.copyOfRange(value, l.offset(), value.length), headers);
                }
                case "PROTOBUF" -> {
                    Result<ObjectNode> r = Protobuf.decode(l.key(), schema.text(), value, l.offset());
                    if (!r.isOk()) {
                        return Result.err(r.errors());
                    }
                    data = r.value();
                }
                default -> {
                    return Result.err("a " + schema.type() + " schema, which this tool cannot read");
                }
            }
            // Binary-mode CloudEvents carry their attributes in headers, whatever the data's format.
            String ceType = headers.get("ce_type");
            if (ceType != null) {
                String sv = headers.get("ce_schemaversion");
                Integer version = sv == null ? null : inflight.catalog.CloudEvent.wholeNumber(Json.tree(sv));
                return Result.ok(new Records.Decoded(headers.get("ce_id"), ceType, version, data, headers.get("ce_redriveof")));
            }
            return Result.ok(new Records.Decoded(null, null, null, data, headers.get("redriveof")));
        }

        private Result<ObjectNode> avro(String key, String text, byte[] value, int offset) {
            try {
                org.apache.avro.Schema schema = avro.computeIfAbsent(key, k -> new org.apache.avro.Schema.Parser().parse(text));
                GenericData data = new GenericData();
                data.addLogicalTypeConversion(new Conversions.DecimalConversion());
                GenericDatumReader<Object> reader = new GenericDatumReader<>(schema, schema, data);
                BinaryDecoder dec = DecoderFactory.get().binaryDecoder(value, offset, value.length - offset, null);
                Object got = reader.read(null, dec);
                JsonNode json = Avro.toJson(got, schema);
                if (!(json instanceof ObjectNode o)) {
                    return Result.err("the Avro record is a " + schema.getType().getName() + ", not a record");
                }
                return Result.ok(o);
            } catch (RuntimeException | java.io.IOException e) {
                return Result.err("the Avro record cannot be read with its schema: " + e.getMessage());
            }
        }
    }

    /** Avro values as JSON: records as objects, decimals as numbers, timestamps as ISO text, unions as their branch. */
    static final class Avro {
        private static final JsonNodeFactory F = JsonNodeFactory.instance;

        private Avro() {}

        static JsonNode toJson(Object v, org.apache.avro.Schema s) {
            if (v == null) {
                return F.nullNode();
            }
            LogicalType lt = s.getLogicalType();
            switch (s.getType()) {
                case UNION -> {
                    int branch = GenericData.get().resolveUnion(s, v);
                    return toJson(v, s.getTypes().get(branch));
                }
                case RECORD -> {
                    GenericRecord r = (GenericRecord) v;
                    ObjectNode o = F.objectNode();
                    for (org.apache.avro.Schema.Field f : s.getFields()) {
                        o.set(f.name(), toJson(r.get(f.pos()), f.schema()));
                    }
                    return o;
                }
                case ARRAY -> {
                    ArrayNode a = F.arrayNode();
                    for (Object x : (java.util.Collection<?>) v) {
                        a.add(toJson(x, s.getElementType()));
                    }
                    return a;
                }
                case MAP -> {
                    ObjectNode o = F.objectNode();
                    ((Map<?, ?>) v).forEach((k, x) -> o.set(String.valueOf(k), toJson(x, s.getValueType())));
                    return o;
                }
                case STRING, ENUM -> {
                    return F.textNode(String.valueOf(v));
                }
                case INT -> {
                    return F.numberNode((Integer) v);
                }
                case LONG -> {
                    long n = (Long) v;
                    if (lt instanceof LogicalTypes.TimestampMillis) {
                        return F.textNode(Instant.ofEpochMilli(n).toString());
                    }
                    if (lt instanceof LogicalTypes.TimestampMicros) {
                        return F.textNode(Instant.EPOCH.plusNanos(n * 1000).toString());
                    }
                    return F.numberNode(n);
                }
                case FLOAT -> {
                    return F.numberNode((Float) v);
                }
                case DOUBLE -> {
                    return F.numberNode((Double) v);
                }
                case BOOLEAN -> {
                    return F.booleanNode((Boolean) v);
                }
                case BYTES, FIXED -> {
                    if (v instanceof BigDecimal d) {
                        return F.numberNode(d);
                    }
                    byte[] b = v instanceof ByteBuffer bb ? toArray(bb) : ((GenericFixed) v).bytes();
                    if (lt instanceof LogicalTypes.Decimal dec) {
                        return F.numberNode(new BigDecimal(new BigInteger(b), dec.getScale()));
                    }
                    return F.textNode(java.util.Base64.getEncoder().encodeToString(b));
                }
                case NULL -> {
                    return F.nullNode();
                }
                default -> {
                    return F.textNode(String.valueOf(v));
                }
            }
        }

        private static byte[] toArray(ByteBuffer bb) {
            ByteBuffer d = bb.duplicate();
            byte[] out = new byte[d.remaining()];
            d.get(out);
            return out;
        }
    }
}
