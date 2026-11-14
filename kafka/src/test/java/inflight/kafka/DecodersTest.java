package inflight.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import inflight.catalog.Json;
import inflight.catalog.Records;
import inflight.catalog.Result;

/** JSON and CloudEvents as before; Avro through a registry, in Confluent's frame and Apicurio's. */
class DecodersTest {

    static final String SCHEMA = """
            {"type":"record","name":"OrderPlaced","namespace":"shop","fields":[
              {"name":"orderId","type":"string"},
              {"name":"customerId","type":["null","string"],"default":null},
              {"name":"total","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},
              {"name":"placedAt","type":{"type":"long","logicalType":"timestamp-millis"}},
              {"name":"status","type":{"type":"enum","name":"Status","symbols":["NEW","PAID"]}}]}""";

    HttpServer registry;
    String url;

    @BeforeEach
    void serve() throws Exception {
        registry = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        String body = Json.write(Json.object().put("schema", SCHEMA));
        registry.createContext("/schemas/ids/7", ex -> reply(ex, 200, body));
        registry.createContext("/apis/registry/v2/ids/globalIds/99", ex -> reply(ex, 200, SCHEMA));
        registry.createContext("/subjects", ex -> reply(ex, 200, "[\"orders-value\"]"));
        registry.createContext("/", ex -> reply(ex, 404, "{}"));
        registry.start();
        url = "http://localhost:" + registry.getAddress().getPort();
    }

    static void reply(com.sun.net.httpserver.HttpExchange ex, int status, String body) throws java.io.IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        ex.sendResponseHeaders(status, b.length);
        ex.getResponseBody().write(b);
        ex.close();
    }

    @AfterEach
    void stop() {
        registry.stop(0);
    }

    static byte[] avro() throws Exception {
        Schema s = new Schema.Parser().parse(SCHEMA);
        GenericRecord r = new GenericData.Record(s);
        r.put("orderId", "o-1");
        r.put("customerId", "c-9");
        Schema dec = s.getField("total").schema();
        r.put("total", new Conversions.DecimalConversion().toBytes(new BigDecimal("129.95"), dec, dec.getLogicalType()));
        r.put("placedAt", 1_700_000_000_000L);
        r.put("status", new GenericData.EnumSymbol(s.getField("status").schema(), "NEW"));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder enc = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(s).write(r, enc);
        enc.flush();
        return out.toByteArray();
    }

    static byte[] frame(byte[] head, byte[] body) {
        return ByteBuffer.allocate(head.length + body.length).put(head).put(body).array();
    }

    @Test
    void avroInConfluentsFrameIsReadThroughTheRegistry() throws Exception {
        byte[] v = frame(ByteBuffer.allocate(5).put((byte) 0).putInt(7).array(), avro());
        Decoders.Read r = new Decoders(new SchemaRegistry(url, null, null)).read(v, Map.of());
        assertTrue(r.decoded().isOk(), r.decoded().error());
        assertEquals("avro", r.format());
        Records.Decoded d = r.decoded().value();
        assertEquals("o-1", d.data().get("orderId").asText());
        assertEquals("c-9", d.data().get("customerId").asText());
        assertEquals(0, new BigDecimal("129.95").compareTo(d.data().get("total").decimalValue()));
        assertEquals("2023-11-14T22:13:20Z", d.data().get("placedAt").asText());
        assertEquals("NEW", d.data().get("status").asText());
    }

    @Test
    void avroInApicuriosFrameAndBinaryCloudEventsHeaders() throws Exception {
        byte[] v = frame(ByteBuffer.allocate(9).put((byte) 0).putLong(99).array(), avro());
        Result<Records.Decoded> d = new Decoders(new SchemaRegistry(url, null, null)).decode(v,
                Map.of("ce_type", "OrderPlaced", "ce_schemaversion", "3", "ce_id", "e-1", "redriveof", "x"));
        assertTrue(d.isOk(), d.error());
        assertEquals("OrderPlaced", d.value().type());
        assertEquals(3, d.value().version());
        assertEquals("e-1", d.value().id());
        assertEquals("o-1", d.value().data().get("orderId").asText());
    }

    @Test
    void withoutARegistryAFramedRecordSaysWhatItNeeds() throws Exception {
        byte[] v = frame(ByteBuffer.allocate(5).put((byte) 0).putInt(7).array(), avro());
        Decoders.Read r = Decoders.plain().read(v, Map.of());
        assertFalse(r.decoded().isOk());
        assertTrue(r.decoded().error().contains("no registry is configured"), r.decoded().error());
        // JSON still reads, framed or not, and a plain record carries the redrive header.
        Result<Records.Decoded> j = Decoders.plain().decode("{\"a\":1}".getBytes(StandardCharsets.UTF_8), Map.of("redriveof", "q:0:4"));
        assertEquals("q:0:4", j.value().redriveOf());
        assertTrue(Decoders.plain().decode(frame(ByteBuffer.allocate(5).put((byte) 0).putInt(1).array(), "{\"a\":1}".getBytes(StandardCharsets.UTF_8)), Map.of()).isOk());
        assertEquals(java.util.List.of("orders-value"), new SchemaRegistry(url, null, null).subjects().value());
    }
}
