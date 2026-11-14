package inflight.e2e;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Someone's real services, standing in: a shop that knows nothing of in-flight. Its orders are plain JSON with
 * the total in cents; its payments are Avro, framed for a Confluent-compatible registry (Apicurio's); a fulfilment
 * service reads the orders and, when told to break, dead-letters them the way Spring Kafka does; a ledger reads the
 * payments. No CloudEvents, no catalog, no headers of this tool's own.
 */
final class SampleApp implements AutoCloseable {

    static final String ORDERS = "shop.orders";
    static final String PAYMENTS = "shop.payments";
    static final String DLT = "fulfilment-service.DLT";
    static final String FULFILMENT = "fulfilment-service";
    static final String LEDGER = "ledger-service";

    static final String PAYMENT_SCHEMA = """
            {"type":"record","name":"PaymentCaptured","namespace":"shop","fields":[
              {"name":"order_id","type":"string"},
              {"name":"customer_id","type":"string"},
              {"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":12,"scale":2}},
              {"name":"currency","type":"string"}]}""";

    final String bootstrap;
    final String registry;
    final AtomicBoolean broken = new AtomicBoolean();
    final AtomicInteger fulfilled = new AtomicInteger();
    final AtomicInteger deadLettered = new AtomicInteger();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final KafkaProducer<String, byte[]> producer;
    private final Thread fulfilment;
    private final Thread ledger;
    private int schemaId;
    private int seq;

    SampleApp(String bootstrap, String registry) throws Exception {
        this.bootstrap = bootstrap;
        this.registry = registry;
        try (Admin a = Admin.create(props())) {
            a.createTopics(List.of(new NewTopic(ORDERS, 3, (short) 1), new NewTopic(PAYMENTS, 3, (short) 1), new NewTopic(DLT, 1, (short) 1))).all().get();
        }
        Properties p = props();
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        producer = new KafkaProducer<>(p, new StringSerializer(), new ByteArraySerializer());
        schemaId = register();
        fulfilment = Thread.ofPlatform().daemon().name("fulfilment").start(this::fulfil);
        ledger = Thread.ofPlatform().daemon().name("ledger").start(this::keepLedger);
    }

    private Properties props() {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        return p;
    }

    /** Registers the payment schema through the registry's Confluent API, as a Confluent serializer would. */
    private int register() throws Exception {
        String body = "{\"schema\":" + inflight.catalog.Json.write(inflight.catalog.Json.tree(PAYMENT_SCHEMA)) + "}";
        HttpResponse<String> r = E2e.HTTP.send(HttpRequest.newBuilder(URI.create(registry + "/subjects/" + PAYMENTS + "-value/versions"))
                .header("Content-Type", "application/vnd.schemaregistry.v1+json").POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
        if (r.statusCode() != 200) {
            throw new IllegalStateException("registry: " + r.statusCode() + " " + r.body());
        }
        return inflight.catalog.Json.parse(r.body()).value().get("id").asInt();
    }

    /** One order, and its payment: {@code cents} is the order's total. */
    void order(long cents) throws Exception {
        seq++;
        String id = "ord-" + seq;
        String customer = "cus-" + (seq % 7);
        String json = "{\"order_id\":\"" + id + "\",\"customer_id\":\"" + customer + "\",\"total_cents\":" + cents + ",\"currency\":\"USD\",\"items\":"
                + (1 + seq % 3) + "}";
        producer.send(new ProducerRecord<>(ORDERS, id, json.getBytes(StandardCharsets.UTF_8))).get();
        Schema s = new Schema.Parser().parse(PAYMENT_SCHEMA);
        GenericRecord pay = new GenericData.Record(s);
        pay.put("order_id", id);
        pay.put("customer_id", customer);
        Schema dec = s.getField("amount").schema();
        pay.put("amount", new Conversions.DecimalConversion().toBytes(BigDecimal.valueOf(cents, 2), dec, dec.getLogicalType()));
        pay.put("currency", "USD");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(ByteBuffer.allocate(5).put((byte) 0).putInt(schemaId).array());
        BinaryEncoder enc = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(s).write(pay, enc);
        enc.flush();
        producer.send(new ProducerRecord<>(PAYMENTS, id, out.toByteArray())).get();
    }

    private void fulfil() {
        Properties p = props();
        p.put(ConsumerConfig.GROUP_ID_CONFIG, FULFILMENT);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(p, new StringDeserializer(), new StringDeserializer())) {
            c.subscribe(List.of(ORDERS));
            while (running.get()) {
                for (ConsumerRecord<String, String> r : c.poll(Duration.ofMillis(200))) {
                    if (broken.get()) {
                        // As Spring Kafka's DeadLetterPublishingRecoverer writes it: the record as it was, and why.
                        ProducerRecord<String, byte[]> dead = new ProducerRecord<>(DLT, 0, r.key(), r.value().getBytes(StandardCharsets.UTF_8));
                        r.headers().forEach(h -> dead.headers().add(h));
                        dead.headers().add("kafka_dlt-original-topic", r.topic().getBytes(StandardCharsets.UTF_8));
                        dead.headers().add("kafka_dlt-exception-message",
                                ("Warehouse " + (r.offset() % 5) + " unreachable for " + r.key()).getBytes(StandardCharsets.UTF_8));
                        try {
                            producer.send(dead).get();
                        } catch (Exception e) {
                            throw new IllegalStateException(e);
                        }
                        deadLettered.incrementAndGet();
                    } else {
                        fulfilled.incrementAndGet();
                    }
                }
                c.commitSync();
            }
        }
    }

    private void keepLedger() {
        Properties p = props();
        p.put(ConsumerConfig.GROUP_ID_CONFIG, LEDGER);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(p, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            c.subscribe(List.of(PAYMENTS));
            while (running.get()) {
                c.poll(Duration.ofMillis(200));
            }
        }
    }

    @Override
    public void close() throws Exception {
        running.set(false);
        fulfilment.join(10_000);
        ledger.join(10_000);
        producer.close();
    }
}
