// Someone's shop on Kafka, for the usage video: real services that know nothing of in-flight. Run it with the
// jar on the class path (it brings the Kafka client and Avro):
//   java -cp cli/target/in-flight.jar scripts/video/Shop.java <bootstrap> <registry-base-url> [control-port]
// Checkout writes orders (JSON, total in cents) and payments (Avro, through the registry's Confluent API). The
// fulfilment service reads orders and writes shipments; told to break, it dead-letters orders as Spring Kafka does.
// The ledger reads payments; notifications read shipments. Control: POST /break, /fix, /rate?n=4; GET /state.
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

public class Shop {

    static final String ORDERS = "shop.orders", PAYMENTS = "shop.payments", SHIPMENTS = "shop.shipments", DLT = "fulfilment-service.DLT";
    static final String PAYMENT_SCHEMA = """
            {"type":"record","name":"PaymentCaptured","namespace":"shop","fields":[
              {"name":"order_id","type":"string"},
              {"name":"customer_id","type":"string"},
              {"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":12,"scale":2}},
              {"name":"currency","type":"string"}]}""";

    static String bootstrap;
    static final AtomicBoolean broken = new AtomicBoolean();
    static final AtomicInteger rate = new AtomicInteger(3), placed = new AtomicInteger(), shipped = new AtomicInteger(), deadLettered = new AtomicInteger();
    static KafkaProducer<String, byte[]> producer;

    static Properties props() {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        return p;
    }

    public static void main(String[] args) throws Exception {
        bootstrap = args[0];
        String registry = args[1].replaceAll("/+$", "") + "/apis/ccompat/v7";
        int port = args.length > 2 ? Integer.parseInt(args[2]) : 8099;
        try (Admin a = Admin.create(props())) {
            var existing = a.listTopics().names().get();
            var want = List.of(new NewTopic(ORDERS, 3, (short) 1), new NewTopic(PAYMENTS, 3, (short) 1), new NewTopic(SHIPMENTS, 3, (short) 1),
                    new NewTopic(DLT, 1, (short) 1));
            a.createTopics(want.stream().filter(t -> !existing.contains(t.name())).toList()).all().get();
        }
        Properties p = props();
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        producer = new KafkaProducer<>(p, new StringSerializer(), new ByteArraySerializer());
        int schemaId = register(registry);
        Thread.ofPlatform().daemon().name("fulfilment").start(Shop::fulfilment);
        Thread.ofPlatform().daemon().name("ledger").start(() -> drain("ledger-service", PAYMENTS));
        Thread.ofPlatform().daemon().name("notifications").start(() -> drain("notifications-service", SHIPMENTS));
        control(port);
        System.out.println("shop: orders to " + bootstrap + ", control on http://localhost:" + port);
        Random rnd = new Random(7);
        Schema s = new Schema.Parser().parse(PAYMENT_SCHEMA);
        Schema dec = s.getField("amount").schema();
        while (true) {
            int r = rate.get();
            if (r <= 0) {
                Thread.sleep(200);
                continue;
            }
            int n = placed.incrementAndGet();
            String id = "ord-" + (10400 + n), customer = "cus-" + (100 + rnd.nextInt(60));
            long cents = 1500 + rnd.nextInt(30000);
            String json = "{\"order_id\":\"" + id + "\",\"customer_id\":\"" + customer + "\",\"total_cents\":" + cents
                    + ",\"currency\":\"USD\",\"items\":" + (1 + rnd.nextInt(4)) + "}";
            producer.send(new ProducerRecord<>(ORDERS, id, json.getBytes(StandardCharsets.UTF_8)));
            GenericRecord pay = new GenericData.Record(s);
            pay.put("order_id", id);
            pay.put("customer_id", customer);
            pay.put("amount", new Conversions.DecimalConversion().toBytes(BigDecimal.valueOf(cents, 2), dec, dec.getLogicalType()));
            pay.put("currency", "USD");
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(ByteBuffer.allocate(5).put((byte) 0).putInt(schemaId).array());
            BinaryEncoder enc = EncoderFactory.get().binaryEncoder(out, null);
            new GenericDatumWriter<GenericRecord>(s).write(pay, enc);
            enc.flush();
            producer.send(new ProducerRecord<>(PAYMENTS, id, out.toByteArray()));
            Thread.sleep(1000L / r);
        }
    }

    /** Registers the payment schema through the registry's Confluent API, as a Confluent serializer would. */
    static int register(String registry) throws Exception {
        String body = "{\"schema\":\"" + PAYMENT_SCHEMA.replace("\"", "\\\"").replace("\n", "") + "\"}";
        HttpResponse<String> r = HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create(registry + "/subjects/" + PAYMENTS + "-value/versions"))
                .header("Content-Type", "application/vnd.schemaregistry.v1+json").POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
        Matcher m = Pattern.compile("\"id\"\\s*:\\s*(\\d+)").matcher(r.body());
        if (r.statusCode() != 200 || !m.find()) {
            throw new IllegalStateException("registry: " + r.statusCode() + " " + r.body());
        }
        return Integer.parseInt(m.group(1));
    }

    static void fulfilment() {
        Properties p = props();
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "fulfilment-service");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(p, new StringDeserializer(), new StringDeserializer())) {
            c.subscribe(List.of(ORDERS));
            while (true) {
                for (ConsumerRecord<String, String> r : c.poll(Duration.ofMillis(200))) {
                    if (broken.get()) {
                        // As Spring Kafka's DeadLetterPublishingRecoverer writes it: the record as it was, and why.
                        ProducerRecord<String, byte[]> dead = new ProducerRecord<>(DLT, 0, r.key(), r.value().getBytes(StandardCharsets.UTF_8));
                        r.headers().forEach(h -> dead.headers().add(h));
                        dead.headers().add("kafka_dlt-original-topic", r.topic().getBytes(StandardCharsets.UTF_8));
                        dead.headers().add("kafka_dlt-exception-message", "Warehouse API timed out reserving stock".getBytes(StandardCharsets.UTF_8));
                        producer.send(dead);
                        deadLettered.incrementAndGet();
                    } else {
                        String total = r.value().replaceAll(".*\"total_cents\":(\\d+).*", "$1");
                        String customer = r.value().replaceAll(".*\"customer_id\":\"([^\"]+)\".*", "$1");
                        String ship = "{\"order_id\":\"" + r.key() + "\",\"customer_id\":\"" + customer + "\",\"total_cents\":" + total
                                + ",\"carrier\":\"UPS\"}";
                        ProducerRecord<String, byte[]> out = new ProducerRecord<>(SHIPMENTS, r.key(), ship.getBytes(StandardCharsets.UTF_8));
                        r.headers().forEach(h -> out.headers().add(h));
                        producer.send(out);
                        shipped.incrementAndGet();
                    }
                }
                c.commitSync();
            }
        }
    }

    static void drain(String group, String topic) {
        Properties p = props();
        p.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "500");
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(p, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            c.subscribe(List.of(topic));
            while (true) {
                c.poll(Duration.ofMillis(200));
            }
        }
    }

    static void control(int port) throws Exception {
        HttpServer http = HttpServer.create(new InetSocketAddress("localhost", port), 0);
        http.createContext("/", ex -> {
            String path = ex.getRequestURI().getPath(), q = ex.getRequestURI().getQuery();
            switch (path) {
                case "/break" -> broken.set(true);
                case "/fix" -> broken.set(false);
                case "/rate" -> rate.set(Integer.parseInt(q.replaceAll(".*n=(\\d+).*", "$1")));
                default -> { }
            }
            byte[] body = ("{\"broken\":" + broken.get() + ",\"rate\":" + rate.get() + ",\"placed\":" + placed.get() + ",\"shipped\":" + shipped.get()
                    + ",\"deadLettered\":" + deadLettered.get() + "}").getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Content-Type", "application/json");
            ex.sendResponseHeaders(200, body.length);
            ex.getResponseBody().write(body);
            ex.close();
        });
        http.start();
    }
}
