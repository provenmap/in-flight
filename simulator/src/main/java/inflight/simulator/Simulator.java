package inflight.simulator;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.HandlerType;
import inflight.catalog.Catalog;
import inflight.catalog.Json;
import inflight.kafka.Kafka;
import inflight.simulator.Events.Emitter;
import inflight.simulator.Services.Inventory;
import inflight.simulator.Services.Payments;
import inflight.simulator.Services.Shipping;

/**
 * Order traffic and the three services, with a control API on port 8090:
 *
 * <pre>
 *   GET  /state                   the knobs, each service's counts and the last redrive
 *   POST /break | /restore        orders switch to OrderPlaced v2 | back to v1
 *   POST /patch | /unpatch        payments reads both shapes | only v1
 *   POST /redrive                 one pass over payments.dlq with the current payments code
 *   POST /rate?perSecond=N        order traffic
 * </pre>
 */
public final class Simulator implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Simulator.class);

    /** The knobs the control API turns. */
    public static final class Controls {
        public volatile int orderVersion = 1;
        public volatile boolean patched = false;
        public volatile double rate = 8;
        public volatile Redrive.Status redrive = Redrive.Status.IDLE;
        /** True when the orders team produces through the proxy, so the schema gate checks every order. */
        public volatile boolean gated = false;
    }

    /** Where to run, from the environment unless given. */
    /**
     * Where to run, from the environment unless given. {@code intentsVia} is the proxy's address: given, each service
     * reads the intent the catalog says it reads by, as {@code intent.<name>} through the proxy, instead of its topics.
     */
    public record Options(String bootstrap, int port, String host, Double rate, String intentsVia) {
        public static Options fromEnv() {
            String port = System.getenv("CONTROL_PORT");
            String host = System.getenv("HOST");
            String via = inflight.catalog.Env.get("INFLIGHT_INTENTS_VIA");
            return new Options(Kafka.bootstrap(), port == null ? 8090 : Integer.parseInt(port), host == null ? "localhost" : host, null,
                    via == null || via.isBlank() ? null : via);
        }

        public Options withPort(int p) {
            return new Options(bootstrap, p, host, rate, intentsVia);
        }

        public Options withBootstrap(String b) {
            return new Options(b, port, host, rate, intentsVia);
        }

        public Options withRate(double r) {
            return new Options(bootstrap, port, host, r, intentsVia);
        }

        public Options withIntentsVia(String proxy) {
            return new Options(bootstrap, port, host, rate, proxy);
        }
    }

    public final Controls controls = new Controls();
    private final Traffic traffic;
    private final Map<String, ServiceLoop> services = new LinkedHashMap<>();
    private final Shipping shipping;
    private final Redrive redrive;
    private final Javalin app;
    private volatile Thread redriving;
    private final String url;
    private final String intentsVia;
    private final String bootstrap;

    private Simulator(Options opts) {
        String bootstrap = opts.bootstrap();
        Catalog catalog = Catalog.defaultCatalog();
        intentsVia = opts.intentsVia();
        this.bootstrap = bootstrap;
        String consumeFrom = intentsVia != null ? intentsVia : bootstrap;
        java.util.function.BiFunction<String, List<String>, List<String>> reads = (group, topics) -> {
            if (intentsVia == null) {
                return topics;
            }
            String intent = catalog.consumer(group).map(Catalog.Consumer::intent).orElse(null);
            if (intent == null) {
                throw new IllegalStateException("the catalog names no intent for " + group + " to read by");
            }
            return List.of(inflight.catalog.Intent.aliasFor(intent));
        };
        try (Admin admin = Kafka.admin(bootstrap)) {
            List<String> topics = new java.util.ArrayList<>(catalog.topics());
            topics.addAll(catalog.dlqs());
            Kafka.ensureTopics(admin, topics, Kafka.PARTITIONS);
            // Two simulators on one Kafka would split the services' partitions between them, and one's redrive would
            // drain the other's dead letters. A group with members means another simulator is attached.
            for (Catalog.Consumer s : catalog.services()) {
                int members = Kafka.members(admin, s.group());
                if (members > 0) {
                    throw new IllegalStateException("another simulator is attached to the Kafka at " + bootstrap + ": consumer group '" + s.group()
                            + "' has " + members + " member(s). Stop it, or use another Kafka. A simulator that was killed rather than stopped holds its "
                            + "groups until their 45 s session timeout.");
                }
            }
        }
        if (opts.rate() != null) {
            controls.rate = opts.rate();
        }
        traffic = new Traffic(bootstrap, intentsVia, controls);
        Emitter paymentsE = new Emitter("/payments");
        services.put("payments", new ServiceLoop("payments-service", bootstrap, consumeFrom, Payments.GROUP, reads.apply(Payments.GROUP, List.of(Orders.TOPIC)), Payments.DLQ, e -> {
            // An external card authorisation takes a few milliseconds; a flood of orders builds real lag.
            Thread.sleep(10 + ThreadLocalRandom.current().nextInt(15));
            return Payments.handle(paymentsE, e, controls.patched, Instant.now());
        }));
        Emitter inventoryE = new Emitter("/inventory");
        services.put("inventory", new ServiceLoop("inventory-service", bootstrap, consumeFrom, Inventory.GROUP, reads.apply(Inventory.GROUP, List.of(Orders.TOPIC)), Inventory.DLQ,
                e -> Inventory.handle(inventoryE, e, Instant.now())));
        shipping = new Shipping(new Emitter("/shipping"));
        services.put("shipping", new ServiceLoop("shipping-service", bootstrap, consumeFrom, Shipping.GROUP, reads.apply(Shipping.GROUP, Shipping.READS), Shipping.DLQ,
                e -> shipping.handle(e, Instant.now())));

        String redriveGroup = catalog.redriveGroupOf(Payments.DLQ);
        if (redriveGroup == null) {
            throw new IllegalStateException("the catalog names no redrive group for " + Payments.DLQ);
        }
        redrive = new Redrive(bootstrap, Payments.DLQ, redriveGroup, e -> Payments.handle(paymentsE, e, controls.patched, Instant.now(), true));

        traffic.start();
        services.values().forEach(ServiceLoop::start);

        app = Javalin.create(config -> {
            config.startup.showJavalinBanner = false;
            for (String path : List.of("/state", "/break", "/restore", "/patch", "/unpatch", "/redrive", "/rate", "/gate", "/rehearse")) {
                for (HandlerType method : List.of(HandlerType.GET, HandlerType.POST, HandlerType.PUT, HandlerType.DELETE, HandlerType.PATCH)) {
                    config.routes.addHttpHandler(method, path, ctx -> control(ctx, path));
                }
            }
            config.routes.error(404, ctx -> json(ctx, 404, error("no such endpoint " + ctx.path())));
        });
        app.start(opts.host(), opts.port());
        url = "http://localhost:" + app.port();
        log.info("simulator: Kafka at {}, {} orders/s, {}, control API on {}", bootstrap, number(controls.rate),
                intentsVia == null ? "services read their topics" : "services read by intent through " + intentsVia, url);
    }

    public static Simulator start(Options opts) {
        return new Simulator(opts);
    }

    public String url() {
        return url;
    }

    private void control(Context ctx, String path) {
        boolean allowed = path.equals("/state") ? HandlerType.GET.equals(ctx.method()) : HandlerType.POST.equals(ctx.method());
        if (!allowed) {
            json(ctx, 405, error(ctx.method() + " not allowed"));
            return;
        }
        if (path.equals("/rehearse")) {
            rehearse(ctx);
            return;
        }
        String problem = switch (path) {
            case "/break" -> { controls.orderVersion = 2; yield null; }
            case "/restore" -> { controls.orderVersion = 1; yield null; }
            case "/patch" -> { controls.patched = true; yield null; }
            case "/unpatch" -> { controls.patched = false; yield null; }
            case "/redrive" -> startRedrive();
            case "/rate" -> setRate(ctx.queryParam("perSecond"));
            case "/gate" -> setGate(ctx.queryParam("on"));
            default -> null;
        };
        if (problem != null) {
            json(ctx, problem.startsWith("409 ") ? 409 : 400, error(problem.substring(4)));
            return;
        }
        if (HandlerType.POST.equals(ctx.method())) {
            String q = ctx.queryString();
            log.info("control: {}{}", path, q == null ? "" : " " + q.replace('&', ' '));
        }
        json(ctx, 200, state());
    }

    /**
     * POST /rehearse?intent=order-total-changes&build=patched&minutes=10: the intent's events of the last minutes, through
     * the build that runs now and the candidate build, the candidate's output on a branch topic, and the difference.
     */
    private void rehearse(Context ctx) {
        String intent = ctx.queryParam("intent");
        String build = ctx.queryParam("build");
        double minutes;
        try {
            minutes = ctx.queryParam("minutes") == null ? 10 : Double.parseDouble(ctx.queryParam("minutes"));
        } catch (NumberFormatException e) {
            minutes = -1;
        }
        if (intent == null || build == null || !(minutes > 0 && minutes <= 24 * 60)) {
            json(ctx, 400, error("rehearse needs intent, build (strict or patched) and minutes from 1 to 1440"));
            return;
        }
        Instant to = Instant.now();
        var report = Rehearsal.run(bootstrap, intent, to.minusMillis((long) (minutes * 60_000)), to, controls.patched, build);
        if (!report.isOk()) {
            json(ctx, 409, error(report.error()));
            return;
        }
        log.info("rehearsal: {}", report.value().text());
        json(ctx, 200, report.value().json());
    }

    private String setRate(String perSecond) {
        double r;
        try {
            r = perSecond == null || perSecond.isBlank() ? Double.NaN : Double.parseDouble(perSecond);
        } catch (NumberFormatException e) {
            r = Double.NaN;
        }
        if (!(r >= 0 && r <= 500)) {
            return "400 rate needs perSecond between 0 and 500";
        }
        controls.rate = r;
        return null;
    }

    private String setGate(String on) {
        if (intentsVia == null) {
            return "409 there is no proxy to produce through: start the simulator with INFLIGHT_INTENTS_VIA";
        }
        if (!"true".equals(on) && !"false".equals(on)) {
            return "400 gate needs on=true or on=false";
        }
        controls.gated = Boolean.parseBoolean(on);
        return null;
    }

    private synchronized String startRedrive() {
        if (controls.redrive.running()) {
            return "409 a redrive is already running";
        }
        controls.redrive = new Redrive.Status(true, 0, 0, null);
        redriving = Thread.ofPlatform().name("redrive").start(() -> {
            Redrive.Status result;
            try {
                result = redrive.run(s -> controls.redrive = s, 60_000);
            } catch (RuntimeException e) {
                result = new Redrive.Status(false, 0, 0, e.toString());
            }
            controls.redrive = result;
            log.info("redrive: {} redriven, {} remaining{}", result.redriven(), result.remaining(),
                    result.error() != null ? ", stopped at " + result.error() : "");
        });
        return null;
    }

    public ObjectNode state() {
        ObjectNode s = Json.object();
        s.put("orderVersion", controls.orderVersion);
        s.put("patched", controls.patched);
        s.set("rate", number(controls.rate));
        s.put("placed", traffic.placed());
        s.put("placedV2", traffic.placedV2());
        s.put("ordersVia", controls.gated ? "proxy" : "kafka");
        s.put("proxy", intentsVia != null);
        s.put("refusedByGate", traffic.refused());
        ObjectNode svc = s.putObject("services");
        services.forEach((n, l) -> {
            ObjectNode x = svc.putObject(n).put("processed", l.processed()).put("deadLettered", l.deadLettered());
            ArrayNode reads = x.putArray("reads");
            l.topics().forEach(reads::add);
        });
        s.put("shippingWaiting", shipping.pending());
        Redrive.Status r = controls.redrive;
        ObjectNode rd = s.putObject("redrive");
        rd.put("running", r.running()).put("redriven", r.redriven()).put("remaining", r.remaining());
        if (r.error() == null) {
            rd.putNull("error");
        } else {
            rd.put("error", r.error());
        }
        return s;
    }

    /** A whole number as one, so a rate of 8 reads 8 and not 8.0. */
    static com.fasterxml.jackson.databind.JsonNode number(double d) {
        return d == Math.rint(d) && Math.abs(d) < 1e15 ? Json.tree((long) d) : Json.tree(d);
    }

    private static ObjectNode error(String msg) {
        return Json.object().put("error", msg);
    }

    private static void json(Context ctx, int status, Object body) {
        ctx.status(status).contentType("application/json").result(Json.write(body));
    }

    @Override
    public void close() throws InterruptedException {
        app.stop();
        traffic.stop();
        for (ServiceLoop s : services.values()) {
            s.stop();
        }
        Thread r = redriving;
        if (r != null) {
            r.join(65_000);
        }
    }

    /**
     * The orders team at work: orders arrive as a Poisson process at the current rate, in the current schema version,
     * straight to Kafka or, gated, through the proxy. A version the gate refuses was never released: that order goes out
     * in the shape the registry accepts, v1, as the team's deploy of v2 is held back.
     */
    static final class Traffic {
        private final String bootstrap;
        private final String proxy;
        private final Controls controls;
        private volatile boolean running = true;
        private final java.util.concurrent.atomic.AtomicLong placed = new java.util.concurrent.atomic.AtomicLong();
        private final java.util.concurrent.atomic.AtomicLong placedV2 = new java.util.concurrent.atomic.AtomicLong();
        private final java.util.concurrent.atomic.AtomicLong refused = new java.util.concurrent.atomic.AtomicLong();
        private final java.util.concurrent.ConcurrentLinkedQueue<inflight.catalog.CloudEvent> resend = new java.util.concurrent.ConcurrentLinkedQueue<>();
        private Thread thread;

        Traffic(String bootstrap, String proxy, Controls controls) {
            this.bootstrap = bootstrap;
            this.proxy = proxy;
            this.controls = controls;
        }

        long placed() {
            return placed.get();
        }

        long placedV2() {
            return placedV2.get();
        }

        long refused() {
            return refused.get();
        }

        void start() {
            thread = Thread.ofPlatform().name("traffic").start(this::run);
        }

        private void run() {
            Emitter emit = new Emitter("/orders");
            Orders.Rng rng = Orders.unseeded();
            KafkaProducer<String, byte[]> gated = proxy == null ? null : Kafka.gatedProducer(proxy);
            try (KafkaProducer<String, byte[]> p = Kafka.producer(bootstrap)) {
                // Gaps are scheduled against the clock, not slept one by one, so timer granularity does not slow fast traffic.
                long due = System.nanoTime();
                while (running) {
                    for (inflight.catalog.CloudEvent e; (e = resend.poll()) != null; ) {
                        send(gated, new Events.Out(Orders.TOPIC, e.data().get("orderId").asText(), e), false);
                    }
                    double rate = controls.rate;
                    if (rate <= 0) {
                        LockSupport.parkNanos(200_000_000L);
                        due = System.nanoTime();
                        continue;
                    }
                    Events.Out o = Orders.placed(emit, rng, controls.orderVersion, Instant.now());
                    if (controls.gated && gated != null) {
                        send(gated, o, true);
                    } else {
                        p.send(o.record());
                        placed.incrementAndGet();
                        if (o.event().version() == 2) {
                            placedV2.incrementAndGet();
                        }
                    }
                    due += (long) (Math.min(-Math.log(1 - rng.next()) / rate, 5.0) * 1e9);
                    long wait = due - System.nanoTime();
                    if (wait > 0) {
                        LockSupport.parkNanos(wait);
                    } else if (wait < -1_000_000_000L) {
                        due = System.nanoTime(); // fell far behind: carry on from now, no burst
                    }
                }
            } catch (RuntimeException e) {
                log.error("traffic: {}", e.getMessage(), e);
            } finally {
                if (gated != null) {
                    gated.close();
                }
            }
        }

        /** Through the gate; a refused order is sent again as v1, which the registry has. */
        private void send(KafkaProducer<String, byte[]> gated, Events.Out o, boolean fresh) {
            gated.send(o.record(), (meta, err) -> {
                if (err == null) {
                    placed.incrementAndGet();
                    if (o.event().version() == 2) {
                        placedV2.incrementAndGet();
                    }
                } else if (err instanceof org.apache.kafka.common.InvalidRecordException
                        || err instanceof org.apache.kafka.common.errors.PolicyViolationException) {
                    if (fresh && o.event().version() == 2) {
                        refused.incrementAndGet();
                    }
                    resend.add(Orders.asV1(o.event()));
                } else {
                    log.warn("traffic: an order through the proxy failed: {}", err.getMessage());
                }
            });
        }

        void stop() throws InterruptedException {
            running = false;
            if (thread != null) {
                thread.join(10_000);
            }
        }
    }
}
