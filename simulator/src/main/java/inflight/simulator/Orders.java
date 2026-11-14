package inflight.simulator;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import inflight.catalog.CloudEvent;
import inflight.catalog.Json;
import inflight.simulator.Events.Emitter;
import inflight.simulator.Events.Out;

/**
 * The orders team: synthetic shoppers placing orders. Version 1 writes {@code total} as a decimal string; version 2, the
 * breaking change, writes it as {@code {amount, currency}}.
 */
public final class Orders {

    private Orders() {}

    public static final String TOPIC = "orders.placed";
    public static final int CUSTOMERS = 2000;
    public static final String CURRENCY = "USD";

    /** Random numbers from a seed, so tests are repeatable. */
    public interface Rng {
        /** In [0, 1). */
        double next();
    }

    /** mulberry32: small, fast and good enough for synthetic shoppers. */
    public static Rng seeded(int seed) {
        int[] a = {seed};
        return () -> {
            a[0] += 0x6d2b79f5;
            int t = (a[0] ^ (a[0] >>> 15)) * (1 | a[0]);
            t = (t + ((t ^ (t >>> 7)) * (61 | t))) ^ t;
            return ((t ^ (t >>> 14)) & 0xffffffffL) / 4294967296.0;
        };
    }

    public static Rng unseeded() {
        return seeded(ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
    }

    static long below(Rng rng, long n) {
        return (long) Math.floor(rng.next() * n);
    }

    public record Product(String sku, BigDecimal price) {}

    private static Product product(String sku, String price) {
        return new Product(sku, new BigDecimal(price));
    }

    /** A shop's range, cheap things far more often than dear ones: weight, then product. */
    static final List<Map.Entry<Integer, Product>> RANGE = List.of(
            Map.entry(30, product("SKU-SOCKS-3PK", "12.99")),
            Map.entry(28, product("SKU-MUG-CERAMIC", "16.50")),
            Map.entry(25, product("SKU-TSHIRT", "24.00")),
            Map.entry(22, product("SKU-PHONE-CASE", "29.95")),
            Map.entry(18, product("SKU-BOOK-HC", "34.99")),
            Map.entry(15, product("SKU-SNEAKERS", "89.00")),
            Map.entry(12, product("SKU-BACKPACK", "119.00")),
            Map.entry(10, product("SKU-HEADPHONES", "149.00")),
            Map.entry(6, product("SKU-SMARTWATCH", "329.00")),
            Map.entry(4, product("SKU-ESPRESSO", "549.00")),
            Map.entry(2, product("SKU-LAPTOP-14", "1299.00")),
            Map.entry(1, product("SKU-TV-65", "1899.00")));

    private static final int TOTAL_WEIGHT = RANGE.stream().mapToInt(Map.Entry::getKey).sum();

    public static String customerId(int i) {
        return String.format("cus-%04d", i);
    }

    static Product pick(Rng rng) {
        long n = below(rng, TOTAL_WEIGHT);
        for (Map.Entry<Integer, Product> e : RANGE) {
            n -= e.getKey();
            if (n < 0) {
                return e.getValue();
            }
        }
        throw new IllegalStateException("unreachable");
    }

    /** One line of a basket. */
    record Line(Product product, int qty) {}

    /** One to four distinct lines, usually one, mostly one of each; now and then a business buys ten of something cheap. */
    static List<Line> basket(Rng rng) {
        double r = rng.next();
        int lines = r < 0.5 ? 1 : r < 0.8 ? 2 : r < 0.95 ? 3 : 4;
        Set<Product> picked = new LinkedHashSet<>();
        for (int i = 0; i < lines; i++) {
            picked.add(pick(rng));
        }
        List<Line> out = new ArrayList<>();
        for (Product p : picked) {
            double q = rng.next();
            int qty = q < 0.03 && p.price().compareTo(BigDecimal.valueOf(50)) < 0 ? 10 : q < 0.2 ? 2 + (int) below(rng, 2) : 1;
            out.add(new Line(p, qty));
        }
        return out;
    }

    public static Out placed(Emitter emit, Rng rng, int version, Instant at) {
        // 40 random bits, as ten hex digits.
        String orderId = String.format("ord-%02x%08x", below(rng, 256), below(rng, 1L << 32));
        String customer = customerId(1 + (int) below(rng, CUSTOMERS));
        List<Line> lines = basket(rng);
        BigDecimal total = lines.stream().map(l -> l.product().price().multiply(BigDecimal.valueOf(l.qty()))).reduce(BigDecimal.ZERO, BigDecimal::add);
        ArrayNode items = Json.array();
        lines.forEach(l -> items.addObject().put("sku", l.product().sku()).put("qty", l.qty()).put("price", Events.formatMoney(l.product().price())));
        ObjectNode data = Json.object();
        data.put("orderId", orderId).put("customerId", customer);
        if (version == 1) {
            data.put("total", Events.formatMoney(total)).put("currency", CURRENCY);
        } else {
            data.putObject("total").put("amount", Events.formatMoney(total)).put("currency", CURRENCY);
        }
        data.set("items", items);
        data.put("placedAt", CloudEvent.iso(at));
        return new Out(TOPIC, orderId, emit.emit("OrderPlaced", version, at, data));
    }

    /**
     * The same order in the v1 shape: what the orders team sends while v2 is held at the gate. A new event, since the
     * refused one never existed.
     */
    public static CloudEvent asV1(CloudEvent e) {
        ObjectNode d = e.data().deepCopy();
        if (d.get("total") != null && d.get("total").isObject()) {
            ObjectNode total = (ObjectNode) d.get("total");
            String amount = total.get("amount").asText();
            String currency = total.path("currency").asText(CURRENCY);
            ObjectNode v1 = Json.object();
            d.fields().forEachRemaining(f -> {
                if (f.getKey().equals("total")) {
                    v1.put("total", amount).put("currency", currency);
                } else {
                    v1.set(f.getKey(), f.getValue());
                }
            });
            d = v1;
        }
        return CloudEvent.create(e.source(), e.type(), 1, e.time(), e.sequence(), d, null);
    }

    public static boolean isPlaced(CloudEvent e) {
        return e.type().equals("OrderPlaced");
    }
}
