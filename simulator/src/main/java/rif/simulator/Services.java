package rif.simulator;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import rif.catalog.CloudEvent;
import rif.catalog.Json;
import rif.catalog.Result;
import rif.simulator.Events.Emitter;
import rif.simulator.Events.Out;
import rif.simulator.generated.OrderTotalChanges;

/** The three services of the demo flow: what each reads, what it writes, and why it dead-letters what it cannot read. */
public final class Services {

    private Services() {}

    private static String stripPrefix(String s, String prefix) {
        return s.startsWith(prefix) ? s.substring(prefix.length()) : s;
    }

    /**
     * payments-service: charges every order placed. It reads {@code total} as a decimal string, so an OrderPlaced v2 fails
     * and is dead-lettered. Patched, it reads both shapes. About one charge in twenty is declined.
     */
    public static final class Payments {
        public static final String GROUP = "payments-service";
        public static final String DLQ = "payments.dlq";
        public static final String AUTHORIZED = "payments.authorized";
        public static final String DECLINED = "payments.declined";

        private Payments() {}

        /** What payments needs from an order to charge it. */
        public record Charge(String orderId, String customerId, BigDecimal amount, String currency) {}

        /**
         * Unpatched, payments reads an order the way it always has: {@code total} as a decimal string. Patched, it reads
         * through its generated binding for intent order-total-changes, which knows every version the catalog routes to it
         * and what each does to the order's total: the patch is the binding, regenerated and merged.
         */
        public static Result<Charge> read(CloudEvent e, boolean patched) {
            if (!Orders.isPlaced(e)) {
                return Result.err("payments cannot read " + e.ref().show());
            }
            return patched ? readByBinding(e) : readV1(e);
        }

        private static Result<Charge> readV1(CloudEvent e) {
            ObjectNode d = e.data();
            Result<String> orderId = Events.str(d, "orderId");
            if (!orderId.isOk()) {
                return Result.err(orderId.errors());
            }
            Result<String> customerId = Events.str(d, "customerId");
            if (!customerId.isOk()) {
                return Result.err(customerId.errors());
            }
            Result<JsonNode> total = Events.field(d, "total");
            if (!total.isOk()) {
                return Result.err(total.errors());
            }
            Result<BigDecimal> amount = Events.decimal(d, "total");
            if (!amount.isOk()) {
                return Result.err(amount.errors());
            }
            Result<String> currency = Events.str(d, "currency");
            if (!currency.isOk()) {
                return Result.err(currency.errors());
            }
            return Result.ok(new Charge(orderId.value(), customerId.value(), amount.value(), currency.value()));
        }

        private static Result<Charge> readByBinding(CloudEvent e) {
            return switch (OrderTotalChanges.decode(e.toJson())) {
                case OrderTotalChanges.Ok ok -> {
                    BigDecimal total = ok.event().effects().stream()
                            .filter(x -> x instanceof OrderTotalChanges.Effect.AssignOrderTotal)
                            .map(x -> ((OrderTotalChanges.Effect.AssignOrderTotal) x).value()).findFirst().orElse(null);
                    if (total == null) {
                        yield Result.err(e.ref().show() + " sets no total");
                    }
                    yield switch (ok.event()) {
                        case OrderTotalChanges.OrderPlacedV1 v1 -> Result.ok(new Charge(v1.orderId(), v1.customerId(), total, v1.currency()));
                        case OrderTotalChanges.OrderPlacedV2 v2 -> Result.ok(new Charge(v2.orderId(), v2.customerId(), total, v2.total().currency()));
                    };
                }
                case OrderTotalChanges.Undecodable.UnknownEvent u -> Result.err("payments' binding (catalog revision " + OrderTotalChanges.CATALOG_REVISION
                        + ") has no " + u.eventType() + "@v" + u.version());
                case OrderTotalChanges.Undecodable.Malformed m -> Result.err(m.reason());
            };
        }

        /** The card issuer's answer, fixed per order so a redrive decides the same way. */
        public static boolean declines(String orderId) {
            return Math.floorMod(Events.scalaStringHash(orderId), 100) < 5;
        }

        public static Result<List<Out>> handle(Emitter emit, CloudEvent e, boolean patched, Instant now, boolean redrive) {
            Result<Charge> c = read(e, patched);
            if (!c.isOk()) {
                return Result.err(c.errors());
            }
            Charge ch = c.value();
            ObjectNode data = Json.object();
            data.put("orderId", ch.orderId()).put("customerId", ch.customerId()).put("amount", Events.formatMoney(ch.amount())).put("currency", ch.currency());
            String redriveOf = redrive ? e.id() : null;
            if (declines(ch.orderId())) {
                data.put("reason", "card declined").put("declinedAt", CloudEvent.iso(now));
                return Result.ok(List.of(new Out(DECLINED, ch.orderId(), emit.emit("PaymentDeclined", 1, now, data, redriveOf))));
            }
            data.put("paymentId", "pay-" + stripPrefix(ch.orderId(), "ord-")).put("authorizedAt", CloudEvent.iso(now));
            return Result.ok(List.of(new Out(AUTHORIZED, ch.orderId(), emit.emit("PaymentAuthorized", 1, now, data, redriveOf))));
        }

        public static Result<List<Out>> handle(Emitter emit, CloudEvent e, boolean patched, Instant now) {
            return handle(emit, e, patched, now, false);
        }
    }

    /** inventory-service: reserves stock for every order placed. It reads only the items, so v2 does not bother it. */
    public static final class Inventory {
        public static final String GROUP = "inventory-service";
        public static final String DLQ = "inventory.dlq";
        public static final String RESERVED = "inventory.reserved";

        private Inventory() {}

        private record Line(String sku, int qty, BigDecimal price) {}

        public static Result<List<Out>> handle(Emitter emit, CloudEvent e, Instant now) {
            if (!Orders.isPlaced(e)) {
                return Result.err("inventory cannot read " + e.ref().show());
            }
            ObjectNode d = e.data();
            Result<String> orderId = Events.str(d, "orderId");
            if (!orderId.isOk()) {
                return Result.err(orderId.errors());
            }
            Result<String> customerId = Events.str(d, "customerId");
            if (!customerId.isOk()) {
                return Result.err(customerId.errors());
            }
            Result<JsonNode> items = Events.field(d, "items");
            if (!items.isOk()) {
                return Result.err(items.errors());
            }
            if (!items.value().isArray()) {
                return Result.err("'items' is not an array");
            }
            if (items.value().isEmpty()) {
                return Result.err("no items");
            }
            List<Line> lines = new ArrayList<>();
            for (JsonNode it : items.value()) {
                Result<String> sku = Events.str(it, "sku");
                if (!sku.isOk()) {
                    return Result.err(sku.errors());
                }
                Result<JsonNode> q = Events.field(it, "qty");
                if (!q.isOk() || !q.value().isIntegralNumber() || q.value().asLong() <= 0) {
                    return Result.err("'qty' is not a positive whole number");
                }
                Result<BigDecimal> price = Events.decimal(it, "price");
                if (!price.isOk()) {
                    return Result.err(price.errors());
                }
                lines.add(new Line(sku.value(), q.value().asInt(), price.value()));
            }
            BigDecimal value = lines.stream().map(l -> l.price().multiply(BigDecimal.valueOf(l.qty()))).reduce(BigDecimal.ZERO, BigDecimal::add);
            ObjectNode data = Json.object();
            data.put("reservationId", "res-" + stripPrefix(orderId.value(), "ord-")).put("orderId", orderId.value()).put("customerId", customerId.value());
            ArrayNode reserved = data.putArray("items");
            lines.forEach(l -> reserved.addObject().put("sku", l.sku()).put("qty", l.qty()));
            data.put("value", Events.formatMoney(value)).put("reservedAt", CloudEvent.iso(now));
            return Result.ok(List.of(new Out(RESERVED, orderId.value(), emit.emit("StockReserved", 1, now, data))));
        }
    }

    /**
     * shipping-service: dispatches an order once it has both its payment and its stock, in either order. An order that
     * never completes, such as a declined one whose stock was reserved, is dropped after {@code ttlMs}.
     */
    public static final class Shipping {
        public static final String GROUP = "shipping-service";
        public static final String DLQ = "shipping.dlq";
        public static final String DISPATCHED = "shipping.dispatched";
        public static final List<String> READS = List.of(Payments.AUTHORIZED, Inventory.RESERVED);
        private static final List<String> CARRIERS = List.of("ups", "fedex", "usps", "dhl");

        private record Waiting(String customer, BigDecimal payment, boolean stock, long since) {}

        private final Map<String, Waiting> waiting = new LinkedHashMap<>();
        private final Emitter emit;
        private final long ttlMs;

        public Shipping(Emitter emit, long ttlMs) {
            this.emit = emit;
            this.ttlMs = ttlMs;
        }

        public Shipping(Emitter emit) {
            this(emit, 30 * 60_000L);
        }

        public static String carrier(String orderId) {
            return CARRIERS.get(Math.floorMod(Events.javaHash(orderId), CARRIERS.size()));
        }

        public synchronized int pending() {
            return waiting.size();
        }

        public synchronized Result<List<Out>> handle(CloudEvent e, Instant now) {
            expire(now);
            ObjectNode d = e.data();
            Result<String> orderId = Events.str(d, "orderId");
            if (!orderId.isOk()) {
                return Result.err(orderId.errors());
            }
            Result<String> customer = Events.str(d, "customerId");
            if (!customer.isOk()) {
                return Result.err(customer.errors());
            }
            String id = orderId.value();
            Waiting w = waiting.getOrDefault(id, new Waiting(customer.value(), null, false, now.toEpochMilli()));
            if (e.type().equals("PaymentAuthorized")) {
                Result<BigDecimal> amount = Events.decimal(d, "amount");
                if (!amount.isOk()) {
                    return Result.err(amount.errors());
                }
                w = new Waiting(w.customer(), amount.value(), w.stock(), w.since());
            } else if (e.type().equals("StockReserved")) {
                w = new Waiting(w.customer(), w.payment(), true, w.since());
            } else {
                return Result.err("shipping cannot read " + e.ref().show());
            }
            if (w.payment() != null && w.stock()) {
                waiting.remove(id);
                ObjectNode data = Json.object();
                data.put("orderId", id).put("customerId", w.customer()).put("orderTotal", Events.formatMoney(w.payment()))
                        .put("carrier", carrier(id)).put("dispatchedAt", CloudEvent.iso(now));
                return Result.ok(List.of(new Out(DISPATCHED, id, emit.emit("OrderDispatched", 1, now, data))));
            }
            waiting.put(id, w);
            return Result.ok(List.of());
        }

        private void expire(Instant now) {
            long cutoff = now.toEpochMilli() - ttlMs;
            for (Iterator<Waiting> it = waiting.values().iterator(); it.hasNext(); ) {
                if (it.next().since() >= cutoff) {
                    break;
                }
                it.remove();
            }
        }
    }
}
