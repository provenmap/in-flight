package rif.observer;

import java.math.BigDecimal;
import java.util.Map;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import rif.catalog.Catalog;
import rif.catalog.CloudEvent;
import rif.catalog.Json;
import rif.catalog.Pricing.Priced;
import rif.observer.Ledger.Msg;
import rif.observer.Ledger.Stats;
import rif.observer.Ledger.Stuck;

/**
 * The observer's WebSocket protocol, message for message as the TypeScript and Scala builds wrote it. Money goes on the
 * wire as a JSON number; until then it is decimal.
 */
public final class Messages {

    private Messages() {}

    static double num(BigDecimal b) {
        return b == null ? 0 : b.doubleValue();
    }

    private static final java.util.Map<String, String[]> FACTS = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Two facts about a stuck event that only the catalog knows: the intent its reader reads by, and whether the catalog
     * predicted that reader could not read this version (its impact named the reader), knew the version and foresaw no
     * trouble, or has never seen it.
     */
    static String[] facts(Catalog c, Stuck s) {
        String owner = c.ownerOfDlq(s.dlq()).map(Catalog.Consumer::name).orElse("");
        String event = s.unpriced() != null ? s.unpriced().event() : s.priced().ref().show();
        return FACTS.computeIfAbsent(c.flow() + "|" + c.revision() + "|" + owner + "|" + event, k -> {
            String intent = c.ownerOfDlq(s.dlq()).map(Catalog.Consumer::intent).orElse(null);
            var ref = rif.catalog.EventRef.parse(event);
            if (!ref.isOk() || c.semantics() == null) {
                return new String[] {intent, "unknown", event + " cannot be checked against the catalog's meaning"};
            }
            if (c.semantics().model().find(ref.value()).isEmpty()) {
                return new String[] {intent, "unseen", event + " is a version the catalog has never seen"};
            }
            var impact = rif.catalog.Impact.of(ref.value(), c.semantics(), c.readers());
            boolean named = impact.isOk() && impact.value().breaks().stream().anyMatch(b -> b.consumer().equals(owner));
            return named ? new String[] {intent, "predicted", "The catalog predicted it: the impact of " + event + " says " + owner + " must change first"}
                    : new String[] {intent, "unpredicted", "The catalog knows " + event + " and foresaw no trouble for " + owner};
        });
    }

    /** A stuck event, with what the catalog says about it. */
    public static ObjectNode stuck(Stuck s, Catalog c) {
        ObjectNode o = stuck(s);
        String[] f = facts(c, s);
        if (f[0] == null) {
            o.putNull("intent");
        } else {
            o.put("intent", f[0]);
        }
        o.put("prediction", f[1]);
        o.put("predicted", f[2]);
        return o;
    }

    public static ObjectNode msg(Msg m, Catalog c) {
        return m instanceof Ledger.Dead d ? stuck(d.stuck(), c) : msg(m);
    }

    public static ObjectNode stuck(Stuck s) {
        ObjectNode o = Json.object();
        o.put("type", "dead");
        o.put("id", s.id());
        o.put("dlq", s.dlq());
        o.put("district", s.district());
        o.put("event", s.unpriced() != null ? s.unpriced().event() : s.priced().ref().show());
        o.put("value", num(s.priced().value()));
        o.put("priced", s.unpriced() == null);
        o.put("customer", s.priced().customer());
        o.put("orderId", s.priced().orderId());
        o.put("error", s.unpriced() != null ? s.error() + " (not priced: " + s.unpriced().reason() + ")" : s.error());
        o.put("at", CloudEvent.iso(s.at()));
        return o;
    }

    public static ObjectNode msg(Msg m) {
        return switch (m) {
            case Ledger.Event e -> {
                Priced p = e.priced();
                ObjectNode o = Json.object();
                o.put("type", "event");
                o.put("id", p.eventId());
                o.put("event", p.ref().show());
                o.put("topic", p.topic());
                o.put("district", p.district());
                ArrayNode to = o.putArray("to");
                e.to().forEach(to::add);
                o.put("value", num(p.value()));
                o.put("customer", p.customer());
                o.put("orderId", p.orderId());
                o.put("redrive", p.redriveOf() != null);
                yield o;
            }
            case Ledger.Dead d -> stuck(d.stuck());
            case Ledger.Recovered r -> {
                ObjectNode o = Json.object();
                o.put("type", "recovered");
                o.put("id", r.stuck().id());
                o.put("district", r.stuck().district());
                o.put("value", num(r.stuck().priced().value()));
                if (r.afterMs() == null) {
                    o.putNull("afterMs");
                } else {
                    o.put("afterMs", r.afterMs());
                }
                yield o;
            }
        };
    }

    public static ObjectNode stats(Stats s) {
        ObjectNode o = Json.object();
        o.put("type", "stats");
        o.putObject("atRisk").put("value", num(s.atRiskValue())).put("orders", s.atRiskOrders()).put("customers", s.atRiskCustomers())
                .put("unpriced", s.stuckUnpriced());
        ObjectNode stuck = o.putObject("stuck");
        stuck.put("value", num(s.stuckValue())).put("events", s.stuckEvents()).put("customers", s.stuckCustomers()).put("unpriced", s.stuckUnpriced());
        if (s.stuckSince() == null) {
            stuck.putNull("since");
        } else {
            stuck.put("since", CloudEvent.iso(s.stuckSince()));
        }
        ObjectNode byDistrict = stuck.putObject("byDistrict");
        s.stuckByDistrict().forEach((d, v) -> byDistrict.put(d, num(v)));
        ObjectNode lag = o.putObject("lag");
        lag.put("value", num(s.lagValue())).put("events", s.lagEvents());
        ObjectNode byGroup = lag.putObject("byGroup");
        for (Map.Entry<String, Ledger.GroupLag> e : s.lag().entrySet()) {
            byGroup.putObject(e.getKey()).put("events", e.getValue().events()).put("value", num(e.getValue().value())).put("customers", e.getValue().customers());
        }
        o.putObject("recovered").put("value", num(s.recoveredValue())).put("events", s.recoveredEvents());
        if (s.lastEpisode() == null) {
            o.putNull("lastEpisode");
        } else {
            o.putObject("lastEpisode").put("value", num(s.lastEpisode().value())).put("events", s.lastEpisode().events()).put("tookMs", s.lastEpisode().tookMs());
        }
        o.put("unpriced", s.unpriced());
        ObjectNode intents = o.putObject("intents");
        s.byIntent().forEach((name, l) -> {
            ObjectNode x = intents.putObject(name);
            x.put("topic", l.topic()).put("events", l.events()).put("value", num(l.value()));
            ArrayNode gs = x.putArray("groups");
            l.groups().forEach(gs::add);
        });
        return o;
    }

    /** The map: districts, and each flow topic with where it comes from and who reads it. */
    public static ObjectNode flow(Catalog c) {
        ObjectNode o = Json.object();
        o.put("name", c.flow());
        if (c.currency() == null) {
            o.putNull("currency");
        } else {
            o.put("currency", c.currency());
        }
        ArrayNode ds = o.putArray("districts");
        c.districts().forEach(d -> ds.addObject().put("id", d.id()).put("name", d.name()).put("sink", d.sink()));
        ArrayNode ts = o.putArray("topics");
        for (String t : c.topics()) {
            ObjectNode x = ts.addObject();
            x.put("topic", t);
            String from = c.districtOf(t);
            x.put("from", from == null ? "" : from);
            ArrayNode to = x.putArray("to");
            c.readersOf(t).forEach(k -> to.add(k.district()));
            ArrayNode events = x.putArray("events");
            c.events().stream().filter(e -> e.topic().equals(t)).forEach(e -> events.add(e.ref().show()));
        }
        ArrayNode dlqs = o.putArray("dlqs");
        for (Catalog.Consumer k : c.services()) {
            if (k.dlq() == null) {
                continue;
            }
            ObjectNode q = dlqs.addObject().put("dlq", k.dlq()).put("district", k.district()).put("group", k.name());
            if (k.redrive() == null) {
                q.putNull("redrive");
            } else {
                q.put("redrive", k.redrive());
            }
        }
        ArrayNode groups = o.putArray("groups");
        c.services().forEach(k -> groups.addObject().put("group", k.group()).put("district", k.district()));
        // What each consumer cares about, in the catalog's terms: the console draws these on the map.
        ArrayNode intents = o.putArray("intents");
        if (c.semantics() != null) {
            for (rif.catalog.RoutingTable t : c.semantics().compiled()) {
                ObjectNode x = intents.addObject();
                x.put("intent", t.intent()).put("predicate", t.predicate()).put("keyedBy", t.entity()).put("topic", rif.catalog.Intent.aliasFor(t.intent()));
                ArrayNode from = x.putArray("from");
                t.sourceTopics().stream().map(c::districtOf).filter(d -> d != null).distinct().forEach(from::add);
                ArrayNode sources = x.putArray("sources");
                t.sourceTopics().forEach(sources::add);
                ArrayNode events = x.putArray("events");
                t.routes().forEach(r -> events.add(r.event().show()));
                ArrayNode to = x.putArray("to");
                c.consumersOf(t.intent()).forEach(k -> to.add(k.district()));
                ArrayNode readers = x.putArray("readers");
                c.consumersOf(t.intent()).forEach(k -> readers.add(k.name()));
            }
        }
        return o;
    }
}
