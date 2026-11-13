package rif.catalog;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Intents and impacts, in words for a person and as JSON for a program. The CLI and the MCP server both use these. */
public final class Describe {

    private Describe() {}

    public static final String NO_SEMANTICS =
            "This catalog does not say what its events mean. Give it \"entities\", and each event \"fields\", \"keys\" and \"effects\", to declare intents over it.";

    /** One compiled intent, in lines: what it matches, why, and what its consumers read from each match. */
    public static String table(RoutingTable table, List<Impact.Reader> consumers) {
        List<String> lines = new ArrayList<>();
        lines.add(table.intent() + ": " + table.predicate());
        lines.add("  keyed by " + table.entity() + ", from " + String.join(", ", table.sourceTopics()));
        for (RoutingTable.Route r : table.routes()) {
            lines.add("  " + r.event().show() + " on " + r.topic() + (r.reasons().isEmpty() ? "" : ": " + String.join("; ", r.reasons())));
            for (RoutingTable.Read read : r.reads()) {
                lines.add("      reads " + read.what() + " " + read.source() + " (" + read.type() + ")");
            }
        }
        if (consumers != null) {
            lines.add("  consumers: " + (consumers.isEmpty() ? "none declared" : consumers.stream().map(Impact.Reader::name).collect(Collectors.joining(", "))));
        }
        return String.join("\n", lines);
    }

    public static ObjectNode tableJson(RoutingTable table, List<Impact.Reader> consumers) {
        ObjectNode o = Json.object();
        o.put("intent", table.intent());
        o.put("predicate", table.predicate());
        o.put("keyedBy", table.entity());
        ArrayNode topics = o.putArray("topics");
        table.sourceTopics().forEach(topics::add);
        ArrayNode matches = o.putArray("matches");
        for (RoutingTable.Route r : table.routes()) {
            ObjectNode m = matches.addObject();
            m.put("event", r.event().show());
            m.put("topic", r.topic());
            m.put("key", r.keyPath().show());
            ArrayNode why = m.putArray("why");
            r.reasons().forEach(why::add);
            ArrayNode reads = m.putArray("reads");
            for (RoutingTable.Read read : r.reads()) {
                reads.addObject().put("what", read.what()).put("source", read.source()).put("type", read.type());
            }
        }
        if (consumers != null) {
            ArrayNode cs = o.putArray("consumers");
            consumers.forEach(c -> cs.add(c.name()));
        }
        return o;
    }

    public static ObjectNode impactJson(Impact impact) {
        ObjectNode o = Json.object();
        ArrayNode changes = o.putArray("changes");
        impact.changes().forEach(changes::add);
        ArrayNode must = o.putArray("mustChangeFirst");
        for (Impact.Breakage b : impact.breaks()) {
            ObjectNode m = must.addObject();
            m.put("consumer", b.consumer());
            m.put("intent", b.intent());
            m.put("event", b.event().show());
            m.put("because", b.arrival().wire);
            ArrayNode d = m.putArray("differences");
            b.differences().forEach(d::add);
            if (b.dlq() != null) {
                m.put("deadLettersTo", b.dlq());
            } else {
                m.putNull("deadLettersTo");
            }
        }
        ArrayNode intents = o.putArray("intents");
        for (Impact.IntentImpact i : impact.intents()) {
            ObjectNode x = intents.addObject();
            x.put("intent", i.intent());
            x.put("predicate", i.predicate());
            ArrayNode cs = x.putArray("consumers");
            i.consumers().forEach(c -> cs.add(c.name()));
            ArrayNode arrivals = x.putArray("arrivals");
            for (Impact.RouteArrival a : i.arrivals()) {
                ObjectNode y = arrivals.addObject();
                y.put("event", a.route().event().show());
                y.put("topic", a.route().topic());
                y.put("arrival", a.arrival().wire);
                if (a.previous() != null) {
                    y.put("comparedWith", a.previous().show());
                } else {
                    y.putNull("comparedWith");
                }
                ArrayNode d = y.putArray("differences");
                a.differences().forEach(d::add);
            }
            ArrayNode removed = x.putArray("removed");
            i.removed().forEach(r -> removed.add(r.event().show()));
        }
        ArrayNode unaffected = o.putArray("unaffected");
        impact.unaffected().forEach(unaffected::add);
        ArrayNode blocked = o.putArray("blocked");
        impact.errors().forEach(blocked::add);
        return o;
    }
}
