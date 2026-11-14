package inflight.catalog;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Writes a catalog back out in the file format {@link CatalogReader} reads, so a proposal can be stored and sent. */
final class CatalogWriter {

    private CatalogWriter() {}

    static JsonNode write(Catalog c) {
        ObjectNode out = Json.object();
        out.put("flow", c.flow());
        out.put("revision", c.revision());
        if (c.currency() != null) {
            out.put("currency", c.currency());
        }
        Catalog.Worth w = c.worth();
        if (w.unit() != null) {
            out.put("unit", w.unit());
        }
        if (w.name() != null) {
            out.put("measureName", w.name());
        }
        if (w.count()) {
            out.put("count", true);
        }
        if (w.caseNoun() != null || w.partyNoun() != null) {
            ObjectNode n = out.putObject("nouns");
            if (w.caseNoun() != null) {
                n.put("case", w.caseNoun());
            }
            if (w.partyNoun() != null) {
                n.put("party", w.partyNoun());
            }
        }
        if (c.roles() != null) {
            ObjectNode r = out.putObject("roles");
            if (c.roles().caseEntity() != null) {
                r.put("case", c.roles().caseEntity());
            }
            if (c.roles().party() != null) {
                r.put("party", c.roles().party());
            }
            if (c.roles().measure() != null) {
                r.put("measure", c.roles().measure().show());
            }
        }
        ArrayNode ds = out.putArray("districts");
        for (Catalog.District d : c.districts()) {
            ObjectNode o = ds.addObject().put("id", d.id()).put("name", d.name());
            if (d.sink()) {
                o.put("sink", true);
            }
        }
        Semantics s = c.semantics();
        if (s != null) {
            ObjectNode es = out.putObject("entities");
            s.model().entities().forEach((name, attrs) -> es.set(name, FieldType.fieldsJson(attrs)));
        }
        ArrayNode evs = out.putArray("events");
        for (Catalog.EventSpec e : c.events()) {
            ObjectNode o = evs.addObject();
            o.put("type", e.ref().name()).put("version", e.ref().version()).put("topic", e.topic()).put("district", e.district());
            if (e.value() != null) {
                o.put("value", e.value().show());
            }
            if (e.customer() != null) {
                o.put("customer", e.customer().show());
            }
            if (e.order() != null) {
                o.put("order", e.order().show());
            }
            if (e.amount().minorUnits()) {
                o.put("minorUnits", true);
            }
            if (e.amount().currency() != null) {
                o.put("currency", e.amount().currency());
            }
            if (e.amount().currencyField() != null) {
                o.put("currencyField", e.amount().currencyField().show());
            }
            c.meaning(e.ref()).ifPresent(m -> {
                o.set("fields", FieldType.fieldsJson(m.fields()));
                ObjectNode keys = o.putObject("keys");
                m.keys().forEach((entity, path) -> keys.put(entity, path.show()));
                ArrayNode effects = o.putArray("effects");
                for (Effect eff : m.effects()) {
                    ObjectNode x = effects.addObject().put("op", eff.op().show()).put("attribute", eff.attribute().show());
                    switch (eff.source()) {
                        case Effect.FromField f -> x.put("from", f.field().show());
                        case Effect.Const k -> x.set("value", k.value());
                    }
                }
            });
        }
        if (s != null && !s.intents().isEmpty()) {
            ArrayNode is = out.putArray("intents");
            for (Intent i : s.intents()) {
                ObjectNode o = is.addObject().put("name", i.name()).put("predicate", i.predicate().show());
                if (i.by() != null) {
                    o.put("by", i.by());
                }
            }
        }
        ArrayNode ks = out.putArray("consumers");
        for (Catalog.Consumer k : c.consumers()) {
            ObjectNode o = ks.addObject();
            if (k.group() != null) {
                o.put("group", k.group());
            }
            o.put("district", k.district());
            ArrayNode reads = o.putArray("reads");
            k.reads().forEach(reads::add);
            if (k.dlq() != null) {
                o.put("dlq", k.dlq());
            }
            if (k.redrive() != null) {
                o.put("redrive", k.redrive());
            }
            if (k.intent() != null) {
                o.put("intent", k.intent());
            }
        }
        if (!c.rates().isEmpty()) {
            ObjectNode rates = out.putObject("rates");
            c.rates().forEach(rates::put);
        }
        if (!c.deadLetterHeaders().isEmpty()) {
            ObjectNode h = out.putObject("deadLetterHeaders");
            if (c.deadLetterHeaders().error() != null) {
                h.put("error", c.deadLetterHeaders().error());
            }
            if (c.deadLetterHeaders().failedAt() != null) {
                h.put("failedAt", c.deadLetterHeaders().failedAt());
            }
            if (c.deadLetterHeaders().sourceTopic() != null) {
                h.put("sourceTopic", c.deadLetterHeaders().sourceTopic());
            }
        }
        return out;
    }
}
