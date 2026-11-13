package rif.catalog;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;

import rif.catalog.Catalog.Consumer;
import rif.catalog.Catalog.DeadLetterHeaders;
import rif.catalog.Catalog.District;
import rif.catalog.Catalog.EventSpec;
import rif.catalog.Catalog.Roles;

/**
 * Reads the catalog file. Every problem is reported, not just the first, and each names the place: {@code events[1]}
 * for a field of the second event, {@code catalog} for a top-level one.
 *
 * <p>It reads both lines of descent: this repository's catalogs, and intent-bus's, whose events name a {@code producer}
 * where these name a {@code district}, and which declare no districts, consumers or currency.
 */
final class CatalogReader {

    private CatalogReader() {}

    private static final class Issues {
        final List<String> list = new ArrayList<>();

        void add(String where, String msg) {
            list.add(where == null ? msg : where + ": " + msg);
        }
    }

    static Result<Catalog> read(JsonNode json) {
        if (!Json.isObject(json)) {
            return Result.err("catalog: must be a JSON object");
        }
        Issues issues = new Issues();

        String flow = "flow";
        JsonNode flowNode = json.get("flow");
        if (flowNode != null) {
            if (flowNode.isTextual()) {
                flow = flowNode.asText();
            } else {
                issues.add("catalog", "'flow' must be a string");
            }
        }
        int revision = 1;
        JsonNode rev = json.get("revision");
        if (rev != null) {
            if (rev.isIntegralNumber()) {
                revision = rev.asInt();
            } else {
                issues.add("catalog", "'revision' must be a whole number");
            }
        }
        String currency = null;
        JsonNode cur = json.get("currency");
        if (cur != null) {
            if (cur.isTextual()) {
                currency = cur.asText();
            } else {
                issues.add("catalog", "'currency' must be a string");
            }
        }

        Roles roles = null;
        JsonNode r = json.get("roles");
        if (r != null) {
            if (!r.isObject()) {
                issues.add("catalog", "'roles' must be an object of case, party and measure");
            } else {
                AttrRef measure = null;
                String m = Json.text(r, "measure");
                if (m != null) {
                    Result<AttrRef> a = AttrRef.parse(m);
                    if (a.isOk()) {
                        measure = a.value();
                    } else {
                        issues.add("roles", "measure: " + a.error());
                    }
                }
                roles = new Roles(Json.text(r, "case"), Json.text(r, "party"), measure);
            }
        }

        // ---- events ----
        List<EventSpec> events = new ArrayList<>();
        List<Model.RawEvent> raw = new ArrayList<>();
        JsonNode evs = json.get("events");
        if (evs == null || !evs.isArray()) {
            issues.add(null, "'events' must be an array");
        } else {
            for (int i = 0; i < evs.size(); i++) {
                JsonNode e = evs.get(i);
                String where = "events[" + i + "]";
                if (!e.isObject()) {
                    issues.add(where, "must be an object");
                    continue;
                }
                int before = issues.list.size();
                String type = str(e, "type", where, issues);
                JsonNode v = e.get("version");
                int version = 0;
                if (v == null || !v.isIntegralNumber()) {
                    issues.add(where, "'version' must be a whole number");
                } else {
                    version = v.asInt();
                }
                String topic = str(e, "topic", where, issues);
                String district;
                if (e.has("district") || !e.has("producer")) {
                    district = str(e, "district", where, issues);
                } else {
                    district = str(e, "producer", where, issues); // intent-bus's name for it
                }
                FieldPath value = path(e, "value", where, issues);
                FieldPath customer = path(e, "customer", where, issues);
                FieldPath order = path(e, "order", where, issues);
                Catalog.Amount amount = amount(e, where, issues);
                if (issues.list.size() > before) {
                    continue;
                }
                EventRef ref = new EventRef(type, version);
                events.add(new EventSpec(ref, topic, district, value, customer, order, amount));
                raw.add(new Model.RawEvent(ref, topic, district, e.get("fields"), e.get("keys"), e.get("effects")));
            }
        }

        // ---- districts: declared, or else every district the events and consumers name ----
        List<District> districts = new ArrayList<>();
        JsonNode ds = json.get("districts");
        boolean declaredDistricts = ds != null;
        if (ds != null && !ds.isArray()) {
            issues.add(null, "'districts' must be an array");
        } else if (ds != null) {
            for (int i = 0; i < ds.size(); i++) {
                JsonNode d = ds.get(i);
                String where = "districts[" + i + "]";
                if (!d.isObject()) {
                    issues.add(where, "must be an object");
                    continue;
                }
                int before = issues.list.size();
                String id = str(d, "id", where, issues);
                String name = str(d, "name", where, issues);
                if (issues.list.size() == before) {
                    districts.add(new District(id, name, d.path("sink").asBoolean(false) && d.get("sink").isBoolean()));
                }
            }
        }

        // ---- consumers ----
        List<RawConsumer> rawConsumers = new ArrayList<>();
        JsonNode ks = json.get("consumers");
        if (ks != null && !ks.isArray()) {
            issues.add(null, "'consumers' must be an array");
        } else if (ks != null) {
            for (int i = 0; i < ks.size(); i++) {
                JsonNode k = ks.get(i);
                String where = "consumers[" + i + "]";
                if (!k.isObject()) {
                    issues.add(where, "must be an object");
                    continue;
                }
                int before = issues.list.size();
                String district = str(k, "district", where, issues);
                if (issues.list.size() > before) {
                    continue;
                }
                List<String> reads = new ArrayList<>();
                JsonNode rs = k.get("reads");
                if (rs != null && rs.isArray()) {
                    rs.forEach(t -> {
                        if (t.isTextual()) {
                            reads.add(t.asText());
                        }
                    });
                }
                rawConsumers.add(new RawConsumer(Json.text(k, "group"), district, reads, Json.text(k, "dlq"), Json.text(k, "redrive"), Json.text(k, "intent")));
            }
        }

        DeadLetterHeaders headers = DeadLetterHeaders.NONE;
        JsonNode h = json.get("deadLetterHeaders");
        if (h != null) {
            if (h.isObject()) {
                headers = new DeadLetterHeaders(Json.text(h, "error"), Json.text(h, "failedAt"), Json.text(h, "sourceTopic"));
            } else {
                issues.add("catalog", "'deadLetterHeaders' must be an object");
            }
        }

        Map<String, java.math.BigDecimal> rates = new java.util.TreeMap<>();
        JsonNode rs = json.get("rates");
        if (rs != null) {
            if (!rs.isObject()) {
                issues.add("catalog", "'rates' must be an object of currency codes to what one of each is worth in the catalog's currency");
            } else {
                rs.properties().forEach(en -> {
                    java.math.BigDecimal rate = Pricing.money(en.getValue());
                    if (rate == null || rate.signum() <= 0) {
                        issues.add("rates", en.getKey() + ": must be a positive number");
                    } else {
                        rates.put(en.getKey(), rate);
                    }
                });
            }
        }

        if (!issues.list.isEmpty()) {
            return Result.err(issues.list);
        }

        if (!declaredDistricts) {
            Set<String> seen = new LinkedHashSet<>();
            events.forEach(e -> seen.add(e.district()));
            rawConsumers.forEach(k -> seen.add(k.district()));
            seen.forEach(id -> districts.add(new District(id, id, false)));
        }

        // ---- meaning ----
        Result<Semantics> semantics = Semantics.parse(new Semantics.Raw(json.get("entities"), json.get("intents"), raw), revision);
        if (!semantics.isOk()) {
            return Result.err(semantics.errors());
        }
        Semantics s = semantics.orElse(null);

        List<String> byIntent = new ArrayList<>();
        List<Consumer> consumers = new ArrayList<>();
        for (RawConsumer k : rawConsumers) {
            String name = k.group() != null ? k.group() : k.district();
            if (k.intent() == null) {
                consumers.add(new Consumer(k.group(), k.district(), k.reads(), k.dlq(), k.redrive(), null, name));
                continue;
            }
            RoutingTable table = s == null ? null : s.table(k.intent());
            if (table == null) {
                byIntent.add("consumer " + name + ": reads by intent '" + k.intent() + "', which the catalog does not declare");
                consumers.add(new Consumer(k.group(), k.district(), k.reads(), k.dlq(), k.redrive(), k.intent(), name));
                continue;
            }
            // Subscribe by intent: with no topics listed, the consumer reads the topics its intent draws on.
            if (k.reads().isEmpty()) {
                consumers.add(new Consumer(k.group(), k.district(), table.sourceTopics(), k.dlq(), k.redrive(), k.intent(), name));
                continue;
            }
            for (String t : table.sourceTopics()) {
                if (!k.reads().contains(t)) {
                    byIntent.add("consumer " + name + ": intent '" + k.intent() + "' draws on topic '" + t + "', which it does not read");
                }
            }
            consumers.add(new Consumer(k.group(), k.district(), k.reads(), k.dlq(), k.redrive(), k.intent(), name));
        }

        Catalog catalog = new Catalog(flow, revision, currency, districts, events, consumers, headers, roles, s, rates);
        List<String> errors = new ArrayList<>(byIntent);
        errors.addAll(Catalog.validate(catalog));
        return errors.isEmpty() ? Result.ok(catalog) : Result.err(errors);
    }

    private record RawConsumer(String group, String district, List<String> reads, String dlq, String redrive, String intent) {}

    private static String str(JsonNode obj, String key, String where, Issues issues) {
        JsonNode v = obj.get(key);
        if (v == null || !v.isTextual()) {
            issues.add(where, "'" + key + "' must be a string");
            return null;
        }
        return v.asText();
    }

    /** {@code minorUnits}, {@code currency} and {@code currencyField}, all optional. */
    private static Catalog.Amount amount(JsonNode e, String where, Issues issues) {
        boolean minor = false;
        JsonNode m = e.get("minorUnits");
        if (m != null) {
            if (m.isBoolean()) {
                minor = m.asBoolean();
            } else {
                issues.add(where, "'minorUnits' must be true or false");
            }
        }
        String currency = null;
        JsonNode c = e.get("currency");
        if (c != null) {
            if (c.isTextual() && c.asText().matches("[A-Z]{3}")) {
                currency = c.asText();
            } else {
                issues.add(where, "'currency' must be a three-letter currency code, such as EUR");
            }
        }
        FieldPath field = path(e, "currencyField", where, issues);
        if (currency != null && field != null) {
            issues.add(where, "give 'currency' or 'currencyField', not both");
        }
        return new Catalog.Amount(minor, currency, field);
    }

    /** An optional dotted path. */
    private static FieldPath path(JsonNode obj, String key, String where, Issues issues) {
        JsonNode v = obj.get(key);
        if (v == null || v.isNull()) {
            return null;
        }
        if (!v.isTextual()) {
            issues.add(where, "'" + key + "' must be a string");
            return null;
        }
        Result<FieldPath> p = FieldPath.parse(v.asText());
        if (!p.isOk()) {
            issues.add(where, key + ": " + p.error());
            return null;
        }
        return p.value();
    }
}
