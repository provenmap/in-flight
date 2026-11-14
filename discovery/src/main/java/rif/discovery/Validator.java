package rif.discovery;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import rif.catalog.Catalog;
import rif.catalog.EventRef;
import rif.catalog.Json;
import rif.catalog.Pricing;
import rif.catalog.Records;
import rif.catalog.Result;

/**
 * Checks a catalog, then dry-runs it on sampled records: for each event, how many of its topic's records it prices,
 * what they would be worth, and for the rest the first reason each could not be priced. Nothing is read from a live
 * flow and nothing is written; the samples are all it sees.
 */
public final class Validator {

    /** One event's dry run. */
    public record EventRun(String event, String topic, int sampled, int priced, int unknownValue, BigDecimal total, BigDecimal min, BigDecimal max,
                           Map<String, Integer> problems) {}

    /** The verdict: the catalog's own errors, each event's run, the topics it names that do not exist, and whether it is usable. */
    public record Report(List<String> errors, List<EventRun> events, List<String> missingTopics, List<String> warnings, boolean ok) {}

    private Validator() {}

    public static Report validate(JsonNode catalogJson, Discovery.Found found, Map<String, List<Sampler.Sampled>> samples) {
        Result<Catalog> parsed = Catalog.fromJson(catalogJson);
        if (!parsed.isOk()) {
            return new Report(parsed.errors(), List.of(), List.of(), List.of(), false);
        }
        Catalog c = parsed.value();
        List<String> missing = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        if (found != null) {
            List<String> wanted = new ArrayList<>(c.topics());
            wanted.addAll(c.dlqs());
            wanted.stream().filter(t -> found.topic(t) == null).forEach(missing::add);
            for (Catalog.Consumer k : c.services()) {
                if (found.groups().stream().noneMatch(g -> g.group().equals(k.group()))) {
                    warnings.add("consumer group " + k.group() + " is not on the cluster: its lag reads as nothing until it commits");
                }
            }
        }
        if (!c.prices()) {
            warnings.add("no event declares a value: its worth cannot be counted, only events. Declare a value, or set count to count the events");
        }
        // One pricing per topic, so each record is priced once, by the version it fits, as the observer would.
        Map<String, EventRun> runs = new LinkedHashMap<>();
        for (Catalog.EventSpec e : c.events()) {
            runs.put(e.ref().show(), new EventRun(e.ref().show(), e.topic(), 0, 0, 0, BigDecimal.ZERO, null, null, new LinkedHashMap<>()));
        }
        for (String topic : c.topics()) {
            Pricing pricing = new Pricing(c);
            List<Sampler.Sampled> recs = samples.getOrDefault(topic, List.of());
            for (Sampler.Sampled s : recs) {
                if (!s.decoded().isOk()) {
                    String first = c.events().stream().filter(e -> e.topic().equals(topic)).findFirst().map(e -> e.ref().show()).orElse(topic);
                    runs.computeIfPresent(first, (k, r) -> bump(r, null, "unreadable: " + s.decoded().error(), true));
                    continue;
                }
                Records.Decoded d = s.decoded().value();
                Result<Pricing.Priced> p = pricing.priceDecoded(d, List.of(topic), topic + ":" + s.partition() + ":" + s.offset());
                String key = eventFor(c, topic, d, p);
                if (key == null) {
                    continue;
                }
                runs.computeIfPresent(key, (k, r) -> p.isOk() ? bump(r, p.value(), null, true) : bump(r, null, p.error(), true));
            }
        }
        List<EventRun> events = new ArrayList<>(runs.values());
        boolean ok = missing.isEmpty() || missing.size() < c.topics().size() + c.dlqs().size();
        for (EventRun r : events) {
            if (r.sampled() > 0 && r.priced() == 0) {
                ok = false;
            }
        }
        return new Report(List.of(), events, missing, warnings, ok);
    }

    /** Which declared event a record counts against: the one that priced it, else the one its type names, else its topic's first. */
    private static String eventFor(Catalog c, String topic, Records.Decoded d, Result<Pricing.Priced> p) {
        if (p.isOk()) {
            EventRef by = p.value().pricedAs() != null ? p.value().pricedAs() : p.value().ref();
            return by.show();
        }
        if (d.type() != null && d.version() != null) {
            EventRef ref = new EventRef(d.type(), d.version());
            if (c.find(ref).isPresent()) {
                return ref.show();
            }
        }
        return c.events().stream().filter(e -> e.topic().equals(topic) && (d.type() == null || e.ref().name().equals(d.type())))
                .findFirst().map(e -> e.ref().show()).orElse(null);
    }

    private static EventRun bump(EventRun r, Pricing.Priced p, String problem, boolean sampled) {
        Map<String, Integer> problems = new LinkedHashMap<>(r.problems());
        if (problem != null) {
            // The record's id varies; the reason does not. Group by the reason.
            String reason = problem.replaceAll("\\([^)]*\\)", "").replaceAll("\\s+", " ").trim();
            problems.merge(reason, 1, Integer::sum);
        }
        BigDecimal total = r.total();
        BigDecimal min = r.min();
        BigDecimal max = r.max();
        int priced = r.priced();
        int unknown = r.unknownValue();
        if (p != null) {
            priced++;
            if (p.known()) {
                total = total.add(p.value());
                min = min == null || p.value().compareTo(min) < 0 ? p.value() : min;
                max = max == null || p.value().compareTo(max) > 0 ? p.value() : max;
            } else {
                unknown++;
            }
        }
        return new EventRun(r.event(), r.topic(), r.sampled() + (sampled ? 1 : 0), priced, unknown, total, min, max, problems);
    }

    /** The report, with how the catalog shows what it counts (its currency or unit, and its nouns) when it reads. */
    public static ObjectNode json(Report r, JsonNode catalog) {
        ObjectNode o = json(r, catalog == null ? null : catalog.path("currency").asText(null));
        Result<Catalog> c = catalog == null ? null : Catalog.fromJson(catalog);
        if (c != null && c.isOk()) {
            o.set("measure", c.value().display().json());
        }
        return o;
    }

    public static ObjectNode json(Report r, String currency) {
        ObjectNode o = Json.object();
        o.put("ok", r.ok() && r.errors().isEmpty());
        ArrayNode es = o.putArray("errors");
        r.errors().forEach(es::add);
        ArrayNode mt = o.putArray("missingTopics");
        r.missingTopics().forEach(mt::add);
        ArrayNode ws = o.putArray("warnings");
        r.warnings().forEach(ws::add);
        ArrayNode evs = o.putArray("events");
        for (EventRun e : r.events()) {
            ObjectNode x = evs.addObject().put("event", e.event()).put("topic", e.topic()).put("sampled", e.sampled()).put("priced", e.priced())
                    .put("unknownValue", e.unknownValue()).put("total", e.total());
            if (e.min() != null) {
                x.put("min", e.min()).put("max", e.max());
            }
            ObjectNode ps = x.putObject("problems");
            e.problems().forEach(ps::put);
        }
        if (currency != null) {
            o.put("currency", currency);
        }
        return o;
    }

    /** The verdict in a few lines for a person. */
    public static String text(Report r) {
        if (!r.errors().isEmpty()) {
            return "The catalog is not usable:\n  " + String.join("\n  ", r.errors());
        }
        StringBuilder b = new StringBuilder(r.ok() ? "The catalog checks out on the sampled records." : "The catalog loads, but does not price what was sampled.");
        for (EventRun e : r.events()) {
            b.append("\n  ").append(e.event()).append(" on ").append(e.topic()).append(": ");
            if (e.sampled() == 0) {
                b.append("no records sampled");
                continue;
            }
            b.append(e.priced()).append(" of ").append(e.sampled()).append(" priced");
            if (e.min() != null) {
                b.append(", ").append(e.min().stripTrailingZeros().toPlainString()).append(" to ").append(e.max().stripTrailingZeros().toPlainString())
                        .append(" each");
            }
            if (!e.problems().isEmpty()) {
                Map.Entry<String, Integer> first = e.problems().entrySet().iterator().next();
                b.append("; ").append(first.getValue()).append(" not: ").append(first.getKey());
            }
        }
        r.missingTopics().forEach(t -> b.append("\n  topic ").append(t).append(" does not exist yet: it will be read when it does"));
        r.warnings().forEach(w -> b.append("\n  ").append(w));
        return b.toString();
    }
}
