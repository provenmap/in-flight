package rif.server;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import rif.catalog.Json;
import rif.observer.Ledger;
import rif.observer.Observer;
import rif.recovery.Redrive;

/**
 * What is stuck, grouped by the error that stopped it, with what varies between records taken out: one row per cause,
 * where it is stuck, how much, since when, and one error as it was written. An incident is usually one or two causes.
 */
final class Causes {

    private Causes() {}

    private static final class Acc {
        final String cause;
        String example;
        final Map<String, Integer> dlqs = new LinkedHashMap<>();
        long events;
        long unpriced;
        BigDecimal value = BigDecimal.ZERO;
        Instant first;
        Instant last;

        Acc(String cause) {
            this.cause = cause;
        }
    }

    static ObjectNode of(Observer o) {
        return of(o.reader().stuck(), o.catalog().currency());
    }

    static ObjectNode of(List<Ledger.Stuck> stuck, String currency) {
        Map<String, Acc> by = new LinkedHashMap<>();
        for (Ledger.Stuck s : stuck) {
            String c = Redrive.cause(s.error());
            Acc a = by.computeIfAbsent(c, Acc::new);
            if (a.example == null) {
                a.example = s.error();
            }
            a.dlqs.merge(s.dlq(), 1, Integer::sum);
            a.events++;
            if (s.unpriced() != null) {
                a.unpriced++;
            }
            a.value = a.value.add(s.priced().valueOrZero());
            a.first = a.first == null || s.at().isBefore(a.first) ? s.at() : a.first;
            a.last = a.last == null || s.at().isAfter(a.last) ? s.at() : a.last;
        }
        ObjectNode out = Json.object();
        if (currency != null) {
            out.put("currency", currency);
        }
        ArrayNode cs = out.putArray("causes");
        by.values().stream().sorted((x, y) -> y.value.compareTo(x.value) != 0 ? y.value.compareTo(x.value) : Long.compare(y.events, x.events)).forEach(a -> {
            ObjectNode x = cs.addObject().put("cause", a.cause).put("example", a.example).put("events", a.events).put("unpriced", a.unpriced)
                    .put("value", a.value).put("first", a.first.toString()).put("last", a.last.toString());
            ObjectNode q = x.putObject("dlqs");
            a.dlqs.forEach(q::put);
        });
        return out;
    }
}
