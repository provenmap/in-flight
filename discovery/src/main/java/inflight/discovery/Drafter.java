package inflight.discovery;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import inflight.catalog.Json;
import inflight.catalog.Records;

/**
 * Drafts a catalog from what a cluster shows. Every guess it makes is written into the catalog, so the draft is usable
 * as it is, and listed beside it with a confidence, a question for a person, and the answers to choose from, each a
 * JSON Patch that makes it so. Nothing is invented that the cluster does not show: a topic with no readable records,
 * or no amount on them, is left out or left unpriced and asked about.
 */
public final class Drafter {

    /** One answer to a question: what it says, and the patch to the draft that makes it so. */
    public record Option(String label, ArrayNode patch) {}

    /** A guess: what it is about, the question to ask, how sure the drafter is (0 to 1), and the answers. */
    public record Guess(String id, String about, String question, double confidence, String chosen, List<Option> options) {}

    public record Draft(ObjectNode catalog, List<Guess> guesses, List<String> skipped) {}

    static final Pattern MONEY_STRONG = Pattern.compile("(?i)^(grand_?)?(total|amount|value|price|order_?total|total_?amount|subtotal|revenue|sum)([._-]?(cents|minor|in_?cents|pence))?$");
    static final Pattern MONEY_WEAK = Pattern.compile("(?i)(total|amount|value|price|revenue|cost|sum|paid|charge|balance)");
    static final Pattern NOT_MONEY = Pattern.compile("(?i)(^id$|_id$|Id$|qty|quantity|count|version|time|_at$|At$|date|epoch|tax|discount|shipping|fee|rate|percent|score|index|seq|number$|zip|phone|lat|lon)");
    static final Pattern MINOR = Pattern.compile("(?i)(cents|minor|pence|in_?cents)");
    static final Pattern ORDER = Pattern.compile("(?i)^(order[._-]?id|orderid|order[._-]?number|order[._-]?ref|order)$");
    static final Pattern CUSTOMER = Pattern.compile("(?i)^(customer[._-]?id|customerid|customer|user[._-]?id|userid|account[._-]?id|accountid|client[._-]?id|buyer[._-]?id|member[._-]?id)$");
    static final Pattern CURRENCY = Pattern.compile("(?i)^(currency|currency[._-]?code|ccy|iso[._-]?currency)$");
    static final Pattern GROUP_SUFFIX = Pattern.compile("(?i)([._-](service|svc|consumer|consumers|app|worker|workers|group|processor|handler|listener))+$");

    private Drafter() {}

    /**
     * A draft of {@code flow} from the cluster, its topics' samples, and the topics to draft from (null: every topic
     * that looks like a flow and has records).
     */
    public static Draft draft(String flow, Discovery.Found found, Map<String, List<Sampler.Sampled>> samples, List<String> only) {
        List<Guess> guesses = new ArrayList<>();
        List<String> skipped = new ArrayList<>();
        ObjectNode cat = Json.object();
        cat.put("flow", flow);
        cat.put("revision", 1);

        // ---- the topics ----
        List<String> topics = new ArrayList<>();
        for (Discovery.Topic t : found.flowTopics()) {
            if (only != null && !only.isEmpty() && !only.contains(t.name())) {
                continue;
            }
            List<Sampler.Sampled> s = samples.getOrDefault(t.name(), List.of());
            if (s.isEmpty()) {
                skipped.add(t.name() + ": no records to learn its shape from");
                continue;
            }
            if (s.stream().noneMatch(x -> x.decoded().isOk())) {
                skipped.add(t.name() + ": none of its records could be read (" + s.get(0).decoded().error() + ")");
                continue;
            }
            topics.add(t.name());
        }
        if (only != null) {
            only.stream().filter(t -> found.topic(t) == null).forEach(t -> skipped.add(t + ": no such topic"));
        }

        // ---- currency: the one the records say, else asked ----
        Map<String, Integer> currencies = new TreeMap<>();
        for (String t : topics) {
            for (Sampler.Sampled s : samples.get(t)) {
                if (s.decoded().isOk()) {
                    currencyValues(s.decoded().value().data(), currencies);
                }
            }
        }
        String currency = currencies.entrySet().stream().max(Map.Entry.comparingByValue()).map(Map.Entry::getKey).orElse(null);
        cat.put("currency", currency != null ? currency : "USD");
        if (currency == null) {
            // Not every flow is money: a count, or a unit set in the catalog ('unit'), is as good an answer.
            ArrayNode count = remove("/currency");
            count.add(Json.object().put("op", "add").put("path", "/count").put("value", true));
            guesses.add(new Guess("currency", "/currency", "Which currency are the values in? No record names one, so USD is assumed. "
                    + "If the flow is not about money, count its events instead, or give the catalog a unit, such as parcels or kg.", 0.3, "USD",
                    List.of(option("USD", replace("/currency", "USD")), option("EUR", replace("/currency", "EUR")), option("GBP", replace("/currency", "GBP")),
                            option("Not money: count the events", count))));
        } else if (currencies.size() > 1) {
            guesses.add(new Guess("currency", "/currency", "The records are in " + String.join(", ", currencies.keySet()) + ". Which one should the total be counted in? "
                    + "The others need a rate under 'rates' (what one of each is worth in it), or their events are counted with their value unknown.", 0.5,
                    currency, currencies.keySet().stream().map(c -> option(c, replace("/currency", c))).toList()));
        }

        // ---- districts and events ----
        Set<String> districts = new LinkedHashSet<>();
        ArrayNode events = cat.putArray("events");
        for (String topic : topics) {
            Sampler.Shape shape = Sampler.shape(topic, samples.get(topic), false);
            String district = districtOfTopic(topic);
            districts.add(district);
            List<EventGuess> kinds = new ArrayList<>();
            if (!shape.eventTypes().isEmpty()) {
                for (String typed : shape.eventTypes().keySet()) {
                    String type = typed.contains("@v") ? typed.substring(0, typed.indexOf("@v")) : typed;
                    int version = typed.contains("@v") ? Integer.parseInt(typed.substring(typed.indexOf("@v") + 2)) : 1;
                    List<Sampler.Sampled> mine = samples.get(topic).stream().filter(s -> s.decoded().isOk() && type.equals(s.decoded().value().type())
                            && (s.decoded().value().version() == null ? 1 : s.decoded().value().version()) == version).toList();
                    kinds.add(new EventGuess(type, version, Sampler.shape(topic, mine, false), mine, true));
                }
            } else {
                kinds.add(new EventGuess(typeFromTopic(topic), 1, shape, samples.get(topic), false));
            }
            for (EventGuess k : kinds) {
                int i = events.size();
                ObjectNode e = events.addObject();
                e.put("type", k.type()).put("version", k.version()).put("topic", topic).put("district", district);
                String at = "/events/" + i;
                if (!k.named()) {
                    String alt = typeFromTopicPlain(topic);
                    List<Option> opts = new ArrayList<>(List.of(option(k.type(), replace(at + "/type", k.type()))));
                    if (!alt.equals(k.type())) {
                        opts.add(option(alt, replace(at + "/type", alt)));
                    }
                    guesses.add(new Guess("events." + i + ".type", at + "/type", "What is the event on " + topic + " called? Guessed " + k.type()
                            + " from the topic's name; its records do not say.", 0.5, k.type(), opts));
                }
                fieldGuesses(e, at, topic, k, guesses, currency);
            }
        }

        // ---- consumers: the groups reading these topics, and the dead-letter queues they write ----
        ArrayNode consumers = cat.putArray("consumers");
        Set<String> owned = new LinkedHashSet<>();
        for (Discovery.Group g : found.groups()) {
            List<String> reads = g.reads().stream().filter(topics::contains).toList();
            if (reads.isEmpty()) {
                continue;
            }
            String district = districtOfGroup(g.group());
            districts.add(district);
            int i = consumers.size();
            ObjectNode k = consumers.addObject().put("group", g.group()).put("district", district);
            ArrayNode rs = k.putArray("reads");
            reads.forEach(rs::add);
            List<Discovery.DeadLetterQueue> mine = found.dlqs().stream().filter(q -> g.group().equals(q.owner())).toList();
            if (!mine.isEmpty()) {
                Discovery.DeadLetterQueue q = mine.get(0);
                owned.add(q.topic());
                k.put("dlq", q.topic());
                k.put("redrive", g.group() + "-redrive");
                boolean byName = Discovery.stem(q.topic()).length() > 0 && g.group().toLowerCase(Locale.ROOT).startsWith(Discovery.stem(q.topic()).toLowerCase(Locale.ROOT));
                guesses.add(new Guess("consumers." + i + ".dlq", "/consumers/" + i + "/dlq", "Does " + g.group() + " dead-letter what it cannot read to "
                        + q.topic() + "? " + (byName ? "Their names match." : "Its letters came from a topic the group reads."), byName ? 0.8 : 0.6, q.topic(),
                        List.of(option("Yes, " + q.topic(), Json.array()), option("No dead-letter queue", remove("/consumers/" + i + "/dlq", "/consumers/" + i + "/redrive")))));
            }
        }
        for (Discovery.DeadLetterQueue q : found.dlqs()) {
            if (!owned.contains(q.topic()) && (q.sources().stream().anyMatch(topics::contains) || q.owner() == null)) {
                guesses.add(new Guess("dlq." + q.topic(), "/consumers", "Who writes " + q.topic() + "? No consumer group in this flow matches it, so its "
                        + "dead letters are not counted. Name the group that dead-letters to it, or leave it out.", 0.2, null, List.of(option("Leave it out", Json.array()))));
            }
        }
        if (consumers.isEmpty() && !topics.isEmpty()) {
            guesses.add(new Guess("consumers", "/consumers", "No consumer group reads these topics yet, so there is no lag and no dead-letter queue to watch: "
                    + "only the money moving. Is a consumer missing, or not running?", 0.3, null, List.of(option("Watch the money moving only", Json.array()))));
        }

        // ---- the districts, as the teams ----
        ArrayNode ds = Json.array();
        districts.forEach(d -> ds.addObject().put("id", d).put("name", title(d)));
        cat.set("districts", ds);
        if (!districts.isEmpty()) {
            guesses.add(new Guess("districts", "/districts", "Are these the teams in the flow: " + String.join(", ", districts.stream().map(Drafter::title).toList())
                    + "? They are guessed from the topics' and groups' names; rename or merge any by editing districts and each event's district.", 0.5, null,
                    List.of(option("Yes", Json.array()))));
        }

        // A reorder for the reader: districts, then events and consumers, as a person writes one.
        ObjectNode ordered = Json.object();
        for (String key : List.of("flow", "revision", "currency", "districts", "events", "consumers")) {
            if (cat.has(key)) {
                ordered.set(key, cat.get(key));
            }
        }
        guesses.sort(Comparator.comparingDouble(Guess::confidence));
        return new Draft(ordered, guesses, skipped);
    }

    private record EventGuess(String type, int version, Sampler.Shape shape, List<Sampler.Sampled> records, boolean named) {}

    /** The value, order and customer paths of one event, each a guess unless certain. */
    private static void fieldGuesses(ObjectNode e, String at, String topic, EventGuess k, List<Guess> guesses, String catalogCurrency) {
        String what = k.type() + " on " + topic;
        List<Sampler.Field> fields = k.shape().fields();

        // The amount.
        List<Sampler.Field> money = fields.stream().filter(f -> f.presence() >= 0.9 && numeric(f) && !NOT_MONEY.matcher(leaf(f.path())).find()
                && MONEY_WEAK.matcher(leaf(f.path())).find()).sorted(Comparator.comparingInt(Drafter::moneyRank)).toList();
        if (money.isEmpty()) {
            List<Sampler.Field> numbers = fields.stream().filter(f -> numeric(f) && !NOT_MONEY.matcher(leaf(f.path())).find()).toList();
            guesses.add(new Guess("events." + idx(at) + ".value", at + "/value", "Which field of " + what + " is its value? None is named like an amount"
                    + (numbers.isEmpty() ? ", and none is a number, so it is counted but not priced." : "."), numbers.isEmpty() ? 0.0 : 0.2, null,
                    numbers.stream().map(f -> option(f.path(), valuePatch(at, f))).toList()));
        } else {
            Sampler.Field best = money.get(0);
            e.put("value", best.path());
            boolean minor = MINOR.matcher(best.path()).find();
            if (minor) {
                e.put("minorUnits", true);
            }
            double conf = money.size() == 1 && MONEY_STRONG.matcher(leaf(best.path())).matches() ? 0.9 : money.size() == 1 ? 0.7 : 0.55;
            String question = money.size() == 1
                    ? "Is " + best.path() + " the value of " + what + (minor ? ", in minor units (cents)?" : "?")
                    : "Which field is the value of " + what + ": " + String.join(" or ", money.stream().map(Sampler.Field::path).toList()) + "?";
            if (!minor && best.wholeNumbers() && !best.numericText()) {
                question += " Its values are whole numbers: if they are cents, choose the minor-units answer.";
                conf = Math.min(conf, 0.6);
            }
            List<Option> opts = new ArrayList<>();
            money.forEach(f -> opts.add(option(f.path() + (MINOR.matcher(f.path()).find() ? " (minor units)" : ""), valuePatch(at, f))));
            if (!minor && best.wholeNumbers()) {
                ArrayNode p = valuePatch(at, best);
                p.add(Json.object().put("op", "add").put("path", at + "/minorUnits").put("value", true));
                opts.add(option(best.path() + " in minor units (cents)", p));
            }
            guesses.add(new Guess("events." + idx(at) + ".value", at + "/value", question, conf, best.path(), opts));
        }

        // The order and the customer: whose money it is, and which case it moves.
        key(e, at, "order", what, fields, ORDER, guesses, "an order id or reference");
        key(e, at, "customer", what, fields, CUSTOMER, guesses, "a customer, user or account id");

        // A currency field that varies: the event names its own.
        Map<String, Integer> seen = new TreeMap<>();
        String currencyPath = null;
        for (Sampler.Field f : fields) {
            if (CURRENCY.matcher(leaf(f.path())).matches() && f.types().contains("string")) {
                currencyPath = f.path();
                break;
            }
        }
        if (currencyPath != null) {
            for (Sampler.Sampled s : k.records()) {
                if (s.decoded().isOk()) {
                    JsonNode v = inflight.catalog.FieldPath.of(currencyPath).read(s.decoded().value().data()).orElse(null);
                    if (v != null && v.isTextual()) {
                        seen.merge(v.asText().toUpperCase(Locale.ROOT), 1, Integer::sum);
                    }
                }
            }
            if (seen.size() > 1 || seen.size() == 1 && !seen.containsKey(catalogCurrency)) {
                e.put("currencyField", currencyPath);
            }
        }
    }

    private static void key(ObjectNode e, String at, String role, String what, List<Sampler.Field> fields, Pattern named, List<Guess> guesses, String meaning) {
        List<Sampler.Field> exact = fields.stream().filter(f -> f.presence() >= 0.9 && named.matcher(leaf(f.path())).matches() && keyLike(f)).toList();
        List<Sampler.Field> loose = fields.stream().filter(f -> f.presence() >= 0.9 && keyLike(f) && leaf(f.path()).toLowerCase(Locale.ROOT).contains(role)).toList();
        List<Sampler.Field> pool = !exact.isEmpty() ? exact : !loose.isEmpty() ? loose
                : role.equals("order") ? fields.stream().filter(f -> f.presence() >= 0.9 && keyLike(f) && leaf(f.path()).equalsIgnoreCase("id")).toList() : List.of();
        String id = "events." + idx(at) + "." + role;
        if (pool.isEmpty()) {
            List<Sampler.Field> any = fields.stream().filter(f -> f.presence() >= 0.9 && keyLike(f)).toList();
            guesses.add(new Guess(id, at + "/" + role, "Which field of " + what + " is " + meaning + "? None is named like one; without it the event is not priced.",
                    0.0, null, any.stream().map(f -> option(f.path(), replace(at + "/" + role, f.path(), true))).toList()));
            return;
        }
        Sampler.Field best = pool.get(0);
        e.put(role, best.path());
        double conf = !exact.isEmpty() && exact.size() == 1 ? 0.9 : 0.6;
        if (conf < 0.9 || pool.size() > 1) {
            guesses.add(new Guess(id, at + "/" + role, "Is " + best.path() + " the " + role + " of " + what + "?", conf, best.path(),
                    pool.stream().map(f -> option(f.path(), replace(at + "/" + role, f.path()))).toList()));
        } else {
            guesses.add(new Guess(id, at + "/" + role, "Is " + best.path() + " the " + role + " of " + what + "?", conf, best.path(),
                    List.of(option(best.path(), Json.array()))));
        }
    }

    private static boolean numeric(Sampler.Field f) {
        Set<String> t = new LinkedHashSet<>(f.types());
        t.remove("null");
        if (t.isEmpty()) {
            return false;
        }
        return t.stream().allMatch(x -> x.equals("integer") || x.equals("number")) || t.equals(Set.of("string")) && f.numericText();
    }

    private static boolean keyLike(Sampler.Field f) {
        Set<String> t = new LinkedHashSet<>(f.types());
        t.remove("null");
        return !t.isEmpty() && t.stream().allMatch(x -> x.equals("string") || x.equals("integer"));
    }

    /** Lower is better: an exact amount name first, then the shallowest path. */
    private static int moneyRank(Sampler.Field f) {
        String leaf = leaf(f.path());
        int base = MONEY_STRONG.matcher(leaf).matches() ? 0 : 10;
        if (leaf.toLowerCase(Locale.ROOT).contains("total")) {
            base -= 2;
        }
        if (leaf.toLowerCase(Locale.ROOT).contains("sub")) {
            base += 3;
        }
        return base + f.path().split("\\.").length;
    }

    private static ArrayNode valuePatch(String at, Sampler.Field f) {
        ArrayNode p = replace(at + "/value", f.path(), true);
        if (MINOR.matcher(f.path()).find()) {
            p.add(Json.object().put("op", "add").put("path", at + "/minorUnits").put("value", true));
        } else {
            p.add(Json.object().put("op", "add").put("path", at + "/minorUnits").put("value", false));
        }
        return p;
    }

    private static int idx(String at) {
        return Integer.parseInt(at.substring(at.lastIndexOf('/') + 1));
    }

    static String leaf(String path) {
        return path.contains(".") ? path.substring(path.lastIndexOf('.') + 1) : path;
    }

    private static void currencyValues(JsonNode node, Map<String, Integer> out) {
        node.properties().forEach(e -> {
            JsonNode v = e.getValue();
            if (CURRENCY.matcher(e.getKey()).matches() && v.isTextual() && v.asText().trim().matches("(?i)[a-z]{3}")) {
                out.merge(v.asText().trim().toUpperCase(Locale.ROOT), 1, Integer::sum);
            } else if (v.isObject()) {
                currencyValues(v, out);
            }
        });
    }

    // ---- names ----------------------------------------------------------------------------------------------------

    /** The team a topic belongs to, by its first word: {@code orders.placed} is {@code orders}. */
    static String districtOfTopic(String topic) {
        String[] parts = topic.split("[._-]");
        return clean(parts.length > 1 ? parts[0] : topic);
    }

    /** The team a group belongs to: {@code payments-service} is {@code payments}. */
    static String districtOfGroup(String group) {
        String g = GROUP_SUFFIX.matcher(group).replaceAll("");
        String[] parts = g.split("[._-]");
        return clean(parts.length > 0 && !parts[0].isEmpty() ? parts[0] : g);
    }

    private static String clean(String s) {
        String c = s.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9-]", "");
        return c.isEmpty() ? "unknown" : c;
    }

    /** {@code orders.placed} is {@code OrderPlaced}: the first word singular, then each word capitalised. */
    static String typeFromTopic(String topic) {
        String[] words = topic.split("[._-]+");
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < words.length; i++) {
            String w = words[i];
            if (w.isEmpty() || w.matches("v\\d+")) {
                continue;
            }
            if (i == 0 && words.length > 1) {
                w = singular(w);
            }
            b.append(Character.toUpperCase(w.charAt(0))).append(w.substring(1));
        }
        return b.isEmpty() ? "Event" : b.toString();
    }

    /** The topic's words as they are: {@code orders.placed} is {@code OrdersPlaced}. */
    static String typeFromTopicPlain(String topic) {
        StringBuilder b = new StringBuilder();
        for (String w : topic.split("[._-]+")) {
            if (!w.isEmpty() && !w.matches("v\\d+")) {
                b.append(Character.toUpperCase(w.charAt(0))).append(w.substring(1));
            }
        }
        return b.isEmpty() ? "Event" : b.toString();
    }

    static String singular(String w) {
        if (w.endsWith("ies") && w.length() > 4) {
            return w.substring(0, w.length() - 3) + "y";
        }
        if (w.endsWith("ses") || w.endsWith("ss") || w.endsWith("us")) {
            return w.endsWith("ses") ? w.substring(0, w.length() - 2) : w;
        }
        if (w.endsWith("s") && w.length() > 3) {
            return w.substring(0, w.length() - 1);
        }
        return w;
    }

    static String title(String id) {
        if (id.isEmpty()) {
            return id;
        }
        return Character.toUpperCase(id.charAt(0)) + id.substring(1).replace('-', ' ');
    }

    // ---- patches ----------------------------------------------------------------------------------------------------

    private static Option option(String label, ArrayNode patch) {
        return new Option(label, patch);
    }

    private static ArrayNode replace(String path, String value) {
        return replace(path, value, false);
    }

    /** A patch setting one value; {@code add} works whether or not it is there yet. */
    private static ArrayNode replace(String path, String value, boolean add) {
        ArrayNode p = Json.array();
        p.add(Json.object().put("op", add ? "add" : "replace").put("path", path).put("value", value));
        return p;
    }

    private static ArrayNode remove(String... paths) {
        ArrayNode p = Json.array();
        for (String path : paths) {
            p.add(Json.object().put("op", "remove").put("path", path));
        }
        return p;
    }

    public static ObjectNode json(Draft d) {
        ObjectNode o = Json.object();
        o.set("catalog", d.catalog());
        ArrayNode gs = o.putArray("guesses");
        for (Guess g : d.guesses()) {
            ObjectNode x = gs.addObject().put("id", g.id()).put("about", g.about()).put("question", g.question()).put("confidence", g.confidence());
            String chosen = chosenLabel(g);
            if (chosen == null) {
                x.putNull("chosen");
            } else {
                x.put("chosen", chosen);
            }
            ArrayNode os = x.putArray("options");
            g.options().forEach(op -> os.addObject().put("label", op.label()).set("patch", op.patch()));
        }
        ArrayNode sk = o.putArray("skipped");
        d.skipped().forEach(sk::add);
        return o;
    }

    /** The answer the draft already holds, by its label: the value it chose may be only part of the label. */
    static String chosenLabel(Guess g) {
        if (g.chosen() == null) {
            return null;
        }
        for (Option o : g.options()) {
            if (o.label().equals(g.chosen()) || o.label().equals("Yes, " + g.chosen())) {
                return o.label();
            }
        }
        for (Option o : g.options()) {
            if (o.label().startsWith(g.chosen() + " (")) {
                return o.label();
            }
        }
        return g.chosen();
    }

    /** For the decoded records of a sample: only those that read. */
    static List<Records.Decoded> readable(List<Sampler.Sampled> s) {
        return s.stream().filter(x -> x.decoded().isOk()).map(x -> x.decoded().value()).toList();
    }

    static Map<String, Integer> counts(List<String> xs) {
        Map<String, Integer> m = new LinkedHashMap<>();
        xs.forEach(x -> m.merge(x, 1, Integer::sum));
        return m;
    }
}
