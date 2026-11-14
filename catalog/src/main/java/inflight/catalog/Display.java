package inflight.catalog;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Locale;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * How a flow's numbers and things are named wherever they are shown: the console, the MCP answers, alerts, redrive
 * reports. Money by default (amounts in the catalog's currency, "Money at risk", orders and customers); a flow that
 * counts something else says so in its catalog, and every one of those places follows.
 *
 * @param currency the currency amounts are in when the worth is money; null for USD, or when it is not money
 * @param unit what the worth is counted in when it is not money, such as parcels, kg or GPU-hours; null for money
 * @param name what the headline calls the worth: "Money", "Parcels", "Weight"
 * @param count true when every event is worth one of the unit, whatever its fields say
 * @param caseNoun one thing in flight, lower case: order, shipment, claim
 * @param partyNoun one who waits on it, lower case: customer, recipient, patient
 */
public record Display(String currency, String unit, String name, boolean count, String caseNoun, String partyNoun) {

    /** Money in US dollars, orders and customers: what a flow is shown as when nothing says otherwise. */
    public static final Display MONEY = new Display(null, null, "Money", false, "order", "customer");

    /** Money in this currency, with the default nouns. */
    public static Display money(String currency) {
        return new Display(currency, null, "Money", false, "order", "customer");
    }

    public boolean isMoney() {
        return unit == null;
    }

    /** "$1,234.50", "EUR 1,234.50", "1,234 parcels", "1 parcel", "12.5 kg". */
    public String amount(BigDecimal v) {
        if (v == null) {
            return "unknown";
        }
        if (isMoney()) {
            String n = String.format(Locale.US, "%,.2f", v.setScale(2, RoundingMode.HALF_UP));
            return currency == null || currency.equals("USD") ? "$" + n : currency + " " + n;
        }
        BigDecimal r = v.setScale(2, RoundingMode.HALF_UP).stripTrailingZeros();
        String n = r.scale() <= 0 ? String.format(Locale.US, "%,d", r.toBigInteger()) : String.format(Locale.US, "%,." + r.scale() + "f", r);
        return n + " " + (r.compareTo(BigDecimal.ONE) == 0 ? singular(unit) : unit);
    }

    public String amount(double v) {
        return amount(BigDecimal.valueOf(v));
    }

    /** "1 order", "3 orders". */
    public String cases(long n) {
        return n + " " + (n == 1 ? caseNoun : plural(caseNoun));
    }

    /** "1 customer", "3 customers". */
    public String parties(long n) {
        return n + " " + (n == 1 ? partyNoun : plural(partyNoun));
    }

    /** "Money at risk", "Parcels at risk". */
    public String atRisk() {
        return name + " at risk";
    }

    /** What the console and the MCP answers are given, so they show the flow the same way. */
    public ObjectNode json() {
        ObjectNode o = Json.object();
        if (currency != null) {
            o.put("currency", currency);
        }
        if (unit != null) {
            o.put("unit", unit);
        }
        o.put("name", name).put("count", count).put("case", caseNoun).put("party", partyNoun);
        return o;
    }

    // ---- words -------------------------------------------------------------------------------------------------------

    /** An entity's name as a noun: LabOrder is "lab order". */
    static String noun(String entity) {
        return entity.replaceAll("([a-z0-9])([A-Z])", "$1 $2").replace('_', ' ').toLowerCase(Locale.ROOT).trim();
    }

    /** English plurals, enough for nouns: order, orders; batch, batches; delivery, deliveries. */
    static String plural(String noun) {
        if (noun.matches(".*[^aeiou]y")) {
            return noun.substring(0, noun.length() - 1) + "ies";
        }
        if (noun.matches(".*(s|x|z|ch|sh)")) {
            return noun + "es";
        }
        return noun + "s";
    }

    /** A unit for exactly one: parcels is "parcel", GPU-hours is "GPU-hour"; kg stays kg. */
    static String singular(String unit) {
        return unit.length() > 2 && unit.endsWith("s") && !unit.endsWith("ss") ? unit.substring(0, unit.length() - 1) : unit;
    }

    /** "parcels" is "Parcels": what the headline calls a unit when the catalog names it nothing else. */
    static String capitalised(String s) {
        return s.isEmpty() ? s : s.substring(0, 1).toUpperCase(Locale.ROOT) + s.substring(1);
    }
}
