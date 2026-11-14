package inflight.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static inflight.catalog.Fixtures.edit;
import static inflight.catalog.Fixtures.errors;
import static inflight.catalog.PricingTest.data;
import static inflight.catalog.PricingTest.event;

import java.math.BigDecimal;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;

/** Money by default; anything else a flow counts, and the words it is shown in, from its catalog. */
class DisplayTest {

    /** A flow drafted from a cluster: no roles, each event with its own paths. */
    static final String SHIPMENTS = """
            {"flow": "deliveries", "unit": "parcels",
             "districts": [{"id": "depot", "name": "Depot"}, {"id": "vans", "name": "Vans"}],
             "events": [{"type": "ShipmentBooked", "version": 1, "topic": "shipments", "district": "depot",
                         "value": "parcels", "customer": "recipientId", "order": "shipmentId"}],
             "consumers": [{"group": "vans-service", "district": "vans", "reads": ["shipments"], "dlq": "shipments.dlq"}]}
            """;

    static Catalog parse(String json) {
        Result<Catalog> r = Catalog.parse(json);
        assertTrue(r.isOk(), () -> String.join("; ", r.errors()));
        return r.value();
    }

    static Catalog edited(String json, java.util.function.Consumer<ObjectNode> f) {
        ObjectNode j = (ObjectNode) Json.parse(json).value();
        f.accept(j);
        return parse(j.toString());
    }

    @Test
    void moneyIsTheDefaultInTheCatalogsCurrencyWithTheRolesNouns() {
        Display d = Catalog.defaultCatalog().display();
        assertTrue(d.isMoney());
        assertEquals("Money at risk", d.atRisk());
        assertEquals("$1,234.50", d.amount(new BigDecimal("1234.5")));
        assertEquals("1 order", d.cases(1));
        assertEquals("3 customers", d.parties(3));
        Display eur = edit(j -> j.put("currency", "EUR")).value().display();
        assertEquals("EUR 1,234.50", eur.amount(1234.5));
    }

    @Test
    void aUnitIsShownAfterEachNumberAndNamesTheHeadline() {
        Display d = parse(SHIPMENTS).display();
        assertFalse(d.isMoney());
        assertEquals("Parcels at risk", d.atRisk());
        assertEquals("1,234 parcels", d.amount(1234));
        assertEquals("1 parcel", d.amount(1));
        assertEquals("12.5 parcels", d.amount(12.5));
        assertEquals("order", d.caseNoun(), "no roles and no nouns: the defaults");
        Display kg = edited(SHIPMENTS, j -> j.put("unit", "kg").put("measureName", "Weight")).display();
        assertEquals("Weight at risk", kg.atRisk());
        assertEquals("1 kg", kg.amount(1));
        assertEquals("0.25 kg", kg.amount(0.25));
    }

    @Test
    void nounsComeFromTheCatalogElseItsRoles() {
        Display d = edited(SHIPMENTS, j -> j.putObject("nouns").put("case", "Shipment").put("party", "recipient")).display();
        assertEquals("2 shipments", d.cases(2));
        assertEquals("1 recipient", d.parties(1));
        assertEquals("lab order", Display.noun("LabOrder"));
        assertEquals(List.of("deliveries", "batches", "parcels"), List.of(Display.plural("delivery"), Display.plural("batch"), Display.plural("parcel")));
    }

    @Test
    void countingMakesEveryEventWorthOneWhateverItsFieldsSay() {
        Catalog c = edited(SHIPMENTS, j -> {
            j.remove("unit");
            j.put("count", true);
            ((ObjectNode) j.get("events").get(0)).remove("value");
        });
        assertTrue(c.prices());
        assertEquals("Events at risk", c.display().atRisk());
        assertEquals("1 event", c.display().amount(1));
        Pricing.Priced p = new Pricing(c).price(event("ShipmentBooked", 1, data("{\"shipmentId\":\"s-1\",\"recipientId\":\"r-9\",\"parcels\":4}"))).value();
        assertEquals(BigDecimal.ONE, p.value());
        assertEquals("s-1", p.orderId());

        // A unit names what is counted; a value path is ignored, not an error.
        Catalog shipments = edited(SHIPMENTS, j -> j.put("unit", "shipments").put("count", true));
        assertEquals("3 shipments", shipments.display().amount(3));
        assertEquals(BigDecimal.ONE, new Pricing(shipments).price(event("ShipmentBooked", 1, data("{\"shipmentId\":\"s-1\",\"recipientId\":\"r-9\",\"parcels\":4}"))).value().value());
    }

    @Test
    void aCountedEventStillNeedsItsCaseAndWhoWaits() {
        ObjectNode j = (ObjectNode) Json.parse(SHIPMENTS).value();
        j.remove("unit");
        j.put("count", true);
        ((ObjectNode) j.get("events").get(0)).remove("value");
        ((ObjectNode) j.get("events").get(0)).remove("customer");
        assertEquals(List.of("ShipmentBooked@v1: declares one of 'customer' and 'order' but not both; a counted event declares both or none"),
                errors(Catalog.fromJson(j)));
    }

    @Test
    void aUnitAndACurrencyTogetherAreRefused() {
        assertEquals(List.of("catalog: give 'currency' for money or 'unit' for anything else, not both"), errors(edit(j -> j.put("unit", "parcels"))));
        assertEquals(List.of("catalog: 'count' counts events, so there is no currency: give 'unit' instead, or leave it out to count events"),
                errors(edit(j -> j.put("count", true))));
        assertEquals(List.of("catalog: 'unit' must be a word or two"), errors(edit(j -> {
            j.remove("currency");
            j.put("unit", " ");
        })));
    }

    @Test
    void theWorthSurvivesBeingWrittenOut() {
        Catalog c = edited(SHIPMENTS, j -> j.put("measureName", "Parcels").put("count", true).putObject("nouns").put("case", "shipment"));
        Catalog again = Catalog.fromJson(c.toJson()).value();
        assertEquals(c.worth(), again.worth());
        assertEquals(c.display(), again.display());
    }
}
