package rif.contracts;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import rif.catalog.Catalog;

/** The binding payments runs on is exactly what the generator writes for the shipped catalog: nobody edited it by hand. */
class CheckedInBindingTest {

    @Test
    void theCheckedInBindingIsWhatTheGeneratorProducesForTheShippedCatalog() throws Exception {
        ConsumerManifest payments = ConsumerManifest.loadAll(Path.of("contracts/consumers")).stream()
                .filter(m -> m.name().equals("payments-service")).findFirst().orElseThrow();
        Catalog shipped = Catalog.load(Path.of("catalog/order-to-cash.json")).value();
        String generated = BindingGenerator.generate(shipped, payments.intent(), payments.target()).value();
        assertEquals(generated, Files.readString(Path.of(payments.bindingPath())),
                "regenerate it: revenue-in-flight contracts generate --catalog catalog/order-to-cash.json");
    }
}
