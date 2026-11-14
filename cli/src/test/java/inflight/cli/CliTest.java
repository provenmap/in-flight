package inflight.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import inflight.catalog.Catalog;
import inflight.catalog.EventRef;
import inflight.catalog.Json;

/** The commands that need no Kafka: impact for a pull request, the rehearsal protocol, the setup flags. */
class CliTest {

    static String out(java.util.concurrent.Callable<Integer> run, int[] code) throws Exception {
        PrintStream was = System.out;
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        System.setOut(new PrintStream(b, true, java.nio.charset.StandardCharsets.UTF_8));
        try {
            code[0] = run.call();
        } finally {
            System.setOut(was);
        }
        return b.toString(java.nio.charset.StandardCharsets.UTF_8);
    }

    @Test
    void impactSinceChecksEveryVersionAPullRequestAdds(@TempDir Path dir) throws Exception {
        Path head = dir.resolve("head.json");
        Path base = dir.resolve("base.json");
        Files.writeString(head, Json.pretty(Catalog.defaultCatalog().toJson()));
        Files.writeString(base, Json.pretty(Catalog.defaultCatalog().withoutEvent(new EventRef("OrderPlaced", 2)).value().toJson()));
        int[] code = {0};
        String text = out(() -> ImpactSince.run(new String[] {base.toString(), head.toString()}), code);
        assertEquals(1, code[0], text);
        assertTrue(text.startsWith("Not safe to ship yet: 1 new event version (OrderPlaced@v2)."), text);
        assertTrue(text.contains("payments-service must change"), text);
        String none = out(() -> ImpactSince.run(new String[] {head.toString(), head.toString()}), code);
        assertEquals(0, code[0]);
        assertTrue(none.startsWith("No new event versions"), none);
    }

    @Test
    void rehearsalCountsResultsAndComparesLinesAsJson() {
        assertEquals("dead-letter", Rehearse.result("{\"result\":\"dead-letter\",\"why\":\"x\"}"));
        assertEquals("plain text", Rehearse.result("plain text"));
        assertTrue(Rehearse.same("{\"a\":1,\"b\":2}", "{\"b\":2, \"a\":1}"));
        assertFalse(Rehearse.same("{\"a\":1}", "{\"a\":2}"));
        assertEquals(java.util.List.of("docker", "run", "--rm", "-i", "img:1"), Rehearse.command(null, "img:1"));
    }

    @Test
    void setupFlagsTakeValuesAndRepeats() {
        Setups.Args a = Setups.Args.of(new String[] {"staging", "k:9092", "--auth", "scram-sha-512", "--secret", "username=alice", "--secret=password=env:PW",
                "--allow-writes"}, "auth", "secret");
        assertEquals(java.util.List.of("staging", "k:9092"), a.positional());
        assertEquals("scram-sha-512", a.one("auth"));
        assertEquals(java.util.List.of("username=alice", "password=env:PW"), a.all("secret"));
        assertTrue(a.has("allow-writes"));
    }
}
