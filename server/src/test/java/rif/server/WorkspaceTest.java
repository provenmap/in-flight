package rif.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.node.ObjectNode;

import rif.catalog.Catalog;
import rif.catalog.Json;
import rif.kafka.Profile;

/** The workspace's files, an empty one's status, and setup refusing what it must. */
class WorkspaceTest {

    @Test
    void profilesAndTheActiveFlowRoundTripWithoutSecrets(@TempDir Path home) throws Exception {
        Workspace ws = new Workspace(home);
        assertEquals(Workspace.Config.empty(), ws.load());
        Profile p = new Profile("staging", "k:9092", "scram-sha-512", Map.of("username", "alice", "password", "env:KAFKA_PW"),
                Map.of("ssl.truststore.location", "/ca.p12"), new Profile.Registry("http://r:8081/", null, null), true);
        ws.save(ws.load().withProfile(p).withActive("staging", "checkout").withAlerts(new Workspace.Alerts("https://hooks.example/x", 500)));
        Workspace.Config c = ws.load();
        assertEquals(p, c.active());
        assertEquals("http://r:8081", c.active().registry().url());
        assertEquals("checkout", c.activeFlow());
        assertEquals(500, c.alerts().threshold());
        String file = Files.readString(ws.configFile());
        assertTrue(file.contains("env:KAFKA_PW"));
        assertEquals("rw-------", java.nio.file.attribute.PosixFilePermissions.toString(Files.getPosixFilePermissions(ws.configFile())));
        Workspace.Config gone = c.withoutProfile("staging");
        assertNull(gone.active());
        assertNull(gone.activeFlow());
    }

    @Test
    void flowsAndDraftsAreFilesOfTheirOwn(@TempDir Path home) {
        Workspace ws = new Workspace(home);
        ws.saveFlow("checkout", Catalog.defaultCatalog().toJson());
        ws.saveDraft("returns", Json.object().put("flow", "returns"));
        assertEquals(java.util.List.of("checkout"), ws.flows());
        assertTrue(ws.flow("checkout").isOk());
        assertEquals("returns", ws.draft("returns").get("flow").asText());
        ws.deleteDraft("returns");
        assertNull(ws.draft("returns"));
        assertFalse(Workspace.validName("Check Out"));
    }

    @Test
    void anEmptyWorkspaceOpensOnConnect(@TempDir Path home) {
        Engine e = Engine.app(new Workspace(home)).start();
        ObjectNode s = e.status();
        assertEquals("app", s.get("mode").asText());
        assertEquals("connect", s.get("stage").asText());
        assertTrue(s.get("profile").isNull());
        assertTrue(s.get("next").asText().startsWith("Connect to a cluster"));
        Setup setup = new Setup(e);
        assertFalse(setup.discover().ok());
        assertEquals("nothing is being watched: activate a flow first", setup.redrive("payments.dlq", null, true, 50, 0, 0).text());
    }

    @Test
    void connectRefusesASecretTypedIntoTheChat(@TempDir Path home) {
        Engine e = Engine.app(new Workspace(home)).start();
        Mcp.Tool connect = Mcp.tools(e, new Setup(e), null).stream().filter(t -> t.name().equals("connect")).findFirst().orElseThrow();
        var r = connect.handler().apply(Map.of("name", "staging", "bootstrap", "k:9092", "auth", "sasl-plain",
                "secrets", Map.of("username", "KEY", "password", "hunter2")));
        assertTrue(r.isError());
        String text = ((io.modelcontextprotocol.spec.McpSchema.TextContent) r.content().get(0)).text();
        assertTrue(text.startsWith("secrets.password looks like a secret itself"), text);
        assertFalse(text.contains("hunter2"));
        assertFalse(Files.exists(new Workspace(home).configFile()), "nothing saved");
        assertTrue(connect.annotations().readOnlyHint() == Boolean.FALSE);
        Mcp.Tool redrive = Mcp.tools(e, new Setup(e), null).stream().filter(t -> t.name().equals("redrive")).findFirst().orElseThrow();
        assertEquals(Boolean.TRUE, redrive.annotations().destructiveHint());
    }

    @Test
    void theSetupPromptWalksEveryStepInOrder() {
        String text = ((io.modelcontextprotocol.spec.McpSchema.TextContent) Mcp.setupMessages(Map.of("flow", "checkout")).messages().get(0).content()).text();
        int last = -1;
        for (String step : new String[] {"status", "connect", "discover", "sample_topic", "draft_catalog", "validate_catalog", "activate"}) {
            int at = text.indexOf(step);
            assertTrue(at > last, step + " in order");
            last = at;
        }
    }
}
