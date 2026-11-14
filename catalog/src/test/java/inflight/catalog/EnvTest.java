package inflight.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import org.junit.jupiter.api.Test;

/** Settings made before the rename from revenue-in-flight still work. */
class EnvTest {

    @Test
    void anOldVariableIsReadUnderItsNewNameUnlessThatIsSet() {
        Map<String, String> env = Env.withOldNames(Map.of("RIF_HOME", "/old", "RIF_TOKEN", "old", "INFLIGHT_TOKEN", "new", "PATH", "/bin"));
        assertEquals("/old", env.get("INFLIGHT_HOME"));
        assertEquals("new", env.get("INFLIGHT_TOKEN"));
        assertEquals("/bin", env.get("PATH"));
    }
}
