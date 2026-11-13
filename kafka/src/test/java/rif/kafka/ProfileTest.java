package rif.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Profiles hold references to secrets, never secrets; presets turn them into client settings. */
class ProfileTest {

    @AfterEach
    void realEnv() {
        Secrets.env = System::getenv;
    }

    @Test
    void referencesAreReadWhereTheyPoint(@TempDir Path dir) throws Exception {
        Secrets.useEnv(Map.of("KAFKA_PASSWORD", "s3cret"));
        assertEquals("s3cret", Secrets.resolve("env:KAFKA_PASSWORD"));
        Path f = dir.resolve("pw");
        Files.writeString(f, "from-a-file\n");
        assertEquals("from-a-file", Secrets.resolve("file:" + f));
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> Secrets.resolve("env:MISSING"));
        assertEquals("env:MISSING is empty or not set", e.getMessage());
        assertFalse(Secrets.isReference("hunter2"));
        assertFalse(Secrets.isReference("env:has space"));
        assertTrue(Secrets.isReference("keychain:kafka-staging/alice"));
    }

    @Test
    void aSecretGivenAsItselfIsRefusedAndNotEchoed() {
        Profile p = new Profile("staging", "k:9092", "scram-sha-512", Map.of("username", "alice", "password", "hunter2"), Map.of("sasl.jaas.config", "x"), null, false);
        String all = String.join("\n", p.problems());
        assertTrue(all.contains("secrets.password: give a reference"), all);
        assertTrue(all.contains("settings.sasl.jaas.config: a secret goes in secrets"), all);
        assertFalse(all.contains("hunter2"));
    }

    @Test
    void presetsBecomeClientSettings() {
        Secrets.useEnv(Map.of("PW", "p\"w"));
        Map<String, String> scram = new Profile("s", "k:9092", "scram-sha-256", Map.of("username", "alice", "password", "env:PW"), Map.of(), null, false).clientSettings();
        assertEquals("SASL_SSL", scram.get("security.protocol"));
        assertEquals("SCRAM-SHA-256", scram.get("sasl.mechanism"));
        assertEquals("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"alice\" password=\"p\\\"w\";", scram.get("sasl.jaas.config"));
        Map<String, String> cloud = new Profile("c", "k:9092", "sasl-plain", Map.of("username", "env:PW", "password", "env:PW"), Map.of(), null, false).clientSettings();
        assertEquals("PLAIN", cloud.get("sasl.mechanism"));
        Map<String, String> mtls = new Profile("m", "k:9092", "mtls", Map.of("keystorePassword", "env:PW"), Map.of("ssl.keystore.location", "/k.p12"), null, false).clientSettings();
        assertEquals("SSL", mtls.get("security.protocol"));
        assertEquals("p\"w", mtls.get("ssl.keystore.password"));
        assertEquals("/k.p12", mtls.get("ssl.keystore.location"));
        assertTrue(new Profile("n", "k:9092", null, null, null, null, false).clientSettings().isEmpty());
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> new Profile("i", "k:9092", "msk-iam", Map.of(), Map.of(), null, false).clientSettings());
        assertTrue(e.getMessage().contains("aws-msk-iam-auth"));
    }
}
