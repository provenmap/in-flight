package inflight.kafka;

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

import inflight.broker.Brokers;
import inflight.broker.Profile;
import inflight.broker.Secrets;

/** Profiles hold references to secrets, never secrets; presets turn them into client settings. */
class ProfileTest {

    @AfterEach
    void realEnv() {
        Secrets.useRealEnv();
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
        Map<String, String> scram = KafkaAuth.clientSettings(new Profile("s", "k:9092", "scram-sha-256", Map.of("username", "alice", "password", "env:PW"), Map.of(), null, false));
        assertEquals("SASL_SSL", scram.get("security.protocol"));
        assertEquals("SCRAM-SHA-256", scram.get("sasl.mechanism"));
        assertEquals("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"alice\" password=\"p\\\"w\";", scram.get("sasl.jaas.config"));
        Map<String, String> cloud = KafkaAuth.clientSettings(new Profile("c", "k:9092", "sasl-plain", Map.of("username", "env:PW", "password", "env:PW"), Map.of(), null, false));
        assertEquals("PLAIN", cloud.get("sasl.mechanism"));
        Map<String, String> mtls = KafkaAuth.clientSettings(new Profile("m", "k:9092", "mtls", Map.of("keystorePassword", "env:PW"), Map.of("ssl.keystore.location", "/k.p12"), null, false));
        assertEquals("SSL", mtls.get("security.protocol"));
        assertEquals("p\"w", mtls.get("ssl.keystore.password"));
        assertEquals("/k.p12", mtls.get("ssl.keystore.location"));
        assertTrue(KafkaAuth.clientSettings(new Profile("n", "k:9092", null, null, null, null, false)).isEmpty());
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> KafkaAuth.clientSettings(new Profile("i", "k:9092", "msk-iam", Map.of(), Map.of(), null, false)));
        assertTrue(e.getMessage().contains("aws-msk-iam-auth"));
    }

    @Test
    void theAdapterIsFoundByItsKindAndChecksItsPresets() {
        assertTrue(Brokers.kinds().contains("kafka"));
        assertTrue(Brokers.adapter(null) instanceof KafkaAdapter);
        Profile p = new Profile("staging", "k:9092", "scram-sha-512", Map.of("username", "alice"), Map.of(), null, false);
        assertEquals("kafka", p.broker());
        assertTrue(p.problems().contains("scram-sha-512 needs secrets.username and secrets.password"), p.problems().toString());
        assertTrue(new Profile("x", "k:9092", "kerberos", null, null, null, false).problems().get(0).startsWith("auth: one of none, ssl"));
        assertEquals(java.util.List.of("broker: one of kafka"), new Profile("x", "rabbitmq", "r:5672", "none", null, null, null, false).problems());
        assertEquals(java.util.List.of("bootstrap: the cluster's bootstrap servers, such as kafka-1:9092,kafka-2:9092"),
                new Profile("x", "", "none", null, null, null, false).problems());
    }
}
