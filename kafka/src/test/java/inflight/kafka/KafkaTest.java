package inflight.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import org.junit.jupiter.api.Test;

/** Ported from src/kafka.test.ts: the variables now name Java client properties rather than librdkafka ones. */
class KafkaTest {

    @Test
    void rifKafkaVariablesBecomeClientPropertiesAndNothingElseDoes() {
        Map<String, String> env = Map.of(
                "INFLIGHT_KAFKA_SECURITY_PROTOCOL", "SASL_SSL",
                "INFLIGHT_KAFKA_SASL_MECHANISM", "PLAIN",
                "INFLIGHT_KAFKA_SSL_TRUSTSTORE_LOCATION", "/etc/ca.jks",
                "INFLIGHT_KAFKA_", "ignored",
                "KAFKA_HOME", "/opt/kafka");
        assertEquals(Map.of("security.protocol", "SASL_SSL", "sasl.mechanism", "PLAIN", "ssl.truststore.location", "/etc/ca.jks"), Kafka.settingsFrom(env));
    }

    @Test
    void keysAreTopicColonPartition() {
        assertEquals("payments.dlq:2", Kafka.key("payments.dlq", 2));
        assertEquals("payments.dlq", Kafka.topicOf("payments.dlq:2"));
    }
}
