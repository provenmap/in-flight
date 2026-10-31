import { describe, expect, test } from 'vitest';
import { settingsFrom } from './kafka.js';

describe('Kafka settings', () => {
  test('RIF_KAFKA_ variables become librdkafka properties, and nothing else does', () => {
    const env = {
      RIF_KAFKA_SECURITY_PROTOCOL: 'SASL_SSL',
      RIF_KAFKA_SASL_MECHANISMS: 'PLAIN',
      RIF_KAFKA_SSL_CA_LOCATION: '/etc/ca.pem',
      RIF_KAFKA_: 'ignored',
      KAFKA_HOME: '/opt/kafka',
    };
    expect(settingsFrom(env)).toEqual({ 'security.protocol': 'SASL_SSL', 'sasl.mechanisms': 'PLAIN', 'ssl.ca.location': '/etc/ca.pem' });
  });
});
