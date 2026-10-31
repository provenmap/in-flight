import { describe, expect, test } from 'vitest';
import { deadLetterHeaders } from './reader.js';

describe('dead-letter headers', () => {
  test("the catalog's names come first, then this app's, Kafka Connect's and Spring Kafka's", () => {
    expect(deadLetterHeaders({ error: 'x-error' }).error).toEqual(['x-error', 'rif-error', '__connect.errors.exception.message', 'kafka_dlt-exception-message']);
    expect(deadLetterHeaders({}).sourceTopic).toEqual(['rif-source-topic', '__connect.errors.topic', 'kafka_dlt-original-topic']);
  });
});
