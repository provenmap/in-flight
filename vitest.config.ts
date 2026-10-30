import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    projects: [
      { test: { name: 'unit', include: ['src/**/*.test.ts'] } },
      // Against the Compose Kafka, one at a time: they share its topics and consumer groups.
      { test: { name: 'e2e', include: ['test/e2e/**/*.test.ts'], testTimeout: 120_000, hookTimeout: 60_000, fileParallelism: false } },
    ],
  },
});
