import { defineConfig, devices } from '@playwright/test';

// The setup flow in a real browser: the built jar as the app, on a workspace of its own, against the shop
// test/setup/seed.sh puts on a throwaway Kafka (localhost:9392). It also retakes the guide's setup screenshots.
//   test/setup/seed.sh && npx playwright test -c playwright.setup.config.ts && test/setup/seed.sh down
const home = process.env.INFLIGHT_SETUP_HOME ?? `${process.env.TMPDIR ?? '/tmp'}/in-flight-setup-${process.pid}`;
const run = process.env.INFLIGHT_BIN ?? 'java -jar cli/target/in-flight.jar';

export default defineConfig({
  testDir: 'test/setup',
  timeout: 180_000,
  workers: 1,
  reporter: process.env.CI ? 'github' : 'list',
  use: { baseURL: 'http://localhost:28188', ...devices['Desktop Chrome'], viewport: { width: 1440, height: 960 }, colorScheme: 'light' },
  webServer: [
    {
      command: `rm -rf ${home} && ${run} app --no-open --home ${home}`, url: 'http://localhost:28188/api/status', env: { PORT: '28188' },
      reuseExistingServer: false, stdout: 'pipe', gracefulShutdown: { signal: 'SIGINT', timeout: 10_000 },
    },
  ],
});
