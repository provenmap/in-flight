import { defineConfig, devices } from '@playwright/test';

// The browser test runs the simulator and the server itself, against the Kafka in BOOTSTRAP_SERVERS, on ports of its
// own so a demo on 8088 and 8090 can keep running (on a different Kafka: both would join the same consumer groups).
const bootstrap = process.env.BOOTSTRAP_SERVERS ?? 'localhost:9092';
const env = { BOOTSTRAP_SERVERS: bootstrap, CONTROL_PORT: '28090', PORT: '28088', SIMULATOR_URL: 'http://localhost:28090' };
// The built jar: ./mvnw package first. INFLIGHT_BIN overrides it, for a jar elsewhere or another JDK.
const run = process.env.INFLIGHT_BIN ?? 'java -jar cli/target/in-flight.jar';

export default defineConfig({
  testDir: 'test/browser',
  timeout: 120_000,
  workers: 1,
  reporter: process.env.CI ? 'github' : 'list',
  use: { baseURL: 'http://localhost:28088', ...devices['Desktop Chrome'], launchOptions: { args: ['--use-angle=swiftshader', '--enable-unsafe-swiftshader'] } },
  // Stopped with SIGINT, not killed, so the services leave their consumer groups and the next run need not wait out
  // the session timeout before its own services get partitions.
  webServer: [
    { command: `${run} simulate`, url: 'http://localhost:28090/state', env, reuseExistingServer: false, stdout: 'pipe', gracefulShutdown: { signal: 'SIGINT', timeout: 10_000 } },
    { command: `${run} serve`, url: 'http://localhost:28088/api/sim/state', env, reuseExistingServer: false, stdout: 'pipe', gracefulShutdown: { signal: 'SIGINT', timeout: 10_000 } },
  ],
});
