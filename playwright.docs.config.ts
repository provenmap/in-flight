import { defineConfig, devices } from '@playwright/test';

// The guide's screenshots: the built jar, on ports of its own, with a console in each mode. It runs its own
// simulator, so stop the demo first, or point BOOTSTRAP_SERVERS at another Kafka.
const bootstrap = process.env.BOOTSTRAP_SERVERS ?? 'localhost:9092';
const env = { BOOTSTRAP_SERVERS: bootstrap, CONTROL_PORT: '28090', SIMULATOR_URL: 'http://localhost:28090' };
const stop = { signal: 'SIGINT', timeout: 10_000 } as const;

export default defineConfig({
  testDir: 'test/docs',
  timeout: 300_000,
  workers: 1,
  reporter: 'list',
  use: { ...devices['Desktop Chrome'], viewport: { width: 1440, height: 960 }, colorScheme: 'light' },
  webServer: [
    { command: 'java -jar cli/target/in-flight.jar simulate', url: 'http://localhost:28090/state', env, reuseExistingServer: false, gracefulShutdown: stop },
    { command: 'java -jar cli/target/in-flight.jar serve', url: 'http://localhost:28088/api/sim/state', env: { ...env, PORT: '28088' }, reuseExistingServer: false, gracefulShutdown: stop },
    { command: 'java -jar cli/target/in-flight.jar watch catalog/order-to-cash.json', url: 'http://localhost:28089/', env: { ...env, PORT: '28089', SIMULATOR_URL: 'none' }, reuseExistingServer: false, gracefulShutdown: stop },
  ],
});
