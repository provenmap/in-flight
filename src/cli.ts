#!/usr/bin/env node
// revenue-in-flight: up · simulate · serve · watch · mcp · down
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { createMcpServer } from './mcp/server.js';
import { Catalog, catalogFile } from './pricing/catalog.js';
import { Observer } from './observer/observer.js';
import { startServer } from './server/server.js';
import { startSimulator } from './simulator/main.js';

const usage = `usage: revenue-in-flight <command>

  up         start Kafka (Docker Compose, KRaft, port 9092) and wait until it is healthy
  simulate   run order traffic and the payments, inventory and shipping services, control API on :8090
  serve      run the observer and the view on :8088, with MCP over HTTP on /mcp
  watch [catalog.json]
             serve with no simulator, for a flow of your own on your own Kafka: the figures, no controls
  mcp        run the MCP server over stdio, with an observer of its own
  down       stop Kafka and delete its data

The demo is three terminals: up, then simulate, then serve, and open http://localhost:8088.

environment: BOOTSTRAP_SERVERS (localhost:9092), PORT (8088), CONTROL_PORT (8090),
             SIMULATOR_URL (http://localhost:8090, or none to watch), HOST (localhost), RIF_MCP_TOKEN,
             RIF_CATALOG (the flow), RIF_KAFKA_* (librdkafka settings, RIF_KAFKA_SECURITY_PROTOCOL=SASL_SSL)`;

const compose = fileURLToPath(new URL('../docker-compose.yml', import.meta.url));

function docker(args: string[]): never {
  const res = spawnSync('docker', ['compose', '-f', compose, ...args], { stdio: 'inherit' });
  if (res.error) {
    console.error(`docker is not available: ${res.error.message}`);
    process.exit(1);
  }
  process.exit(res.status ?? 1);
}

function onSignal(close: () => Promise<void>): void {
  let closing = false;
  for (const sig of ['SIGINT', 'SIGTERM'] as const) {
    process.on(sig, () => {
      if (closing) process.exit(1);
      closing = true;
      close().then(() => process.exit(0), e => { console.error(e); process.exit(1); });
    });
  }
}

async function main(argv: string[]): Promise<void> {
  const [command] = argv;
  switch (command) {
    case 'up':
      return docker(['up', '-d', '--wait']);
    case 'down':
      return docker(['down']);
    case 'simulate': {
      const sim = await startSimulator();
      onSignal(() => sim.close());
      return;
    }
    case 'serve': {
      const server = await startServer();
      onSignal(() => server.close());
      return;
    }
    case 'watch': {
      const file = argv[1] ?? catalogFile;
      const catalog = Catalog.load(file);
      if (!catalog.ok) throw new Error(`${file} is not a usable catalog:\n  ${catalog.error.join('\n  ')}`);
      const server = await startServer({ catalog: catalog.value, simulatorUrl: null });
      onSignal(() => server.close());
      return;
    }
    case 'mcp': {
      // stdout is the protocol; everything else goes to stderr.
      console.log = console.error;
      const observer = new Observer().start();
      const mcp = createMcpServer(observer);
      await mcp.connect(new StdioServerTransport());
      const close = async () => {
        await mcp.close();
        await observer.close();
      };
      onSignal(close);
      process.stdin.on('close', () => void close().then(() => process.exit(0)));
      return;
    }
    case undefined:
    case 'help':
    case '--help':
    case '-h':
      console.log(usage);
      return;
    default:
      console.error(`unknown command '${command}'\n\n${usage}`);
      process.exit(2);
  }
}

main(process.argv.slice(2)).catch(e => {
  console.error(e instanceof Error ? e.message : e);
  process.exit(1);
});
