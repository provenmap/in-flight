import { setTimeout as sleep } from 'node:timers/promises';

export interface SimState {
  orderVersion: number;
  patched: boolean;
  rate: number;
  placed: number;
  services: Record<'payments' | 'inventory' | 'shipping', { processed: number; deadLettered: number }>;
  shippingWaiting: number;
  redrive: { running: boolean; redriven: number; remaining: number; error: string | null };
}

export async function state(url: string): Promise<SimState> {
  return (await fetch(`${url}/state`)).json() as Promise<SimState>;
}

export function control(url: string, action: string, query = ''): Promise<Response> {
  return fetch(`${url}/${action}${query}`, { method: 'POST' });
}

/**
 * Leaves the simulator in a known state: v1, unpatched, nothing stuck from an earlier run, and the services caught up.
 */
export async function settle(url: string): Promise<void> {
  await control(url, 'restore');
  await control(url, 'patch');
  await control(url, 'redrive');
  for (let i = 0; i < 120 && (await state(url)).redrive.running; i++) await sleep(500);
  await control(url, 'unpatch');
  await sleep(1500);
}

/** The simulator in a process of its own, as it runs in production, so it does not share an event loop with the test. */
export async function spawnSimulator(bootstrap: string, port: number): Promise<{ url: string; close(): Promise<void> }> {
  const { spawn } = await import('node:child_process');
  const child = spawn(process.execPath, ['--import', 'tsx', 'src/cli.ts', 'simulate'], {
    env: { ...process.env, BOOTSTRAP_SERVERS: bootstrap, CONTROL_PORT: String(port) },
    stdio: ['ignore', 'inherit', 'inherit'],
  });
  const exited = new Promise<void>(resolve => child.once('exit', () => resolve()));
  const url = `http://localhost:${port}`;
  for (let i = 0; ; i++) {
    if (child.exitCode !== null) throw new Error(`the simulator exited with ${child.exitCode}`);
    if (await fetch(`${url}/state`).then(r => r.ok, () => false)) break;
    if (i > 120) throw new Error('the simulator did not start');
    await sleep(250);
  }
  return {
    url,
    async close() {
      child.kill('SIGINT');
      await exited;
    },
  };
}
