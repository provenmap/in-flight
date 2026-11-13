// The simulator's control API, as the browser tests reach it before they open the console.
import { setTimeout as sleep } from 'node:timers/promises';

export async function state(url: string): Promise<{ redrive: { running: boolean } }> {
  return (await fetch(`${url}/state`)).json() as Promise<{ redrive: { running: boolean } }>;
}

export function control(url: string, action: string, query = ''): Promise<Response> {
  return fetch(`${url}/${action}${query}`, { method: 'POST' });
}

/** Leaves the simulator in a known state: v1, unpatched, nothing stuck from an earlier run, and the services caught up. */
export async function settle(url: string): Promise<void> {
  await control(url, 'restore');
  await control(url, 'patch');
  await control(url, 'redrive');
  for (let i = 0; i < 120 && (await state(url)).redrive.running; i++) await sleep(500);
  await control(url, 'unpatch');
  await sleep(1500);
}
