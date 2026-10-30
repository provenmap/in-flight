// Milestone 1: the simulator jams on OrderPlaced v2, a redrive before the patch moves nothing, and patch plus
// redrive recovers every dead letter. Runs against a Kafka with no other simulator attached.
import { setTimeout as sleep } from 'node:timers/promises';
import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import { Admin } from '../../src/kafka.js';
import { startSimulator, type Simulator } from '../../src/simulator/main.js';
import { control, settle, state } from './helpers.js';

const bootstrap = process.env.BOOTSTRAP_SERVERS ?? 'localhost:9092';
const port = 28090;

describe('the simulator', () => {
  let sim: Simulator;
  const admin = Admin.connect(bootstrap);
  const redriveOffsets = async () => [...(await admin.committed('payments-service-redrive')).entries()].sort();

  beforeAll(async () => {
    sim = await startSimulator({ bootstrap, port, rate: 20 });
    await settle(sim.url);
  });

  afterAll(async () => {
    await sim?.close();
    admin.close();
  });

  test('breaking the schema jams payments and only payments', async () => {
    const before = (await state(sim.url)).services;
    await control(sim.url, 'break');
    await sleep(4000);
    await control(sim.url, 'restore');
    await sleep(1500);
    const after = (await state(sim.url)).services;
    expect(after.payments.deadLettered - before.payments.deadLettered).toBeGreaterThan(20);
    expect(after.inventory.deadLettered).toBe(before.inventory.deadLettered);
    expect(after.inventory.processed - before.inventory.processed).toBeGreaterThan(40);
  });

  test('a redrive before the patch stops at the first unreadable record and moves no offsets', async () => {
    const offsets = await redriveOffsets();
    expect((await control(sim.url, 'redrive')).status).toBe(200);
    expect((await control(sim.url, 'redrive')).status).toBe(409);
    const r = await waitForRedrive(sim.url);
    expect(r.redriven).toBe(0);
    expect(r.remaining).toBeGreaterThan(20);
    expect(r.error).toMatch(/'total' is an object, expected a decimal string/);
    expect(await redriveOffsets()).toEqual(offsets);
  });

  test('patch, then redrive: every dead letter is redriven and the group is at the end of the queue', async () => {
    await control(sim.url, 'patch');
    expect((await control(sim.url, 'redrive')).status).toBe(200);
    const r = await waitForRedrive(sim.url);
    expect(r).toMatchObject({ remaining: 0, error: null });
    expect(r.redriven).toBeGreaterThan(20);
    const parts = await admin.partitions(['payments.dlq']);
    const ends = await admin.ends(parts);
    expect(new Map(await redriveOffsets())).toEqual(ends);
    await control(sim.url, 'unpatch');
  });
});

async function waitForRedrive(url: string) {
  for (let i = 0; i < 120; i++) {
    const r = (await state(url)).redrive;
    if (!r.running) return r;
    await sleep(500);
  }
  throw new Error('the redrive did not finish');
}
