/// <reference lib="dom" />
// The console, end to end in a real browser: it connects and money flows, breaking the schema raises the counter
// and fills the dead-letter table, patching and redriving drain it to $0, the details open, and nothing scrolls
// sideways at phone width. It reads `window.rif` and the `data-testid`s, never pixels.
import { expect, test, type Page } from '@playwright/test';
import { settle } from './helpers.js';

const sim = 'http://localhost:28090';

function watchErrors(page: Page): string[] {
  const errors: string[] = [];
  page.on('pageerror', e => errors.push(e.message));
  page.on('console', m => { if (m.type() === 'error') errors.push(m.text()); });
  return errors;
}

const rif = <T>(page: Page, expr: string) => page.evaluate(`window.rif && (${expr})`) as Promise<T>;

async function open(page: Page) {
  await page.goto('/');
  await expect(page.getByTestId('connection')).toContainText('Live', { timeout: 30_000 });
}

test.beforeAll(async () => {
  await settle(sim);
});

test('the console connects and money flows', async ({ page }) => {
  const errors = watchErrors(page);
  await open(page);
  await expect.poll(() => rif<string>(page, 'window.rif.view')).toBe('map');
  await expect.poll(() => rif<number>(page, 'window.rif.frames'), { timeout: 15_000 }).toBeGreaterThan(10);
  await expect.poll(() => rif<number>(page, 'window.rif.particles'), { timeout: 15_000 }).toBeGreaterThan(0);
  for (const id of ['at-risk', 'at-risk-sub', 'stuck', 'lag', 'recovered', 'hint', 'break', 'patch', 'redrive', 'rate', 'sim-state', 'connection']) {
    await expect(page.getByTestId(id)).toHaveCount(1);
  }
  await expect(page.getByTestId('stuck')).toHaveText('Nothing stuck');
  expect(errors).toEqual([]);
});

test('break raises the counter and fills the dead letters; patch and redrive drain it to $0', async ({ page }) => {
  const errors = watchErrors(page);
  await open(page);
  await expect(page.getByTestId('break')).toBeEnabled({ timeout: 15_000 });

  await page.getByTestId('break').click();
  await expect(page.getByTestId('stuck')).toContainText(/\$[\d,.]+ for \d+ s/, { timeout: 15_000 });
  await expect.poll(() => rif<number>(page, 'window.rif.blocks'), { timeout: 15_000 }).toBeGreaterThan(5);
  await expect(page.getByTestId('at-risk')).not.toHaveText('$0.00');
  await expect(page.getByTestId('hint')).toContainText('Patch payments');
  await expect(page.getByTestId('dead-letters').locator('tbody tr').first()).toContainText("'total' is an object, expected a decimal string");
  await expect(page.getByTestId('activity')).toContainText('Orders team shipped OrderPlaced v2');

  await page.getByTestId('patch').click();
  await expect(page.getByTestId('hint')).toContainText('Redrive the DLQ', { timeout: 15_000 });
  await page.getByTestId('redrive').click();
  await expect(page.getByTestId('at-risk')).toHaveText('$0.00', { timeout: 60_000 });
  await expect(page.getByTestId('recovered')).toBeVisible({ timeout: 15_000 });
  await expect(page.getByTestId('recovered')).toContainText('Recovered $');
  await expect(page.getByTestId('activity')).toContainText('Recovered $');
  await expect.poll(() => rif<number>(page, 'window.rif.blocks'), { timeout: 30_000 }).toBe(0);
  await page.request.post(`${sim}/restore`);
  await page.request.post(`${sim}/unpatch`);
  expect(errors).toEqual([]);
});

test('a district opens its details, and every tab has its table', async ({ page }) => {
  const errors = watchErrors(page);
  await open(page);
  await page.locator('[data-district=payments]').click();
  const sheet = page.getByRole('dialog');
  await expect(sheet).toContainText('payments-service');
  await expect(sheet).toContainText('payments.dlq');
  await page.keyboard.press('Escape');
  await expect(sheet).toBeHidden();

  await page.getByRole('tab', { name: 'Consumer groups' }).click();
  await expect(page.getByTestId('groups').locator('tbody tr')).toHaveCount(3);
  await page.getByRole('tab', { name: 'Throughput' }).click();
  await expect(page.getByTestId('throughput').locator('tbody tr')).toHaveCount(5);
  await page.getByRole('tab', { name: 'Live events' }).click();
  await expect.poll(() => page.getByTestId('live-events').locator('tbody tr').count(), { timeout: 15_000 }).toBeGreaterThan(0);
  expect(errors).toEqual([]);
});

test('the theme follows the toggle and is remembered', async ({ page }) => {
  await open(page);
  const dark = () => page.evaluate(() => document.documentElement.classList.contains('dark'));
  const before = await dark();
  await page.getByRole('button', { name: before ? 'Use the light theme' : 'Use the dark theme' }).click();
  expect(await dark()).toBe(!before);
  await page.reload();
  expect(await dark()).toBe(!before);
});

test('at phone width nothing scrolls sideways, and the runbook is in reach', async ({ page }) => {
  const errors = watchErrors(page);
  await page.setViewportSize({ width: 390, height: 844 });
  await open(page);
  const [scroll, width] = await page.evaluate(() => [document.documentElement.scrollWidth, window.innerWidth]);
  expect(scroll).toBeLessThanOrEqual(width);
  await expect(page.getByTestId('hint')).toBeInViewport();
  expect(errors).toEqual([]);
});

test('the intents view compiles a predicate as you type, lights its matches up, and names what the catalog lacks', async ({ page }) => {
  const errors = watchErrors(page);
  await open(page);
  await page.getByTestId('edges-intents').click();
  await expect(page.getByText('intent.order-total-changes').first()).toBeVisible();

  await page.getByTestId('view-intents').click();
  await expect(page.getByTestId('intents-view')).toContainText('intent.orders-placed');
  await page.getByTestId('predicate').fill('changes(Order.paymnt)');
  await expect(page.getByTestId('predicate-error')).toHaveText("Order has no attribute 'paymnt' (has: total, status, payment, stock)");
  await page.getByTestId('predicate').fill('changes(Order.payment)');
  await expect(page.getByTestId('predicate-matches')).toContainText('PaymentAuthorized@v1');
  await expect(page.locator('[data-lit]')).toHaveCount(2);

  // Without the proxy, the Changes view says what the whole loop needs rather than offering a gate there is not.
  await page.getByTestId('view-changes').click();
  await expect(page.getByTestId('changes-view')).toContainText('no proxy, so no gate');
  await expect(page.getByTestId('gate')).toHaveCount(0);
  await page.getByTestId('view-incidents').click();
  await expect(page.getByTestId('incident-facts')).toBeVisible();
  expect(errors).toEqual([]);
});
