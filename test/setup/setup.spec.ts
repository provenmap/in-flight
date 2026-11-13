/// <reference lib="dom" />
// From nothing to a watched flow in the console, with no JSON written: connect, discover, confirm, activate; then the
// shop's dead letters are priced, grouped by cause, and a redrive dry-runs. Screenshots go to docs/images/setup-*.png.
import { expect, test, type Page } from '@playwright/test';

const shot = (page: Page, name: string) => page.screenshot({ path: `docs/images/setup-${name}.png` });

test('connect, discover, confirm, watch', async ({ page }) => {
  const errors: string[] = [];
  page.on('pageerror', e => errors.push(e.message));
  page.on('console', m => { if (m.type() === 'error' && !m.text().includes('WebSocket')) errors.push(m.text()); });

  await page.goto('/');
  await expect(page.getByTestId('connect-form')).toBeVisible({ timeout: 30_000 });
  await page.getByTestId('connect-name').fill('local');
  await page.getByTestId('connect-bootstrap').fill('localhost:9392');
  await shot(page, 'connect');
  await page.getByTestId('connect-test').click();
  await expect(page.getByTestId('connect-result')).toContainText('Connected to localhost:9392', { timeout: 30_000 });
  await page.getByTestId('connect-save').click();

  await expect(page.getByTestId('discover-view')).toBeVisible({ timeout: 30_000 });
  await expect(page.getByTestId('discover-topics')).toContainText('shop.orders', { timeout: 30_000 });
  await expect(page.getByTestId('discover-dlqs')).toContainText('fulfilment-service.DLT');
  await expect(page.getByTestId('discover-dlqs')).toContainText('fulfilment-service');
  await page.getByTestId('topic-shop.orders').click();
  await expect(page.getByTestId('sample-fields')).toContainText('total_cents', { timeout: 30_000 });
  await shot(page, 'discover');
  await page.getByTestId('flow-name').fill('shop');
  await page.getByTestId('draft-button').click();

  await expect(page.getByTestId('confirm-view')).toBeVisible({ timeout: 60_000 });
  await expect(page.getByTestId('draft-catalog')).toContainText('total_cents');
  await expect(page.getByTestId('validation-events')).toContainText('40', { timeout: 60_000 });
  // Answer the teams question, as a person would.
  const teams = page.getByTestId('guess-districts-option').first();
  if (await teams.count()) {
    await teams.click();
    await expect(page.getByTestId('guess-districts')).toHaveCount(0, { timeout: 15_000 });
  }
  await shot(page, 'confirm');
  await page.getByTestId('activate-button').click();

  await expect(page.getByTestId('connection')).toContainText('Live', { timeout: 60_000 });
  // The three dead letters, 18.75 + 20.00 + 21.25.
  await expect(page.getByTestId('stuck')).toContainText('$60.00', { timeout: 60_000 });
  await page.getByTestId('view-incidents').click();
  await expect(page.getByTestId('causes-view')).toContainText('unreachable', { timeout: 30_000 });
  await page.getByTestId('redrive-dry-fulfilment-service.DLT').click();
  await expect(page.getByTestId('redrive-report-fulfilment-service.DLT')).toContainText('would write 3 of 3', { timeout: 30_000 });
  await expect(page.getByTestId('redrive-run-fulfilment-service.DLT')).toBeDisabled();
  await shot(page, 'watch');
  expect(errors).toEqual([]);
});
