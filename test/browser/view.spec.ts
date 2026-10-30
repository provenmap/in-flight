/// <reference lib="dom" />
// The view, end to end in a real browser: it renders and flows, breaking the schema raises the counter, patching and
// redriving drains it to $0, the 2D view works, and nothing scrolls sideways at phone width. It reads `window.rif`
// and the `data-testid`s, never pixels.
import { expect, test, type Page } from '@playwright/test';
import { settle } from '../e2e/helpers.js';

const sim = 'http://localhost:28090';

function watchErrors(page: Page): string[] {
  const errors: string[] = [];
  page.on('pageerror', e => errors.push(e.message));
  page.on('console', m => { if (m.type() === 'error') errors.push(m.text()); });
  return errors;
}

const rif = <T>(page: Page, expr: string) => page.evaluate(`window.rif && (${expr})`) as Promise<T>;

test.beforeAll(async () => {
  await settle(sim);
});

test('the 3D view renders, connects and particles flow', async ({ page }) => {
  const errors = watchErrors(page);
  await page.goto('/?view=3d');
  await expect(page.getByTestId('connection')).toContainText('live', { timeout: 30_000 });
  await expect.poll(() => rif<string>(page, 'window.rif.view')).toBe('3d');
  await expect.poll(() => rif<number>(page, 'window.rif.frames'), { timeout: 15_000 }).toBeGreaterThan(10);
  await expect.poll(() => rif<number>(page, 'window.rif.particles'), { timeout: 15_000 }).toBeGreaterThan(0);
  for (const id of ['at-risk', 'at-risk-sub', 'stuck', 'lag', 'recovered', 'hint', 'break', 'patch', 'redrive', 'rate', 'sim-state', 'connection']) {
    await expect(page.getByTestId(id)).toHaveCount(1);
  }
  expect(errors).toEqual([]);
});

test('break raises the counter; patch and redrive drain it to $0', async ({ page }) => {
  const errors = watchErrors(page);
  await page.goto('/');
  await expect(page.getByTestId('connection')).toContainText('live', { timeout: 30_000 });
  await expect(page.getByTestId('break')).toBeEnabled({ timeout: 15_000 });

  await page.getByTestId('break').click();
  await expect(page.getByTestId('stuck')).toContainText('stuck $', { timeout: 15_000 });
  await expect.poll(() => rif<number>(page, 'window.rif.blocks'), { timeout: 15_000 }).toBeGreaterThan(5);
  await expect(page.getByTestId('at-risk')).not.toHaveText('$0.00');
  await expect(page.getByTestId('hint')).toContainText('Patch payments');

  await page.getByTestId('patch').click();
  await expect(page.getByTestId('hint')).toContainText('Redrive the DLQ', { timeout: 15_000 });
  await page.getByTestId('redrive').click();
  await expect(page.getByTestId('at-risk')).toHaveText('$0.00', { timeout: 60_000 });
  await expect(page.getByTestId('recovered')).toBeVisible({ timeout: 15_000 });
  await expect(page.getByTestId('recovered')).toContainText('Recovered $');
  await expect.poll(() => rif<number>(page, 'window.rif.blocks'), { timeout: 30_000 }).toBe(0);
  expect(errors).toEqual([]);
});

test('the 2D view draws the same scene', async ({ page }) => {
  const errors = watchErrors(page);
  await page.goto('/?view=2d');
  await expect(page.getByTestId('connection')).toContainText('live', { timeout: 30_000 });
  await expect.poll(() => rif<string>(page, 'window.rif.view')).toBe('2d');
  await expect.poll(() => rif<number>(page, 'window.rif.particles'), { timeout: 15_000 }).toBeGreaterThan(0);
  expect(errors).toEqual([]);
});

test('at phone width nothing scrolls sideways', async ({ page }) => {
  const errors = watchErrors(page);
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/');
  await expect(page.getByTestId('connection')).toContainText('live', { timeout: 30_000 });
  const [scroll, width] = await page.evaluate(() => [document.documentElement.scrollWidth, window.innerWidth]);
  expect(scroll).toBeLessThanOrEqual(width);
  await expect(page.getByTestId('break')).toBeInViewport();
  expect(errors).toEqual([]);
});
