/// <reference lib="dom" />
// The screenshots in docs/guide.md, taken from the real thing: a healthy flow, the jam after OrderPlaced v2, the
// details behind it, the same flow watched with no simulator, and the recovery. `npm run docs:screenshots`.
import { expect, test, type Locator, type Page } from '@playwright/test';
import { settle } from '../e2e/helpers.js';

const sim = 'http://localhost:28090';
const demo = 'http://localhost:28088';
const watch = 'http://localhost:28089';
const out = (name: string) => `docs/images/${name}.png`;

const rif = <T>(page: Page, expr: string) => page.evaluate(`window.rif && (${expr})`) as Promise<T>;

async function open(page: Page, url: string) {
  const errors: string[] = [];
  page.on('pageerror', e => errors.push(e.message));
  await page.goto(url);
  await expect(page.getByTestId('connection')).toContainText('Live', { timeout: 30_000 });
  return errors;
}

/** A card, by its title. */
const card = (page: Page, title: string): Locator => page.locator('[data-slot=card]').filter({ has: page.getByText(title, { exact: true }) });

test('the guide, in pictures', async ({ page, browser }) => {
  await settle(sim);
  await page.request.post(`${sim}/rate?perSecond=8`);
  const errors = await open(page, demo);
  // Long enough for the edges to show a steady money-a-minute.
  await page.waitForTimeout(20_000);
  await page.screenshot({ path: out('overview') });

  await page.getByTestId('break').click();
  await expect.poll(() => rif<number>(page, 'window.rif.blocks'), { timeout: 30_000 }).toBeGreaterThan(60);
  await page.screenshot({ path: out('incident') });
  // The tabs and the first rows are the point; the rest of the table is more of the same.
  const tabs = page.locator('[data-slot=card]').filter({ has: page.getByTestId('dead-letters') });
  // Clear of the sticky top bar.
  await tabs.evaluate(el => window.scrollTo(0, el.getBoundingClientRect().top + window.scrollY - 76));
  const box = (await tabs.boundingBox())!;
  await page.screenshot({ path: out('dead-letters'), clip: { ...box, height: Math.min(box.height, 470) } });

  await page.locator('[data-district=payments]').click();
  await expect(page.getByRole('dialog')).toContainText('payments.dlq');
  await page.waitForTimeout(400);
  await page.screenshot({ path: out('district') });
  await page.keyboard.press('Escape');

  // The same Kafka, watched by a console with no simulator.
  const watching = await browser.newPage({ viewport: { width: 1440, height: 960 }, colorScheme: 'light' });
  const watchErrors = await open(watching, watch);
  await expect(watching.getByTestId('queues')).toContainText('payments.dlq');
  await expect.poll(() => rif<number>(watching, 'window.rif.blocks'), { timeout: 30_000 }).toBeGreaterThan(60);
  await watching.waitForTimeout(3000);
  await watching.screenshot({ path: out('watch') });
  await watching.close();

  const dark = await browser.newPage({ viewport: { width: 1440, height: 960 }, colorScheme: 'dark' });
  await open(dark, demo);
  await expect.poll(() => rif<number>(dark, 'window.rif.blocks'), { timeout: 30_000 }).toBeGreaterThan(60);
  await dark.waitForTimeout(3000);
  await dark.screenshot({ path: out('dark') });
  await dark.close();

  const phone = await browser.newPage({ viewport: { width: 390, height: 844 }, deviceScaleFactor: 2, colorScheme: 'light' });
  await open(phone, demo);
  await phone.waitForTimeout(2000);
  await phone.screenshot({ path: out('phone') });
  await phone.close();

  await page.getByTestId('patch').click();
  await expect(page.getByTestId('hint')).toContainText('Redrive the DLQ', { timeout: 15_000 });
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.getByTestId('redrive').click();
  // Mid-wave: blocks going green and dropping back into Payments.
  await expect.poll(() => rif<number>(page, 'window.rif.model.pile.filter(b => b.greenAt && b.greenAt < performance.now()).length'), { timeout: 30_000 }).toBeGreaterThan(15);
  await card(page, 'Order to cash, live').screenshot({ path: out('recovering') });
  await expect(page.getByTestId('at-risk')).toHaveText('$0.00', { timeout: 60_000 });
  await expect(page.getByTestId('recovered')).toBeVisible({ timeout: 15_000 });
  await page.waitForTimeout(1500);
  await page.screenshot({ path: out('resolved') });

  await page.request.post(`${sim}/restore`);
  await page.request.post(`${sim}/unpatch`);
  expect([...errors, ...watchErrors]).toEqual([]);
});
