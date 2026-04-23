import fs from 'node:fs';
import path from 'node:path';
import { expect, test } from '@playwright/test';

const evidenceDir = path.resolve(
  process.cwd(),
  '..',
  '.harness',
  'cathedral-v1',
  'checkpoints',
  '06',
  'iter-1',
  'evidence'
);

test.beforeEach(() => {
  fs.mkdirSync(evidenceDir, { recursive: true });
});

test('onboarding persists across dismiss and returns after localStorage reset', async ({ page }) => {
  const errors: string[] = [];

  page.on('console', (message) => {
    if (message.type() === 'error') {
      errors.push(message.text());
    }
  });
  page.on('pageerror', (error) => {
    errors.push(error.message);
  });

  await page.goto('/');
  await page.evaluate(() => {
    window.localStorage.removeItem('pms.onboarded');
  });
  await page.reload();
  await expect(page.getByRole('dialog', { name: 'Quick start' })).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: path.join(evidenceDir, 'cp06-home-onboarding.png')
  });

  await page.getByRole('button', { name: 'Dismiss onboarding' }).click();
  await expect(page.getByRole('dialog', { name: 'Quick start' })).toBeHidden();

  await page.reload();
  await expect(page.getByRole('dialog', { name: 'Quick start' })).toBeHidden();

  await page.evaluate(() => {
    window.localStorage.removeItem('pms.onboarded');
  });
  await page.reload();
  await expect(page.getByRole('dialog', { name: 'Quick start' })).toBeVisible();

  expect(errors).toEqual([]);
});

test('positions and trades pages render populated and empty states without console errors', async ({
  page
}) => {
  const errors: string[] = [];

  page.on('console', (message) => {
    if (message.type() === 'error') {
      errors.push(message.text());
    }
  });
  page.on('pageerror', (error) => {
    errors.push(error.message);
  });

  await page.goto('/positions');
  await expect(page.getByRole('heading', { name: 'Positions' })).toBeVisible();
  await expect(page.getByRole('cell', { name: 'market-000', exact: true })).toBeVisible();
  await page.screenshot({
    fullPage: true,
    path: path.join(evidenceDir, 'cp06-positions-populated.png')
  });

  await page.route('**/api/pms/positions', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify({ positions: [] })
    });
  });
  await page.goto('/positions');
  await expect(page.getByText('No open positions yet.')).toBeVisible();
  await page.screenshot({
    fullPage: true,
    path: path.join(evidenceDir, 'cp06-positions-empty.png')
  });
  await page.unroute('**/api/pms/positions');

  await page.goto('/trades');
  await expect(page.getByRole('heading', { name: 'Trades' })).toBeVisible();
  await expect(page.getByText('Will market 000 settle above consensus?')).toBeVisible();
  await page.screenshot({
    fullPage: true,
    path: path.join(evidenceDir, 'cp06-trades-populated.png')
  });

  await page.route('**/api/pms/trades?limit=20', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify({ trades: [], limit: 20 })
    });
  });
  await page.goto('/trades');
  await expect(page.getByText('No trades yet.')).toBeVisible();
  await page.screenshot({
    fullPage: true,
    path: path.join(evidenceDir, 'cp06-trades-empty.png')
  });

  expect(errors).toEqual([]);
});
