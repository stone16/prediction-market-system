import fs from 'node:fs';
import path from 'node:path';
import { expect, test } from '@playwright/test';

const evidenceDir = path.resolve(
  process.cwd(),
  '..',
  '.harness',
  'cathedral-v1',
  'checkpoints',
  '03',
  'iter-1',
  'evidence'
);

test.beforeEach(() => {
  fs.mkdirSync(evidenceDir, { recursive: true });
});

test('cathedral nav labels render without console errors', async ({ page }) => {
  const errors: string[] = [];

  page.on('console', (message) => {
    if (message.type() === 'error') {
      errors.push(message.text());
    }
  });
  page.on('pageerror', (error) => {
    errors.push(error.message);
  });

  await page.setViewportSize({ width: 1440, height: 1024 });
  await page.goto('/');

  await expect(
    page.getByRole('link', {
      name: 'Markets'
    })
  ).toBeVisible();
  await expect(page.getByRole('link', { name: 'Watchlist' })).toBeVisible();
  await expect(page.getByRole('link', { name: 'Ideas' })).toBeVisible();
  await expect(page.getByRole('link', { name: 'Trades' })).toBeVisible();
  await expect(page.getByRole('link', { name: 'Positions' })).toBeVisible();
  await expect(page.getByRole('link', { name: 'Performance' })).toBeVisible();
  await expect(page.getByRole('link', { name: 'Strategies' })).toBeVisible();
  await expect(page.getByRole('link', { name: 'Backtest' })).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: path.join(evidenceDir, 'cp03-nav-ia.png')
  });

  expect(errors).toEqual([]);
});
