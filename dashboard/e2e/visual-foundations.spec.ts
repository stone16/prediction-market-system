import fs from 'node:fs';
import path from 'node:path';
import { expect, test } from '@playwright/test';

const evidenceDir = path.resolve(
  process.cwd(),
  '..',
  '.harness',
  'cathedral-v1',
  'checkpoints',
  '02',
  'iter-1',
  'evidence'
);

const routes = ['/', '/overview', '/signals', '/factors', '/strategies', '/decisions', '/metrics', '/backtest'];

test.beforeEach(() => {
  fs.mkdirSync(evidenceDir, { recursive: true });
});

test('visual foundations render across the existing desktop routes without console errors', async ({
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

  await page.setViewportSize({ width: 1440, height: 1024 });

  for (const route of routes) {
    await page.goto(route);
    await expect(page.locator('body')).toBeVisible();
  }

  await page.goto('/');
  await page.screenshot({
    fullPage: true,
    path: path.join(evidenceDir, 'cp02-home-foundations.png')
  });

  expect(errors).toEqual([]);
});
