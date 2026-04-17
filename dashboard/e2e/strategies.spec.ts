import fs from 'node:fs';
import path from 'node:path';
import { expect, test } from '@playwright/test';

const evidenceDir = path.resolve(
  process.cwd(),
  '..',
  '.harness',
  'pms-strategy-aggregate-v1',
  'checkpoints',
  '07',
  'iter-1',
  'evidence'
);

test.beforeAll(() => {
  fs.mkdirSync(evidenceDir, { recursive: true });
});

test('strategies page renders seeded registry row without console errors', async ({ page }) => {
  const errors: string[] = [];
  page.on('console', (message) => {
    if (message.type() === 'error') errors.push(message.text());
  });
  page.on('pageerror', (error) => errors.push(error.message));

  await page.goto('/strategies');

  await expect(page.getByRole('columnheader', { name: 'Strategy' })).toBeVisible();
  await expect(page.getByRole('columnheader', { name: 'Active version' })).toBeVisible();
  await expect(page.getByRole('columnheader', { name: 'Created' })).toBeVisible();
  await expect(page.locator('tbody tr')).toHaveCount(1);
  await expect(page.locator('tbody tr').first().locator('td').first()).toHaveText('default');

  await page.screenshot({
    path: path.join(evidenceDir, 'strategies-page.png'),
    fullPage: true
  });

  expect(errors).toEqual([]);
});
