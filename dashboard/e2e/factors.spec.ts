import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { expect, test } from '@playwright/test';
import { applySchema, resetMiddleRing, resetOuterRing } from './support/pg';

const rootDir = path.resolve(process.cwd(), '..');
const evidenceDir = path.join(
  rootDir,
  '.harness',
  'pms-factor-panel-v1',
  'checkpoints',
  '08',
  'iter-1',
  'evidence'
);
const marketId = 'factor-panel-e2e';

function seedFactorFixture() {
  const databaseUrl =
    process.env.PMS_TEST_DATABASE_URL ??
    process.env.DATABASE_URL ??
    'postgresql://postgres:postgres@localhost:5432/pms_test';

  execFileSync('uv', ['run', 'python', 'tests/support/seed_factor_panel_fixture.py'], {
    cwd: rootDir,
    env: {
      ...process.env,
      DATABASE_URL: databaseUrl,
      PMS_TEST_DATABASE_URL: databaseUrl
    },
    stdio: 'pipe'
  });
}

test.beforeAll(() => {
  fs.mkdirSync(evidenceDir, { recursive: true });
  applySchema();
  resetOuterRing();
  resetMiddleRing();
  seedFactorFixture();
});

test('factors page renders seeded factor rows without console errors', async ({ page }) => {
  const errors: string[] = [];
  page.on('console', (message) => {
    if (message.type() === 'error') errors.push(message.text());
  });
  page.on('pageerror', (error) => errors.push(error.message));

  await page.goto(`/factors?factor_id=orderbook_imbalance&market_id=${marketId}`);

  await expect(page.getByRole('heading', { name: 'Factors' })).toBeVisible();
  await expect(page.getByLabel('Factor')).toHaveValue('orderbook_imbalance');
  await expect(page.getByLabel('Market ID')).toHaveValue(marketId);
  await expect(page.getByTestId('factor-chart')).toBeVisible();
  await expect(page.getByTestId('factor-row')).toHaveCount(2);

  const timestamps = await page.getByTestId('factor-row').locator('td:first-child').allTextContents();
  expect(new Set(timestamps).size).toBeGreaterThanOrEqual(2);
  expect(timestamps).toEqual([...timestamps].sort());

  await page.screenshot({
    path: path.join(evidenceDir, 'factors-page.png'),
    fullPage: true
  });

  expect(errors).toEqual([]);
});
