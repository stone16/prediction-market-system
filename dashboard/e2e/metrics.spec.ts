import fs from 'node:fs';
import path from 'node:path';
import { expect, test } from '@playwright/test';
import { applySchema, executeSql, resetInnerRing } from './support/pg';

const evidenceDir = path.resolve(
  process.cwd(),
  '..',
  '.harness',
  'pms-controller-per-strategy-v1',
  'checkpoints',
  '07',
  'iter-1',
  'evidence'
);

function seedMetricsFixture() {
  applySchema();
  resetInnerRing();
  executeSql(`
    INSERT INTO eval_records (
      decision_id, market_id, prob_estimate, resolved_outcome, brier_score, fill_status,
      recorded_at, citations, category, model_id, pnl, slippage_bps, filled, strategy_id, strategy_version_id
    ) VALUES
      ('alpha-eval-1', 'market-1', 0.61, 1.0, 0.09, 'matched', '2026-04-19T00:00:00+00:00', '["trade-1"]', 'model-a', 'model-a', 5.0, 10.0, TRUE, 'alpha', 'alpha-v1'),
      ('alpha-eval-2', 'market-1', 0.52, 0.0, 0.16, 'matched', '2026-04-19T00:05:00+00:00', '["trade-2"]', 'model-a', 'model-a', -3.0, 20.0, TRUE, 'alpha', 'alpha-v1'),
      ('beta-eval-1', 'market-2', 0.48, 0.0, 0.25, 'matched', '2026-04-19T00:10:00+00:00', '["trade-3"]', 'model-b', 'model-b', -2.0, 8.0, TRUE, 'beta', 'beta-v1'),
      ('beta-eval-2', 'market-2', 0.7, 1.0, 0.36, 'matched', '2026-04-19T00:15:00+00:00', '["trade-4"]', 'model-b', 'model-b', 6.0, 12.0, TRUE, 'beta', 'beta-v1');
  `);
}

test.beforeAll(() => {
  fs.mkdirSync(evidenceDir, { recursive: true });
});

test.beforeEach(() => {
  seedMetricsFixture();
});

test('metrics page shows per-strategy breakdown before ops view', async ({ page }) => {
  const errors: string[] = [];
  page.on('console', (message) => {
    if (message.type() === 'error') errors.push(message.text());
  });
  page.on('pageerror', (error) => errors.push(error.message));

  await page.goto('/metrics');

  await expect(page.getByTestId('metrics-per-strategy')).toBeVisible();
  await expect(page.getByTestId('metrics-strategy-row')).toHaveCount(2);
  await expect(page.getByRole('heading', { name: 'ops view (cross-strategy)' })).toBeVisible();
  await expect(page.getByText('Brier score over time')).toBeVisible();
  await expect(page.getByText('Calibration curve')).toBeVisible();
  await expect(page.getByText('P&L over time')).toBeVisible();
  await expect(page.getByTestId('metrics-strategy-row').filter({ hasText: 'alpha' })).toContainText(
    '0.125'
  );
  await expect(page.getByTestId('metrics-strategy-row').filter({ hasText: 'beta' })).toContainText(
    '0.305'
  );

  const inDomOrder = await page.evaluate(() => {
    const perStrategy = document.querySelector('[data-testid="metrics-per-strategy"]');
    const opsView = document.querySelector('[data-testid="metrics-ops-view"]');
    if (!perStrategy || !opsView) {
      return false;
    }
    return Boolean(perStrategy.compareDocumentPosition(opsView) & Node.DOCUMENT_POSITION_FOLLOWING);
  });
  expect(inDomOrder).toBe(true);

  await page.screenshot({
    path: path.join(evidenceDir, 'metrics-page.png'),
    fullPage: true
  });

  expect(errors).toEqual([]);
});
