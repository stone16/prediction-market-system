import { expect, test } from '@playwright/test';
import fs from 'node:fs';
import path from 'node:path';
import { applySchema, executeSql, resetInnerRing } from './support/pg';

const evidenceDir = path.resolve(process.cwd(), '..', '.harness', 'pms-v2', 'checkpoints', '10', 'iter-1', 'evidence');

function seedFeedback() {
  applySchema();
  resetInnerRing();
  const now = '2026-04-14T00:00:00+00:00';
  executeSql(`
    INSERT INTO feedback (feedback_id, target, source, message, severity, created_at, resolved, resolved_at, category, metadata, strategy_id, strategy_version_id)
    VALUES
      ('fb-open-1', 'controller', 'evaluator', 'Brier score crossed the review threshold for model-a.', 'warning', '${now}', FALSE, NULL, 'brier:model-a', '{"market_id":"pm-synthetic-010"}', 'default', 'default-v1'),
      ('fb-open-2', 'controller', 'actuator', 'Paper fill slippage exceeded the configured limit.', 'warning', '${now}', FALSE, NULL, 'slippage', '{"market_id":"pm-synthetic-011"}', 'default', 'default-v1'),
      ('fb-resolved', 'controller', 'evaluator', 'Win-rate feedback already handled.', 'info', '${now}', TRUE, '${now}', 'win_rate', '{"market_id":"pm-synthetic-012"}', 'default', 'default-v1');

    INSERT INTO eval_records (
      decision_id, market_id, prob_estimate, resolved_outcome, brier_score, fill_status,
      recorded_at, citations, category, model_id, pnl, slippage_bps, filled, strategy_id, strategy_version_id
    ) VALUES
      ('eval-1', 'pm-synthetic-010', 0.61, 1.0, 0.1521, 'matched', '${now}', '["trade-1"]', 'model-a', 'model-a', 12.5, 8.0, TRUE, 'default', 'default-v1'),
      ('eval-2', 'pm-synthetic-011', 0.44, 0.0, 0.1936, 'matched', '2026-04-14T00:05:00+00:00', '["trade-2"]', 'model-b', 'model-b', -4.0, 11.5, TRUE, 'default', 'default-v1'),
      ('eval-3', 'pm-synthetic-012', 0.71, 1.0, 0.0841, 'matched', '2026-04-14T00:10:00+00:00', '["trade-3"]', 'model-a', 'model-a', 7.0, 5.5, TRUE, 'default', 'default-v1');
  `);
}

test.beforeEach(() => {
  seedFeedback();
  fs.mkdirSync(evidenceDir, { recursive: true });
});

test('feedback panel resolves without full page reload and required pages are quiet', async ({ page }) => {
  const errors: string[] = [];
  page.on('console', (message) => {
    if (message.type() === 'error') errors.push(message.text());
  });
  page.on('pageerror', (error) => errors.push(error.message));

  await page.goto('/');
  await expect(page.getByRole('heading', { name: 'Cybernetic Console' })).toBeVisible();
  await expect(page.getByTestId('layer-card')).toHaveCount(4);
  await expect(page.getByTestId('feedback-item')).toHaveCount(2);
  await page.getByRole('button', { name: 'Mark Resolved' }).first().click();
  await expect(page.getByTestId('feedback-item')).toHaveCount(1);
  await page.screenshot({ path: path.join(evidenceDir, 'dashboard-main.png'), fullPage: true });

  await page.goto('/metrics');
  await expect(page.getByText('Brier score over time')).toBeVisible();
  await expect(page.getByText('Calibration curve')).toBeVisible();
  await expect(page.getByText('P&L over time')).toBeVisible();
  await page.screenshot({ path: path.join(evidenceDir, 'dashboard-metrics.png'), fullPage: true });

  await page.goto('/decisions');
  await expect(page.getByRole('heading', { name: 'Decision Ledger' })).toBeVisible();

  await page.goto('/backtest');
  await expect(page.getByRole('heading', { name: 'Backtest Run' })).toBeVisible();

  expect(errors).toEqual([]);
});
