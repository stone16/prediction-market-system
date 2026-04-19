import fs from 'node:fs';
import path from 'node:path';
import { expect, test } from '@playwright/test';
import { applySchema, executeSql } from './support/pg';

const evidenceDir = path.resolve(
  process.cwd(),
  '..',
  '.harness',
  'pms-controller-per-strategy-v1',
  'checkpoints',
  '06',
  'iter-1',
  'evidence'
);

test.beforeAll(() => {
  fs.mkdirSync(evidenceDir, { recursive: true });
});

function sqlString(value: string): string {
  return value.replace(/'/g, "''");
}

function strategyConfigJson(strategyId: string): string {
  return JSON.stringify({
    config: {
      strategy_id: strategyId,
      factor_composition: [],
      metadata: [
        ['owner', 'system'],
        ['tier', 'default']
      ]
    },
    eval_spec: {
      max_brier_score: 0.3,
      metrics: ['brier', 'pnl', 'fill_rate'],
      min_win_rate: 0.5,
      slippage_threshold_bps: 50
    },
    forecaster: { forecasters: [] },
    market_selection: {
      resolution_time_max_horizon_days: 7,
      venue: 'polymarket',
      volume_min_usdc: 500
    },
    risk: {
      max_daily_drawdown_pct: 2.5,
      max_position_notional_usdc: 100,
      min_order_size_usdc: 1
    }
  });
}

function seedStrategiesMetrics() {
  applySchema();
  executeSql(`
    TRUNCATE TABLE opportunities, feedback, eval_records, fills, orders, strategy_factors, strategy_versions, strategies RESTART IDENTITY CASCADE;
  `);
  applySchema();
  const alphaConfig = sqlString(strategyConfigJson('alpha'));
  const betaConfig = sqlString(strategyConfigJson('beta'));
  executeSql(`
    INSERT INTO strategies (strategy_id, active_version_id)
    VALUES
      ('alpha', 'alpha-v1'),
      ('beta', 'beta-v1');

    INSERT INTO strategy_versions (strategy_version_id, strategy_id, config_json)
    VALUES
      ('alpha-v1', 'alpha', '${alphaConfig}'::jsonb),
      ('beta-v1', 'beta', '${betaConfig}'::jsonb);

    INSERT INTO orders (order_id, market_id, ts, strategy_id, strategy_version_id)
    VALUES
      ('alpha-order-1', 'market-1', NOW(), 'alpha', 'alpha-v1'),
      ('beta-order-1', 'market-2', NOW(), 'beta', 'beta-v1');

    INSERT INTO fills (fill_id, order_id, market_id, ts, strategy_id, strategy_version_id)
    VALUES
      ('alpha-fill-1', 'alpha-order-1', 'market-1', NOW(), 'alpha', 'alpha-v1'),
      ('beta-fill-1', 'beta-order-1', 'market-2', NOW(), 'beta', 'beta-v1');

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

test.beforeEach(() => {
  seedStrategiesMetrics();
});

test('strategies page renders comparative metrics without console errors', async ({ page }) => {
  const errors: string[] = [];
  page.on('console', (message) => {
    if (message.type() === 'error') errors.push(message.text());
  });
  page.on('pageerror', (error) => errors.push(error.message));

  await page.goto('/strategies');

  await expect(page.getByRole('columnheader', { name: 'Strategy' })).toBeVisible();
  await expect(page.getByRole('columnheader', { name: 'Version' })).toBeVisible();
  await expect(page.getByRole('columnheader', { name: 'Records' })).toBeVisible();
  await expect(page.getByRole('columnheader', { name: 'Brier' })).toBeVisible();
  await expect(page.getByRole('columnheader', { name: 'Drawdown' })).toBeVisible();
  await expect(page.getByTestId('strategy-metrics-row')).toHaveCount(3);
  await expect(page.getByText('0 records (insufficient samples)')).toBeVisible();
  await expect(page.locator('tbody')).not.toContainText('—');
  await expect(page.getByTestId('strategy-metrics-row').filter({ hasText: 'alpha' })).toContainText('0.125');
  await expect(page.getByTestId('strategy-metrics-row').filter({ hasText: 'alpha' })).toContainText('15.0 bps');
  await expect(page.getByTestId('strategy-metrics-row').filter({ hasText: 'beta' })).toContainText('0.305');
  await expect(page.getByRole('columnheader', { name: 'Created' })).toBeVisible();

  await page.screenshot({
    path: path.join(evidenceDir, 'strategies-page.png'),
    fullPage: true
  });

  expect(errors).toEqual([]);
});
