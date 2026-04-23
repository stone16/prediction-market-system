import fs from 'node:fs';
import path from 'node:path';
import { expect, test } from '@playwright/test';
import { applySchema, executeSql } from './support/pg';

const evidenceDir = path.resolve(
  process.cwd(),
  '..',
  '.harness',
  'cathedral-v1',
  'checkpoints',
  '11',
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
      metadata: [['owner', 'system']]
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

function seedShareFixtures() {
  applySchema();
  executeSql(`
    TRUNCATE TABLE opportunities, feedback, eval_records, fills, orders, strategy_factors, strategy_versions, strategies RESTART IDENTITY CASCADE;
  `);
  applySchema();
  const alphaConfig = sqlString(strategyConfigJson('alpha'));
  executeSql(`
    INSERT INTO strategies (
      strategy_id,
      active_version_id,
      title,
      description,
      archived,
      share_enabled
    ) VALUES (
      'alpha',
      'alpha-v1234567',
      'Alpha Theory',
      'Buy dislocations when liquidity is deep.',
      FALSE,
      TRUE
    );

    INSERT INTO strategy_versions (strategy_version_id, strategy_id, config_json)
    VALUES ('alpha-v1234567', 'alpha', '${alphaConfig}'::jsonb);

    INSERT INTO eval_records (
      decision_id, market_id, prob_estimate, resolved_outcome, brier_score, fill_status,
      recorded_at, citations, category, model_id, pnl, slippage_bps, filled, strategy_id, strategy_version_id
    ) VALUES (
      'alpha-eval-1', 'market-cp11', 0.61, 1.0, 0.125, 'matched',
      '2026-04-23T12:00:00+00:00', '["seed"]', 'cp11', 'model-cp11', 5.0, 10.0, TRUE, 'alpha', 'alpha-v1234567'
    );

    INSERT INTO fills (
      fill_id, order_id, market_id, ts, fill_notional_usdc, fill_quantity, strategy_id, strategy_version_id
    ) VALUES (
      'alpha-fill-1', 'alpha-order-1', 'market-cp11', '2026-04-23T12:00:00+00:00', 25.0, 50.0, 'alpha', 'alpha-v1234567'
    );
  `);
}

test.beforeEach(() => {
  seedShareFixtures();
});

test('share page renders public strategy summary and neutral 404 copy without console errors', async ({ page }) => {
  const errors: string[] = [];

  function isExpected404Resource(message: string) {
    return message.includes('Failed to load resource') && message.includes('404');
  }

  page.on('console', (message) => {
    if (message.type() === 'error' && !isExpected404Resource(message.text())) {
      errors.push(message.text());
    }
  });
  page.on('pageerror', (error) => errors.push(error.message));

  await page.goto('/share/alpha');

  await expect(page.getByRole('heading', { level: 1, name: 'Alpha Theory' })).toBeVisible();
  await expect(page.getByRole('heading', { level: 2, name: 'Theory' })).toBeVisible();
  await expect(page.getByRole('heading', { level: 2, name: 'Performance' })).toBeVisible();
  await expect(page.getByRole('heading', { level: 2, name: 'Calibration' })).toBeVisible();
  await expect(
    page.getByTestId('share-hero').getByText('Buy dislocations when liquidity is deep.')
  ).toBeVisible();
  await page.screenshot({
    path: path.join(evidenceDir, 'share-page.png'),
    fullPage: true
  });

  await page.goto('/share/missing');
  await expect(page.getByText("This strategy doesn't exist or has been unshared")).toBeVisible();
  await page.screenshot({
    path: path.join(evidenceDir, 'share-404.png'),
    fullPage: true
  });

  expect(errors).toEqual([]);
});
