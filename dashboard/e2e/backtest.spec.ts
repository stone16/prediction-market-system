import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { expect, test } from '@playwright/test';
import { applySchema, executeSql } from './support/pg';

const evidenceDir = path.resolve(
  process.cwd(),
  '..',
  '.harness',
  'pms-research-backtest-v1',
  'checkpoints',
  '10',
  'iter-1',
  'evidence'
);

const completedRunId = '11111111-1111-1111-1111-111111111111';

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
        ['tier', 'research']
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

function portfolioTargetJson(entries: Array<{ marketId: string; tokenId: string; timestamp: string }>): string {
  return JSON.stringify(
    entries.map((entry, index) => ({
      market_id: entry.marketId,
      token_id: entry.tokenId,
      side: 'buy_yes',
      timestamp: entry.timestamp,
      target_size_usdc: 20 + index * 5
    }))
  );
}

function seedBacktestWorkspace() {
  applySchema();
  executeSql(`
    TRUNCATE TABLE
      backtest_live_comparisons,
      evaluation_reports,
      strategy_runs,
      backtest_runs,
      opportunities,
      feedback,
      eval_records,
      fills,
      orders,
      strategy_factors,
      strategy_versions,
      strategies
    RESTART IDENTITY CASCADE;
  `);
  applySchema();

  const alphaConfig = sqlString(strategyConfigJson('alpha'));
  const betaConfig = sqlString(strategyConfigJson('beta'));
  const gammaConfig = sqlString(strategyConfigJson('gamma'));
  const completedSpec = sqlString(
    JSON.stringify({
      strategy_versions: [
        ['alpha', 'alpha-v1'],
        ['beta', 'beta-v1'],
        ['gamma', 'gamma-v1']
      ],
      dataset: {
        source: 'fixture',
        version: 'v1',
        coverage_start: '2026-04-01T00:00:00+00:00',
        coverage_end: '2026-04-30T00:00:00+00:00',
        market_universe_filter: { market_ids: ['market-a', 'market-b', 'market-c'] },
        data_quality_gaps: []
      },
      execution_model: {
        fee_rate: 0.04,
        slippage_bps: 8.0,
        latency_ms: 250.0,
        staleness_ms: 60000.0,
        fill_policy: 'immediate_or_cancel'
      },
      risk_policy: {
        max_position_notional_usdc: 100.0,
        max_daily_drawdown_pct: 2.5,
        min_order_size_usdc: 1.0
      },
      date_range_start: '2026-04-01T00:00:00+00:00',
      date_range_end: '2026-04-30T00:00:00+00:00'
    })
  );
  const runningSpec = sqlString(
    JSON.stringify({
      strategy_versions: [['alpha', 'alpha-v1']],
      dataset: {
        source: 'fixture',
        version: 'v1',
        coverage_start: '2026-04-15T00:00:00+00:00',
        coverage_end: '2026-04-20T00:00:00+00:00',
        market_universe_filter: { market_ids: ['market-a'] },
        data_quality_gaps: []
      },
      execution_model: {
        fee_rate: 0.0,
        slippage_bps: 0.0,
        latency_ms: 0.0,
        staleness_ms: 60000.0,
        fill_policy: 'immediate_or_cancel'
      },
      risk_policy: {
        max_position_notional_usdc: 100.0,
        max_daily_drawdown_pct: 2.5,
        min_order_size_usdc: 1.0
      },
      date_range_start: '2026-04-15T00:00:00+00:00',
      date_range_end: '2026-04-20T00:00:00+00:00'
    })
  );
  const alphaTargets = sqlString(
    portfolioTargetJson([
      { marketId: 'market-a', tokenId: 'token-a', timestamp: '2026-04-09T10:00:00+00:00' },
      { marketId: 'market-b', tokenId: 'token-b', timestamp: '2026-04-09T11:00:00+00:00' }
    ])
  );
  const betaTargets = sqlString(
    portfolioTargetJson([
      { marketId: 'market-c', tokenId: 'token-c', timestamp: '2026-04-09T12:00:00+00:00' },
      { marketId: 'market-d', tokenId: 'token-d', timestamp: '2026-04-09T13:00:00+00:00' }
    ])
  );
  const gammaTargets = sqlString(
    portfolioTargetJson([
      { marketId: 'market-e', tokenId: 'token-e', timestamp: '2026-04-09T14:00:00+00:00' }
    ])
  );
  const rankedStrategies = sqlString(
    JSON.stringify([
      {
        strategy_id: 'alpha',
        strategy_version_id: 'alpha-v1',
        metric_value: 2.5,
        rank: 1
      }
    ])
  );

  executeSql(`
    INSERT INTO strategies (strategy_id, active_version_id)
    VALUES
      ('alpha', 'alpha-v1'),
      ('beta', 'beta-v1'),
      ('gamma', 'gamma-v1');

    INSERT INTO strategy_versions (strategy_version_id, strategy_id, config_json)
    VALUES
      ('alpha-v1', 'alpha', '${alphaConfig}'::jsonb),
      ('beta-v1', 'beta', '${betaConfig}'::jsonb),
      ('gamma-v1', 'gamma', '${gammaConfig}'::jsonb);

    INSERT INTO backtest_runs (
      run_id,
      spec_hash,
      status,
      strategy_ids,
      date_range_start,
      date_range_end,
      exec_config_json,
      spec_json,
      queued_at,
      started_at,
      finished_at
    ) VALUES
      (
        '${completedRunId}'::uuid,
        'spec-completed',
        'completed',
        ARRAY['alpha', 'beta', 'gamma']::text[],
        '2026-04-01T00:00:00+00:00',
        '2026-04-30T00:00:00+00:00',
        '{"chunk_days":7,"time_budget":1800}'::jsonb,
        '${completedSpec}'::jsonb,
        '2026-04-18T09:00:00+00:00',
        '2026-04-18T09:00:05+00:00',
        '2026-04-18T09:03:05+00:00'
      ),
      (
        '22222222-2222-2222-2222-222222222222'::uuid,
        'spec-running',
        'running',
        ARRAY['alpha']::text[],
        '2026-04-15T00:00:00+00:00',
        '2026-04-20T00:00:00+00:00',
        '{"chunk_days":7,"time_budget":900}'::jsonb,
        '${runningSpec}'::jsonb,
        '2026-04-19T09:00:00+00:00',
        '2026-04-19T09:00:10+00:00',
        NULL
      );

    INSERT INTO strategy_runs (
      strategy_run_id,
      run_id,
      strategy_id,
      strategy_version_id,
      brier,
      pnl_cum,
      drawdown_max,
      fill_rate,
      slippage_bps,
      opportunity_count,
      decision_count,
      fill_count,
      portfolio_target_json,
      started_at,
      finished_at
    ) VALUES
      (
        'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'::uuid,
        '${completedRunId}'::uuid,
        'alpha',
        'alpha-v1',
        0.11,
        3.0,
        4.0,
        0.92,
        8.0,
        6,
        5,
        3,
        '${alphaTargets}'::jsonb,
        '2026-04-18T09:00:05+00:00',
        '2026-04-18T09:01:05+00:00'
      ),
      (
        'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb'::uuid,
        '${completedRunId}'::uuid,
        'beta',
        'beta-v1',
        0.26,
        12.0,
        3.0,
        0.81,
        12.0,
        5,
        4,
        2,
        '${betaTargets}'::jsonb,
        '2026-04-18T09:00:10+00:00',
        '2026-04-18T09:01:10+00:00'
      ),
      (
        'cccccccc-cccc-cccc-cccc-cccccccccccc'::uuid,
        '${completedRunId}'::uuid,
        'gamma',
        'gamma-v1',
        0.18,
        -1.0,
        1.0,
        0.67,
        6.0,
        4,
        4,
        1,
        '${gammaTargets}'::jsonb,
        '2026-04-18T09:00:20+00:00',
        '2026-04-18T09:01:20+00:00'
      );

    INSERT INTO fills (
      fill_id,
      order_id,
      market_id,
      ts,
      strategy_id,
      strategy_version_id
    ) VALUES
      ('fill-alpha-1', 'order-alpha-1', 'market-a', '2026-04-10T12:00:00+00:00', 'alpha', 'alpha-v1'),
      ('fill-alpha-2', 'order-alpha-2', 'market-c', '2026-04-11T12:00:00+00:00', 'alpha', 'alpha-v1');

    INSERT INTO opportunities (
      opportunity_id,
      market_id,
      token_id,
      side,
      selected_factor_values,
      expected_edge,
      rationale,
      target_size_usdc,
      expiry,
      staleness_policy,
      strategy_id,
      strategy_version_id,
      created_at
    ) VALUES
      (
        'opp-alpha-1',
        'market-a',
        'token-a',
        'yes',
        '{}'::jsonb,
        0.05,
        'fixture',
        25.0,
        NULL,
        'fresh',
        'alpha',
        'alpha-v1',
        '2026-04-10T08:00:00+00:00'
      ),
      (
        'opp-alpha-2',
        'market-c',
        'token-c',
        'yes',
        '{}'::jsonb,
        0.04,
        'fixture',
        20.0,
        NULL,
        'fresh',
        'alpha',
        'alpha-v1',
        '2026-04-11T08:00:00+00:00'
      );

    INSERT INTO evaluation_reports (
      report_id,
      run_id,
      ranking_metric,
      ranked_strategies,
      benchmark_rows,
      attribution_commentary,
      warnings,
      next_action,
      generated_at
    ) VALUES
      (
        'dddddddd-dddd-dddd-dddd-dddddddddddd'::uuid,
        '${completedRunId}'::uuid,
        'pnl_cum',
        '${rankedStrategies}'::jsonb,
        '[]'::jsonb,
        'fixture',
        '[]'::jsonb,
        'fixture',
        '2026-04-12T12:00:00+00:00'
      );
  `);
}

test.beforeEach(() => {
  fs.mkdirSync(evidenceDir, { recursive: true });
  seedBacktestWorkspace();
});

test('backtest list view exports YAML and redirects after enqueue', async ({ page }) => {
  await page.goto('/backtest');

  await expect(page.getByTestId('backtest-run-row')).toHaveCount(2);
  await page.getByTestId('new-sweep-open').click();
  await expect(page.getByTestId('new-sweep-modal')).toBeVisible();
  await page.getByLabel('alpha').check();
  await page.getByLabel('beta').check();

  const [download] = await Promise.all([
    page.waitForEvent('download'),
    page.getByTestId('export-sweep-yaml').click()
  ]);
  const downloadPath = path.join(os.tmpdir(), `cp10-${download.suggestedFilename()}`);
  await download.saveAs(downloadPath);
  expect(fs.readFileSync(downloadPath, 'utf8')).toContain('base_spec:');
  execFileSync(
    'uv',
    ['run', 'python', '-c', 'import sys, yaml; yaml.safe_load(open(sys.argv[1]).read())', downloadPath],
    { stdio: 'pipe' }
  );

  await page.getByTestId('run-sweep-here').click();
  await expect(page).toHaveURL(/\/backtest\/[0-9a-f-]+$/);
  await expect(page.getByTestId('backtest-run-view')).toBeVisible();
});

test('run detail supports sorting, inline expansion, and bookmarkable details', async ({ page }) => {
  await page.goto(`/backtest/${completedRunId}`);

  await expect(page.getByTestId('backtest-run-view')).toBeVisible();
  await expect(page.getByTestId('strategy-row-alpha')).toBeVisible();
  await expect(page.locator('[data-testid^="strategy-row-"]').first()).toContainText('alpha');

  await page.getByLabel('Ranking metric').selectOption('sharpe');
  await expect(page.locator('[data-testid^="strategy-row-"]').first()).toContainText('beta');

  await page.getByTestId('strategy-row-alpha').click();
  await expect(page.getByTestId('strategy-detail-panel-alpha')).toBeVisible();
  await page.getByTestId('strategy-row-alpha').click();
  await expect(page.getByTestId('strategy-detail-panel-alpha')).toBeHidden();

  await page.getByTestId('strategy-row-beta').click();
  await page.getByTestId('strategy-detail-link-beta').click();
  await expect(page).toHaveURL(new RegExp(`/backtest/${completedRunId}/beta$`));
  await expect(page.getByTestId('backtest-strategy-detail-view')).toBeVisible();

  await page.goto(`/backtest/${completedRunId}/beta`);
  await expect(page.getByTestId('backtest-strategy-detail-view')).toBeVisible();
});

test('compare workspace computes and renders the four result panels', async ({ page }) => {
  await page.goto(`/backtest/${completedRunId}`);

  await page.getByTestId('compare-with-live').click();
  await expect(page).toHaveURL(new RegExp(`/backtest/${completedRunId}/compare$`));
  await page.getByLabel('Strategy').selectOption('alpha::alpha-v1');
  await page.getByLabel('Live window start').fill('2026-04-10');
  await page.getByLabel('Live window end').fill('2026-04-12');
  await page.getByLabel('Overlap denominator').selectOption('union');
  await page.getByTestId('compute-comparison').click();

  await expect(page.getByTestId('compare-overlap-panel')).toBeVisible();
  await expect(page.getByTestId('compare-equity-panel')).toBeVisible();
  await expect(page.getByTestId('compare-backtest-only-panel')).toBeVisible();
  await expect(page.getByTestId('compare-live-only-panel')).toBeVisible();
  await expect(page.getByTestId('compare-overlap-value')).not.toHaveText('');
  await expect(page.getByTestId('compare-equity-table')).toBeVisible();

  await page.screenshot({
    path: path.join(evidenceDir, 'backtest-compare-page.png'),
    fullPage: true
  });
});
