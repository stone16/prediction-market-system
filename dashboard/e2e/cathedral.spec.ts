import fs from 'node:fs';
import path from 'node:path';
import { expect, test, type Page } from '@playwright/test';

const evidenceDir = path.resolve(
  process.cwd(),
  '..',
  '.harness',
  'cathedral-v1',
  'checkpoints',
  '12',
  'iter-1',
  'evidence'
);

const idea = {
  decision_id: 'decision-cp12',
  market_id: 'market-cp12',
  token_id: 'token-cp12-yes',
  venue: 'polymarket',
  side: 'BUY',
  notional_usdc: 25,
  order_type: 'limit',
  max_slippage_bps: 50,
  stop_conditions: ['cp12'],
  prob_estimate: 0.67,
  expected_edge: 0.18,
  time_in_force: 'GTC',
  opportunity_id: 'opportunity-cp12',
  strategy_id: 'default',
  strategy_version_id: 'default-v1',
  limit_price: 0.41,
  action: 'BUY',
  outcome: 'YES',
  model_id: 'model-cp12',
  status: 'pending',
  factor_snapshot_hash: 'snapshot-cp12',
  created_at: '2026-04-23T10:00:00+00:00',
  updated_at: '2026-04-23T10:00:00+00:00',
  expires_at: '2026-04-23T10:15:00+00:00',
  forecaster: 'model-e2e',
  kelly_size: 25,
  opportunity: {
    opportunity_id: 'opportunity-cp12',
    market_id: 'market-cp12',
    token_id: 'token-cp12-yes',
    side: 'yes',
    selected_factor_values: { edge: 0.18, liquidity: 0.04 },
    expected_edge: 0.18,
    rationale: 'Edge is high while liquidity remains deep.',
    target_size_usdc: 25,
    expiry: '2026-04-23T10:15:00+00:00',
    staleness_policy: 'cp12',
    strategy_id: 'default',
    strategy_version_id: 'default-v1',
    created_at: '2026-04-23T10:00:00+00:00',
    factor_snapshot_hash: 'snapshot-cp12',
    composition_trace: { kind: 'e2e' }
  }
};

async function seedCathedralRoutes(page: Page) {
  let accepted = false;

  await page.route('**/api/pms/status', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify({
        mode: 'paper',
        source: 'live',
        runner_started_at: '2026-04-23T10:00:00+00:00',
        running: true,
        sensors: [{ name: 'Historical feed', status: 'live', last_signal_at: '2026-04-23T10:02:00+00:00' }],
        controller: { decisions_total: 3 },
        actuator: { fills_total: accepted ? 1 : 0, mode: 'paper' },
        evaluator: { eval_records_total: 5, brier_overall: 0.18 }
      })
    });
  });
  await page.route('**/api/pms/metrics', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify({
        brier_overall: 0.18,
        brier_by_category: {},
        pnl: 12.5,
        slippage_bps: 9.2,
        fill_rate: 0.8,
        win_rate: 0.6,
        per_strategy: [],
        ops_view: {
          brier_overall: 0.18,
          brier_by_category: {},
          pnl: 12.5,
          slippage_bps: 9.2,
          fill_rate: 0.8,
          win_rate: 0.6
        }
      })
    });
  });
  await page.route('**/api/pms/feedback?resolved=false', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify([
        {
          feedback_id: 'fb-cp12',
          target: 'review',
          source: 'runtime',
          message: 'One open review item remains.',
          severity: 'warning',
          created_at: '2026-04-23T10:01:00+00:00',
          resolved: false,
          resolved_at: null,
          category: 'review',
          metadata: {}
        }
      ])
    });
  });
  await page.route('**/api/pms/markets**', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify({
        markets: [
          {
            market_id: 'market-cp12',
            question: 'Will market-cp12 settle above consensus?',
            venue: 'polymarket',
            volume_24h: 2400,
            updated_at: '2026-04-23T10:03:00+00:00',
            yes_token_id: 'token-cp12-yes',
            no_token_id: 'token-cp12-no',
            subscribed: true
          }
        ],
        limit: 20,
        offset: 0,
        total: 1
      })
    });
  });
  await page.route('**/api/pms/decisions?status=pending&include=opportunity&limit=50', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify([idea])
    });
  });
  await page.route('**/api/pms/decisions/decision-cp12/accept', async (route) => {
    accepted = true;
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify({ decision_id: 'decision-cp12', status: 'accepted', fill_id: 'fill-cp12' })
    });
  });
  await page.route('**/api/pms/trades?limit=20', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify({
        limit: 20,
        trades: accepted
          ? [
              {
                trade_id: 'trade-cp12',
                fill_id: 'fill-cp12',
                order_id: 'order-cp12',
                decision_id: 'decision-cp12',
                market_id: 'market-cp12',
                question: 'Will market-cp12 settle above consensus?',
                token_id: 'token-cp12-yes',
                venue: 'polymarket',
                side: 'BUY',
                fill_price: 0.41,
                fill_notional_usdc: 25,
                fill_quantity: 60.9,
                executed_at: '2026-04-23T10:03:00+00:00',
                filled_at: '2026-04-23T10:03:00+00:00',
                status: 'matched',
                strategy_id: 'default',
                strategy_version_id: 'default-v1'
              }
            ]
          : []
      })
    });
  });
}

test.beforeEach(() => {
  fs.mkdirSync(evidenceDir, { recursive: true });
});

test('cathedral happy path runs from today hero through first trade', async ({ page }) => {
  const errors: string[] = [];
  const nav = page.getByRole('navigation', { name: 'Dashboard navigation' });
  page.on('console', (message) => {
    if (message.type() === 'error') errors.push(message.text());
  });
  page.on('pageerror', (error) => errors.push(error.message));

  await seedCathedralRoutes(page);
  await page.goto('/');

  await expect(page.getByRole('heading', { level: 2, name: 'Quick start' })).toBeVisible();
  await page.getByRole('button', { name: 'Dismiss onboarding' }).click();
  await expect(page.getByTestId('dashboard-hero')).toBeVisible();
  await expect(page.getByTestId('today-feed')).toBeVisible();
  await page.screenshot({ path: path.join(evidenceDir, 'today-home.png'), fullPage: true });

  await nav.getByRole('link', { name: 'Markets', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Markets' })).toBeVisible();
  await expect(page.getByText('Will market-cp12 settle above consensus?')).toBeVisible();

  await nav.getByRole('link', { name: 'Ideas', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Ideas' })).toBeVisible();
  await page.getByRole('button', { name: 'Accept' }).click();
  await expect(page.getByRole('link', { name: 'First trade placed · View in /trades' })).toBeVisible();
  await page.getByRole('link', { name: 'First trade placed · View in /trades' }).click();

  await expect(page).toHaveURL(/\/trades/);
  await expect(page.getByText('Will market-cp12 settle above consensus?')).toBeVisible();
  expect(errors).toEqual([]);
});
