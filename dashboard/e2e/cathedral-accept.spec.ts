import fs from 'node:fs';
import path from 'node:path';
import { expect, test, type Page } from '@playwright/test';

const evidenceDir = path.resolve(
  process.cwd(),
  '..',
  '.harness',
  'cathedral-v1',
  'checkpoints',
  '09',
  'iter-1',
  'evidence'
);

const idea = {
  decision_id: 'decision-e2e',
  market_id: 'market-e2e',
  token_id: 'token-e2e-yes',
  venue: 'polymarket',
  side: 'BUY',
  notional_usdc: 25,
  order_type: 'limit',
  max_slippage_bps: 50,
  stop_conditions: ['cp09'],
  prob_estimate: 0.67,
  expected_edge: 0.18,
  time_in_force: 'GTC',
  opportunity_id: 'opportunity-e2e',
  strategy_id: 'default',
  strategy_version_id: 'default-v1',
  limit_price: 0.41,
  action: 'BUY',
  outcome: 'YES',
  model_id: 'model-e2e',
  status: 'pending',
  factor_snapshot_hash: 'snapshot-e2e',
  created_at: '2026-04-23T10:00:00+00:00',
  updated_at: '2026-04-23T10:00:00+00:00',
  expires_at: '2026-04-23T10:15:00+00:00',
  forecaster: 'model-e2e',
  kelly_size: 25,
  opportunity: {
    opportunity_id: 'opportunity-e2e',
    market_id: 'market-e2e',
    token_id: 'token-e2e-yes',
    side: 'yes',
    selected_factor_values: { edge: 0.18, liquidity: 0.04 },
    expected_edge: 0.18,
    rationale: 'Edge is high while liquidity remains deep.',
    target_size_usdc: 25,
    expiry: '2026-04-23T10:15:00+00:00',
    staleness_policy: 'cp09',
    strategy_id: 'default',
    strategy_version_id: 'default-v1',
    created_at: '2026-04-23T10:00:00+00:00',
    factor_snapshot_hash: 'snapshot-e2e',
    composition_trace: { kind: 'e2e' }
  }
};

async function seedIdeaRoutes(page: Page, options: { conflict?: boolean } = {}) {
  let accepted = false;
  const networkLog: string[] = [];

  await page.route('**/api/pms/decisions?status=pending&include=opportunity&limit=50', async (route) => {
    networkLog.push(route.request().url());
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify([idea])
    });
  });
  await page.route('**/api/pms/decisions/decision-e2e?include=opportunity', async (route) => {
    networkLog.push(route.request().url());
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify({ ...idea, factor_snapshot_hash: 'snapshot-fresh' })
    });
  });
  await page.route('**/api/pms/decisions/decision-e2e/accept', async (route) => {
    networkLog.push(route.request().url());
    if (options.conflict) {
      await route.fulfill({
        status: 409,
        contentType: 'application/json',
        body: JSON.stringify({
          detail: 'market_changed',
          current_factor_snapshot_hash: 'snapshot-fresh'
        })
      });
      return;
    }
    accepted = true;
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify({ decision_id: 'decision-e2e', status: 'accepted', fill_id: null })
    });
  });
  await page.route('**/api/pms/trades?limit=20', async (route) => {
    networkLog.push(route.request().url());
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify({
        limit: 20,
        trades: accepted
          ? [
              {
                trade_id: 'trade-e2e',
                fill_id: 'fill-e2e',
                order_id: 'order-e2e',
                decision_id: 'decision-e2e',
                market_id: 'market-e2e',
                question: 'Will market-e2e settle above consensus?',
                token_id: 'token-e2e-yes',
                venue: 'polymarket',
                side: 'BUY',
                fill_price: 0.41,
                fill_notional_usdc: 25,
                fill_quantity: 60.9,
                executed_at: '2026-04-23T10:00:00+00:00',
                filled_at: '2026-04-23T10:00:00+00:00',
                status: 'matched',
                strategy_id: 'default',
                strategy_version_id: 'default-v1'
              }
            ]
          : []
      })
    });
  });

  return networkLog;
}

test.beforeEach(() => {
  fs.mkdirSync(evidenceDir, { recursive: true });
});

test('accepting an idea shows pending, success toast, and a trade row', async ({ page }) => {
  const errors: string[] = [];
  page.on('console', (message) => {
    if (message.type() === 'error') errors.push(message.text());
  });
  page.on('pageerror', (error) => errors.push(error.message));

  await seedIdeaRoutes(page);
  await page.goto('/ideas');

  await expect(page.getByRole('heading', { name: 'Ideas' })).toBeVisible();
  await expect(page.getByTestId('idea-card')).toHaveCount(1);
  await page.getByRole('button', { name: 'Why' }).click();
  await expect(page.getByRole('dialog', { name: 'Why this idea?' })).toBeVisible();
  await page.screenshot({ path: path.join(evidenceDir, 'ideas-why-open.png'), fullPage: true });
  await page.keyboard.press('Escape');

  await page.getByRole('button', { name: 'Accept' }).click();
  await expect(page.getByRole('button', { name: 'Accepting...' })).toBeDisabled();
  await expect(page.getByRole('link', { name: 'First trade placed · View in /trades' })).toBeVisible();
  await page.screenshot({ path: path.join(evidenceDir, 'ideas-success-toast.png'), fullPage: true });
  await page.getByRole('link', { name: 'First trade placed · View in /trades' }).click();

  await expect(page).toHaveURL(/\/trades/);
  await expect(page.getByText('Will market-e2e settle above consensus?')).toBeVisible();
  expect(errors).toEqual([]);
});

test('409 conflict shows market-changed toast and refetches the idea', async ({ page }) => {
  const networkLog = await seedIdeaRoutes(page, { conflict: true });
  await page.goto('/ideas');

  const accept = page.getByRole('button', { name: 'Accept' });
  await accept.click();

  await expect(page.getByText('Market changed... refresh loaded')).toBeVisible();
  await expect(accept).toBeDisabled();
  await expect.poll(() => networkLog.some((url) => url.includes('/api/pms/decisions/decision-e2e?include=opportunity'))).toBeTruthy();
  await page.waitForTimeout(700);
  await expect(accept).toBeEnabled();
  await page.screenshot({ path: path.join(evidenceDir, 'ideas-conflict-toast.png'), fullPage: true });
});
