import fs from 'node:fs';
import path from 'node:path';
import { expect, test } from '@playwright/test';

const evidenceDir = path.resolve(
  process.cwd(),
  '..',
  '.harness',
  'pms-markets-browser-v1',
  'checkpoints',
  '09',
  'iter-1',
  'evidence'
);

test.beforeEach(() => {
  fs.mkdirSync(evidenceDir, { recursive: true });
});

test('markets page renders redesigned columns at desktop width without console errors', async ({ page }) => {
  const errors: string[] = [];
  const marketsPayload = {
    markets: [
      {
        market_id: 'market-000',
        question: 'Will market 000 settle above consensus?',
        venue: 'polymarket',
        volume_24h: 2400.5,
        updated_at: '2026-04-23T10:00:00+00:00',
        yes_token_id: 'market-000-yes',
        no_token_id: 'market-000-no',
        yes_price: 0.525,
        no_price: 0.475,
        best_bid: 0.51,
        best_ask: 0.54,
        last_trade_price: 0.52,
        liquidity: 34000.25,
        spread_bps: 300,
        price_updated_at: new Date().toISOString(),
        resolves_at: '2026-05-01T00:00:00+00:00',
        subscription_source: 'user',
        subscribed: true
      }
    ],
    limit: 20,
    offset: 0,
    total: 1
  };

  page.on('console', (message) => {
    if (message.type() === 'error') {
      errors.push(message.text());
    }
  });
  page.on('pageerror', (error) => {
    errors.push(error.message);
  });

  await page.setViewportSize({ width: 1440, height: 1024 });
  await page.route('**/api/pms/markets?limit=20', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify(marketsPayload)
    });
  });
  await page.route('**/api/pms/status', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify({
        mode: 'paper',
        source: 'live',
        runner_started_at: '2026-04-23T10:00:00+00:00',
        running: true,
        sensors: [],
        controller: { decisions_total: 1 },
        actuator: { fills_total: 1, mode: 'paper' },
        evaluator: { eval_records_total: 1, brier_overall: 0.12 }
      })
    });
  });

  await page.goto('/markets');
  await expect(page.getByRole('heading', { name: 'Markets' })).toBeVisible();
  await expect(page.getByRole('columnheader', { name: 'Market' })).toBeVisible();
  await expect(page.getByRole('columnheader', { name: 'YES' })).toBeVisible();
  await expect(page.getByRole('columnheader', { name: 'NO' })).toBeVisible();
  await expect(page.getByRole('columnheader', { name: 'Vol 24h' })).toBeVisible();
  await expect(page.getByRole('columnheader', { name: 'Liquidity' })).toBeVisible();
  await expect(page.getByRole('columnheader', { name: 'Spread' })).toBeVisible();
  await expect(page.getByRole('columnheader', { name: 'Resolves' })).toBeVisible();
  await expect(page.getByRole('columnheader', { name: 'Subscription' })).toBeVisible();
  await expect(page.getByText('Will market 000 settle above consensus?')).toBeVisible();
  await expect(page.getByText('52.5%')).toBeVisible();
  await expect(page.getByText('47.5%')).toBeVisible();
  await expect(page.getByText('300 bps')).toBeVisible();
  await expect(page.getByLabel('User subscription')).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: path.join(evidenceDir, 'cp09-markets-table.png')
  });

  expect(errors).toEqual([]);
});
