import fs from 'node:fs';
import path from 'node:path';
import { expect, test } from '@playwright/test';

const evidenceDir = path.resolve(
  process.cwd(),
  '..',
  '.harness',
  'pms-markets-browser-v1',
  'checkpoints',
  '11',
  'iter-1',
  'evidence'
);

test.beforeEach(() => {
  fs.mkdirSync(evidenceDir, { recursive: true });
});

test('markets page opens the detail drawer from a row and restores it after reload', async ({ page }) => {
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
        subscription_source: null,
        subscribed: false
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
  await page.route('**/api/pms/markets/market-000/price-history**', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify({
        condition_id: 'market-000',
        snapshots: [
          {
            snapshot_at: '2026-04-24T11:57:00+00:00',
            yes_price: 0.51,
            no_price: 0.49,
            best_bid: 0.5,
            best_ask: 0.52,
            last_trade_price: 0.51,
            liquidity: 2400,
            volume_24h: 1000
          },
          {
            snapshot_at: '2026-04-24T11:58:00+00:00',
            yes_price: 0.53,
            no_price: 0.47,
            best_bid: 0.52,
            best_ask: 0.54,
            last_trade_price: 0.53,
            liquidity: 2450,
            volume_24h: 1025
          },
          {
            snapshot_at: '2026-04-24T11:59:00+00:00',
            yes_price: 0.55,
            no_price: 0.45,
            best_bid: 0.54,
            best_ask: 0.56,
            last_trade_price: 0.55,
            liquidity: 2500,
            volume_24h: 1050
          }
        ]
      })
    });
  });
  await page.route('**/api/pms/markets/market-000-yes/subscribe', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify({
        token_id: 'market-000-yes',
        source: 'user',
        created_at: '2026-04-24T12:00:00+00:00'
      })
    });
  });

  await page.goto('/markets');
  await expect(page.getByRole('heading', { name: 'Markets' })).toBeVisible();
  await page.getByRole('row', { name: /Will market 000 settle above consensus/i }).click();
  await expect(page).toHaveURL(/\/markets\?detail=market-000/);
  await expect(page.getByRole('dialog', { name: 'Market details' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Will market 000 settle above consensus?' })).toBeVisible();
  await expect(page.getByText('market-000-yes')).toBeVisible();
  await expect(page.getByTestId('price-history-line-chart')).toBeVisible();
  await page.getByRole('button', { name: 'Subscribe market' }).click();
  await expect(page.getByLabel('User subscription')).toBeVisible();
  await page.getByRole('button', { name: 'Close market details' }).click();
  await expect(page.getByRole('dialog', { name: 'Market details' })).toBeHidden();
  await page.getByRole('row', { name: /Will market 000 settle above consensus/i }).click();
  await expect(page.getByLabel('User subscription')).toBeVisible();

  await page.reload();
  await expect(page.getByRole('dialog', { name: 'Market details' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Will market 000 settle above consensus?' })).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: path.join(evidenceDir, 'cp11-market-detail-chart-subscribe.png')
  });

  expect(errors).toEqual([]);
});
