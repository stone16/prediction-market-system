import fs from 'node:fs';
import path from 'node:path';
import { expect, test } from '@playwright/test';

const evidenceDir = path.resolve(
  process.cwd(),
  '..',
  '.harness',
  'cathedral-v1',
  'checkpoints',
  '06',
  'iter-1',
  'evidence'
);

test.beforeEach(() => {
  fs.mkdirSync(evidenceDir, { recursive: true });
});

test('onboarding persists across dismiss and returns after localStorage reset', async ({ page }) => {
  const errors: string[] = [];

  page.on('console', (message) => {
    if (message.type() === 'error') {
      errors.push(message.text());
    }
  });
  page.on('pageerror', (error) => {
    errors.push(error.message);
  });

  await page.goto('/');
  await page.evaluate(() => {
    window.localStorage.removeItem('pms.onboarded');
  });
  await page.reload();
  await expect(page.getByRole('dialog', { name: 'Quick start' })).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: path.join(evidenceDir, 'cp06-home-onboarding.png')
  });

  await page.getByRole('button', { name: 'Dismiss onboarding' }).click();
  await expect(page.getByRole('dialog', { name: 'Quick start' })).toBeHidden();

  await page.reload();
  await expect(page.getByRole('dialog', { name: 'Quick start' })).toBeHidden();

  await page.evaluate(() => {
    window.localStorage.removeItem('pms.onboarded');
  });
  await page.reload();
  await expect(page.getByRole('dialog', { name: 'Quick start' })).toBeVisible();

  expect(errors).toEqual([]);
});

test('positions and trades pages render populated and empty states without console errors', async ({
  page
}) => {
  const errors: string[] = [];
  let positionsPayload = {
    positions: [
      {
        market_id: 'market-000',
        token_id: 'market-000-yes',
        venue: 'polymarket',
        side: 'BUY',
        shares_held: 50.0,
        avg_entry_price: 0.41,
        unrealized_pnl: 0.0,
        locked_usdc: 20.5
      }
    ]
  };
  let tradesPayload = {
    trades: [
      {
        trade_id: 'trade-000',
        fill_id: 'fill-000',
        order_id: 'order-000',
        decision_id: 'decision-000',
        market_id: 'market-000',
        question: 'Will market 000 settle above consensus?',
        token_id: 'market-000-yes',
        venue: 'polymarket',
        side: 'BUY',
        fill_price: 0.41,
        fill_notional_usdc: 20.5,
        fill_quantity: 50.0,
        executed_at: '2026-04-23T10:00:00+00:00',
        filled_at: '2026-04-23T10:00:00+00:00',
        status: 'matched',
        strategy_id: 'default',
        strategy_version_id: 'default-v1'
      }
    ],
    limit: 20
  };

  page.on('console', (message) => {
    if (message.type() === 'error') {
      errors.push(message.text());
    }
  });
  page.on('pageerror', (error) => {
    errors.push(error.message);
  });
  await page.route('**/api/pms/positions', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify(positionsPayload)
    });
  });
  await page.route('**/api/pms/trades?limit=20', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify(tradesPayload)
    });
  });

  await page.goto('/positions');
  await expect(page.getByRole('heading', { name: 'Positions' })).toBeVisible();
  await expect(page.getByRole('cell', { name: 'market-000', exact: true })).toBeVisible();
  await page.screenshot({
    fullPage: true,
    path: path.join(evidenceDir, 'cp06-positions-populated.png')
  });

  positionsPayload = { positions: [] };
  await page.goto('/positions');
  await expect(page.getByText('No open positions yet.')).toBeVisible();
  await page.screenshot({
    fullPage: true,
    path: path.join(evidenceDir, 'cp06-positions-empty.png')
  });

  await page.goto('/trades');
  await expect(page.getByRole('heading', { name: 'Trades' })).toBeVisible();
  await expect(page.getByText('Will market 000 settle above consensus?')).toBeVisible();
  await page.screenshot({
    fullPage: true,
    path: path.join(evidenceDir, 'cp06-trades-populated.png')
  });

  tradesPayload = { trades: [], limit: 20 };
  await page.goto('/trades');
  await expect(page.getByText('No trades yet.')).toBeVisible();
  await page.screenshot({
    fullPage: true,
    path: path.join(evidenceDir, 'cp06-trades-empty.png')
  });

  expect(errors).toEqual([]);
});
