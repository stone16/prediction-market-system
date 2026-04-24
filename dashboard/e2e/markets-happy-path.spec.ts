import fs from 'node:fs';
import path from 'node:path';
import { expect, test, type Route } from '@playwright/test';

const evidenceDir = path.resolve(
  process.cwd(),
  '..',
  '.harness',
  'pms-markets-browser-v1',
  'checkpoints',
  '14',
  'iter-1',
  'evidence'
);

type MarketFixture = {
  id: string;
  index: number;
  subscribed: boolean;
};

function marketRow({ id, index, subscribed }: MarketFixture) {
  const yesPrice = 0.35 + (index % 40) / 100;
  return {
    market_id: id,
    question: `Will market ${String(index).padStart(3, '0')} settle above consensus?`,
    venue: 'polymarket',
    volume_24h: 100000 + index * 250,
    updated_at: '2026-04-23T10:00:00+00:00',
    yes_token_id: `${id}-yes`,
    no_token_id: `${id}-no`,
    yes_price: yesPrice,
    no_price: 1 - yesPrice,
    best_bid: yesPrice - 0.01,
    best_ask: yesPrice + 0.01,
    last_trade_price: yesPrice,
    liquidity: 25000 + index * 10,
    spread_bps: 200,
    price_updated_at: new Date().toISOString(),
    resolves_at: '2026-05-01T00:00:00+00:00',
    subscription_source: subscribed ? 'user' : null,
    subscribed
  };
}

function allMarkets() {
  return Array.from({ length: 485 }, (_, index) =>
    marketRow({
      id: `market-${String(index + 1).padStart(3, '0')}`,
      index: index + 1,
      subscribed: (index + 1) % 3 === 0
    })
  );
}

async function fulfillJson(route: Route, body: unknown) {
  await route.fulfill({
    contentType: 'application/json',
    body: JSON.stringify(body)
  });
}

async function routeStatus(route: Route) {
  await fulfillJson(route, {
    mode: 'paper',
    source: 'live',
    runner_started_at: '2026-04-23T10:00:00+00:00',
    running: true,
    sensors: [],
    controller: { decisions_total: 1 },
    actuator: { fills_total: 1, mode: 'paper' },
    evaluator: { eval_records_total: 1, brier_overall: 0.12 }
  });
}

test.beforeEach(() => {
  fs.mkdirSync(evidenceDir, { recursive: true });
});

test('markets happy path paginates, changes size, filters, and resets to page one', async ({
  page
}) => {
  const errors: string[] = [];
  const rows = allMarkets();

  page.on('console', (message) => {
    if (message.type() === 'error') {
      errors.push(message.text());
    }
  });
  page.on('pageerror', (error) => {
    errors.push(error.message);
  });

  await page.route('**/api/pms/status', routeStatus);
  await page.route('**/api/pms/markets**', async (route) => {
    const requestUrl = new URL(route.request().url());
    const limit = Number(requestUrl.searchParams.get('limit') ?? '50');
    const offset = Number(requestUrl.searchParams.get('offset') ?? '0');
    const subscribed = requestUrl.searchParams.get('subscribed') ?? 'all';
    const filteredRows =
      subscribed === 'only' ? rows.filter((row) => row.subscribed) : rows;
    await fulfillJson(route, {
      markets: filteredRows.slice(offset, offset + limit),
      limit,
      offset,
      total: filteredRows.length
    });
  });

  await page.goto('/markets');
  await expect(page.getByRole('heading', { name: 'Markets' })).toBeVisible();
  await expect(page.getByText('1-50 of 485')).toBeVisible();
  await expect(page.getByText('Page 1 of 10')).toBeVisible();
  await expect(page.getByRole('row', { name: /Will market 001 settle/i })).toBeVisible();

  await page.getByRole('button', { name: 'Next page' }).click();
  await expect(page).toHaveURL(/page=2/);
  await expect(page.getByText('51-100 of 485')).toBeVisible();
  await expect(page.getByRole('row', { name: /Will market 051 settle/i })).toBeVisible();

  await page.getByLabel('Page number').fill('10');
  await expect(page).toHaveURL(/page=10/);
  await expect(page.getByText('451-485 of 485')).toBeVisible();

  await page.getByLabel('Page size').selectOption('100');
  await expect(page).not.toHaveURL(/page=10/);
  await expect(page).toHaveURL(/limit=100/);
  await expect(page.getByText('1-100 of 485')).toBeVisible();
  await expect(page.getByText('Page 1 of 5')).toBeVisible();

  await page.getByRole('button', { name: 'Next page' }).click();
  await expect(page).toHaveURL(/page=2/);
  const filterRegion = page.getByLabel('Market filters');
  await filterRegion.getByRole('button', { name: 'Filters' }).click();
  await filterRegion.getByLabel(/Subscription/i).selectOption('only');
  await expect(page).not.toHaveURL(/page=2/);
  await expect(page.getByText('1-100 of 161')).toBeVisible();
  await expect(page.getByText('Subscribed only')).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: path.join(evidenceDir, 'cp14-markets-happy-path.png')
  });

  expect(errors).toEqual([]);
});
