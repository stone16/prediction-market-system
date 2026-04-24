import fs from 'node:fs';
import path from 'node:path';
import { expect, test, type Route } from '@playwright/test';

const evidenceDir = path.resolve(
  process.cwd(),
  '..',
  '.harness',
  'pms-markets-browser-v1',
  'checkpoints',
  '13',
  'iter-1',
  'evidence'
);

type MarketFixture = {
  id: string;
  question: string;
  volume24h: number;
  liquidity: number;
  spreadBps: number;
  yesPrice: number;
  resolvesAt: string;
  subscribed: boolean;
};

function marketRow({
  id,
  question,
  volume24h,
  liquidity,
  spreadBps,
  yesPrice,
  resolvesAt,
  subscribed
}: MarketFixture) {
  return {
    market_id: id,
    question,
    venue: 'polymarket',
    volume_24h: volume24h,
    updated_at: '2026-04-23T10:00:00+00:00',
    yes_token_id: `${id}-yes`,
    no_token_id: `${id}-no`,
    yes_price: yesPrice,
    no_price: 1 - yesPrice,
    best_bid: yesPrice - 0.015,
    best_ask: yesPrice + 0.015,
    last_trade_price: yesPrice,
    liquidity,
    spread_bps: spreadBps,
    price_updated_at: new Date().toISOString(),
    resolves_at: resolvesAt,
    subscription_source: subscribed ? 'user' : null,
    subscribed
  };
}

function marketsPayload(markets: ReturnType<typeof marketRow>[]) {
  return {
    markets,
    limit: 20,
    offset: 0,
    total: markets.length
  };
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

test('markets page opens the detail drawer from a row and restores it after reload', async ({ page }) => {
  const errors: string[] = [];
  const singleMarketPayload = marketsPayload([
    marketRow({
      id: 'market-000',
      question: 'Will market 000 settle above consensus?',
      volume24h: 2400.5,
      liquidity: 34000.25,
      spreadBps: 300,
      yesPrice: 0.525,
      resolvesAt: '2026-05-01T00:00:00+00:00',
      subscribed: false
    })
  ]);

  page.on('console', (message) => {
    if (message.type() === 'error') {
      errors.push(message.text());
    }
  });
  page.on('pageerror', (error) => {
    errors.push(error.message);
  });

  await page.setViewportSize({ width: 1440, height: 1024 });
  await page.route('**/api/pms/markets**', async (route) => {
    await fulfillJson(route, singleMarketPayload);
  });
  await page.route('**/api/pms/status', routeStatus);
  await page.route('**/api/pms/markets/market-000/price-history**', async (route) => {
    await fulfillJson(route, {
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
    });
  });
  await page.route('**/api/pms/markets/market-000-yes/subscribe', async (route) => {
    await fulfillJson(route, {
      token_id: 'market-000-yes',
      source: 'user',
      created_at: '2026-04-24T12:00:00+00:00'
    });
  });

  await page.goto('/markets');
  await expect(page.getByRole('heading', { name: 'Markets' })).toBeVisible();
  await page.getByRole('row', { name: /Will market 000 settle above consensus/i }).click();
  await expect(page).toHaveURL(/\/markets\?detail=market-000/);
  const drawer = page.getByRole('dialog', { name: 'Market details' });
  await expect(drawer).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Will market 000 settle above consensus?' })).toBeVisible();
  await expect(page.getByText('market-000-yes')).toBeVisible();
  await expect(page.getByTestId('price-history-line-chart')).toBeVisible();
  await page.getByRole('button', { name: 'Subscribe market' }).click();
  await expect(drawer.getByLabel('User subscription')).toBeVisible();
  await page.getByRole('button', { name: 'Close market details' }).click();
  await expect(drawer).toBeHidden();
  await page.getByRole('row', { name: /Will market 000 settle above consensus/i }).click();
  await expect(drawer.getByLabel('User subscription')).toBeVisible();

  await page.reload();
  await expect(drawer).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Will market 000 settle above consensus?' })).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: path.join(evidenceDir, 'cp13-market-detail-chart-subscribe.png')
  });

  expect(errors).toEqual([]);
});

test('markets filters update the shown count, chips, and survive reload', async ({ page }) => {
  const errors: string[] = [];
  const allMarkets = [
    marketRow({
      id: 'market-a',
      question: 'Will Alpha settle above target?',
      volume24h: 250000,
      liquidity: 51000,
      spreadBps: 120,
      yesPrice: 0.62,
      resolvesAt: '2026-05-01T00:00:00+00:00',
      subscribed: true
    }),
    marketRow({
      id: 'market-b',
      question: 'Will Beta reach quorum?',
      volume24h: 190000,
      liquidity: 47000,
      spreadBps: 180,
      yesPrice: 0.55,
      resolvesAt: '2026-06-01T00:00:00+00:00',
      subscribed: false
    }),
    marketRow({
      id: 'market-c',
      question: 'Will Gamma hold support?',
      volume24h: 72000,
      liquidity: 12000,
      spreadBps: 260,
      yesPrice: 0.44,
      resolvesAt: '2026-07-01T00:00:00+00:00',
      subscribed: true
    })
  ];

  page.on('console', (message) => {
    if (message.type() === 'error') {
      errors.push(message.text());
    }
  });
  page.on('pageerror', (error) => {
    errors.push(error.message);
  });

  await page.setViewportSize({ width: 1440, height: 1024 });
  await page.route('**/api/pms/markets**', async (route) => {
    const requestUrl = new URL(route.request().url());
    const volumeMin = Number(requestUrl.searchParams.get('volume_min') ?? '0');
    const subscribed = requestUrl.searchParams.get('subscribed') ?? 'all';
    const filtered = allMarkets.filter((market) => {
      const passesVolume = market.volume_24h !== null && market.volume_24h >= volumeMin;
      const passesSubscription =
        subscribed === 'all' ||
        (subscribed === 'only' && market.subscribed) ||
        (subscribed === 'idle' && !market.subscribed);
      return passesVolume && passesSubscription;
    });
    await fulfillJson(route, marketsPayload(filtered));
  });
  await page.route('**/api/pms/status', routeStatus);

  await page.goto('/markets');
  await expect(page.getByRole('heading', { name: 'Markets' })).toBeVisible();
  await expect(page.getByText('3 visible')).toBeVisible();
  const filterRegion = page.getByLabel('Market filters');

  await filterRegion.getByLabel('Search markets').fill('settle');
  await expect(page).toHaveURL(/q=settle/);
  await filterRegion.getByLabel('Search markets').fill('');

  await filterRegion.getByRole('button', { name: 'Filters' }).click();
  await filterRegion.getByLabel(/Minimum volume/i).fill('100000');
  await expect(page).toHaveURL(/volume_min=100000/);
  await expect(page.getByText('2 visible')).toBeVisible();
  await filterRegion.getByLabel(/Subscription/i).selectOption('only');
  await expect(page).toHaveURL(/subscribed=only/);
  await expect(page.getByText('1 visible')).toBeVisible();
  const activeFilters = page.getByLabel('Active market filters');
  await expect(activeFilters.getByText('Volume >= 100000')).toBeVisible();
  await expect(activeFilters.getByText('Subscribed only')).toBeVisible();

  await page.getByRole('button', { name: 'Remove Subscribed only filter' }).click();
  await expect(page).not.toHaveURL(/subscribed=only/);
  await expect(page.getByText('2 visible')).toBeVisible();

  await page.reload();
  await expect(page.getByRole('heading', { name: 'Markets' })).toBeVisible();
  const reloadedFilterRegion = page.getByLabel('Market filters');
  await reloadedFilterRegion.getByRole('button', { name: 'Filters' }).click();
  await expect(reloadedFilterRegion.getByLabel(/Minimum volume/i)).toHaveValue('100000');
  await expect(reloadedFilterRegion.getByLabel(/Subscription/i)).toHaveValue('all');
  await expect(page.getByLabel('Active market filters').getByText('Volume >= 100000')).toBeVisible();
  await expect(page.getByText('2 visible')).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: path.join(evidenceDir, 'cp13-markets-filters.png')
  });

  expect(errors).toEqual([]);
});
