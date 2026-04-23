import fs from 'node:fs';
import path from 'node:path';
import { expect, test } from '@playwright/test';

const evidenceDir = path.resolve(
  process.cwd(),
  '..',
  '.harness',
  'cathedral-v1',
  'checkpoints',
  '04',
  'iter-1',
  'evidence'
);

test.beforeEach(() => {
  fs.mkdirSync(evidenceDir, { recursive: true });
});

test('markets page renders a populated table, then an empty state, without console errors', async ({
  page
}) => {
  const errors: string[] = [];
  let marketsPayload = {
    markets: [
      {
        market_id: 'market-000',
        question: 'Will market 000 settle above consensus?',
        venue: 'polymarket',
        volume_24h: 2400.5,
        updated_at: '2026-04-23T10:00:00+00:00',
        yes_token_id: 'market-000-yes',
        no_token_id: 'market-000-no',
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
  await expect(page.getByRole('link', { name: 'market-000' })).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: path.join(evidenceDir, 'cp04-markets-populated.png')
  });

  await page.getByRole('link', { name: 'market-000' }).click();
  await expect(page).toHaveURL(/\/signals\?market_id=market-000/);

  marketsPayload = {
    markets: [],
    limit: 20,
    offset: 0,
    total: 0
  };

  await page.goto('/markets');
  await expect(page.getByText('No markets yet.')).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: path.join(evidenceDir, 'cp04-markets-empty.png')
  });

  expect(errors).toEqual([]);
});
