import fs from 'node:fs';
import path from 'node:path';
import { expect, test } from '@playwright/test';

const evidenceDir = path.resolve(
  process.cwd(),
  '..',
  '.harness',
  'cathedral-v1',
  'checkpoints',
  '10',
  'iter-1',
  'evidence'
);

function sseBody() {
  const createdAt = '2026-04-23T12:00:00+00:00';
  return [
    'retry: 60000',
    '',
    `id: 1\nevent: sensor.signal\ndata: ${JSON.stringify({
      event_id: 1,
      event_type: 'sensor.signal',
      created_at: createdAt,
      summary: 'Signal market-cp10 @ 41.0¢',
      market_id: 'market-cp10'
    })}`,
    '',
    `id: 2\nevent: controller.decision\ndata: ${JSON.stringify({
      event_id: 2,
      event_type: 'controller.decision',
      created_at: createdAt,
      summary: 'Accepted BUY $25.00 on market-cp10',
      market_id: 'market-cp10',
      decision_id: 'decision-cp10'
    })}`,
    '',
    `id: 3\nevent: actuator.fill\ndata: ${JSON.stringify({
      event_id: 3,
      event_type: 'actuator.fill',
      created_at: createdAt,
      summary: 'Filled BUY $25.00 on market-cp10',
      market_id: 'market-cp10',
      decision_id: 'decision-cp10',
      fill_id: 'fill-cp10'
    })}`,
    '',
  ].join('\n') + '\n';
}

test.beforeEach(() => {
  fs.mkdirSync(evidenceDir, { recursive: true });
});

test('event log drawer shows streamed entries and remains pinned after reload', async ({ page }) => {
  const errors: string[] = [];
  page.on('console', (message) => {
    if (message.type() === 'error') errors.push(message.text());
  });
  page.on('pageerror', (error) => errors.push(error.message));

  await page.route('**/api/pms/status', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify({
        mode: 'paper',
        runner_started_at: '2026-04-23T12:00:00+00:00',
        running: true,
        sensors: [{ name: 'MarketDataSensor', status: 'live', last_signal_at: '2026-04-23T12:00:00+00:00' }],
        controller: { decisions_total: 12, diagnostics_total: 0 },
        actuator: { fills_total: 5, mode: 'paper' },
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
      body: JSON.stringify([])
    });
  });
  await page.route('**/api/pms/stream/events**', async (route) => {
    await route.fulfill({
      contentType: 'text/event-stream',
      body: sseBody()
    });
  });

  await page.goto('/');
  await page.getByRole('button', { name: 'Dismiss onboarding' }).click();
  await page.getByRole('button', { name: 'Event log' }).click();

  await expect(page.getByText('Signal market-cp10 @ 41.0¢')).toBeVisible();
  await expect(page.getByText('Accepted BUY $25.00 on market-cp10')).toBeVisible();
  await expect(page.getByText('Filled BUY $25.00 on market-cp10')).toBeVisible();

  await page.getByRole('button', { name: 'Pin event log' }).click();
  await page.reload();

  await expect(page.getByRole('complementary', { name: 'Event log' })).toBeVisible();
  await page.screenshot({ path: path.join(evidenceDir, 'event-log-drawer-open.png'), fullPage: true });
  expect(errors).toEqual([]);
});
