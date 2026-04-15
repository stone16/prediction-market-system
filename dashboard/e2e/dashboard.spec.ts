import { expect, test } from '@playwright/test';
import fs from 'node:fs';
import path from 'node:path';

// Smoke-capture storage: dashboard/e2e/baseline/ is NOT gitignored
// (checked against root .gitignore and dashboard/.gitignore), so screenshots
// are committed and can be visually diffed across PRs.  To regenerate:
//   cd dashboard && PMS_API_BASE_URL=http://127.0.0.1:8000 \
//     npx playwright test dashboard.spec.ts -g "smoke capture"
//
// IMPORTANT — these screenshots are NOT deterministic across runs.
// Backend state (feedback count, decision count, fill timestamps) varies
// per session and per developer machine.  Two engineers regenerating at
// the same commit SHA will get different PNGs.  They are committed so
// PRs *can* be visually diffed, but any diff must be manually reviewed
// against expected state changes — do NOT treat a diff as an automatic
// regression.
//
// Known open question (Task 4 README): a /feedback?limit=N cap (mirroring
// /signals and /decisions in src/pms/api/app.py) would bound home.png
// structurally.  Not addressed here.

const evidenceDir = path.resolve(process.cwd(), '..', '.harness', 'pms-v2', 'checkpoints', '10', 'iter-1', 'evidence');
const dataDir = path.resolve(process.cwd(), '..', '.data');
const feedbackPath = path.join(dataDir, 'feedback.jsonl');

function seedFeedback() {
  fs.mkdirSync(dataDir, { recursive: true });
  const now = '2026-04-14T00:00:00+00:00';
  const rows = [
    {
      feedback_id: 'fb-open-1',
      target: 'controller',
      source: 'evaluator',
      message: 'Brier score crossed the review threshold for model-a.',
      severity: 'warning',
      created_at: now,
      resolved: false,
      resolved_at: null,
      category: 'brier:model-a',
      metadata: { market_id: 'pm-synthetic-010' }
    },
    {
      feedback_id: 'fb-open-2',
      target: 'controller',
      source: 'actuator',
      message: 'Paper fill slippage exceeded the configured limit.',
      severity: 'warning',
      created_at: now,
      resolved: false,
      resolved_at: null,
      category: 'slippage',
      metadata: { market_id: 'pm-synthetic-011' }
    },
    {
      feedback_id: 'fb-resolved',
      target: 'controller',
      source: 'evaluator',
      message: 'Win-rate feedback already handled.',
      severity: 'info',
      created_at: now,
      resolved: true,
      resolved_at: now,
      category: 'win_rate',
      metadata: { market_id: 'pm-synthetic-012' }
    }
  ];
  fs.writeFileSync(feedbackPath, `${rows.map((row) => JSON.stringify(row)).join('\n')}\n`);
}

test.beforeEach(() => {
  seedFeedback();
  fs.mkdirSync(evidenceDir, { recursive: true });
});

test('feedback panel resolves without full page reload and required pages are quiet', async ({ page }) => {
  const errors: string[] = [];
  page.on('console', (message) => {
    if (message.type() === 'error') errors.push(message.text());
  });
  page.on('pageerror', (error) => errors.push(error.message));

  await page.goto('/');
  await expect(page.getByRole('heading', { name: 'Cybernetic Console' })).toBeVisible();
  await expect(page.getByTestId('layer-card')).toHaveCount(4);
  await expect(page.getByTestId('feedback-item')).toHaveCount(2);
  await page.getByRole('button', { name: 'Mark Resolved' }).first().click();
  await expect(page.getByTestId('feedback-item')).toHaveCount(1);
  await page.screenshot({ path: path.join(evidenceDir, 'dashboard-main.png'), fullPage: true });

  await page.goto('/metrics');
  await expect(page.getByText('Brier score over time')).toBeVisible();
  await expect(page.getByText('Calibration curve')).toBeVisible();
  await expect(page.getByText('P&L over time')).toBeVisible();
  await page.screenshot({ path: path.join(evidenceDir, 'dashboard-metrics.png'), fullPage: true });

  await page.goto('/decisions');
  await expect(page.getByRole('heading', { name: 'Decision Ledger' })).toBeVisible();

  await page.goto('/backtest');
  await expect(page.getByRole('heading', { name: 'Backtest Run' })).toBeVisible();

  expect(errors).toEqual([]);
});

test('degenerate calibration explains one-probability data', async ({ page }) => {
  await page.route('**/api/pms/metrics', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      json: {
        brier_overall: 0.25,
        brier_by_category: { stub: 0.25 },
        pnl: 0,
        slippage_bps: 0,
        fill_rate: 1,
        win_rate: 0.5,
        brier_series: [
          { recorded_at: '2026-04-13T00:00:00+00:00', brier_score: 0.25 },
          { recorded_at: '2026-04-14T00:00:00+00:00', brier_score: 0.25 }
        ],
        calibration_curve: [
          { prob_estimate: 0.5, resolved_outcome: 0 },
          { prob_estimate: 0.5, resolved_outcome: 1 }
        ],
        pnl_series: [
          { recorded_at: '2026-04-13T00:00:00+00:00', pnl: 0 },
          { recorded_at: '2026-04-14T00:00:00+00:00', pnl: 0 }
        ]
      }
    });
  });

  await page.goto('/metrics');

  await expect(
    page.getByText('One probability level recorded. Calibration needs varied forecasts before a curve appears.')
  ).toBeVisible();
});

test('smoke capture: screenshot all five dashboard pages against live backend', async ({ page }) => {
  // Heading strings verified against the actual page.tsx h1 elements:
  //   app/page.tsx            → 'Cybernetic Console'  (confirmed in existing spec line 66)
  //   app/signals/page.tsx    → 'Signal Stream'        (grep-confirmed)
  //   app/decisions/page.tsx  → 'Decision Ledger'      (confirmed in existing spec line 80)
  //   app/metrics/page.tsx    → 'Metric Review'        (grep-confirmed — NOT 'Metrics')
  //   app/backtest/page.tsx   → 'Backtest Run'         (confirmed in existing spec line 83)
  const pages: Array<{ path: string; heading: string; file: string }> = [
    { path: '/',          heading: 'Cybernetic Console', file: 'home.png'      },
    { path: '/signals',   heading: 'Signal Stream',      file: 'signals.png'   },
    { path: '/decisions', heading: 'Decision Ledger',    file: 'decisions.png' },
    { path: '/metrics',   heading: 'Metric Review',      file: 'metrics.png'   },
    { path: '/backtest',  heading: 'Backtest Run',       file: 'backtest.png'  },
  ];

  const errors: string[] = [];
  page.on('console', (m) => { if (m.type() === 'error') errors.push(`${m.location().url}: ${m.text()}`); });
  page.on('pageerror', (e) => errors.push(e.message));

  const baselineDir = path.join(__dirname, 'baseline');
  fs.mkdirSync(baselineDir, { recursive: true });

  for (const p of pages) {
    await page.goto(p.path);
    await page.waitForLoadState('networkidle');
    // Soft assertion: if the heading renames the test still captures the screenshot
    // so the visual diff is informative rather than just failing with no evidence.
    await expect.soft(page.getByRole('heading', { name: p.heading })).toBeVisible({ timeout: 5_000 });
    await page.screenshot({
      path: path.join(baselineDir, p.file),
      fullPage: p.path !== '/', // home: viewport-only to bound size against unbounded feedback list
    });
  }

  // If any soft heading assertion failed above, surface it as a hard failure
  // so CI does not show green when headings drift.
  expect(test.info().errors).toHaveLength(0);

  // Console errors on any page are a real regression — fail hard.
  expect(errors).toEqual([]);
});
