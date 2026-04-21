import fs from 'node:fs';
import path from 'node:path';
import { expect, test } from '@playwright/test';

const evidenceDir = path.resolve(
  process.cwd(),
  '..',
  '.harness',
  'pms-correctness-bundle-v1',
  'checkpoints',
  '07',
  'iter-1',
  'evidence'
);

const sourceMode = process.env.PMS_CP07_SOURCE_MODE ?? 'live';

async function captureConsoleErrors(page: Parameters<typeof test>[0]['page'], errors: string[]) {
  page.on('console', (message) => {
    if (message.type() === 'error') {
      errors.push(message.text());
    }
  });
  page.on('pageerror', (error) => {
    errors.push(error.message);
  });
}

test.beforeEach(() => {
  fs.mkdirSync(evidenceDir, { recursive: true });
});

test('mock mode shows source banner and badges on key pages', async ({ page }) => {
  test.skip(sourceMode !== 'mock');

  const errors: string[] = [];
  await captureConsoleErrors(page, errors);

  for (const [route, screenshotName] of [
    ['/', 'cp07-home-mock.png'],
    ['/overview', 'cp07-overview-mock.png'],
    ['/backtest', 'cp07-backtest-mock.png']
  ] as const) {
    await page.goto(route);
    await expect(
      page.getByText('MOCK DATA — backend disconnected. Set `PMS_API_BASE_URL` to connect.')
    ).toBeVisible();
    await expect(page.getByTestId('source-badge').first()).toBeVisible();
    await page.screenshot({
      path: path.join(evidenceDir, screenshotName),
      fullPage: true
    });
  }

  expect(errors).toEqual([]);
});

test('live mode hides source banner and badges on key pages', async ({ page }) => {
  test.skip(sourceMode !== 'live');

  const errors: string[] = [];
  await captureConsoleErrors(page, errors);

  for (const route of ['/', '/overview', '/backtest'] as const) {
    await page.goto(route);
    await expect(
      page.getByText('MOCK DATA — backend disconnected. Set `PMS_API_BASE_URL` to connect.')
    ).toHaveCount(0);
    await expect(page.getByTestId('source-badge')).toHaveCount(0);
  }

  expect(errors).toEqual([]);
});
