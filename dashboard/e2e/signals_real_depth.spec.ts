import fs from 'node:fs';
import path from 'node:path';
import { expect, test } from '@playwright/test';
import { applySchema, executeSql, resetOuterRing } from './support/pg';

const evidenceDir = path.resolve(process.cwd(), 'e2e', 'evidence');
const marketId = 'pm-depth-e2e';
const tokenId = 'pm-depth-e2e-yes';

function sqlLiteral(value: string) {
  return `'${value.replaceAll("'", "''")}'`;
}

function seedDepthFixture() {
  resetOuterRing();
  const marketIdSql = sqlLiteral(marketId);
  const tokenIdSql = sqlLiteral(tokenId);
  executeSql(`
    INSERT INTO markets (condition_id, slug, question, venue, resolves_at, created_at, last_seen_at)
    VALUES (${marketIdSql}, ${marketIdSql}, 'Will the depth ladder render real rows?', 'polymarket', NULL, NOW(), NOW());

    INSERT INTO tokens (token_id, condition_id, outcome)
    VALUES (${tokenIdSql}, ${marketIdSql}, 'YES');

    WITH inserted_snapshot AS (
      INSERT INTO book_snapshots (market_id, token_id, ts, hash, source)
      VALUES (${marketIdSql}, ${tokenIdSql}, NOW(), 'playwright-snapshot', 'subscribe')
      RETURNING id
    )
    INSERT INTO book_levels (snapshot_id, market_id, side, price, size)
    SELECT id, ${marketIdSql}, 'BUY', 0.58, 140.0 FROM inserted_snapshot
    UNION ALL
    SELECT id, ${marketIdSql}, 'BUY', 0.56, 95.0 FROM inserted_snapshot
    UNION ALL
    SELECT id, ${marketIdSql}, 'SELL', 0.62, 110.0 FROM inserted_snapshot
    UNION ALL
    SELECT id, ${marketIdSql}, 'SELL', 0.64, 155.0 FROM inserted_snapshot;
  `);
}

test.beforeAll(() => {
  fs.mkdirSync(evidenceDir, { recursive: true });
  applySchema();
});

test.beforeEach(() => {
  seedDepthFixture();
});

test('signals page renders persisted depth without console errors', async ({ page }) => {
  const errors: string[] = [];
  page.on('console', (message) => {
    if (message.type() === 'error') errors.push(message.text());
  });
  page.on('pageerror', (error) => errors.push(error.message));

  await page.goto(`/signals?market_id=${marketId}`);

  const ladder = page.getByTestId('depth-ladder');
  await expect(ladder).toBeVisible();
  await expect(ladder.getByText('58.0¢')).toBeVisible();
  await expect(ladder.getByText('56.0¢')).toBeVisible();
  await expect(ladder.getByText('62.0¢')).toBeVisible();
  await expect(ladder.getByText('64.0¢')).toBeVisible();

  expect(await page.getByTestId('bid-row').count()).toBeGreaterThanOrEqual(2);
  expect(await page.getByTestId('ask-row').count()).toBeGreaterThanOrEqual(2);

  await page.screenshot({
    path: path.join(evidenceDir, 'signals-real-depth.png'),
    fullPage: true
  });

  expect(errors).toEqual([]);
});
