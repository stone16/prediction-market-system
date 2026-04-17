import { execFileSync } from 'node:child_process';
import path from 'node:path';

const rootDir = path.resolve(process.cwd(), '..');
const schemaPath = path.join(rootDir, 'schema.sql');
const databaseUrl =
  process.env.PMS_TEST_DATABASE_URL ??
  process.env.DATABASE_URL ??
  'postgresql://postgres:postgres@localhost:5432/pms_test';

function runPsql(args: string[]) {
  execFileSync('psql', [databaseUrl, '--set', 'ON_ERROR_STOP=1', ...args], {
    stdio: 'pipe'
  });
}

export function applySchema() {
  runPsql(['--file', schemaPath]);
}

export function executeSql(sql: string) {
  runPsql(['--command', sql]);
}

export function resetOuterRing() {
  executeSql(`
    TRUNCATE TABLE book_levels, book_snapshots, price_changes, trades, tokens, markets RESTART IDENTITY CASCADE;
  `);
}

export function resetInnerRing() {
  executeSql(`
    TRUNCATE TABLE feedback, eval_records RESTART IDENTITY CASCADE;
  `);
}
