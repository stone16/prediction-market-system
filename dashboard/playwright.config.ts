import { defineConfig, devices } from '@playwright/test';

const databaseUrl =
  process.env.PMS_TEST_DATABASE_URL ??
  process.env.DATABASE_URL ??
  'postgresql://postgres:postgres@localhost:5432/pms_test';

export default defineConfig({
  testDir: './e2e',
  timeout: 30_000,
  expect: { timeout: 5_000 },
  workers: 1,
  use: {
    baseURL: 'http://127.0.0.1:3100',
    trace: 'retain-on-failure',
    ...devices['Desktop Chrome']
  },
  webServer: [
    {
      command:
        'bash -lc \'cd .. && psql "$DATABASE_URL" --set ON_ERROR_STOP=1 --file schema.sql && uv run pms-api\'',
      env: {
        ...process.env,
        DATABASE_URL: databaseUrl
      },
      url: 'http://127.0.0.1:8000/status',
      reuseExistingServer: false,
      timeout: 120_000
    },
    {
      command: 'PMS_API_BASE_URL=http://127.0.0.1:8000 npm run dev',
      url: 'http://127.0.0.1:3100',
      reuseExistingServer: false,
      timeout: 120_000
    }
  ]
});
