import { defineConfig, devices } from '@playwright/test';

const databaseUrl =
  process.env.PMS_TEST_DATABASE_URL ??
  process.env.DATABASE_URL ??
  'postgresql://postgres:postgres@localhost:5432/pms_test';

const dashboardApiBaseUrlOverride = process.env.PMS_DASHBOARD_API_BASE_URL;
const shouldStartApiServer = dashboardApiBaseUrlOverride !== '';
const dashboardEnv = Object.fromEntries(
  Object.entries(process.env).filter((entry): entry is [string, string] => entry[1] !== undefined)
);

if (dashboardApiBaseUrlOverride !== undefined) {
  if (dashboardApiBaseUrlOverride === '') {
    delete dashboardEnv.PMS_API_BASE_URL;
  } else {
    dashboardEnv.PMS_API_BASE_URL = dashboardApiBaseUrlOverride;
  }
} else {
  dashboardEnv.PMS_API_BASE_URL = 'http://127.0.0.1:8000';
}

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
    ...(shouldStartApiServer
      ? [
          {
            command:
              'bash -lc \'cd .. && uv run alembic upgrade head && uv run pms-api\'',
            env: {
              ...process.env,
              DATABASE_URL: databaseUrl
            },
            url: 'http://127.0.0.1:8000/status',
            reuseExistingServer: false,
            timeout: 120_000
          }
        ]
      : []),
    {
      command: 'npm run dev',
      env: dashboardEnv,
      url: 'http://127.0.0.1:3100',
      reuseExistingServer: false,
      timeout: 120_000
    }
  ]
});
