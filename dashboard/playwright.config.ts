import { defineConfig, devices } from '@playwright/test';

const databaseUrl =
  process.env.PMS_TEST_DATABASE_URL ??
  process.env.DATABASE_URL ??
  'postgresql://postgres:postgres@localhost:5432/pms_test';

const dashboardApiBaseUrlOverride = process.env.PMS_DASHBOARD_API_BASE_URL;
const dashboardApiPort = validatedTcpPort(process.env.PMS_DASHBOARD_API_PORT ?? '8000');
const localApiBaseUrl = `http://127.0.0.1:${dashboardApiPort}`;
const shouldStartApiServer = dashboardApiBaseUrlOverride === undefined;
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
  dashboardEnv.PMS_API_BASE_URL = localApiBaseUrl;
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
              `bash -lc 'cd .. && uv run alembic upgrade head && uv run pms-api --port ${dashboardApiPort}'`,
            env: {
              ...process.env,
              DATABASE_URL: databaseUrl
            },
            url: `${localApiBaseUrl}/status`,
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

function validatedTcpPort(rawPort: string): string {
  if (!/^\d+$/.test(rawPort)) {
    throw new Error('PMS_DASHBOARD_API_PORT must be a numeric TCP port');
  }
  const port = Number(rawPort);
  if (!Number.isInteger(port) || port < 1 || port > 65_535) {
    throw new Error('PMS_DASHBOARD_API_PORT must be between 1 and 65535');
  }
  return String(port);
}
