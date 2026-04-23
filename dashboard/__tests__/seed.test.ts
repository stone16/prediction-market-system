import { getDashboardSource } from '@/lib/dashboard-source';
import type { StatusResponse } from '@/lib/types';
import { expect, test } from 'vitest';

test('vitest resolves dashboard aliases in jsdom', () => {
  const status: StatusResponse = {
    mode: 'paper',
    source: 'mock',
    runner_started_at: null,
    sensors: [],
    controller: { decisions_total: 0 },
    actuator: { fills_total: 0, mode: 'paper' },
    evaluator: { eval_records_total: 0, brier_overall: null },
  };

  document.body.innerHTML = '<main data-testid="seed-root"></main>';

  const originalBaseUrl = process.env.PMS_API_BASE_URL;

  try {
    delete process.env.PMS_API_BASE_URL;
    expect(getDashboardSource()).toBe('mock');

    process.env.PMS_API_BASE_URL = 'http://127.0.0.1:8000';

    expect(status.mode).toBe('paper');
    expect(getDashboardSource()).toBe('live');
    expect(document.querySelector('[data-testid="seed-root"]')).not.toBeNull();
    expect(true).toBe(true);
  } finally {
    if (originalBaseUrl === undefined) {
      delete process.env.PMS_API_BASE_URL;
    } else {
      process.env.PMS_API_BASE_URL = originalBaseUrl;
    }
  }
});
