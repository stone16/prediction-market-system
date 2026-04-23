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

  expect(status.mode).toBe('paper');
  expect(document.querySelector('[data-testid="seed-root"]')).not.toBeNull();
  expect(false).toBe(true);
});
