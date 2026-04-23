import { render, screen } from '@testing-library/react';
import { test } from 'vitest';
import { Today } from '@/components/Today';
import type { Feedback, MetricsResponse, StatusResponse } from '@/lib/types';

const status: StatusResponse = {
  mode: 'paper',
  source: 'live',
  runner_started_at: '2026-04-23T10:00:00+00:00',
  running: true,
  sensors: [{ name: 'Historical feed', status: 'live', last_signal_at: '2026-04-23T10:00:00+00:00' }],
  controller: { decisions_total: 3 },
  actuator: { fills_total: 1, mode: 'paper' },
  evaluator: { eval_records_total: 5, brier_overall: 0.18 }
};

const metrics: MetricsResponse = {
  brier_overall: 0.18,
  brier_by_category: {},
  pnl: 12.5,
  slippage_bps: 9.2,
  fill_rate: 0.8,
  win_rate: 0.6,
  brier_series: [],
  calibration_curve: [],
  pnl_series: [],
  per_strategy: [],
  ops_view: {
    brier_overall: 0.18,
    brier_by_category: {},
    pnl: 12.5,
    slippage_bps: 9.2,
    fill_rate: 0.8,
    win_rate: 0.6,
    brier_series: [],
    calibration_curve: [],
    pnl_series: []
  }
};

const feedback: Feedback[] = [
  {
    feedback_id: 'fb-1',
    target: 'review',
    source: 'runtime',
    message: 'One item needs review.',
    severity: 'warning',
    created_at: '2026-04-23T10:01:00+00:00',
    resolved: false,
    resolved_at: null,
    category: 'review',
    metadata: {}
  }
];

test('Today renders the new dashboard hero and feed links', () => {
  render(<Today feedback={feedback} metrics={metrics} status={status} />);

  screen.getByTestId('dashboard-hero');
  screen.getByTestId('today-feed');
  screen.getByRole('heading', { level: 1, name: 'Today' });
  screen.getByRole('link', { name: 'Browse markets' });
  screen.getByRole('link', { name: 'Review ideas' });
  screen.getByRole('link', { name: 'See trades' });
});
