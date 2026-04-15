export type StatusResponse = {
  mode: string;
  runner_started_at: string | null;
  running?: boolean;
  sensors: Array<{ name: string; status: string; last_signal_at: string | null }>;
  controller: { decisions_total: number };
  actuator: { fills_total: number; mode: string };
  evaluator: { eval_records_total: number; brier_overall: number | null };
};

export type Feedback = {
  feedback_id: string;
  target: string;
  source: string;
  message: string;
  severity: string;
  created_at: string;
  resolved: boolean;
  resolved_at: string | null;
  category: string | null;
  metadata: Record<string, unknown>;
};

export type Decision = {
  decision_id: string;
  market_id: string;
  forecaster: string;
  prob_estimate: number;
  expected_edge: number;
  kelly_size: number;
  resolved_outcome?: number | null;
  price?: number;
  side?: string;
};

export type MetricsResponse = {
  brier_overall: number | null;
  brier_by_category: Record<string, number>;
  pnl: number;
  slippage_bps: number;
  fill_rate: number;
  win_rate: number;
  brier_series?: Array<{ recorded_at: string; brier_score: number }>;
  calibration_curve?: Array<{ prob_estimate: number; resolved_outcome: number }>;
  pnl_series?: Array<{ recorded_at: string; pnl: number }>;
};

export type Signal = {
  market_id: string;
  title: string;
  yes_price: number;
  fetched_at: string;
};
