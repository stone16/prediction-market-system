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

export type DepthLevel = {
  price: number;
  size: number;
};

export type SignalDepth = {
  best_bid: number | null;
  best_ask: number | null;
  bids: DepthLevel[];
  asks: DepthLevel[];
  last_update_ts: string | null;
  stale: boolean;
};

export type StrategyRow = {
  strategy_id: string;
  active_version_id: string | null;
  created_at: string;
};

export type StrategiesResponse = {
  strategies: StrategyRow[];
};

export type StrategyMetricsRow = {
  strategy_id: string;
  strategy_version_id: string;
  created_at: string;
  record_count: number;
  insufficient_samples: boolean;
  brier_overall: number | null;
  pnl: number;
  fill_rate: number;
  slippage_bps: number;
  drawdown: number;
};

export type StrategyMetricsResponse = {
  strategies: StrategyMetricsRow[];
};

export type FactorCatalogEntry = {
  factor_id: string;
  name: string;
  description: string;
  output_type: string;
  direction: string;
};

export type FactorCatalogResponse = {
  catalog: FactorCatalogEntry[];
};

export type FactorPoint = {
  ts: string;
  value: number;
};

export type FactorSeriesResponse = {
  factor_id: string;
  param: string;
  market_id: string;
  points: FactorPoint[];
};
