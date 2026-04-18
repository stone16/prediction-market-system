import fs from 'node:fs';
import path from 'node:path';
import type {
  Decision,
  FactorCatalogEntry,
  FactorCatalogResponse,
  FactorSeriesResponse,
  Feedback,
  MetricsResponse,
  Signal,
  SignalDepth,
  StatusResponse,
  StrategiesResponse
} from './types';

const rootDir = path.resolve(process.cwd(), '..');
const dataDir = path.join(rootDir, '.data');
const feedbackPath = path.join(dataDir, 'feedback.jsonl');

export function mockStatus(): StatusResponse {
  return {
    mode: 'backtest',
    runner_started_at: '2026-04-14T00:00:00+00:00',
    running: false,
    sensors: [{ name: 'HistoricalSensor', status: 'idle', last_signal_at: '2026-04-07T22:39:00+00:00' }],
    controller: { decisions_total: mockDecisions().length },
    actuator: { fills_total: 18, mode: 'backtest' },
    evaluator: { eval_records_total: 18, brier_overall: 0.18 }
  };
}

export function mockSignals(): Signal[] {
  return Array.from({ length: 60 }, (_, index) => ({
    market_id: `pm-synthetic-${String(index).padStart(3, '0')}`,
    title: `Synthetic market ${String(index).padStart(3, '0')}`,
    yes_price: 0.3 + (index % 30) / 100,
    fetched_at: new Date(Date.UTC(2026, 3, 1, index % 24, 0, 0)).toISOString()
  }));
}

export function mockDecisions(): Decision[] {
  return Array.from({ length: 18 }, (_, index) => ({
    decision_id: `decision-${index}`,
    market_id: `pm-synthetic-${String(index).padStart(3, '0')}`,
    forecaster: index % 2 === 0 ? 'StatisticalForecaster' : 'RulesForecaster',
    prob_estimate: 0.56 + (index % 4) / 100,
    expected_edge: 0.08 + (index % 3) / 100,
    kelly_size: 12 + index,
    resolved_outcome: index % 3 === 0 ? 1 : 0,
    price: 0.42 + (index % 5) / 100,
    side: 'BUY'
  }));
}

export function mockMetrics(): MetricsResponse {
  const decisions = mockDecisions();
  return {
    brier_overall: 0.18,
    brier_by_category: { StatisticalForecaster: 0.16, RulesForecaster: 0.21 },
    pnl: 42.75,
    slippage_bps: 18.4,
    fill_rate: 0.92,
    win_rate: 0.61,
    brier_series: decisions.map((_decision, index) => ({
      recorded_at: `2026-04-${String(1 + index).padStart(2, '0')}T00:00:00+00:00`,
      brier_score: 0.08 + (index % 6) / 100
    })),
    calibration_curve: decisions.map((decision) => ({
      prob_estimate: decision.prob_estimate,
      resolved_outcome: decision.resolved_outcome ?? 0
    })),
    pnl_series: decisions.map((_decision, index) => ({
      recorded_at: `2026-04-${String(1 + index).padStart(2, '0')}T00:00:00+00:00`,
      pnl: -8 + index * 3.1
    }))
  };
}

export function mockStrategies(): StrategiesResponse {
  return {
    strategies: [
      {
        strategy_id: 'default',
        active_version_id: 'd50c4db65699c222620c85f0cf84c0324c148a34b212c5f69903dbf4b950757c',
        created_at: '2026-04-14T00:00:00+00:00'
      }
    ]
  };
}

const mockFactorCatalog: FactorCatalogEntry[] = [
  {
    factor_id: 'fair_value_spread',
    name: 'Fair Value Spread',
    description: 'Signed difference between external fair value and the current YES price.',
    output_type: 'scalar',
    direction: 'neutral'
  },
  {
    factor_id: 'metaculus_prior',
    name: 'Metaculus Prior',
    description: 'Raw Metaculus probability from the external signal payload.',
    output_type: 'scalar',
    direction: 'neutral'
  },
  {
    factor_id: 'orderbook_imbalance',
    name: 'Orderbook Imbalance',
    description: 'Normalized bid-versus-ask depth imbalance from the current orderbook signal.',
    output_type: 'scalar',
    direction: 'neutral'
  }
];

const mockFactorSeriesIndex: Record<string, FactorSeriesResponse['points']> = {
  'orderbook_imbalance::factor-depth::': [
    { ts: '2026-04-18T09:00:00+00:00', value: 0.3333 },
    { ts: '2026-04-18T09:01:00+00:00', value: 0.25 },
    { ts: '2026-04-18T09:02:00+00:00', value: 0.1667 },
    { ts: '2026-04-18T09:03:00+00:00', value: 0.1 }
  ],
  'metaculus_prior::factor-depth::': [
    { ts: '2026-04-18T09:00:00+00:00', value: 0.62 },
    { ts: '2026-04-18T09:03:00+00:00', value: 0.64 }
  ]
};

export function mockFactorsCatalog(): FactorCatalogResponse {
  return { catalog: mockFactorCatalog };
}

export function mockFactorSeries({
  factorId,
  marketId,
  param,
  since,
  limit
}: {
  factorId: string;
  marketId: string;
  param?: string;
  since?: string | null;
  limit?: number;
}): FactorSeriesResponse {
  const normalizedParam = param ?? '';
  const key = `${factorId}::${marketId}::${normalizedParam}`;
  const fallbackKey = `${factorId}::factor-depth::${normalizedParam}`;
  const points = (mockFactorSeriesIndex[key] ?? mockFactorSeriesIndex[fallbackKey] ?? []).filter(
    (point) => !since || point.ts >= since
  );
  return {
    factor_id: factorId,
    param: normalizedParam,
    market_id: marketId,
    points: points.slice(0, limit ?? 500)
  };
}

export function mockSignalDepth(_marketId: string): SignalDepth {
  return {
    best_bid: 0.58,
    best_ask: 0.62,
    bids: [
      { price: 0.58, size: 140 },
      { price: 0.56, size: 95 },
      { price: 0.55, size: 80 }
    ],
    asks: [
      { price: 0.62, size: 110 },
      { price: 0.64, size: 155 },
      { price: 0.66, size: 120 }
    ],
    last_update_ts: new Date(Date.UTC(2026, 3, 14, 0, 0, 0)).toISOString(),
    stale: false
  };
}

export function readFeedback(): Feedback[] {
  if (!fs.existsSync(feedbackPath)) return [];
  return fs
    .readFileSync(feedbackPath, 'utf-8')
    .split('\n')
    .filter(Boolean)
    .map((line) => JSON.parse(line) as Feedback);
}

export function writeFeedback(items: Feedback[]) {
  fs.mkdirSync(dataDir, { recursive: true });
  fs.writeFileSync(feedbackPath, `${items.map((item) => JSON.stringify(item)).join('\n')}\n`);
}

export function resolveFeedback(feedbackId: string): Feedback | null {
  const items = readFeedback();
  const index = items.findIndex((item) => item.feedback_id === feedbackId);
  if (index === -1) return null;
  items[index] = {
    ...items[index],
    resolved: true,
    resolved_at: new Date().toISOString()
  };
  writeFeedback(items);
  return items[index];
}
