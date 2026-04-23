import fs from 'node:fs';
import path from 'node:path';
import type {
  BacktestEnqueueResponse,
  BacktestLiveComparisonResponse,
  BacktestRunRow,
  BacktestStrategyRunRow,
  Decision,
  FactorCatalogEntry,
  FactorCatalogResponse,
  FactorSeriesResponse,
  Feedback,
  MarketRow,
  MetricsResponse,
  PositionRow,
  PositionsResponse,
  Signal,
  SignalDepth,
  StatusResponse,
  TradeRow,
  TradesResponse,
  StrategyMetricsResponse,
  StrategiesResponse
} from './types';

const rootDir = path.resolve(process.cwd(), '..');
const dataDir = path.join(rootDir, '.data');
const feedbackPath = path.join(dataDir, 'feedback.jsonl');

export function mockStatus(): StatusResponse {
  return {
    mode: 'backtest',
    source: 'mock',
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

export function mockMarkets(): MarketRow[] {
  return Array.from({ length: 20 }, (_, index) => ({
    market_id: `market-${String(index).padStart(3, '0')}`,
    question: `Will market ${String(index).padStart(3, '0')} settle above consensus?`,
    venue: 'polymarket',
    volume_24h: 2400 - index * 73.5,
    updated_at: new Date(Date.UTC(2026, 3, 23, 12, index, 0)).toISOString(),
    yes_token_id: `market-${String(index).padStart(3, '0')}-yes`,
    no_token_id: `market-${String(index).padStart(3, '0')}-no`,
    subscribed: index % 3 === 0
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

export function mockPositions(): PositionsResponse {
  const positions: PositionRow[] = [
    {
      market_id: 'market-000',
      token_id: 'market-000-yes',
      venue: 'polymarket',
      side: 'BUY',
      shares_held: 50.0,
      avg_entry_price: 0.41,
      unrealized_pnl: 0.0,
      locked_usdc: 20.5
    },
    {
      market_id: 'market-003',
      token_id: 'market-003-yes',
      venue: 'polymarket',
      side: 'BUY',
      shares_held: 32.0,
      avg_entry_price: 0.53,
      unrealized_pnl: 0.0,
      locked_usdc: 16.96
    }
  ];

  return { positions };
}

export function mockTrades(limit = 20): TradesResponse {
  const rows: TradeRow[] = [
    {
      trade_id: 'trade-000',
      fill_id: 'fill-000',
      order_id: 'order-000',
      decision_id: 'decision-000',
      market_id: 'market-000',
      question: 'Will market 000 settle above consensus?',
      token_id: 'market-000-yes',
      venue: 'polymarket',
      side: 'BUY',
      fill_price: 0.41,
      fill_notional_usdc: 20.5,
      fill_quantity: 50.0,
      executed_at: '2026-04-23T09:00:00Z',
      filled_at: '2026-04-23T09:00:00Z',
      status: 'matched',
      strategy_id: 'default',
      strategy_version_id: 'default-v1'
    },
    {
      trade_id: 'trade-001',
      fill_id: 'fill-001',
      order_id: 'order-001',
      decision_id: 'decision-001',
      market_id: 'market-003',
      question: 'Will market 003 settle above consensus?',
      token_id: 'market-003-yes',
      venue: 'polymarket',
      side: 'BUY',
      fill_price: 0.53,
      fill_notional_usdc: 16.96,
      fill_quantity: 32.0,
      executed_at: '2026-04-23T08:30:00Z',
      filled_at: '2026-04-23T08:30:00Z',
      status: 'matched',
      strategy_id: 'default',
      strategy_version_id: 'default-v1'
    }
  ];

  return {
    trades: rows.slice(0, limit),
    limit
  };
}

export function mockMetrics(): MetricsResponse {
  const decisions = mockDecisions();
  const opsView = {
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
  return {
    ...opsView,
    per_strategy: [
      {
        strategy_id: 'alpha',
        strategy_version_id: 'alpha-v1',
        record_count: 8,
        insufficient_samples: false,
        brier_overall: 0.125,
        pnl: 14.2,
        fill_rate: 0.95,
        slippage_bps: 15.0,
        drawdown: 3.0
      },
      {
        strategy_id: 'beta',
        strategy_version_id: 'beta-v1',
        record_count: 7,
        insufficient_samples: false,
        brier_overall: 0.305,
        pnl: 9.8,
        fill_rate: 0.89,
        slippage_bps: 10.0,
        drawdown: 2.0
      },
      {
        strategy_id: 'default',
        strategy_version_id: 'default-v1',
        record_count: 0,
        insufficient_samples: true,
        brier_overall: null,
        pnl: 0.0,
        fill_rate: 0.0,
        slippage_bps: 0.0,
        drawdown: 0.0
      }
    ],
    ops_view: opsView
  };
}

export function mockStrategies(): StrategiesResponse {
  return {
    strategies: [
      {
        strategy_id: 'alpha',
        active_version_id: 'alpha-v1',
        created_at: '2026-04-18T00:00:00+00:00'
      },
      {
        strategy_id: 'beta',
        active_version_id: 'beta-v1',
        created_at: '2026-04-18T00:00:00+00:00'
      },
      {
        strategy_id: 'default',
        active_version_id: 'd50c4db65699c222620c85f0cf84c0324c148a34b212c5f69903dbf4b950757c',
        created_at: '2026-04-14T00:00:00+00:00'
      }
    ]
  };
}

export function mockStrategyMetrics(): StrategyMetricsResponse {
  return {
    strategies: [
      {
        strategy_id: 'alpha',
        strategy_version_id: 'alpha-v1',
        created_at: '2026-04-19T00:00:00+00:00',
        record_count: 2,
        insufficient_samples: false,
        brier_overall: 0.125,
        pnl: 2.0,
        fill_rate: 1.0,
        slippage_bps: 15.0,
        drawdown: 3.0
      },
      {
        strategy_id: 'beta',
        strategy_version_id: 'beta-v1',
        created_at: '2026-04-19T00:00:00+00:00',
        record_count: 2,
        insufficient_samples: false,
        brier_overall: 0.305,
        pnl: 4.0,
        fill_rate: 1.0,
        slippage_bps: 10.0,
        drawdown: 2.0
      },
      {
        strategy_id: 'default',
        strategy_version_id: 'default-v1',
        created_at: '2026-04-14T00:00:00+00:00',
        record_count: 0,
        insufficient_samples: true,
        brier_overall: null,
        pnl: 0.0,
        fill_rate: 0.0,
        slippage_bps: 0.0,
        drawdown: 0.0
      }
    ]
  };
}

export function mockBacktestRuns(): BacktestRunRow[] {
  return [
    {
      run_id: '11111111-1111-1111-1111-111111111111',
      spec_hash: 'spec-completed',
      status: 'completed',
      strategy_ids: ['alpha', 'beta', 'gamma'],
      date_range_start: '2026-04-01T00:00:00+00:00',
      date_range_end: '2026-04-30T00:00:00+00:00',
      exec_config_json: { chunk_days: 7, time_budget: 1800 },
      spec_json: {
        strategy_versions: [
          ['alpha', 'alpha-v1'],
          ['beta', 'beta-v1'],
          ['gamma', 'gamma-v1']
        ]
      },
      queued_at: '2026-04-18T09:00:00+00:00',
      started_at: '2026-04-18T09:00:05+00:00',
      finished_at: '2026-04-18T09:03:05+00:00',
      failure_reason: null,
      worker_pid: null,
      worker_host: null
    },
    {
      run_id: '22222222-2222-2222-2222-222222222222',
      spec_hash: 'spec-running',
      status: 'running',
      strategy_ids: ['alpha'],
      date_range_start: '2026-04-15T00:00:00+00:00',
      date_range_end: '2026-04-20T00:00:00+00:00',
      exec_config_json: { chunk_days: 7, time_budget: 900 },
      spec_json: {
        strategy_versions: [['alpha', 'alpha-v1']]
      },
      queued_at: '2026-04-19T09:00:00+00:00',
      started_at: '2026-04-19T09:00:10+00:00',
      finished_at: null,
      failure_reason: null,
      worker_pid: 99999,
      worker_host: 'mock-host'
    }
  ];
}

export function mockBacktestRun(runId: string): BacktestRunRow | null {
  return mockBacktestRuns().find((run) => run.run_id === runId) ?? null;
}

export function mockBacktestStrategyRuns(runId: string): BacktestStrategyRunRow[] {
  if (runId !== '11111111-1111-1111-1111-111111111111') {
    return [];
  }
  return [
    {
      strategy_run_id: 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
      run_id: runId,
      strategy_id: 'alpha',
      strategy_version_id: 'alpha-v1',
      brier: 0.11,
      pnl_cum: 3.0,
      drawdown_max: 4.0,
      fill_rate: 0.92,
      slippage_bps: 8.0,
      opportunity_count: 6,
      decision_count: 5,
      fill_count: 3,
      portfolio_target_json: [
        {
          market_id: 'market-a',
          token_id: 'token-a',
          side: 'buy_yes',
          timestamp: '2026-04-09T10:00:00+00:00',
          target_size_usdc: 20
        },
        {
          market_id: 'market-b',
          token_id: 'token-b',
          side: 'buy_yes',
          timestamp: '2026-04-09T11:00:00+00:00',
          target_size_usdc: 25
        }
      ],
      started_at: '2026-04-18T09:00:05+00:00',
      finished_at: '2026-04-18T09:01:05+00:00'
    },
    {
      strategy_run_id: 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb',
      run_id: runId,
      strategy_id: 'beta',
      strategy_version_id: 'beta-v1',
      brier: 0.26,
      pnl_cum: 12.0,
      drawdown_max: 3.0,
      fill_rate: 0.81,
      slippage_bps: 12.0,
      opportunity_count: 5,
      decision_count: 4,
      fill_count: 2,
      portfolio_target_json: [
        {
          market_id: 'market-c',
          token_id: 'token-c',
          side: 'buy_yes',
          timestamp: '2026-04-09T12:00:00+00:00',
          target_size_usdc: 20
        }
      ],
      started_at: '2026-04-18T09:00:10+00:00',
      finished_at: '2026-04-18T09:01:10+00:00'
    },
    {
      strategy_run_id: 'cccccccc-cccc-cccc-cccc-cccccccccccc',
      run_id: runId,
      strategy_id: 'gamma',
      strategy_version_id: 'gamma-v1',
      brier: 0.18,
      pnl_cum: -1.0,
      drawdown_max: 1.0,
      fill_rate: 0.67,
      slippage_bps: 6.0,
      opportunity_count: 4,
      decision_count: 4,
      fill_count: 1,
      portfolio_target_json: [
        {
          market_id: 'market-e',
          token_id: 'token-e',
          side: 'buy_yes',
          timestamp: '2026-04-09T14:00:00+00:00',
          target_size_usdc: 20
        }
      ],
      started_at: '2026-04-18T09:00:20+00:00',
      finished_at: '2026-04-18T09:01:20+00:00'
    }
  ];
}

export function mockBacktestComparison(runId: string): BacktestLiveComparisonResponse {
  return {
    comparison_id: 'eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee',
    run_id: runId,
    strategy_id: 'alpha',
    strategy_version_id: 'alpha-v1',
    live_window_start: '2026-04-10T00:00:00+00:00',
    live_window_end: '2026-04-12T23:59:59+00:00',
    denominator: 'union',
    equity_delta_json: [
      { day: '2026-04-10', backtest_equity: 3.0, live_equity: 1.0, delta: 2.0 },
      { day: '2026-04-11', backtest_equity: 6.0, live_equity: 3.5, delta: 2.5 },
      { day: '2026-04-12', backtest_equity: 10.0, live_equity: 3.5, delta: 6.5 }
    ],
    overlap_ratio: 0.33,
    backtest_only_symbols: ['token-b'],
    live_only_symbols: ['token-c'],
    time_alignment_policy_json: {},
    symbol_normalization_policy_json: {},
    computed_at: '2026-04-12T12:00:00+00:00'
  };
}

export function mockEnqueueBacktestRun(): BacktestEnqueueResponse {
  return {
    run_ids: ['33333333-3333-3333-3333-333333333333'],
    unique_run_count: 1,
    runs: [
      {
        run_id: '33333333-3333-3333-3333-333333333333',
        spec_hash: 'spec-enqueued',
        inserted: true
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
  const sinceTime = since ? Date.parse(since) : Number.NaN;
  const points = (mockFactorSeriesIndex[key] ?? mockFactorSeriesIndex[fallbackKey] ?? []).filter(
    (point) => {
      if (!since || Number.isNaN(sinceTime)) {
        return true;
      }
      const pointTime = Date.parse(point.ts);
      return !Number.isNaN(pointTime) && pointTime >= sinceTime;
    }
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
