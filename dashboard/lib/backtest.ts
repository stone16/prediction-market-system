import type {
  BacktestEnqueueResponse,
  BacktestRankingMetric,
  BacktestRunRow,
  BacktestStrategyRunRow,
  StrategyRow
} from './types';

export type SweepParameterRow = {
  id: string;
  field: string;
  values: string;
};

export type SweepRiskPolicy = {
  max_position_notional_usdc: number;
  max_daily_drawdown_pct: number;
  min_order_size_usdc: number;
};

export const DEFAULT_SWEEP_RISK_POLICY: SweepRiskPolicy = {
  max_position_notional_usdc: 100,
  max_daily_drawdown_pct: 2.5,
  min_order_size_usdc: 1
};

const STRATEGY_IDENTITY_SEPARATOR = '::';
const NUMERIC_PARAMETER_PATTERN = /^-?(?:\d+|\d+\.\d+|\.\d+)$/;

const dateTimeFormatter = new Intl.DateTimeFormat(undefined, {
  dateStyle: 'medium',
  timeStyle: 'short'
});

export function formatDateTime(value: string | null): string {
  if (!value) return 'Not started';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return dateTimeFormatter.format(parsed);
}

export function formatRunSpecSummary(run: BacktestRunRow): string {
  const pairs = run.spec_json.strategy_versions ?? [];
  const labels = pairs.map(([strategyId]) => strategyId);
  const labelText =
    labels.length === 0 ? 'No strategies' : labels.length <= 3 ? labels.join(', ') : `${labels.length} strategies`;
  return `${labelText} · ${formatShortDate(run.date_range_start)} to ${formatShortDate(run.date_range_end)}`;
}

export function formatTimeBudgetUsed(run: BacktestRunRow): string {
  const budget = Number(run.exec_config_json.time_budget ?? 0);
  if (!run.started_at) {
    return budget > 0 ? `0s / ${budget}s` : 'Not started';
  }
  const startedAt = Date.parse(run.started_at);
  const finishedAt = run.finished_at ? Date.parse(run.finished_at) : Date.now();
  if (Number.isNaN(startedAt) || Number.isNaN(finishedAt)) {
    return budget > 0 ? `Unknown / ${budget}s` : 'Unknown';
  }
  const elapsedSeconds = Math.max(0, Math.round((finishedAt - startedAt) / 1000));
  return budget > 0 ? `${elapsedSeconds}s / ${budget}s` : `${elapsedSeconds}s`;
}

export function statusTone(status: BacktestRunRow['status']): string {
  switch (status) {
    case 'completed':
      return 'badge ok';
    case 'running':
      return 'badge info';
    case 'failed':
      return 'badge disconnected';
    case 'cancelled':
      return 'badge warning';
    default:
      return 'badge muted-badge';
  }
}

export function rankingMetricLabel(metric: BacktestRankingMetric): string {
  switch (metric) {
    case 'brier':
      return 'Brier';
    case 'sharpe':
      return 'Sharpe';
    default:
      return 'P&L';
  }
}

export function sortStrategyRuns(
  strategyRuns: BacktestStrategyRunRow[],
  metric: BacktestRankingMetric
): BacktestStrategyRunRow[] {
  return [...strategyRuns].sort((left, right) => {
    const leftValue = metricValue(left, metric);
    const rightValue = metricValue(right, metric);
    if (metric === 'brier') {
      if (leftValue !== rightValue) return leftValue - rightValue;
    } else if (leftValue !== rightValue) {
      return rightValue - leftValue;
    }
    return `${left.strategy_id}:${left.strategy_version_id}`.localeCompare(
      `${right.strategy_id}:${right.strategy_version_id}`
    );
  });
}

export function strategyIdentityValue(strategyRun: BacktestStrategyRunRow): string {
  return `${encodeURIComponent(strategyRun.strategy_id)}${STRATEGY_IDENTITY_SEPARATOR}${encodeURIComponent(strategyRun.strategy_version_id)}`;
}

export function parseStrategyIdentity(
  value: string
): { strategyId: string; strategyVersionId: string } | null {
  const separatorIndex = value.indexOf(STRATEGY_IDENTITY_SEPARATOR);
  if (separatorIndex <= 0 || separatorIndex >= value.length - STRATEGY_IDENTITY_SEPARATOR.length) {
    return null;
  }
  try {
    return {
      strategyId: decodeURIComponent(value.slice(0, separatorIndex)),
      strategyVersionId: decodeURIComponent(
        value.slice(separatorIndex + STRATEGY_IDENTITY_SEPARATOR.length)
      )
    };
  } catch {
    return null;
  }
}

export function strategyRowTestId(strategyRun: BacktestStrategyRunRow): string {
  return `strategy-row-${sanitizeTestIdPart(strategyRun.strategy_id)}-${sanitizeTestIdPart(strategyRun.strategy_version_id)}`;
}

export function strategyDetailPanelTestId(strategyRun: BacktestStrategyRunRow): string {
  return `strategy-detail-panel-${sanitizeTestIdPart(strategyRun.strategy_id)}-${sanitizeTestIdPart(strategyRun.strategy_version_id)}`;
}

export function strategyDetailLinkTestId(strategyRun: BacktestStrategyRunRow): string {
  return `strategy-detail-link-${sanitizeTestIdPart(strategyRun.strategy_id)}-${sanitizeTestIdPart(strategyRun.strategy_version_id)}`;
}

export function buildSweepYaml(args: {
  selectedStrategies: StrategyRow[];
  startDate: string;
  endDate: string;
  profile: 'polymarket_paper' | 'polymarket_live_estimate';
  chunkDays: number;
  timeBudget: number;
  parameterRows: SweepParameterRow[];
  riskPolicy?: SweepRiskPolicy;
}): string {
  const riskPolicy = args.riskPolicy ?? DEFAULT_SWEEP_RISK_POLICY;
  const strategyVersions = args.selectedStrategies.map((strategy) => [
    strategy.strategy_id,
    strategy.active_version_id ?? `${strategy.strategy_id}-v1`
  ]);
  const executionModel =
    args.profile === 'polymarket_live_estimate'
      ? {
          fee_rate: 0.04,
          slippage_bps: 10,
          latency_ms: 250,
          staleness_ms: 120000,
          fill_policy: 'immediate_or_cancel'
        }
      : {
          fee_rate: 0,
          slippage_bps: 0,
          latency_ms: 0,
          staleness_ms: Number.POSITIVE_INFINITY,
          fill_policy: 'immediate_or_cancel'
        };
  const parameterGrid: Record<string, Array<boolean | number | string>> = {};
  for (const row of args.parameterRows) {
    const key = row.field.trim();
    if (!key) continue;
    const parsedValues = row.values
      .split(',')
      .map((value) => parseParameterValue(value.trim()))
      .filter((value) => value !== '');
    if (parsedValues.length > 0) {
      parameterGrid[key] = parsedValues;
    }
  }

  return serializeYaml({
    base_spec: {
      strategy_versions: strategyVersions,
      dataset: {
        source: 'fixture',
        version: 'v1',
        coverage_start: `${args.startDate}T00:00:00+00:00`,
        coverage_end: `${args.endDate}T00:00:00+00:00`,
        market_universe_filter: { market_ids: [] },
        data_quality_gaps: []
      },
      execution_model: executionModel,
      risk_policy: {
        max_position_notional_usdc: riskPolicy.max_position_notional_usdc,
        max_daily_drawdown_pct: riskPolicy.max_daily_drawdown_pct,
        min_order_size_usdc: riskPolicy.min_order_size_usdc
      },
      date_range_start: `${args.startDate}T00:00:00+00:00`,
      date_range_end: `${args.endDate}T00:00:00+00:00`
    },
    exec_config: {
      chunk_days: args.chunkDays,
      time_budget: args.timeBudget
    },
    parameter_grid: parameterGrid
  });
}

export function defaultParameterRows(): SweepParameterRow[] {
  return [{ id: 'parameter-row-1', field: '', values: '' }];
}

export function ensureRunId(response: BacktestEnqueueResponse): string {
  const firstRunId = response.run_ids[0];
  if (!firstRunId) {
    throw new Error('Backtest enqueue returned no run_ids');
  }
  return firstRunId;
}

function formatShortDate(value: string): string {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return parsed.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
}

function metricValue(strategyRun: BacktestStrategyRunRow, metric: BacktestRankingMetric): number {
  if (metric === 'brier') {
    return strategyRun.brier ?? 1;
  }
  if (metric === 'pnl_cum') {
    return strategyRun.pnl_cum ?? 0;
  }
  const pnl = strategyRun.pnl_cum ?? 0;
  const drawdown = strategyRun.drawdown_max ?? 0;
  return drawdown > 0 ? pnl / drawdown : pnl;
}

function parseParameterValue(value: string): boolean | number | string {
  if (value === '') {
    return '';
  }
  if (value === 'true') return true;
  if (value === 'false') return false;
  if (NUMERIC_PARAMETER_PATTERN.test(value)) {
    const numeric = Number(value);
    return numeric;
  }
  return value;
}

function serializeYaml(value: unknown, depth = 0): string {
  const indent = '  '.repeat(depth);
  if (Array.isArray(value)) {
    if (value.length === 0) return '[]';
    return value
      .map((item) => {
        if (isScalar(item)) {
          return `${indent}- ${serializeScalar(item)}`;
        }
        const nested = serializeYaml(item, depth + 1);
        return `${indent}-\n${nested}`;
      })
      .join('\n');
  }
  if (value && typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>);
    if (entries.length === 0) return '{}';
    return entries
      .map(([key, entryValue]) => {
        if (
          (Array.isArray(entryValue) && entryValue.length === 0) ||
          (entryValue && typeof entryValue === 'object' && !Array.isArray(entryValue) && Object.keys(entryValue as Record<string, unknown>).length === 0)
        ) {
          return `${indent}${key}: ${Array.isArray(entryValue) ? '[]' : '{}'}`;
        }
        if (isScalar(entryValue)) {
          return `${indent}${key}: ${serializeScalar(entryValue)}`;
        }
        const nested = serializeYaml(entryValue, depth + 1);
        return `${indent}${key}:\n${nested}`;
      })
      .join('\n');
  }
  return `${indent}${serializeScalar(value)}`;
}

function isScalar(value: unknown): value is boolean | number | string | null {
  return value === null || ['boolean', 'number', 'string'].includes(typeof value);
}

function serializeScalar(value: boolean | number | string | null | unknown): string {
  if (value === null) return 'null';
  if (typeof value === 'number') {
    if (Number.isNaN(value)) return '.nan';
    if (value === Number.POSITIVE_INFINITY) return '.inf';
    if (value === Number.NEGATIVE_INFINITY) return '-.inf';
    return String(value);
  }
  if (typeof value === 'boolean') {
    return String(value);
  }
  return JSON.stringify(String(value));
}

function sanitizeTestIdPart(value: string): string {
  return value.replace(/[^A-Za-z0-9_-]+/g, '-');
}
