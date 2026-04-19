'use client';

import { MetricChartsNoSsr } from '@/components/MetricChartsNoSsr';
import { Nav } from '@/components/Nav';
import { useLiveData } from '@/lib/useLiveData';
import type { MetricsAggregate, MetricsPerStrategyRow, MetricsResponse } from '@/lib/types';

const EMPTY_OPS_VIEW: MetricsAggregate = {
  brier_overall: null,
  brier_by_category: {},
  pnl: 0,
  slippage_bps: 0,
  fill_rate: 0,
  win_rate: 0,
  brier_series: [],
  calibration_curve: [],
  pnl_series: []
};

const EMPTY_METRICS: MetricsResponse = {
  ...EMPTY_OPS_VIEW,
  per_strategy: [],
  ops_view: EMPTY_OPS_VIEW
};

export default function MetricsPage() {
  const { data, loading, disconnected } = useLiveData<MetricsResponse>('/metrics');
  const metrics = data ?? EMPTY_METRICS;
  const opsView = metrics.ops_view ?? EMPTY_OPS_VIEW;
  const hasOpsData = (opsView.brier_series?.length ?? 0) > 0;
  const perStrategy = metrics.per_strategy ?? [];

  return (
    <main className="shell">
      <Nav />
      <section className="page">
        <div className="hero">
          <div>
            <p className="eyebrow">Evaluator</p>
            <h1>Metric Review</h1>
            <p className="lede">Brier, calibration, and P&amp;L traces for the current run.</p>
          </div>
          {disconnected ? <span className="badge disconnected">disconnected</span> : null}
        </div>

        {loading && perStrategy.length === 0 && !hasOpsData ? (
          <p className="muted">Loading metrics…</p>
        ) : (
          <>
            <section data-testid="metrics-per-strategy" style={{ marginBottom: 24 }}>
              <p className="eyebrow">Per-strategy breakdown</p>
              <h2>Per-strategy breakdown</h2>
              <p className="lede">
                Strategy-version rollups render first so the default metrics view reflects the
                inner-ring identity boundary.
              </p>
              {perStrategy.length === 0 ? (
                <p className="muted">No strategy metrics yet.</p>
              ) : (
                <div className="table-wrap" style={{ marginTop: 18 }}>
                  <table>
                    <thead>
                      <tr>
                        <th>Strategy</th>
                        <th>Version</th>
                        <th>Records</th>
                        <th>Brier</th>
                        <th>P&amp;L</th>
                        <th>Fill rate</th>
                        <th>Slippage</th>
                        <th>Drawdown</th>
                      </tr>
                    </thead>
                    <tbody>
                      {perStrategy.map((strategy) => (
                        <tr
                          key={`${strategy.strategy_id}:${strategy.strategy_version_id}`}
                          data-testid="metrics-strategy-row"
                        >
                          <td>{strategy.strategy_id}</td>
                          <td>{strategy.strategy_version_id}</td>
                          <td>{formatRecordCount(strategy)}</td>
                          <td>{formatBrier(strategy)}</td>
                          <td>{formatPnl(strategy.pnl)}</td>
                          <td>{formatFillRate(strategy.fill_rate)}</td>
                          <td>{formatSlippage(strategy.slippage_bps)}</td>
                          <td>{formatDrawdown(strategy.drawdown)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </section>

            <section data-testid="metrics-ops-view">
              <p className="eyebrow">ops view</p>
              <h2>ops view (cross-strategy)</h2>
              <p className="lede">Global rollups stay here for cross-strategy operational review.</p>
              <div className="summary-grid">
                <section className="card summary-card">
                  <span className="muted">Brier overall</span>
                  <div className="metric">{opsView.brier_overall?.toFixed(3) ?? 'n/a'}</div>
                </section>
                <section className="card summary-card">
                  <span className="muted">Fill rate</span>
                  <div className="metric">{(opsView.fill_rate * 100).toFixed(1)}%</div>
                </section>
                <section className="card summary-card">
                  <span className="muted">Win rate</span>
                  <div className="metric">{(opsView.win_rate * 100).toFixed(1)}%</div>
                </section>
                <section className="card summary-card">
                  <span className="muted">P&amp;L</span>
                  <div className="metric">{opsView.pnl.toFixed(2)}</div>
                </section>
              </div>
              {hasOpsData ? (
                <MetricChartsNoSsr metrics={opsView} />
              ) : (
                <p className="muted">
                  No eval records yet. Run a backtest or start the paper runner to populate charts.
                </p>
              )}
            </section>
          </>
        )}
      </section>
    </main>
  );
}

function formatRecordCount(strategy: MetricsPerStrategyRow): string {
  return strategy.record_count === 1 ? '1 record' : `${strategy.record_count} records`;
}

function formatBrier(strategy: MetricsPerStrategyRow): string {
  if (strategy.insufficient_samples || strategy.brier_overall === null) {
    return 'insufficient samples';
  }
  return strategy.brier_overall.toFixed(3);
}

function formatPnl(pnl: number): string {
  return pnl.toFixed(2);
}

function formatFillRate(fillRate: number): string {
  return `${(fillRate * 100).toFixed(1)}%`;
}

function formatSlippage(slippageBps: number): string {
  return `${slippageBps.toFixed(1)} bps`;
}

function formatDrawdown(drawdown: number): string {
  return drawdown.toFixed(2);
}
