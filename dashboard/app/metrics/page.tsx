'use client';

import { MetricChartsNoSsr } from '@/components/MetricChartsNoSsr';
import { Nav } from '@/components/Nav';
import { useLiveData } from '@/lib/useLiveData';
import type { MetricsResponse } from '@/lib/types';

const EMPTY_METRICS: MetricsResponse = {
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

export default function MetricsPage() {
  const { data, loading, disconnected } = useLiveData<MetricsResponse>('/metrics');
  const metrics = data ?? EMPTY_METRICS;
  const hasData = (metrics.brier_series?.length ?? 0) > 0;

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
        <div className="summary-grid">
          <section className="card summary-card">
            <span className="muted">Brier overall</span>
            <div className="metric">{metrics.brier_overall?.toFixed(3) ?? 'n/a'}</div>
          </section>
          <section className="card summary-card">
            <span className="muted">Fill rate</span>
            <div className="metric">{(metrics.fill_rate * 100).toFixed(1)}%</div>
          </section>
          <section className="card summary-card">
            <span className="muted">Win rate</span>
            <div className="metric">{(metrics.win_rate * 100).toFixed(1)}%</div>
          </section>
          <section className="card summary-card">
            <span className="muted">P&amp;L</span>
            <div className="metric">{metrics.pnl.toFixed(2)}</div>
          </section>
        </div>
        {loading && !hasData ? (
          <p className="muted">Loading metrics…</p>
        ) : hasData ? (
          <MetricChartsNoSsr metrics={metrics} />
        ) : (
          <p className="muted">
            No eval records yet. Run a backtest or start the paper runner to populate charts.
          </p>
        )}
      </section>
    </main>
  );
}
