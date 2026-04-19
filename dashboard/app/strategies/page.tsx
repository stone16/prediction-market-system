'use client';

import { Nav } from '@/components/Nav';
import { useLiveData } from '@/lib/useLiveData';
import type { StrategyMetricsRow, StrategyMetricsResponse } from '@/lib/types';

const createdAtFormatter = new Intl.DateTimeFormat(undefined, {
  dateStyle: 'medium',
  timeStyle: 'short'
});

function formatCreatedAt(createdAt: string): string {
  const parsed = new Date(createdAt);
  if (Number.isNaN(parsed.getTime())) {
    return createdAt;
  }
  return createdAtFormatter.format(parsed);
}

export default function StrategiesPage() {
  const { data, loading, disconnected } = useLiveData<StrategyMetricsResponse>(
    '/strategies/metrics',
    15_000
  );
  const strategies = data?.strategies ?? [];

  return (
    <main className="shell">
      <Nav />
      <section className="page">
        <div className="hero">
          <div>
            <p className="eyebrow">Controller</p>
            <h1>Strategies</h1>
            <p className="lede">
              Comparative Brier, P&amp;L, fill-rate, slippage, and drawdown rollups by active
              strategy version.
            </p>
          </div>
          {disconnected ? <span className="badge disconnected">disconnected</span> : null}
        </div>

        {loading && strategies.length === 0 ? (
          <p className="muted">Loading strategy metrics…</p>
        ) : disconnected && strategies.length === 0 ? (
          <p className="muted">Strategy metrics unavailable.</p>
        ) : strategies.length === 0 ? (
          <p className="muted">No strategy metrics available.</p>
        ) : (
          <div className="table-wrap">
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
                  <th>Created</th>
                </tr>
              </thead>
              <tbody>
                {strategies.map((strategy) => (
                  <tr
                    key={`${strategy.strategy_id}:${strategy.strategy_version_id}`}
                    data-testid="strategy-metrics-row"
                  >
                    <td>{strategy.strategy_id}</td>
                    <td>{strategy.strategy_version_id}</td>
                    <td>{formatRecordCount(strategy)}</td>
                    <td>{formatBrier(strategy)}</td>
                    <td>{formatPnl(strategy.pnl)}</td>
                    <td>{formatFillRate(strategy.fill_rate)}</td>
                    <td>{formatSlippage(strategy.slippage_bps)}</td>
                    <td>{formatDrawdown(strategy.drawdown)}</td>
                    <td>{formatCreatedAt(strategy.created_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </main>
  );
}

function formatRecordCount(strategy: StrategyMetricsRow): string {
  if (strategy.record_count === 0) {
    return '0 records (insufficient samples)';
  }
  return strategy.record_count === 1 ? '1 record' : `${strategy.record_count} records`;
}

function formatBrier(strategy: StrategyMetricsRow): string {
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
