'use client';

import { Fragment, useMemo, useState } from 'react';
import Link from 'next/link';
import { Nav } from '@/components/Nav';
import {
  formatDateTime,
  formatRunSpecSummary,
  formatTimeBudgetUsed,
  rankingMetricLabel,
  sortStrategyRuns,
  strategyDetailLinkTestId,
  strategyDetailPanelTestId,
  strategyIdentityValue,
  strategyRowTestId,
  statusTone
} from '@/lib/backtest';
import { useLiveData } from '@/lib/useLiveData';
import type { BacktestRankingMetric, BacktestRunRow, BacktestStrategyRunRow } from '@/lib/types';

type BacktestRunViewProps = {
  runId: string;
};

export function BacktestRunView({ runId }: BacktestRunViewProps) {
  const runState = useLiveData<BacktestRunRow>(`/research/backtest/${runId}`, 2_000);
  const strategyState = useLiveData<BacktestStrategyRunRow[]>(
    `/research/backtest/${runId}/strategies`,
    runState.data?.status === 'running' ? 2_000 : 15_000
  );
  const [rankingMetric, setRankingMetric] = useState<BacktestRankingMetric>('brier');
  const [expandedStrategyIdentity, setExpandedStrategyIdentity] = useState<string | null>(null);

  const run = runState.data;
  const strategyRuns = strategyState.data ?? [];
  const sortedStrategies = useMemo(
    () => sortStrategyRuns(strategyRuns, rankingMetric),
    [rankingMetric, strategyRuns]
  );

  if (runState.loading && run === null) {
    return (
      <main className="shell">
        <Nav />
        <section className="page">
          <p className="muted">Loading backtest run…</p>
        </section>
      </main>
    );
  }

  if (run === null) {
    return (
      <main className="shell">
        <Nav />
        <section className="page">
          <h1>Backtest Run</h1>
          <p className="muted">Run not found.</p>
        </section>
      </main>
    );
  }

  return (
    <main className="shell">
      <Nav />
      <section className="page" data-testid="backtest-run-view">
        <div className="hero">
          <div>
            <p className="eyebrow">Research</p>
            <h1>Backtest Run</h1>
            <p className="lede">{formatRunSpecSummary(run)}</p>
          </div>
          <div className="hero-actions">
            <span className={statusTone(run.status)}>{run.status}</span>
            <Link href={`/backtest/${runId}/compare`} className="primary-button" data-testid="compare-with-live">
              Compare with live
            </Link>
          </div>
        </div>

        <div className="summary-grid backtest-summary-grid">
          <section className="card summary-card">
            <span className="muted">Queued</span>
            <p>{formatDateTime(run.queued_at)}</p>
          </section>
          <section className="card summary-card">
            <span className="muted">Completed</span>
            <p>{formatDateTime(run.finished_at)}</p>
          </section>
          <section className="card summary-card">
            <span className="muted">Time budget used</span>
            <p>{formatTimeBudgetUsed(run)}</p>
          </section>
          <section className="card summary-card">
            <span className="muted">Strategies</span>
            <div className="metric compact">{sortedStrategies.length}</div>
          </section>
        </div>

        <div className="card backtest-toolbar">
          <label className="field-group inline-field">
            <span>Ranking metric</span>
            <select
              aria-label="Ranking metric"
              value={rankingMetric}
              onChange={(event) => setRankingMetric(event.target.value as BacktestRankingMetric)}
            >
              <option value="brier">Brier</option>
              <option value="sharpe">Sharpe</option>
              <option value="pnl_cum">Cumulative P&amp;L</option>
            </select>
          </label>
          <span className="muted">{rankingMetricLabel(rankingMetric)} ordering is active.</span>
        </div>

        {strategyState.loading && sortedStrategies.length === 0 ? (
          <p className="muted">Loading strategy runs…</p>
        ) : sortedStrategies.length === 0 ? (
          <section className="card empty-state">
            <h3>No strategy rows yet</h3>
            <p className="muted">This run is queued but no strategy metrics have landed yet.</p>
          </section>
        ) : (
          <div className="table-wrap backtest-table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Rank</th>
                  <th>Strategy</th>
                  <th>Version</th>
                  <th>Brier</th>
                  <th>Sharpe</th>
                  <th>P&amp;L</th>
                  <th>Fill rate</th>
                  <th>Slippage</th>
                  <th>Finished</th>
                </tr>
              </thead>
              <tbody>
                {sortedStrategies.map((strategyRun, index) => {
                  const strategyIdentity = strategyIdentityValue(strategyRun);
                  const expanded = expandedStrategyIdentity === strategyIdentity;
                  return (
                    <Fragment key={strategyIdentity}>
                      <tr
                        className="interactive-row"
                        data-testid={strategyRowTestId(strategyRun)}
                        onClick={() =>
                          setExpandedStrategyIdentity((current) =>
                            current === strategyIdentity ? null : strategyIdentity
                          )
                        }
                      >
                        <td>{index + 1}</td>
                        <td>{strategyRun.strategy_id}</td>
                        <td>{strategyRun.strategy_version_id}</td>
                        <td>{formatFixed(strategyRun.brier, 3)}</td>
                        <td>{formatSharpe(strategyRun)}</td>
                        <td>{formatFixed(strategyRun.pnl_cum, 2)}</td>
                        <td>{formatRate(strategyRun.fill_rate)}</td>
                        <td>{formatBps(strategyRun.slippage_bps)}</td>
                        <td>{formatDateTime(strategyRun.finished_at)}</td>
                      </tr>
                      {expanded ? (
                        <tr className="detail-row">
                          <td colSpan={9}>
                            <div
                              className="detail-panel"
                              data-testid={strategyDetailPanelTestId(strategyRun)}
                            >
                              <div className="detail-grid">
                                <section>
                                  <span className="muted">Opportunities</span>
                                  <div className="metric compact">{strategyRun.opportunity_count ?? 0}</div>
                                </section>
                                <section>
                                  <span className="muted">Decisions</span>
                                  <div className="metric compact">{strategyRun.decision_count ?? 0}</div>
                                </section>
                                <section>
                                  <span className="muted">Fills</span>
                                  <div className="metric compact">{strategyRun.fill_count ?? 0}</div>
                                </section>
                              </div>
                              <div className="detail-actions">
                                <Link
                                  href={`/backtest/${runId}/${strategyIdentity}`}
                                  className="ghost-button"
                                  data-testid={strategyDetailLinkTestId(strategyRun)}
                                >
                                  Details
                                </Link>
                              </div>
                            </div>
                          </td>
                        </tr>
                      ) : null}
                    </Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </main>
  );
}

function formatFixed(value: number | null, digits: number): string {
  return value === null ? 'n/a' : value.toFixed(digits);
}

function formatRate(value: number | null): string {
  return value === null ? 'n/a' : `${(value * 100).toFixed(1)}%`;
}

function formatBps(value: number | null): string {
  return value === null ? 'n/a' : `${value.toFixed(1)} bps`;
}

function formatSharpe(strategyRun: BacktestStrategyRunRow): string {
  const pnl = strategyRun.pnl_cum ?? 0;
  const drawdown = strategyRun.drawdown_max ?? 0;
  const sharpe = drawdown > 0 ? pnl / drawdown : pnl;
  return sharpe.toFixed(2);
}
