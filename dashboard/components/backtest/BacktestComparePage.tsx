'use client';

import Link from 'next/link';
import { useEffect, useState } from 'react';
import { Nav } from '@/components/Nav';
import { apiPost } from '@/lib/api';
import { parseStrategyIdentity, strategyIdentityValue } from '@/lib/backtest';
import { useLiveData } from '@/lib/useLiveData';
import type {
  BacktestLiveComparisonResponse,
  BacktestRunRow,
  BacktestStrategyRunRow,
  SelectionDenominator
} from '@/lib/types';

type BacktestComparePageProps = {
  runId: string;
};

export function BacktestComparePage({ runId }: BacktestComparePageProps) {
  const runState = useLiveData<BacktestRunRow>(`/research/backtest/${runId}`, 15_000);
  const strategyState = useLiveData<BacktestStrategyRunRow[]>(
    `/research/backtest/${runId}/strategies`,
    15_000
  );
  const [strategyIdentity, setStrategyIdentity] = useState('');
  const [liveWindowStart, setLiveWindowStart] = useState('');
  const [liveWindowEnd, setLiveWindowEnd] = useState('');
  const [denominator, setDenominator] = useState<SelectionDenominator>('union');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<BacktestLiveComparisonResponse | null>(null);

  useEffect(() => {
    if (strategyIdentity === '' && strategyState.data && strategyState.data.length > 0) {
      setStrategyIdentity(strategyIdentityValue(strategyState.data[0]));
    }
  }, [strategyIdentity, strategyState.data]);

  useEffect(() => {
    if (runState.data === null || liveWindowStart !== '' || liveWindowEnd !== '') {
      return;
    }
    const end = dateInputValue(runState.data.date_range_end);
    const start = dateInputValue(runState.data.date_range_start);
    setLiveWindowStart(start);
    setLiveWindowEnd(end);
  }, [liveWindowEnd, liveWindowStart, runState.data]);

  async function computeComparison() {
    if (strategyIdentity === '' || liveWindowStart === '' || liveWindowEnd === '') {
      return;
    }
    const parsedIdentity = parseStrategyIdentity(strategyIdentity);
    if (parsedIdentity === null) {
      setError('Invalid strategy selection');
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const response = await apiPost<BacktestLiveComparisonResponse>(
        `/research/backtest/${runId}/compare`,
        {
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({
            strategy_id: parsedIdentity.strategyId,
            strategy_version_id: parsedIdentity.strategyVersionId,
            live_window_start: `${liveWindowStart}T00:00:00+00:00`,
            live_window_end: `${liveWindowEnd}T23:59:59+00:00`,
            denominator
          })
        }
      );
      setResult(response);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unable to compute comparison');
    } finally {
      setLoading(false);
    }
  }

  return (
    <main className="shell">
      <Nav />
      <section className="page">
        <div className="hero">
          <div>
            <p className="eyebrow">Research</p>
            <h1>Compare with Live</h1>
            <p className="lede">
              Align a completed strategy run against live fills and opportunities over a bounded
              window.
            </p>
          </div>
          <div className="hero-actions">
            <Link href={`/backtest/${runId}`} className="ghost-button">
              Back to run
            </Link>
          </div>
        </div>

        <section className="card controls-panel">
          <div className="control-grid compare-control-grid">
            <label className="field-group">
              <span>Strategy</span>
              <select
                aria-label="Strategy"
                value={strategyIdentity}
                onChange={(event) => setStrategyIdentity(event.target.value)}
              >
                {(strategyState.data ?? []).map((strategyRun) => (
                  <option
                    key={`${strategyRun.strategy_id}:${strategyRun.strategy_version_id}`}
                    value={strategyIdentityValue(strategyRun)}
                  >
                    {strategyRun.strategy_id} / {strategyRun.strategy_version_id}
                  </option>
                ))}
              </select>
            </label>
            <label className="field-group">
              <span>Live window start</span>
              <input
                aria-label="Live window start"
                type="date"
                value={liveWindowStart}
                onChange={(event) => setLiveWindowStart(event.target.value)}
              />
            </label>
            <label className="field-group">
              <span>Live window end</span>
              <input
                aria-label="Live window end"
                type="date"
                value={liveWindowEnd}
                onChange={(event) => setLiveWindowEnd(event.target.value)}
              />
            </label>
            <label className="field-group">
              <span>Overlap denominator</span>
              <select
                aria-label="Overlap denominator"
                value={denominator}
                onChange={(event) => setDenominator(event.target.value as SelectionDenominator)}
              >
                <option value="union">Union</option>
                <option value="backtest_set">Backtest set</option>
                <option value="live_set">Live set</option>
              </select>
            </label>
          </div>
          <div className="button-row compare-actions">
            <button
              type="button"
              className="primary-button"
              data-testid="compute-comparison"
              disabled={loading || strategyIdentity === '' || liveWindowStart === '' || liveWindowEnd === ''}
              onClick={() => void computeComparison()}
            >
              {loading ? 'Computing…' : 'Compute'}
            </button>
            {error ? <p className="error">{error}</p> : null}
          </div>
        </section>

        <div className="compare-panel-grid">
          <section className="card compare-panel" data-testid="compare-overlap-panel">
            <div className="section-heading">
              <h3>Overlap ratio</h3>
              <span className="muted">{runState.data?.status ?? 'run'} status</span>
            </div>
            <div className="metric" data-testid="compare-overlap-value">
              {result ? result.overlap_ratio.toFixed(2) : '—'}
            </div>
          </section>

          <section className="card compare-panel" data-testid="compare-equity-panel">
            <div className="section-heading">
              <h3>Equity delta</h3>
              <span className="muted">Backtest equity minus live equity.</span>
            </div>
            {result ? (
              <div className="table-wrap">
                <table data-testid="compare-equity-table">
                  <thead>
                    <tr>
                      <th>Day</th>
                      <th>Backtest</th>
                      <th>Live</th>
                      <th>Delta</th>
                    </tr>
                  </thead>
                  <tbody>
                    {result.equity_delta_json.map((row) => (
                      <tr key={row.day}>
                        <td>{row.day}</td>
                        <td>{row.backtest_equity.toFixed(2)}</td>
                        <td>{row.live_equity.toFixed(2)}</td>
                        <td>{row.delta.toFixed(2)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <p className="muted">Run a comparison to see the daily curve.</p>
            )}
          </section>

          <section className="card compare-panel" data-testid="compare-backtest-only-panel">
            <div className="section-heading">
              <h3>Backtest-only symbols</h3>
              <span className="muted">Targets missing from live selections.</span>
            </div>
            <ResultList values={result?.backtest_only_symbols ?? []} />
          </section>

          <section className="card compare-panel" data-testid="compare-live-only-panel">
            <div className="section-heading">
              <h3>Live-only symbols</h3>
              <span className="muted">Selections missing from the backtest target set.</span>
            </div>
            <ResultList values={result?.live_only_symbols ?? []} />
          </section>
        </div>
      </section>
    </main>
  );
}

function ResultList({ values }: { values: string[] }) {
  if (values.length === 0) {
    return <p className="muted">None</p>;
  }
  return (
    <ul className="symbol-list">
      {values.map((value) => (
        <li key={value}>{value}</li>
      ))}
    </ul>
  );
}

function dateInputValue(value: string): string {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value.slice(0, 10);
  }
  return parsed.toISOString().slice(0, 10);
}
