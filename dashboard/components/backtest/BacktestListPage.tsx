'use client';

import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { useState } from 'react';
import { Nav } from '@/components/Nav';
import { formatDateTime, formatRunSpecSummary, formatTimeBudgetUsed, statusTone } from '@/lib/backtest';
import { useLiveData } from '@/lib/useLiveData';
import type { BacktestRunRow, StrategiesResponse } from '@/lib/types';
import { NewSweepModal } from './NewSweepModal';

export function BacktestListPage() {
  const router = useRouter();
  const runsState = useLiveData<BacktestRunRow[]>('/research/backtest?limit=25', 5_000);
  const strategiesState = useLiveData<StrategiesResponse>('/strategies', 15_000);
  const [modalOpen, setModalOpen] = useState(false);
  const runs = runsState.data ?? [];
  const strategies = strategiesState.data?.strategies ?? [];
  const disconnected = runsState.disconnected || strategiesState.disconnected;

  return (
    <main className="shell">
      <Nav />
      <section className="page" data-testid="backtest-list-view">
        <div className="hero">
          <div>
            <p className="eyebrow">Research</p>
            <h1>Backtest Run</h1>
            <p className="lede">
              Queue research sweeps, inspect ranked strategy runs, and compare completed runs
              against live execution traces.
            </p>
          </div>
          <div className="hero-actions">
            {disconnected ? <span className="badge disconnected">disconnected</span> : null}
            <button
              type="button"
              className="primary-button"
              data-testid="new-sweep-open"
              onClick={() => setModalOpen(true)}
            >
              New sweep
            </button>
          </div>
        </div>

        <div className="summary-grid backtest-summary-grid">
          <section className="card summary-card">
            <span className="muted">Recent runs</span>
            <div className="metric">{runs.length}</div>
          </section>
          <section className="card summary-card">
            <span className="muted">Running now</span>
            <div className="metric">{runs.filter((run) => run.status === 'running').length}</div>
          </section>
          <section className="card summary-card">
            <span className="muted">Strategies available</span>
            <div className="metric">{strategies.length}</div>
          </section>
        </div>

        {runsState.loading && runs.length === 0 ? (
          <p className="muted">Loading recent backtest runs…</p>
        ) : runs.length === 0 ? (
          <section className="card empty-state">
            <h3>No backtest runs yet</h3>
            <p className="muted">Open a sweep to export YAML or enqueue a new research run.</p>
          </section>
        ) : (
          <div className="table-wrap backtest-table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Run</th>
                  <th>Spec summary</th>
                  <th>Status</th>
                  <th>Queued</th>
                  <th>Completed</th>
                  <th>Time budget used</th>
                </tr>
              </thead>
              <tbody>
                {runs.map((run) => (
                  <tr key={run.run_id} data-testid="backtest-run-row">
                    <td>
                      <Link href={`/backtest/${run.run_id}`} className="run-link">
                        {run.run_id.slice(0, 8)}
                      </Link>
                    </td>
                    <td>{formatRunSpecSummary(run)}</td>
                    <td>
                      <span className={statusTone(run.status)}>{run.status}</span>
                    </td>
                    <td>{formatDateTime(run.queued_at)}</td>
                    <td>{formatDateTime(run.finished_at)}</td>
                    <td>{formatTimeBudgetUsed(run)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {modalOpen ? (
          <NewSweepModal
            strategies={strategies}
            onClose={() => setModalOpen(false)}
            onSubmitted={(runId) => {
              setModalOpen(false);
              router.push(`/backtest/${runId}`);
            }}
          />
        ) : null}
      </section>
    </main>
  );
}
