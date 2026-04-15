'use client';

import { RunControls } from '@/components/RunControls';
import { Nav } from '@/components/Nav';
import { useLiveData } from '@/lib/useLiveData';
import type { MetricsResponse, Signal, StatusResponse } from '@/lib/types';

export default function BacktestPage() {
  const status = useLiveData<StatusResponse>('/status');
  const metrics = useLiveData<MetricsResponse>('/metrics');
  const signals = useLiveData<Signal[]>('/signals?limit=200');

  const first = signals.data?.[0]?.fetched_at ?? 'n/a';
  const last = signals.data?.[signals.data.length - 1]?.fetched_at ?? 'n/a';
  const running = status.data?.running ?? false;
  const disconnected = status.disconnected || metrics.disconnected || signals.disconnected;

  return (
    <main className="shell">
      <Nav />
      <section className="page">
        <div className="hero">
          <div>
            <p className="eyebrow">Replay</p>
            <h1>Backtest Run</h1>
            <p className="lede">
              Trigger a backtest replay and watch decisions, fills, and metrics populate.
            </p>
          </div>
          {disconnected ? <span className="badge disconnected">disconnected</span> : null}
        </div>
        <RunControls
          running={running}
          mode={status.data?.mode ?? null}
          onChange={() => {
            // next poll tick refreshes status
          }}
        />
        <div className="summary-grid">
          <section className="card summary-card">
            <span className="muted">Mode</span>
            <div className="metric">{status.data?.mode ?? 'n/a'}</div>
          </section>
          <section className="card summary-card">
            <span className="muted">Total decisions</span>
            <div className="metric">{status.data?.controller.decisions_total ?? 0}</div>
          </section>
          <section className="card summary-card">
            <span className="muted">Fills</span>
            <div className="metric">{status.data?.actuator.fills_total ?? 0}</div>
          </section>
          <section className="card summary-card">
            <span className="muted">P&amp;L</span>
            <div className="metric">{metrics.data?.pnl.toFixed(2) ?? '0.00'}</div>
          </section>
          <section className="card summary-card">
            <span className="muted">Brier overall</span>
            <div className="metric">{metrics.data?.brier_overall?.toFixed(3) ?? 'n/a'}</div>
          </section>
          <section className="card summary-card">
            <span className="muted">Signals window</span>
            <p>{first}</p>
            <p className="muted">to</p>
            <p>{last}</p>
          </section>
        </div>
      </section>
    </main>
  );
}
