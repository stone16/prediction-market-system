import { Nav } from '@/components/Nav';
import { mockDecisions, mockMetrics, mockSignals } from '@/lib/mock-store';

export default function BacktestPage() {
  const decisions = mockDecisions();
  const metrics = mockMetrics();
  const signals = mockSignals();
  const first = signals[0]?.fetched_at ?? 'n/a';
  const last = signals[signals.length - 1]?.fetched_at ?? 'n/a';
  return (
    <main className="shell">
      <Nav />
      <section className="page">
        <div className="hero">
          <div>
            <p className="eyebrow">Replay</p>
            <h1>Backtest Run</h1>
            <p className="lede">Synthetic seven-day replay summary for the current pipeline.</p>
          </div>
        </div>
        <div className="summary-grid">
          <section className="card summary-card">
            <span className="muted">Total decisions</span>
            <div className="metric">{decisions.length}</div>
          </section>
          <section className="card summary-card">
            <span className="muted">P&L</span>
            <div className="metric">{metrics.pnl.toFixed(2)}</div>
          </section>
          <section className="card summary-card">
            <span className="muted">Brier by category</span>
            <div className="metric">{Object.keys(metrics.brier_by_category).length}</div>
          </section>
          <section className="card summary-card">
            <span className="muted">Date range covered</span>
            <p>{first} to {last}</p>
          </section>
        </div>
      </section>
    </main>
  );
}
