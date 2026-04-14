import { MetricChartsNoSsr } from '@/components/MetricChartsNoSsr';
import { Nav } from '@/components/Nav';
import { mockMetrics } from '@/lib/mock-store';

export default function MetricsPage() {
  const metrics = mockMetrics();
  return (
    <main className="shell">
      <Nav />
      <section className="page">
        <div className="hero">
          <div>
            <p className="eyebrow">Evaluator</p>
            <h1>Metric Review</h1>
            <p className="lede">Brier, calibration, and P&L traces for the current backtest run.</p>
          </div>
        </div>
        <MetricChartsNoSsr metrics={metrics} />
      </section>
    </main>
  );
}
