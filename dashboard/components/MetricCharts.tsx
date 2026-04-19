'use client';

import {
  CartesianGrid,
  Line,
  LineChart,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis
} from 'recharts';
import type { MetricsAggregate } from '@/lib/types';

type ChartProps = {
  title: string;
  children: React.ReactNode;
  empty: boolean;
};

function ChartPanel({ title, children, empty }: ChartProps) {
  return (
    <section className="chart-panel">
      <h2>{title}</h2>
      <div className="chart-frame">
        {empty ? <div className="empty-chart">No data yet</div> : null}
        {children}
      </div>
    </section>
  );
}

export function MetricCharts({ metrics }: { metrics: MetricsAggregate }) {
  const brier = metrics.brier_series ?? [];
  const calibration = metrics.calibration_curve ?? [];
  const pnl = metrics.pnl_series ?? [];
  return (
    <div className="chart-grid">
      <ChartPanel empty={brier.length === 0} title="Brier score over time">
        <LineChart data={brier} height={240} width={940}>
          <CartesianGrid stroke="#d9e2d4" />
          <XAxis dataKey="recorded_at" hide />
          <YAxis domain={[0, 1]} />
          <Tooltip />
          <Line dataKey="brier_score" dot={false} stroke="#1f8a70" strokeWidth={3} type="monotone" />
        </LineChart>
      </ChartPanel>
      <ChartPanel empty={calibration.length === 0} title="Calibration curve">
        <ScatterChart height={240} width={940}>
          <CartesianGrid stroke="#d9e2d4" />
          <XAxis dataKey="prob_estimate" name="Probability" type="number" />
          <YAxis dataKey="resolved_outcome" name="Outcome" type="number" />
          <Tooltip />
          <Scatter data={calibration} fill="#d84f3f" name="Calibration" />
        </ScatterChart>
      </ChartPanel>
      <ChartPanel empty={pnl.length === 0} title="P&L over time">
        <LineChart data={pnl} height={240} width={940}>
          <CartesianGrid stroke="#d9e2d4" />
          <XAxis dataKey="recorded_at" hide />
          <YAxis />
          <Tooltip />
          <Line dataKey="pnl" dot={false} stroke="#d6a72d" strokeWidth={3} type="monotone" />
        </LineChart>
      </ChartPanel>
    </div>
  );
}
