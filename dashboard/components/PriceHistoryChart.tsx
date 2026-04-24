'use client';

import { CartesianGrid, Line, LineChart, Tooltip, XAxis, YAxis } from 'recharts';
import type { PriceHistorySnapshot } from '@/lib/types';

type PriceHistoryChartProps = {
  snapshots: PriceHistorySnapshot[];
};

type ChartPoint = {
  snapshotAt: string;
  yesPrice: number;
};

function formatTick(value: string): string {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return `${parsed.getUTCHours().toString().padStart(2, '0')}:${parsed
    .getUTCMinutes()
    .toString()
    .padStart(2, '0')}`;
}

function formatPercent(value: number): string {
  return `${value.toFixed(1)}%`;
}

function toChartPoints(snapshots: PriceHistorySnapshot[]): ChartPoint[] {
  return snapshots
    .filter((snapshot): snapshot is PriceHistorySnapshot & { yes_price: number } => (
      snapshot.yes_price !== null && Number.isFinite(snapshot.yes_price)
    ))
    .map((snapshot) => ({
      snapshotAt: snapshot.snapshot_at,
      yesPrice: snapshot.yes_price * 100
    }));
}

export function PriceHistoryChart({ snapshots }: PriceHistoryChartProps) {
  const points = toChartPoints(snapshots);

  if (points.length === 0) {
    return <div className="empty-chart price-history-empty">Price history not available yet</div>;
  }

  if (points.length === 1) {
    return (
      <div className="price-history-single" data-testid="price-history-single-point">
        <span aria-hidden="true" className="price-history-dot" />
        <div>
          <strong>{formatPercent(points[0].yesPrice)}</strong>
          <span>{formatTick(points[0].snapshotAt)} UTC</span>
        </div>
      </div>
    );
  }

  return (
    <div
      className="price-history-chart"
      data-segments={points.length}
      data-testid="price-history-line-chart"
    >
      <LineChart data={points} height={180} margin={{ top: 14, right: 16, left: -12, bottom: 0 }} width={440}>
        <CartesianGrid stroke="#d9e2d4" />
        <XAxis dataKey="snapshotAt" minTickGap={28} tickFormatter={formatTick} />
        <YAxis domain={[0, 100]} tickFormatter={(value) => `${value}%`} />
        <Tooltip
          formatter={(value) =>
            typeof value === 'number' ? [formatPercent(value), 'YES'] : [String(value ?? ''), 'YES']
          }
          labelFormatter={(label) => (typeof label === 'string' ? label : String(label ?? ''))}
        />
        <Line
          dataKey="yesPrice"
          dot={{ r: 3, strokeWidth: 0 }}
          isAnimationActive={false}
          stroke="#1f8a70"
          strokeWidth={3}
          type="monotone"
        />
      </LineChart>
    </div>
  );
}
