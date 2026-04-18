'use client';

import {
  CartesianGrid,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from 'recharts';
import type { FactorSeriesResponse } from '@/lib/types';

type ChartProps = {
  series: FactorSeriesResponse;
};

function formatTick(ts: string): string {
  const parsed = new Date(ts);
  if (Number.isNaN(parsed.getTime())) {
    return ts;
  }
  return `${parsed.getUTCHours().toString().padStart(2, '0')}:${parsed
    .getUTCMinutes()
    .toString()
    .padStart(2, '0')}`;
}

function formatTooltipLabel(ts: string): string {
  const parsed = new Date(ts);
  if (Number.isNaN(parsed.getTime())) {
    return ts;
  }
  return parsed.toISOString();
}

export function FactorSeriesChart({ series }: ChartProps) {
  return (
    <section className="chart-panel" data-testid="factor-chart">
      <h2>Factor evolution</h2>
      <div className="chart-frame">
        <ResponsiveContainer height={250} width="100%">
          <LineChart data={series.points} margin={{ top: 16, right: 24, left: 4, bottom: 0 }}>
            <CartesianGrid stroke="#d9e2d4" />
            <XAxis dataKey="ts" minTickGap={36} tickFormatter={formatTick} />
            <YAxis />
            <Tooltip
              formatter={(value) =>
                typeof value === 'number' ? value.toFixed(4) : String(value ?? '')
              }
              labelFormatter={(label) =>
                typeof label === 'string' ? formatTooltipLabel(label) : String(label ?? '')
              }
            />
            <ReferenceLine stroke="#5d665b" strokeDasharray="4 4" y={0} />
            <Line
              dataKey="value"
              dot={{ r: 3, strokeWidth: 0 }}
              isAnimationActive={false}
              stroke="#2d8aa6"
              strokeWidth={3}
              type="monotone"
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </section>
  );
}
