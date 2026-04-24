import fs from 'node:fs';
import path from 'node:path';
import { render, screen } from '@testing-library/react';
import { describe, expect, test } from 'vitest';
import { PriceHistoryChart } from '@/components/PriceHistoryChart';
import type { PriceHistorySnapshot } from '@/lib/types';

const snapshots: PriceHistorySnapshot[] = [
  {
    snapshot_at: '2026-04-24T11:57:00+00:00',
    yes_price: 0.51,
    no_price: 0.49,
    best_bid: 0.5,
    best_ask: 0.52,
    last_trade_price: 0.51,
    liquidity: 2400,
    volume_24h: 1000
  },
  {
    snapshot_at: '2026-04-24T11:58:00+00:00',
    yes_price: 0.53,
    no_price: 0.47,
    best_bid: 0.52,
    best_ask: 0.54,
    last_trade_price: 0.53,
    liquidity: 2450,
    volume_24h: 1025
  },
  {
    snapshot_at: '2026-04-24T11:59:00+00:00',
    yes_price: 0.55,
    no_price: 0.45,
    best_bid: 0.54,
    best_ask: 0.56,
    last_trade_price: 0.55,
    liquidity: 2500,
    volume_24h: 1050
  }
];

describe('PriceHistoryChart', () => {
  test('renders a Recharts LineChart with a 3-point fixture', () => {
    render(<PriceHistoryChart snapshots={snapshots} />);

    expect(screen.getByTestId('price-history-line-chart')).toHaveAttribute(
      'data-segments',
      '3'
    );
  });

  test('renders an empty state when no snapshots are available', () => {
    render(<PriceHistoryChart snapshots={[]} />);

    expect(screen.getByText('Price history not available yet')).toBeInTheDocument();
  });

  test('renders a dot instead of a line for a single point', () => {
    render(<PriceHistoryChart snapshots={[snapshots[0]]} />);

    expect(screen.getByTestId('price-history-single-point')).toBeInTheDocument();
    expect(screen.queryByTestId('price-history-line-chart')).not.toBeInTheDocument();
  });

  test('imports Recharts from the existing charting stack', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'components', 'PriceHistoryChart.tsx'),
      'utf8'
    );

    expect(source).toContain("from 'recharts'");
  });
});
