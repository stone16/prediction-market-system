import { render, screen } from '@testing-library/react';
import { describe, expect, test } from 'vitest';
import { MarketsTable } from '@/components/MarketsTable';
import type { MarketRow } from '@/lib/types';

const rows: MarketRow[] = [
  {
    market_id: 'market-001',
    question: 'Will CP04 land today?',
    venue: 'polymarket',
    volume_24h: 1842.5,
    updated_at: '2026-04-23T10:00:00+00:00',
    yes_token_id: 'market-001-yes',
    no_token_id: 'market-001-no',
    subscribed: true
  }
];

describe('MarketsTable', () => {
  test('renders the empty state when rows are empty', () => {
    render(<MarketsTable rows={[]} runnerLabel="paused" />);

    expect(screen.getByText('No markets yet.')).toBeInTheDocument();
    expect(screen.getByText(/Runner is paused/i)).toBeInTheDocument();
    expect(
      screen.getByRole('link', {
        name: 'Start the runner'
      })
    ).toHaveAttribute('href', '/');
  });

  test('renders populated market rows with depth links', () => {
    render(<MarketsTable rows={rows} runnerLabel="running" />);

    expect(screen.getByText('Will CP04 land today?')).toBeInTheDocument();
    expect(screen.getByText('1,842.5')).toBeInTheDocument();
    expect(screen.getByText('subscribed')).toBeInTheDocument();
    expect(
      screen.getByRole('link', {
        name: 'market-001'
      })
    ).toHaveAttribute('href', '/signals?market_id=market-001');
  });
});
