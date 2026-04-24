import { render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import { MarketsTable } from '@/components/MarketsTable';
import type { MarketRow } from '@/lib/types';

const fullRow: MarketRow = {
  market_id: 'market-001',
  question: 'Will CP09 table land today?',
  venue: 'polymarket',
  volume_24h: 1842.5,
  updated_at: '2026-04-23T10:00:00+00:00',
  yes_token_id: 'market-001-yes',
  no_token_id: 'market-001-no',
  yes_price: 0.525,
  no_price: 0.475,
  best_bid: 0.51,
  best_ask: 0.54,
  last_trade_price: 0.52,
  liquidity: 25000.75,
  spread_bps: 300,
  price_updated_at: '2026-04-24T11:59:30+00:00',
  resolves_at: '2026-05-01T00:00:00+00:00',
  subscription_source: 'user',
  subscribed: true
};

const nullPriceRow: MarketRow = {
  ...fullRow,
  market_id: 'market-null',
  question: 'Will null prices render cleanly?',
  volume_24h: null,
  yes_price: null,
  no_price: null,
  best_bid: null,
  best_ask: null,
  last_trade_price: null,
  liquidity: null,
  spread_bps: null,
  price_updated_at: null,
  resolves_at: null,
  subscription_source: null,
  subscribed: false
};

describe('MarketsTable', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-04-24T12:00:00+00:00'));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

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

  test('renders redesigned market columns with all price fields', () => {
    render(<MarketsTable rows={[fullRow]} runnerLabel="running" />);

    expect(screen.getByRole('columnheader', { name: 'Market' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'YES' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'NO' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'Vol 24h' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'Liquidity' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'Spread' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'Resolves' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'Subscription' })).toBeInTheDocument();

    expect(screen.getByText('Will CP09 table land today?')).toBeInTheDocument();
    expect(screen.getByText('polymarket')).toBeInTheDocument();
    expect(screen.getByText('52.5%')).toBeInTheDocument();
    expect(screen.getByText('47.5%')).toBeInTheDocument();
    expect(screen.getByText('1,842.5')).toBeInTheDocument();
    expect(screen.getByText('25,000.8')).toBeInTheDocument();
    expect(screen.getByText('300 bps')).toBeInTheDocument();
    expect(screen.getByText('May 1')).toBeInTheDocument();
    expect(screen.getByLabelText('User subscription')).toBeInTheDocument();
  });

  test('renders dashes for rows with null prices instead of undefined or NaN', () => {
    const { container } = render(<MarketsTable rows={[nullPriceRow]} runnerLabel="running" />);

    expect(screen.getByText('Will null prices render cleanly?')).toBeInTheDocument();
    expect(container).not.toHaveTextContent('undefined');
    expect(container).not.toHaveTextContent('NaN%');
    expect(screen.getAllByText('—').length).toBeGreaterThanOrEqual(6);
  });
});
