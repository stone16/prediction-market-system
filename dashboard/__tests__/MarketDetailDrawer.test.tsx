import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { useState } from 'react';
import { afterEach, describe, expect, test, vi } from 'vitest';
import { MarketDetailDrawer } from '@/components/MarketDetailDrawer';
import type { MarketRow } from '@/lib/types';

const market: MarketRow = {
  market_id: 'market-001',
  question: 'Will CP10 drawer open?',
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

function renderDrawerHarness(
  initialUrl: string,
  {
    initialMarket = market,
    onToast = vi.fn()
  }: {
    initialMarket?: MarketRow;
    onToast?: ReturnType<typeof vi.fn>;
  } = {}
) {
  window.history.replaceState({}, '', initialUrl);

  function Harness() {
    const [currentMarket, setCurrentMarket] = useState(initialMarket);
    const detailId = new URLSearchParams(window.location.search).get('detail');
    const selectedMarket = detailId === currentMarket.market_id ? currentMarket : null;
    function closeDrawer() {
      const params = new URLSearchParams(window.location.search);
      params.delete('detail');
      const query = params.toString();
      window.history.replaceState({}, '', `/markets${query ? `?${query}` : ''}`);
    }

    return (
      <MarketDetailDrawer
        market={selectedMarket}
        onClose={closeDrawer}
        onMarketChange={setCurrentMarket}
        onToast={onToast}
      />
    );
  }

  return render(<Harness />);
}

describe('MarketDetailDrawer', () => {
  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  test('renders market data when the URL has detail and stays closed without it', () => {
    const openRender = renderDrawerHarness('/markets?detail=market-001');

    expect(screen.getByRole('dialog', { name: 'Market details' })).toBeInTheDocument();
    expect(screen.getByText('Will CP10 drawer open?')).toBeInTheDocument();
    expect(screen.getByText('52.5%')).toBeInTheDocument();
    expect(screen.getByText('market-001-yes')).toBeInTheDocument();

    openRender.unmount();
    renderDrawerHarness('/markets');

    expect(screen.queryByRole('dialog', { name: 'Market details' })).not.toBeInTheDocument();
  });

  test('Esc closes the drawer and strips detail from the URL', async () => {
    renderDrawerHarness('/markets?detail=market-001');

    fireEvent.keyDown(document, { key: 'Escape' });

    await waitFor(() => {
      expect(window.location.search).toBe('');
    });
  });

  test('clicking the backdrop closes but clicking inner content does not', async () => {
    renderDrawerHarness('/markets?detail=market-001');

    fireEvent.click(screen.getByRole('dialog', { name: 'Market details' }));
    expect(window.location.search).toBe('?detail=market-001');

    fireEvent.click(screen.getByTestId('market-detail-backdrop'));

    await waitFor(() => {
      expect(window.location.search).toBe('');
    });
  });

  test('copy button writes the token id and shows Copied confirmation', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, 'clipboard', {
      configurable: true,
      value: { writeText }
    });
    renderDrawerHarness('/markets?detail=market-001');

    fireEvent.click(screen.getByRole('button', { name: 'Copy YES token ID' }));

    await waitFor(() => {
      expect(writeText).toHaveBeenCalledWith('market-001-yes');
      expect(screen.getByText('Copied')).toBeInTheDocument();
    });
  });

  test('uses aria-modal and traps focus inside the drawer', () => {
    renderDrawerHarness('/markets?detail=market-001');

    const dialog = screen.getByRole('dialog', { name: 'Market details' });
    expect(dialog).toHaveAttribute('aria-modal', 'true');

    const closeButton = screen.getByRole('button', { name: 'Close market details' });
    const signalsLink = screen.getByRole('link', { name: 'Open in Signals' });

    closeButton.focus();
    fireEvent.keyDown(document, { key: 'Tab', shiftKey: true });
    expect(document.activeElement).toBe(signalsLink);

    fireEvent.keyDown(document, { key: 'Tab' });
    expect(document.activeElement).toBe(closeButton);
  });

  test('subscribe click issues POST and updates the star', async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.includes('/price-history')) {
        return Response.json({ condition_id: 'market-001', snapshots: [] });
      }
      if (url.includes('/subscribe') && init?.method === 'POST') {
        return Response.json({
          token_id: 'market-001-yes',
          source: 'user',
          created_at: '2026-04-24T12:00:00+00:00'
        });
      }
      return Response.json({});
    });
    vi.stubGlobal('fetch', fetchMock);
    renderDrawerHarness('/markets?detail=market-001', {
      initialMarket: { ...market, subscribed: false, subscription_source: null }
    });

    fireEvent.click(screen.getByRole('button', { name: 'Subscribe YES' }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        '/api/pms/markets/market-001-yes/subscribe',
        expect.objectContaining({ method: 'POST' })
      );
      expect(screen.getByLabelText('User subscription')).toBeInTheDocument();
    });
  });

  test('unsubscribe keeps runtime strategy subscription visible while clearing the user source', async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.includes('/price-history')) {
        return Response.json({ condition_id: 'market-001', snapshots: [] });
      }
      if (url.includes('/subscribe') && init?.method === 'DELETE') {
        return Response.json({ token_id: 'market-001-yes', deleted: true });
      }
      return Response.json({});
    });
    vi.stubGlobal('fetch', fetchMock);
    renderDrawerHarness('/markets?detail=market-001', {
      initialMarket: { ...market, subscribed: true, subscription_source: 'user' }
    });

    fireEvent.click(screen.getByRole('button', { name: 'Unsubscribe YES' }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        '/api/pms/markets/market-001-yes/subscribe',
        expect.objectContaining({ method: 'DELETE' })
      );
      expect(screen.getByLabelText('Strategy subscription')).toBeInTheDocument();
    });
  });

  test('subscribe failure rolls back and emits a toast', async () => {
    const onToast = vi.fn();
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.includes('/price-history')) {
        return Response.json({ condition_id: 'market-001', snapshots: [] });
      }
      if (url.includes('/subscribe') && init?.method === 'POST') {
        return Response.json({ detail: 'denied' }, { status: 500 });
      }
      return Response.json({});
    });
    vi.stubGlobal('fetch', fetchMock);
    renderDrawerHarness('/markets?detail=market-001', {
      initialMarket: { ...market, subscribed: false, subscription_source: null },
      onToast
    });

    fireEvent.click(screen.getByRole('button', { name: 'Subscribe YES' }));

    await waitFor(() => {
      expect(screen.getByLabelText('Not subscribed')).toBeInTheDocument();
      expect(onToast).toHaveBeenCalledWith({
        tone: 'error',
        message: 'Subscription failed. Reverted to the previous state.'
      });
    });
  });

  test('documents that v1 user subscriptions target the YES token', () => {
    renderDrawerHarness('/markets?detail=market-001', {
      initialMarket: { ...market, yes_token_id: null, subscription_source: null }
    });

    expect(screen.getByText('User subscriptions track the YES token in v1.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Subscribe YES' })).toBeDisabled();
  });
});
