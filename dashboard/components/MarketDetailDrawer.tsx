'use client';

import { useEffect, useRef, useState } from 'react';
import { PriceBar } from '@/components/PriceBar';
import { PriceHistoryChartNoSsr } from '@/components/PriceHistoryChartNoSsr';
import { SubscribeStar } from '@/components/SubscribeStar';
import type { ToastMessage } from '@/components/Toast';
import type { MarketRow, PriceHistoryResponse } from '@/lib/types';
import { useLiveData } from '@/lib/useLiveData';

type MarketDetailDrawerProps = {
  market: MarketRow | null;
  onClose: () => void;
  onMarketChange?: (market: MarketRow) => void;
  onToast?: (toast: Omit<ToastMessage, 'id'>) => void;
};

function focusableElements(root: HTMLElement) {
  return Array.from(
    root.querySelectorAll<HTMLElement>('a[href], button:not([disabled]), [tabindex]:not([tabindex="-1"])')
  ).filter((element) => !element.hasAttribute('disabled'));
}

function formatNumber(value: number | null) {
  if (value === null) {
    return '—';
  }
  return new Intl.NumberFormat('en-US', {
    maximumFractionDigits: 1,
    minimumFractionDigits: value % 1 === 0 ? 0 : 1
  }).format(value);
}

function formatSpread(spreadBps: number | null) {
  if (spreadBps === null) {
    return '—';
  }
  return `${spreadBps} bps`;
}

function formatDate(value: string | null | undefined) {
  if (value == null) {
    return '—';
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return '—';
  }
  return new Intl.DateTimeFormat('en-US', {
    month: 'short',
    day: 'numeric',
    timeZone: 'UTC'
  }).format(parsed);
}

export function MarketDetailDrawer({
  market,
  onClose,
  onMarketChange,
  onToast
}: MarketDetailDrawerProps) {
  const [copiedTokenId, setCopiedTokenId] = useState<string | null>(null);
  const [subscribeInflight, setSubscribeInflight] = useState(false);
  const dialogRef = useRef<HTMLElement | null>(null);
  const closeButtonRef = useRef<HTMLButtonElement | null>(null);
  const copiedTimerRef = useRef<number | null>(null);
  const priceHistoryState = useLiveData<PriceHistoryResponse>(
    market === null
      ? null
      : `/markets/${encodeURIComponent(market.market_id)}/price-history?limit=1440`,
    15000
  );

  useEffect(() => {
    if (market === null) {
      return undefined;
    }

    const previousFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    closeButtonRef.current?.focus();

    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === 'Escape') {
        event.preventDefault();
        onClose();
        return;
      }

      if (event.key !== 'Tab' || dialogRef.current === null) {
        return;
      }

      const focusable = focusableElements(dialogRef.current);
      if (focusable.length === 0) {
        return;
      }

      const first = focusable[0];
      const last = focusable[focusable.length - 1];

      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    }

    document.addEventListener('keydown', handleKeyDown);
    return () => {
      document.removeEventListener('keydown', handleKeyDown);
      previousFocus?.focus();
    };
  }, [market, onClose]);

  useEffect(() => {
    return () => {
      if (copiedTimerRef.current !== null) {
        window.clearTimeout(copiedTimerRef.current);
      }
    };
  }, []);

  if (market === null) {
    return null;
  }
  const activeMarket = market;

  async function copyToken(tokenId: string) {
    await navigator.clipboard.writeText(tokenId);
    setCopiedTokenId(tokenId);
    if (copiedTimerRef.current !== null) {
      window.clearTimeout(copiedTimerRef.current);
    }
    copiedTimerRef.current = window.setTimeout(() => {
      setCopiedTokenId(null);
      copiedTimerRef.current = null;
    }, 1400);
  }

  async function handleSubscribeToggle() {
    if (subscribeInflight || activeMarket.yes_token_id === null) {
      return;
    }

    const wasUserSubscribed = activeMarket.subscription_source === 'user';
    const previousMarket = activeMarket;
    const nextMarket: MarketRow = {
      ...activeMarket,
      subscribed: !wasUserSubscribed,
      subscription_source: wasUserSubscribed ? null : 'user'
    };

    setSubscribeInflight(true);
    onMarketChange?.(nextMarket);
    try {
      const response = await fetch(
        `/api/pms/markets/${encodeURIComponent(activeMarket.yes_token_id)}/subscribe`,
        { method: wasUserSubscribed ? 'DELETE' : 'POST' }
      );
      if (!response.ok) {
        throw new Error(`Subscription request returned ${response.status}`);
      }
    } catch {
      onMarketChange?.(previousMarket);
      onToast?.({
        tone: 'error',
        message: 'Subscription failed. Reverted to the previous state.'
      });
    } finally {
      setSubscribeInflight(false);
    }
  }

  function tokenRow(label: 'YES' | 'NO', tokenId: string | null) {
    return (
      <div className="market-detail-token-row">
        <dt>{label} token</dt>
        <dd>
          <code>{tokenId ?? '—'}</code>
          {tokenId !== null ? (
            <>
              <button
                className="ghost-button market-detail-copy"
                onClick={() => void copyToken(tokenId)}
                type="button"
              >
                Copy {label} token ID
              </button>
              {copiedTokenId === tokenId ? (
                <span className="market-detail-copied">Copied</span>
              ) : null}
            </>
          ) : null}
        </dd>
      </div>
    );
  }

  return (
    <div
      className="market-detail-backdrop"
      data-testid="market-detail-backdrop"
      onClick={(event) => {
        if (event.target === event.currentTarget) {
          onClose();
        }
      }}
    >
      <section
        aria-label="Market details"
        aria-modal="true"
        className="market-detail-drawer"
        ref={dialogRef}
        role="dialog"
      >
        <header className="market-detail-header">
          <div>
            <p className="eyebrow">Market details</p>
            <h2 id="market-detail-title">{market.question}</h2>
          </div>
          <button
            aria-label="Close market details"
            className="market-detail-close"
            onClick={onClose}
            ref={closeButtonRef}
            type="button"
          >
            ×
          </button>
        </header>

        <div className="market-detail-subscribe">
          <SubscribeStar
            subscribed={market.subscribed}
            subscriptionSource={market.subscription_source}
          />
          <button
            className="ghost-button"
            disabled={subscribeInflight || market.yes_token_id === null}
            onClick={() => void handleSubscribeToggle()}
            type="button"
          >
            {market.subscription_source === 'user' ? 'Unsubscribe market' : 'Subscribe market'}
          </button>
        </div>

        <div className="market-detail-price-grid">
          <div className="market-detail-price-card">
            <span>YES</span>
            <PriceBar label="Drawer YES price" tone="yes" value={market.yes_price} />
          </div>
          <div className="market-detail-price-card">
            <span>NO</span>
            <PriceBar label="Drawer NO price" tone="no" value={market.no_price} />
          </div>
        </div>

        <dl className="market-detail-metrics">
          <div>
            <dt>Volume 24h</dt>
            <dd>{formatNumber(market.volume_24h)}</dd>
          </div>
          <div>
            <dt>Liquidity</dt>
            <dd>{formatNumber(market.liquidity)}</dd>
          </div>
          <div>
            <dt>Spread</dt>
            <dd>{formatSpread(market.spread_bps)}</dd>
          </div>
          <div>
            <dt>Resolves</dt>
            <dd>{formatDate(market.resolves_at)}</dd>
          </div>
        </dl>

        <section className="market-detail-chart chart-panel">
          <h3>YES price history</h3>
          <div className="chart-frame">
            {priceHistoryState.loading && priceHistoryState.data === null ? (
              <p className="muted">Loading price history...</p>
            ) : (
              <PriceHistoryChartNoSsr snapshots={priceHistoryState.data?.snapshots ?? []} />
            )}
          </div>
        </section>

        <details className="market-detail-metadata" open>
          <summary>Metadata</summary>
          <dl>
            {tokenRow('YES', market.yes_token_id)}
            {tokenRow('NO', market.no_token_id)}
            <div className="market-detail-token-row">
              <dt>Signals</dt>
              <dd>
                <a
                  className="run-link"
                  href={`/signals?market_id=${encodeURIComponent(market.market_id)}`}
                >
                  Open in Signals
                </a>
              </dd>
            </div>
          </dl>
        </details>
      </section>
    </div>
  );
}
