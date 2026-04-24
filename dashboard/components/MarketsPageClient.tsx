'use client';

import { useEffect } from 'react';
import { useRef, useState } from 'react';
import { usePathname, useRouter, useSearchParams } from 'next/navigation';
import { MarketDetailDrawer } from '@/components/MarketDetailDrawer';
import { MarketsTable } from '@/components/MarketsTable';
import { Nav } from '@/components/Nav';
import { ToastStack, type ToastMessage } from '@/components/Toast';
import { useLiveData } from '@/lib/useLiveData';
import type { MarketRow, MarketsListResponse, StatusResponse } from '@/lib/types';

type SubscriptionOverride = Pick<MarketRow, 'subscribed' | 'subscription_source'>;

export function MarketsPageClient() {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const marketsState = useLiveData<MarketsListResponse>('/markets?limit=20');
  const statusState = useLiveData<StatusResponse>('/status');
  const [rows, setRows] = useState<MarketRow[]>([]);
  const [subscriptionOverrides, setSubscriptionOverrides] = useState<
    Record<string, SubscriptionOverride>
  >({});
  const [toasts, setToasts] = useState<ToastMessage[]>([]);
  const toastCounterRef = useRef(0);
  const subscribedCount = rows.filter((row) => row.subscribed).length;
  const runnerLabel: 'running' | 'paused' = statusState.data?.running ? 'running' : 'paused';
  const detailMarketId = searchParams.get('detail');
  const detailMarket = rows.find((row) => row.market_id === detailMarketId) ?? null;

  useEffect(() => {
    if (marketsState.data === null) {
      return;
    }
    setRows(
      marketsState.data.markets.map((row) => ({
        ...row,
        ...subscriptionOverrides[row.market_id]
      }))
    );
  }, [marketsState.data, subscriptionOverrides]);

  function replaceDetail(nextMarketId: string | null) {
    const params = new URLSearchParams(searchParams.toString());
    if (nextMarketId === null) {
      params.delete('detail');
    } else {
      params.set('detail', nextMarketId);
    }
    const query = params.toString();
    router.replace(`${pathname}${query ? `?${query}` : ''}`, { scroll: false });
  }

  function updateMarket(updatedMarket: MarketRow) {
    setRows((current) =>
      current.map((row) => (row.market_id === updatedMarket.market_id ? updatedMarket : row))
    );
    setSubscriptionOverrides((current) => ({
      ...current,
      [updatedMarket.market_id]: {
        subscribed: updatedMarket.subscribed,
        subscription_source: updatedMarket.subscription_source
      }
    }));
  }

  function pushToast(toast: Omit<ToastMessage, 'id'>) {
    const id = `toast-${toastCounterRef.current++}`;
    setToasts((current) => [...current.slice(-2), { id, ...toast }]);
  }

  useEffect(() => {
    return () => {
      const params = new URLSearchParams(window.location.search);
      if (!params.has('detail')) {
        return;
      }
      params.delete('detail');
      const query = params.toString();
      window.history.replaceState(null, '', `${window.location.pathname}${query ? `?${query}` : ''}`);
    };
  }, []);

  return (
    <main className="shell">
      <Nav />
      <section className="page">
        <header className="hero">
          <div>
            <h1>Markets</h1>
            <p className="lede">
              Browse the live candidate set, see which contracts are already in the active gaze,
              and jump straight into the persisted depth view.
            </p>
          </div>
          <div className="hero-actions">
            <span className="badge info">{rows.length} visible</span>
            <span className="badge muted-badge">{subscribedCount} subscribed</span>
            <span className={runnerLabel === 'running' ? 'badge ok' : 'badge muted-badge'}>
              runner {runnerLabel}
            </span>
          </div>
        </header>

        {marketsState.loading && rows.length === 0 ? (
          <div className="card signal-callout">
            <p className="muted">Loading markets…</p>
          </div>
        ) : marketsState.disconnected ? (
          <div className="card signal-callout">
            <p className="muted">
              Markets are unavailable right now. The backend connection dropped before the candidate
              set could load.
            </p>
          </div>
        ) : (
          <MarketsTable
            onSelectMarket={(marketId) => replaceDetail(marketId)}
            rows={rows}
            runnerLabel={runnerLabel}
          />
        )}
        <MarketDetailDrawer
          market={detailMarket}
          onClose={() => replaceDetail(null)}
          onMarketChange={updateMarket}
          onToast={pushToast}
        />
      </section>
      <ToastStack
        items={toasts}
        onDismiss={(id) => setToasts((current) => current.filter((toast) => toast.id !== id))}
      />
    </main>
  );
}
