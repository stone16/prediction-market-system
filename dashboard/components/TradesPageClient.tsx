'use client';

import { Nav } from '@/components/Nav';
import { TradesTable } from '@/components/TradesTable';
import { useLiveData } from '@/lib/useLiveData';
import type { TradesResponse } from '@/lib/types';

export function TradesPageClient() {
  const tradesState = useLiveData<TradesResponse>('/trades?limit=20');
  const rows = tradesState.data?.trades ?? [];

  return (
    <main className="shell">
      <Nav />
      <section className="page">
        <header className="hero">
          <div>
            <h1>Trades</h1>
            <p className="lede">
              Chronological fill ledger with market questions joined onto each persisted trade.
            </p>
          </div>
          <div className="hero-actions">
            <span className="badge info">{rows.length} recent</span>
          </div>
        </header>

        {tradesState.loading && rows.length === 0 ? (
          <div className="card signal-callout">
            <p className="muted">Loading trades…</p>
          </div>
        ) : tradesState.disconnected && rows.length === 0 ? (
          <div className="card signal-callout">
            <p className="muted">Trades are unavailable until the authenticated backend responds.</p>
          </div>
        ) : (
          <TradesTable rows={rows} />
        )}
      </section>
    </main>
  );
}
