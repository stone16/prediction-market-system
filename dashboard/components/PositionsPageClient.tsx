'use client';

import { Nav } from '@/components/Nav';
import { PositionsTable } from '@/components/PositionsTable';
import { useLiveData } from '@/lib/useLiveData';
import type { PositionsResponse } from '@/lib/types';

export function PositionsPageClient() {
  const positionsState = useLiveData<PositionsResponse>('/positions');
  const rows = positionsState.data?.positions ?? [];

  return (
    <main className="shell">
      <Nav />
      <section className="page">
        <header className="hero">
          <div>
            <h1>Positions</h1>
            <p className="lede">
              Current exposure aggregated from persisted fills, grouped by market, token, venue, and side.
            </p>
          </div>
          <div className="hero-actions">
            <span className="badge info">{rows.length} open</span>
          </div>
        </header>

        {positionsState.loading && rows.length === 0 ? (
          <div className="card signal-callout">
            <p className="muted">Loading positions…</p>
          </div>
        ) : positionsState.disconnected && rows.length === 0 ? (
          <div className="card signal-callout">
            <p className="muted">Positions are unavailable until the authenticated backend responds.</p>
          </div>
        ) : (
          <PositionsTable rows={rows} />
        )}
      </section>
    </main>
  );
}
