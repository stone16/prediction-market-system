'use client';

import { MarketsTable } from '@/components/MarketsTable';
import { Nav } from '@/components/Nav';
import { useLiveData } from '@/lib/useLiveData';
import type { MarketsListResponse, StatusResponse } from '@/lib/types';

export function MarketsPageClient() {
  const marketsState = useLiveData<MarketsListResponse>('/markets?limit=20');
  const statusState = useLiveData<StatusResponse>('/status');
  const rows = marketsState.data?.markets ?? [];
  const subscribedCount = rows.filter((row) => row.subscribed).length;
  const runnerLabel: 'running' | 'paused' = statusState.data?.running ? 'running' : 'paused';

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
          <MarketsTable rows={rows} runnerLabel={runnerLabel} />
        )}
      </section>
    </main>
  );
}
