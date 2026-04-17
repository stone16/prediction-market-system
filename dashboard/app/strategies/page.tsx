'use client';

import { Nav } from '@/components/Nav';
import { useLiveData } from '@/lib/useLiveData';
import type { StrategiesResponse } from '@/lib/types';

const createdAtFormatter = new Intl.DateTimeFormat(undefined, {
  dateStyle: 'medium',
  timeStyle: 'short'
});

function formatCreatedAt(createdAt: string): string {
  const parsed = new Date(createdAt);
  if (Number.isNaN(parsed.getTime())) {
    return createdAt;
  }
  return createdAtFormatter.format(parsed);
}

export default function StrategiesPage() {
  const { data, loading, disconnected } = useLiveData<StrategiesResponse>('/strategies', 15_000);
  const strategies = data?.strategies ?? [];

  return (
    <main className="shell">
      <Nav />
      <section className="page">
        <div className="hero">
          <div>
            <p className="eyebrow">Controller</p>
            <h1>Strategies</h1>
            <p className="lede">
              Active strategy versions registered in PostgreSQL and available to the runner.
            </p>
          </div>
          {disconnected ? <span className="badge disconnected">disconnected</span> : null}
        </div>

        {loading && strategies.length === 0 ? (
          <p className="muted">Loading strategies…</p>
        ) : disconnected && strategies.length === 0 ? (
          <p className="muted">Strategy registry unavailable.</p>
        ) : strategies.length === 0 ? (
          <p className="muted">No strategies registered.</p>
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Strategy</th>
                  <th>Active version</th>
                  <th>Created</th>
                </tr>
              </thead>
              <tbody>
                {strategies.map((strategy) => (
                  <tr key={strategy.strategy_id}>
                    <td>{strategy.strategy_id}</td>
                    <td>{strategy.active_version_id ?? '—'}</td>
                    <td>{formatCreatedAt(strategy.created_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </main>
  );
}
