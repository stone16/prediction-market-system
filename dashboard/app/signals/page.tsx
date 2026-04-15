'use client';

import { Nav } from '@/components/Nav';
import { useLiveData } from '@/lib/useLiveData';
import type { Signal } from '@/lib/types';

export default function SignalsPage() {
  const { data, loading, disconnected } = useLiveData<Signal[]>('/signals?limit=100');
  const signals = data ?? [];
  const reversed = [...signals].reverse();

  return (
    <main className="shell">
      <Nav />
      <section className="page">
        <div className="hero">
          <div>
            <p className="eyebrow">Sensor</p>
            <h1>Signal Stream</h1>
            <p className="lede">
              Most recent market signals delivered to the controller, newest first.
            </p>
          </div>
          {disconnected ? <span className="badge disconnected">disconnected</span> : null}
        </div>
        {loading && signals.length === 0 ? (
          <p className="muted">Loading signals…</p>
        ) : signals.length === 0 ? (
          <p className="muted">
            No signals yet. Start the runner to begin ingesting Polymarket / historical data.
          </p>
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Fetched at</th>
                  <th>Market</th>
                  <th>Title</th>
                  <th>Yes price</th>
                </tr>
              </thead>
              <tbody>
                {reversed.map((signal) => (
                  <tr key={`${signal.market_id}-${signal.fetched_at}`}>
                    <td>{signal.fetched_at}</td>
                    <td>{signal.market_id}</td>
                    <td>{signal.title}</td>
                    <td>{signal.yes_price.toFixed(3)}</td>
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
