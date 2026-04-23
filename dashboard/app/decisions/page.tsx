'use client';

import { Nav } from '@/components/Nav';
import { useLiveData } from '@/lib/useLiveData';
import type { Decision } from '@/lib/types';

export default function DecisionsPage() {
  const { data, loading, disconnected } = useLiveData<Decision[]>('/decisions?limit=100');
  const decisions = data ?? [];

  return (
    <main className="shell">
      <Nav />
      <section className="page">
        <div className="hero">
          <div>
            <p className="eyebrow">Ideas</p>
            <h1>Decision Ledger</h1>
            <p className="lede">
              Recent trade decisions with forecaster attribution and Kelly sizing.
            </p>
          </div>
          {disconnected ? <span className="badge disconnected">disconnected</span> : null}
        </div>
        {loading && decisions.length === 0 ? (
          <p className="muted">Loading decisions…</p>
        ) : decisions.length === 0 ? (
          <p className="muted">
            No decisions yet. Start the runner from the Overview page to populate this ledger.
          </p>
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Market</th>
                  <th>Forecaster</th>
                  <th>Side</th>
                  <th>Prob</th>
                  <th>Edge</th>
                  <th>Kelly size</th>
                </tr>
              </thead>
              <tbody>
                {decisions.map((decision) => (
                  <tr key={decision.decision_id}>
                    <td>{decision.market_id}</td>
                    <td>{decision.forecaster}</td>
                    <td>{decision.side ?? '—'}</td>
                    <td>{decision.prob_estimate.toFixed(3)}</td>
                    <td>{decision.expected_edge.toFixed(3)}</td>
                    <td>{decision.kelly_size.toFixed(2)}</td>
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
