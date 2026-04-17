'use client';

import Link from 'next/link';
import { DepthLadder } from '@/components/DepthLadder';
import { Nav } from '@/components/Nav';
import { useLiveData } from '@/lib/useLiveData';
import type { Signal, SignalDepth } from '@/lib/types';

type SignalsPageClientProps = {
  marketId: string | null;
};

export function SignalsPageClient({ marketId }: SignalsPageClientProps) {
  const signalState = useLiveData<Signal[]>('/signals?limit=100');
  const depthState = useLiveData<SignalDepth>(
    marketId ? `/signals/${marketId}/depth?limit=20` : null
  );
  const signals = signalState.data ?? [];
  const reversed = [...signals].reverse();
  const selectedSignal = marketId
    ? reversed.find((signal) => signal.market_id === marketId) ?? null
    : null;

  return (
    <main className="shell">
      <Nav />
      <section className="page">
        <div className="hero">
          <div>
            <p className="eyebrow">Sensor</p>
            <h1>Signal Stream</h1>
            <p className="lede">
              Inspect the persisted YES-side ladder for a market while keeping the latest signal
              feed in view.
            </p>
          </div>
          {signalState.disconnected ? <span className="badge disconnected">disconnected</span> : null}
        </div>

        {marketId ? (
          <DepthLadder
            marketId={marketId}
            marketTitle={selectedSignal?.title ?? null}
            depth={depthState.data}
            loading={depthState.loading}
            disconnected={depthState.disconnected}
            error={depthState.error}
          />
        ) : (
          <div className="card signal-callout">
            <p className="muted">
              Add <code>?market_id=&lt;condition_id&gt;</code> to inspect a real orderbook ladder,
              or pick a market from the recent signal list below.
            </p>
          </div>
        )}

        {signalState.loading && signals.length === 0 ? (
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
                    <td>
                      <Link href={`/signals?market_id=${encodeURIComponent(signal.market_id)}`}>
                        {signal.market_id}
                      </Link>
                    </td>
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
