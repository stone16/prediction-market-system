import type { SignalDepth } from '@/lib/types';

type DepthLadderProps = {
  marketId: string;
  marketTitle?: string | null;
  depth: SignalDepth | null;
  loading: boolean;
  disconnected: boolean;
  error: string | null;
};

function formatPrice(price: number | null) {
  if (price === null) return '--';
  return `${(price * 100).toFixed(1)}¢`;
}

function formatSize(size: number) {
  return new Intl.NumberFormat('en-US', {
    maximumFractionDigits: 0
  }).format(size);
}

export function DepthLadder({
  marketId,
  marketTitle,
  depth,
  loading,
  disconnected,
  error
}: DepthLadderProps) {
  return (
    <section className="depth-card card" aria-live="polite">
      <div className="depth-topline">
        <div>
          <p className="eyebrow">Orderbook</p>
          <h2>{marketTitle ?? 'Real depth ladder'}</h2>
          <p className="depth-market-id">{marketId}</p>
        </div>
        <div className="depth-badges">
          {depth?.stale ? <span className="badge warning">stale book</span> : null}
          {disconnected ? <span className="badge disconnected">disconnected</span> : null}
        </div>
      </div>

      <div className="depth-stats">
        <div className="depth-stat">
          <span>Best bid</span>
          <strong>{formatPrice(depth?.best_bid ?? null)}</strong>
        </div>
        <div className="depth-stat">
          <span>Best ask</span>
          <strong>{formatPrice(depth?.best_ask ?? null)}</strong>
        </div>
        <div className="depth-stat">
          <span>Last update</span>
          <strong>{depth?.last_update_ts ?? 'Waiting for depth'}</strong>
        </div>
      </div>

      {loading && depth === null ? (
        <p className="muted">Loading live depth…</p>
      ) : disconnected && depth === null ? (
        <p className="muted">{error ?? 'Depth feed unavailable.'}</p>
      ) : depth !== null && depth.bids.length === 0 && depth.asks.length === 0 ? (
        <p className="muted">No persisted book yet for this market.</p>
      ) : (
        <div className="depth-grid" data-testid="depth-ladder">
          <section className="depth-column bids" aria-label="Bid depth">
            <header>
              <span>Bid</span>
              <span>Size</span>
            </header>
            {depth?.bids.map((level) => (
              <div
                key={`bid-${level.price}-${level.size}`}
                className="depth-row"
                data-testid="bid-row"
              >
                <strong>{formatPrice(level.price)}</strong>
                <span>{formatSize(level.size)}</span>
              </div>
            ))}
          </section>

          <section className="depth-column asks" aria-label="Ask depth">
            <header>
              <span>Ask</span>
              <span>Size</span>
            </header>
            {depth?.asks.map((level) => (
              <div
                key={`ask-${level.price}-${level.size}`}
                className="depth-row"
                data-testid="ask-row"
              >
                <strong>{formatPrice(level.price)}</strong>
                <span>{formatSize(level.size)}</span>
              </div>
            ))}
          </section>
        </div>
      )}
    </section>
  );
}
