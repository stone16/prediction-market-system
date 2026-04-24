import { EmptyState } from '@/components/EmptyState';
import { FreshnessDot } from '@/components/FreshnessDot';
import { PriceBar } from '@/components/PriceBar';
import { SubscribeStar } from '@/components/SubscribeStar';
import type { MarketRow } from '@/lib/types';

type MarketsTableProps = {
  rows: MarketRow[];
  runnerLabel: 'running' | 'paused';
  onSelectMarket?: (marketId: string) => void;
};

function formatNumber(value: number | null) {
  if (value === null) {
    return '—';
  }
  return new Intl.NumberFormat('en-US', {
    maximumFractionDigits: 1,
    minimumFractionDigits: value % 1 === 0 ? 0 : 1
  }).format(value);
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

function formatSpread(spreadBps: number | null) {
  if (spreadBps === null) {
    return '—';
  }
  return `${spreadBps} bps`;
}

export function MarketsTable({ rows, runnerLabel, onSelectMarket }: MarketsTableProps) {
  if (rows.length === 0) {
    return (
      <EmptyState
        title="No markets yet."
        body={`Runner is ${runnerLabel}. Start the runner, then return here to browse the current candidate set.`}
        cta={{ href: '/', label: 'Start the runner' }}
      />
    );
  }

  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Market</th>
            <th>YES</th>
            <th>NO</th>
            <th>Vol 24h</th>
            <th>Liquidity</th>
            <th>Spread</th>
            <th>Resolves</th>
            <th aria-label="Subscription">★</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr
              className={onSelectMarket ? 'interactive-row' : undefined}
              key={row.market_id}
              onClick={onSelectMarket ? () => onSelectMarket(row.market_id) : undefined}
              onKeyDown={
                onSelectMarket
                  ? (event) => {
                      if (event.key === 'Enter' || event.key === ' ') {
                        event.preventDefault();
                        onSelectMarket(row.market_id);
                      }
                    }
                  : undefined
              }
              tabIndex={onSelectMarket ? 0 : undefined}
            >
              <td className="markets-table__market-cell">
                <strong>{row.question}</strong>
                <span className="markets-table__meta">
                  <FreshnessDot priceUpdatedAt={row.price_updated_at} />
                  {row.venue}
                </span>
              </td>
              <td>
                <PriceBar label="YES price" tone="yes" value={row.yes_price} />
              </td>
              <td>
                <PriceBar label="NO price" tone="no" value={row.no_price} />
              </td>
              <td>{formatNumber(row.volume_24h)}</td>
              <td>{formatNumber(row.liquidity)}</td>
              <td>{formatSpread(row.spread_bps)}</td>
              <td>{formatDate(row.resolves_at)}</td>
              <td>
                <SubscribeStar
                  subscribed={row.subscribed}
                  subscriptionSource={row.subscription_source}
                  title="Open details to subscribe"
                />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
