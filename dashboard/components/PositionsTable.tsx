import { EmptyState } from '@/components/EmptyState';
import type { PositionRow } from '@/lib/types';

type PositionsTableProps = {
  rows: PositionRow[];
};

function formatNumber(value: number, digits = 2) {
  return new Intl.NumberFormat('en-US', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits
  }).format(value);
}

export function PositionsTable({ rows }: PositionsTableProps) {
  if (rows.length === 0) {
    return (
      <EmptyState
        title="No open positions yet."
        body="Accept an idea, then return here to inspect the current cost basis and exposure."
        cta={{ href: '/decisions', label: 'Browse ideas' }}
      />
    );
  }

  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Market</th>
            <th>Token</th>
            <th>Venue</th>
            <th>Side</th>
            <th>Shares</th>
            <th>Avg entry</th>
            <th>Locked USDC</th>
            <th>Unrealized PnL</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={`${row.market_id}-${row.token_id}-${row.side}`}>
              <td>{row.market_id}</td>
              <td>{row.token_id ?? '—'}</td>
              <td>{row.venue}</td>
              <td>{row.side}</td>
              <td>{formatNumber(row.shares_held, 1)}</td>
              <td>{formatNumber(row.avg_entry_price, 3)}</td>
              <td>{formatNumber(row.locked_usdc)}</td>
              <td>{formatNumber(row.unrealized_pnl)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
