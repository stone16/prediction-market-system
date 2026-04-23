import Link from 'next/link';
import { EmptyState } from '@/components/EmptyState';
import type { TradeRow } from '@/lib/types';

type TradesTableProps = {
  rows: TradeRow[];
};

function formatNumber(value: number, digits = 2) {
  return new Intl.NumberFormat('en-US', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits
  }).format(value);
}

function formatTimestamp(value: string) {
  return new Intl.DateTimeFormat('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit'
  }).format(new Date(value));
}

export function TradesTable({ rows }: TradesTableProps) {
  if (rows.length === 0) {
    return (
      <EmptyState
        title="No trades yet."
        body="The fill ledger stays empty until the controller emits an idea and the actuator matches it."
        cta={{ href: '/ideas', label: 'Browse ideas' }}
      />
    );
  }

  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Filled</th>
            <th>Market</th>
            <th>Side</th>
            <th>Price</th>
            <th>Notional</th>
            <th>Quantity</th>
            <th>Venue</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.fill_id}>
              <td>{formatTimestamp(row.filled_at)}</td>
              <td>
                <div className="table-primary-cell">
                  <strong>{row.question}</strong>
                  <Link className="run-link" href={`/signals?market_id=${encodeURIComponent(row.market_id)}`}>
                    {row.market_id}
                  </Link>
                </div>
              </td>
              <td>{row.side}</td>
              <td>{formatNumber(row.fill_price, 3)}</td>
              <td>{formatNumber(row.fill_notional_usdc)}</td>
              <td>{formatNumber(row.fill_quantity, 1)}</td>
              <td>{row.venue}</td>
              <td>{row.status}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
