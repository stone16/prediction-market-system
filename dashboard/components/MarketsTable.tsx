import Link from 'next/link';
import { EmptyState } from '@/components/EmptyState';
import type { MarketRow } from '@/lib/types';

type MarketsTableProps = {
  rows: MarketRow[];
  runnerLabel: 'running' | 'paused';
};

function formatVolume(volume: number | null) {
  if (volume === null) {
    return '—';
  }
  return new Intl.NumberFormat('en-US', {
    maximumFractionDigits: 1,
    minimumFractionDigits: volume % 1 === 0 ? 0 : 1
  }).format(volume);
}

function formatUpdatedAt(updatedAt: string) {
  return new Intl.DateTimeFormat('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit'
  }).format(new Date(updatedAt));
}

export function MarketsTable({ rows, runnerLabel }: MarketsTableProps) {
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
            <th>Question</th>
            <th>Venue</th>
            <th>Volume 24h</th>
            <th>Updated</th>
            <th>Subscribed</th>
            <th>Token IDs</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr className="interactive-row" key={row.market_id}>
              <td>
                <Link className="run-link" href={`/signals?market_id=${encodeURIComponent(row.market_id)}`}>
                  {row.market_id}
                </Link>
              </td>
              <td>{row.question}</td>
              <td>{row.venue}</td>
              <td>{formatVolume(row.volume_24h)}</td>
              <td>{formatUpdatedAt(row.updated_at)}</td>
              <td>
                <span className={row.subscribed ? 'badge info' : 'badge muted-badge'}>
                  {row.subscribed ? 'subscribed' : 'idle'}
                </span>
              </td>
              <td>
                <span className="muted">
                  YES {row.yes_token_id ?? '—'} / NO {row.no_token_id ?? '—'}
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
