'use client';

import Link from 'next/link';
import type { Feedback, MetricsResponse, StatusResponse } from '@/lib/types';

type TodayProps = {
  feedback: Feedback[];
  metrics: MetricsResponse | null;
  status: StatusResponse | null;
};

const startedAtFormatter = new Intl.DateTimeFormat(undefined, {
  dateStyle: 'medium',
  timeStyle: 'short',
});

export function Today({ feedback, metrics, status }: TodayProps) {
  const openIdeas = status?.controller.decisions_total ?? 0;
  const placedTrades = status?.actuator.fills_total ?? 0;
  const resolvedChecks = status?.evaluator.eval_records_total ?? 0;
  const firstTradeTime = metrics?.['pms.ui.first_trade_time_seconds'] ?? null;

  return (
    <>
      <section className="hero today-hero" data-testid="dashboard-hero">
        <div>
          <p className="eyebrow">Daily brief</p>
          <h1>Today</h1>
          <p className="lede">
            Start with what changed, where the edge is, and what still needs review before the
            next trade.
          </p>
          <div className="today-strip" aria-label="today summary">
            <div className="today-pill">
              <span>Source</span>
              <strong>{status?.source ?? 'loading'}</strong>
            </div>
            <div className="today-pill">
              <span>Mode</span>
              <strong>{status?.mode ?? 'loading'}</strong>
            </div>
            <div className="today-pill">
              <span>Started</span>
              <strong>{formatStartedAt(status?.runner_started_at ?? null)}</strong>
            </div>
            <div className="today-pill">
              <span>Calibration</span>
              <strong>{formatBrier(metrics?.brier_overall ?? null)}</strong>
            </div>
          </div>
        </div>

        <aside className="card today-frame" aria-label="session pulse">
          <div className="today-frame-topline">
            <p className="eyebrow">Session pulse</p>
            <span className={`badge ${status?.running ? 'ok' : 'muted-badge'}`}>
              {status?.running ? 'runner live' : 'runner idle'}
            </span>
          </div>
          <div className="today-frame-stats">
            <div>
              <span>Open ideas</span>
              <strong>{openIdeas}</strong>
            </div>
            <div>
              <span>Placed trades</span>
              <strong>{placedTrades}</strong>
            </div>
            <div>
              <span>Resolved checks</span>
              <strong>{resolvedChecks}</strong>
            </div>
            <div>
              <span>First trade time</span>
              <strong>{formatFirstTradeTime(firstTradeTime)}</strong>
            </div>
          </div>
          <p className="muted">
            Pending review items stay visible until a human clears them. Use the feed below to move
            from scan to action without hunting through the nav.
          </p>
        </aside>
      </section>

      <section className="today-feed" data-testid="today-feed" aria-label="Today feed">
        <article className="card today-card">
          <p className="today-card-kicker">Markets</p>
          <h2>Scan the freshest markets</h2>
          <p className="muted">
            Watch the live list, confirm what is subscribed, and jump into depth when something
            looks mispriced.
          </p>
          <div className="today-card-meta">
            <span>{status?.sensors[0]?.name ?? 'Live feed'}</span>
            <span>{status?.sensors[0]?.status ?? 'waiting'}</span>
          </div>
          <Link className="today-card-link" href="/markets">
            Browse markets
          </Link>
        </article>

        <article className="card today-card">
          <p className="today-card-kicker">Ideas</p>
          <h2>Review the next opportunities</h2>
          <p className="muted">
            Read the rationale, compare the edge, and pick the trade that deserves the next click.
          </p>
          <div className="today-card-meta">
            <span>{openIdeas} waiting</span>
            <span>{feedback.length} needs review</span>
          </div>
          <Link className="today-card-link" href="/ideas">
            Review ideas
          </Link>
        </article>

        <article className="card today-card">
          <p className="today-card-kicker">Trades</p>
          <h2>Check what landed first</h2>
          <p className="muted">
            Confirm fills, move into positions, and see how quickly the loop reached execution.
          </p>
          <div className="today-card-meta">
            <span>{placedTrades} fills recorded</span>
            <span>{formatPercent(metrics?.fill_rate)}</span>
          </div>
          <Link className="today-card-link" href="/trades">
            See trades
          </Link>
        </article>
      </section>
    </>
  );
}

function formatStartedAt(value: string | null): string {
  if (value === null) {
    return 'not started';
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return startedAtFormatter.format(parsed);
}

function formatBrier(value: number | null): string {
  if (value === null) {
    return 'n/a';
  }
  return value.toFixed(3);
}

function formatFirstTradeTime(value: number | null): string {
  if (value === null) {
    return 'n/a';
  }
  return `${value.toFixed(0)}s`;
}

function formatPercent(value: number | undefined): string {
  if (value === undefined) {
    return 'n/a';
  }
  return `${(value * 100).toFixed(0)}% fill rate`;
}
