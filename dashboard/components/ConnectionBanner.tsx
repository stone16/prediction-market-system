'use client';

import { useConnection } from '@/lib/ConnectionContext';
import { StatusPill } from './StatusPill';

export function ConnectionBanner() {
  const { lastFetchAt, markReconnecting, retryToken, state } = useConnection();

  if (state === 'connected') {
    return null;
  }

  const title =
    state === 'disconnected' ? 'Backend disconnected' : 'Reconnecting to backend';
  const meta = lastFetchAt
    ? `Last fetch ${new Date(lastFetchAt).toLocaleTimeString()}`
    : 'Waiting for the first successful fetch';

  return (
    <div
      aria-live="polite"
      className={`connection-banner connection-banner--${state}`}
      role="alert"
    >
      <div className="connection-banner__copy">
        <div className="connection-banner__title">{title}</div>
        <div className="connection-banner__meta">{meta}</div>
      </div>
      <div className="connection-banner__actions">
        <StatusPill
          label={state === 'disconnected' ? 'offline' : 'retrying'}
          variant={state === 'disconnected' ? 'error' : 'muted'}
        />
        <button
          className="connection-banner__retry"
          onClick={() => {
            retryToken();
            markReconnecting();
          }}
          type="button"
        >
          Retry
        </button>
      </div>
    </div>
  );
}
