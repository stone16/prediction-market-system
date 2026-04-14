'use client';

import { useState } from 'react';
import { apiPost } from '@/lib/api';

type RunControlsProps = {
  running: boolean;
  mode: string | null;
  onChange: () => void;
};

export function RunControls({ running, mode, onChange }: RunControlsProps) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function trigger(action: 'start' | 'stop') {
    setBusy(true);
    setError(null);
    try {
      await apiPost(`/run/${action}`);
      onChange();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'request failed');
    } finally {
      setBusy(false);
    }
  }

  return (
    <section className="card run-controls" aria-label="runner controls">
      <div>
        <h2>Runner Controls</h2>
        <p className="muted">
          Mode: <strong>{mode ?? 'unknown'}</strong>
          {' — '}
          {running ? 'running' : 'idle'}
        </p>
      </div>
      <div className="button-row">
        <button
          type="button"
          onClick={() => void trigger('start')}
          disabled={busy || running}
        >
          Start
        </button>
        <button
          type="button"
          onClick={() => void trigger('stop')}
          disabled={busy || !running}
        >
          Stop
        </button>
      </div>
      {error ? <p className="error">{error}</p> : null}
    </section>
  );
}
