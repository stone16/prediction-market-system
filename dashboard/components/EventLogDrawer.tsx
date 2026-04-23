'use client';

import { useEffect, useRef, useState } from 'react';
import { useSSE } from '@/lib/useSSE';

const PIN_KEY = 'pms.eventlog.pinned';

function formatEventTime(value: string) {
  return new Intl.DateTimeFormat('en-US', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  }).format(new Date(value));
}

export function EventLogDrawer() {
  const { items, state } = useSSE();
  const [pinned, setPinned] = useState(false);
  const [open, setOpen] = useState(false);
  const [hydrated, setHydrated] = useState(false);
  const closeButtonRef = useRef<HTMLButtonElement | null>(null);
  const triggerRef = useRef<HTMLButtonElement | null>(null);

  useEffect(() => {
    const nextPinned = window.localStorage.getItem(PIN_KEY) === 'true';
    setPinned(nextPinned);
    setOpen(nextPinned);
    setHydrated(true);
  }, []);

  useEffect(() => {
    if (!hydrated) {
      return;
    }
    if (pinned) {
      window.localStorage.setItem(PIN_KEY, 'true');
      setOpen(true);
      return;
    }
    window.localStorage.removeItem(PIN_KEY);
  }, [hydrated, pinned]);

  useEffect(() => {
    if (!hydrated) {
      return;
    }
    if (open) {
      closeButtonRef.current?.focus();
      return;
    }
    triggerRef.current?.focus();
  }, [hydrated, open]);

  return (
    <aside className={`event-log-shell ${open ? 'event-log-shell--open' : ''}`}>
      <button
        aria-expanded={open ? 'true' : 'false'}
        className="event-log-tab"
        onClick={() => setOpen((current) => !current)}
        ref={triggerRef}
        type="button"
      >
        Event log
      </button>
      {open ? (
        <section aria-label="Event log" className="event-log-drawer" role="complementary">
          <header className="event-log-drawer__header">
            <div>
              <p className="eyebrow">Runtime stream</p>
              <h2>Event log</h2>
            </div>
            <div className="event-log-drawer__actions">
              <button
                aria-label={pinned ? 'Unpin event log' : 'Pin event log'}
                className="event-log-drawer__pin"
                onClick={() => setPinned((current) => !current)}
                type="button"
              >
                {pinned ? 'Unpin' : 'Pin'}
              </button>
              <button
                aria-label="Close event log"
                className="event-log-drawer__close"
                onClick={() => setOpen(false)}
                ref={closeButtonRef}
                type="button"
              >
                ×
              </button>
            </div>
          </header>

          <div className="event-log-drawer__status">
            <span className={`badge ${state === 'error' ? 'disconnected' : 'info'}`}>
              {state === 'error' ? 'unavailable' : state}
            </span>
          </div>

          {items.length > 0 ? (
            <ol className="event-log-list">
              {[...items].reverse().map((item) => (
                <li className="event-log-entry" data-testid="event-log-entry" key={item.event_id}>
                  <div className="event-log-entry__meta">
                    <span>{formatEventTime(item.created_at)}</span>
                    <strong>{item.event_type}</strong>
                  </div>
                  <p>{item.summary}</p>
                </li>
              ))}
            </ol>
          ) : state === 'error' ? (
            <p className="muted">Event log unavailable</p>
          ) : (
            <p className="muted">Waiting for live events.</p>
          )}
        </section>
      ) : null}
    </aside>
  );
}
