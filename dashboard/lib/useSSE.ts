'use client';

import { useEffect, useRef, useState } from 'react';
import type { EventLogEntry } from './types';

const EVENT_TYPES = ['sensor.signal', 'controller.decision', 'actuator.fill', 'error'] as const;
const FLUSH_INTERVAL_MS = 100;
const MAX_ITEMS = 20;
const LAST_EVENT_ID_KEY = 'pms.eventlog.last_event_id';

export type EventStreamState = 'connecting' | 'live' | 'error';

export function useSSE(path = '/api/pms/stream/events') {
  const [items, setItems] = useState<EventLogEntry[]>([]);
  const [state, setState] = useState<EventStreamState>('connecting');
  const pendingRef = useRef<EventLogEntry[]>([]);
  const flushTimerRef = useRef<number | null>(null);

  useEffect(() => {
    if (typeof EventSource === 'undefined') {
      setState('error');
      return undefined;
    }

    const lastEventId = window.localStorage.getItem(LAST_EVENT_ID_KEY);
    const streamUrl = lastEventId
      ? `${path}?last_event_id=${encodeURIComponent(lastEventId)}`
      : path;
    const source = new EventSource(streamUrl);

    function flushPending() {
      const pending = pendingRef.current.splice(0);
      if (pending.length === 0) {
        return;
      }
      setItems((current) => [...current, ...pending].slice(-MAX_ITEMS));
    }

    function scheduleFlush() {
      if (flushTimerRef.current !== null) {
        return;
      }
      flushTimerRef.current = window.setTimeout(() => {
        flushTimerRef.current = null;
        window.requestAnimationFrame(() => {
          flushPending();
        });
      }, FLUSH_INTERVAL_MS);
    }

    function handlePayload(event: MessageEvent<string>) {
      try {
        const payload = JSON.parse(event.data) as EventLogEntry;
        window.localStorage.setItem(
          LAST_EVENT_ID_KEY,
          event.lastEventId || String(payload.event_id)
        );
        pendingRef.current.push(payload);
        setState('live');
        scheduleFlush();
      } catch {
        setState('error');
      }
    }

    source.onopen = () => {
      setState('live');
    };
    source.onerror = () => {
      setState('error');
    };

    const errorListener = (event: Event) => {
      if (event instanceof MessageEvent) {
        handlePayload(event as MessageEvent<string>);
        return;
      }
      setState('error');
    };

    for (const eventType of EVENT_TYPES) {
      if (eventType === 'error') {
        source.addEventListener(eventType, errorListener);
        continue;
      }
      source.addEventListener(eventType, handlePayload as EventListener);
    }

    return () => {
      if (flushTimerRef.current !== null) {
        window.clearTimeout(flushTimerRef.current);
      }
      source.close();
      source.removeEventListener('error', errorListener);
      for (const eventType of EVENT_TYPES) {
        if (eventType === 'error') {
          continue;
        }
        source.removeEventListener(eventType, handlePayload as EventListener);
      }
    };
  }, [path]);

  return { items, state };
}
