import { act, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import { EventLogDrawer } from '@/components/EventLogDrawer';

type EventPayload = {
  event_id: number;
  event_type: string;
  created_at: string;
  summary: string;
  market_id?: string | null;
  decision_id?: string | null;
  fill_id?: string | null;
};

class MockEventSource {
  static instances: MockEventSource[] = [];

  onerror: ((event: Event) => void) | null = null;
  onopen: ((event: Event) => void) | null = null;
  readonly listeners = new Map<string, Set<(event: MessageEvent) => void>>();
  readonly url: string;

  constructor(url: string) {
    this.url = url;
    MockEventSource.instances.push(this);
  }

  addEventListener(type: string, listener: (event: MessageEvent) => void) {
    const listeners = this.listeners.get(type) ?? new Set();
    listeners.add(listener);
    this.listeners.set(type, listeners);
  }

  removeEventListener(type: string, listener: (event: MessageEvent) => void) {
    this.listeners.get(type)?.delete(listener);
  }

  close() {}

  emit(type: string, payload: EventPayload) {
    const event = new MessageEvent(type, { data: JSON.stringify(payload) });
    Object.defineProperty(event, 'lastEventId', { configurable: true, value: String(payload.event_id) });
    for (const listener of this.listeners.get(type) ?? []) {
      listener(event);
    }
  }

  fail() {
    this.onerror?.(new Event('error'));
  }
}

describe('EventLogDrawer', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    MockEventSource.instances = [];
    vi.stubGlobal('EventSource', MockEventSource);
    window.localStorage.clear();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  test('coalesces burst updates with requestAnimationFrame and keeps the latest 20 entries', async () => {
    const rafCallbacks: FrameRequestCallback[] = [];
    const rafSpy = vi
      .spyOn(window, 'requestAnimationFrame')
      .mockImplementation((callback: FrameRequestCallback) => {
        rafCallbacks.push(callback);
        return rafCallbacks.length;
      });

    render(<EventLogDrawer />);
    fireEvent.click(screen.getByRole('button', { name: 'Event log' }));

    const stream = MockEventSource.instances[0];
    expect(stream.url).toContain('/api/pms/stream/events');

    for (let index = 1; index <= 100; index += 1) {
      stream.emit('sensor.signal', {
        event_id: index,
        event_type: 'sensor.signal',
        created_at: '2026-04-23T12:00:00+00:00',
        summary: `Signal ${index}`,
        market_id: `market-${index}`
      });
    }

    expect(rafSpy).not.toHaveBeenCalled();

    await act(async () => {
      await vi.advanceTimersByTimeAsync(100);
    });
    expect(rafSpy).toHaveBeenCalledTimes(1);

    await act(async () => {
      for (const callback of rafCallbacks.splice(0)) {
        callback(0);
      }
    });

    expect(screen.getAllByTestId('event-log-entry')).toHaveLength(20);
    expect(screen.getByText('Signal 100')).toBeInTheDocument();
  });

  test('persists the pinned state and shows an unavailable message after stream errors', async () => {
    window.localStorage.setItem('pms.eventlog.pinned', 'true');
    render(<EventLogDrawer />);

    await act(async () => {
      await Promise.resolve();
    });

    expect(screen.getByRole('complementary', { name: 'Event log' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Unpin event log' })).toBeInTheDocument();

    const stream = MockEventSource.instances[0];
    stream.fail();

    await act(async () => {
      await Promise.resolve();
    });

    expect(screen.getByText('Event log unavailable')).toBeInTheDocument();
  });
});
