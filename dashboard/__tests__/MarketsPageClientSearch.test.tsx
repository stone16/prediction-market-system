import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import { MarketsPageClient } from '@/components/MarketsPageClient';

const mocks = vi.hoisted(() => ({
  currentSearch: '',
  replace: vi.fn(),
  useLiveData: vi.fn()
}));

vi.mock('next/navigation', () => ({
  usePathname: () => '/markets',
  useRouter: () => ({ replace: mocks.replace }),
  useSearchParams: () => new URLSearchParams(mocks.currentSearch)
}));

vi.mock('@/lib/useLiveData', () => ({
  useLiveData: mocks.useLiveData
}));

vi.mock('@/components/Nav', () => ({
  Nav: () => <nav aria-label="Primary navigation" />
}));

const emptyState = { data: null, loading: false, disconnected: false, error: null };
const marketState = {
  data: { markets: [], limit: 50, offset: 0, total: 0 },
  loading: false,
  disconnected: false,
  error: null
};
const statusState = {
  data: {
    mode: 'paper',
    source: 'live',
    runner_started_at: '2026-04-24T12:00:00+00:00',
    running: true,
    sensors: [],
    controller: { decisions_total: 0 },
    actuator: { fills_total: 0, mode: 'paper' },
    evaluator: { eval_records_total: 0, brier_overall: null }
  },
  loading: false,
  disconnected: false,
  error: null
};

describe('MarketsPageClient search', () => {
  beforeEach(() => {
    mocks.currentSearch = '';
    mocks.replace.mockReset();
    mocks.useLiveData.mockImplementation((path: string | null) => {
      if (path === null) {
        return emptyState;
      }
      if (path === '/status') {
        return statusState;
      }
      return marketState;
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  test('debounces search URL writes and market refetches', async () => {
    render(<MarketsPageClient />);

    fireEvent.change(screen.getByLabelText('Search markets'), {
      target: { value: 'election' }
    });

    expect(mocks.replace).not.toHaveBeenCalled();

    await waitFor(() => {
      expect(mocks.replace).toHaveBeenCalledWith('/markets?q=election', {
        scroll: false
      });
    });
  });
});
