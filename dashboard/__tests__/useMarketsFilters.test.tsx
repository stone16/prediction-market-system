import { fireEvent, render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, test, vi } from 'vitest';
import { useMarketsFilters } from '@/lib/useMarketsFilters';

const replaceMock = vi.fn();
let currentSearch = '';

vi.mock('next/navigation', () => ({
  usePathname: () => '/markets',
  useRouter: () => ({ replace: replaceMock }),
  useSearchParams: () => new URLSearchParams(currentSearch)
}));

function HookHarness() {
  const { filters, setFilter } = useMarketsFilters();
  return (
    <>
      <span data-testid="volume-min">{filters.volumeMin}</span>
      <button onClick={() => setFilter('volumeMin', '250000')} type="button">
        Set volume
      </button>
    </>
  );
}

describe('useMarketsFilters', () => {
  beforeEach(() => {
    currentSearch = '';
    replaceMock.mockReset();
  });

  test('loads volume_min from URL and writes state changes back to URL', () => {
    currentSearch = 'volume_min=100000';

    render(<HookHarness />);

    expect(screen.getByTestId('volume-min')).toHaveTextContent('100000');
    fireEvent.click(screen.getByRole('button', { name: 'Set volume' }));

    expect(replaceMock).toHaveBeenCalledWith('/markets?volume_min=250000', {
      scroll: false
    });
  });
});
