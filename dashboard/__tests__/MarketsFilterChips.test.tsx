import { fireEvent, render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, test, vi } from 'vitest';
import { MarketsFilterChips } from '@/components/MarketsFilterChips';
import { useMarketsFilters } from '@/lib/useMarketsFilters';

const replaceMock = vi.fn();
let currentSearch = '';

vi.mock('next/navigation', () => ({
  usePathname: () => '/markets',
  useRouter: () => ({ replace: replaceMock }),
  useSearchParams: () => new URLSearchParams(currentSearch)
}));

function ChipsHarness() {
  const { activeChips, clearFilter } = useMarketsFilters();
  return <MarketsFilterChips chips={activeChips} onClearFilter={clearFilter} />;
}

describe('MarketsFilterChips', () => {
  beforeEach(() => {
    currentSearch = '';
    replaceMock.mockReset();
  });

  test('renders active chips and removes the corresponding filter from the URL', () => {
    currentSearch = 'volume_min=100000&subscribed=only';

    render(<ChipsHarness />);

    expect(screen.getByText('Volume >= 100000')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Remove Volume >= 100000 filter' }));

    expect(replaceMock).toHaveBeenCalledWith('/markets?subscribed=only', {
      scroll: false
    });
  });
});
