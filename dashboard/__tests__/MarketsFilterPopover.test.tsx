import { fireEvent, render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, test, vi } from 'vitest';
import { MarketsFilterPopover } from '@/components/MarketsFilterPopover';
import { useMarketsFilters } from '@/lib/useMarketsFilters';

const replaceMock = vi.fn();
let currentSearch = '';

vi.mock('next/navigation', () => ({
  usePathname: () => '/markets',
  useRouter: () => ({ replace: replaceMock }),
  useSearchParams: () => new URLSearchParams(currentSearch)
}));

function PopoverHarness() {
  const { filters, setFilter } = useMarketsFilters();
  return <MarketsFilterPopover filters={filters} onFilterChange={setFilter} />;
}

describe('MarketsFilterPopover', () => {
  beforeEach(() => {
    currentSearch = '';
    replaceMock.mockReset();
  });

  test('each filter control updates the URL', () => {
    render(<PopoverHarness />);
    fireEvent.click(screen.getByRole('button', { name: 'Filters' }));

    const cases: Array<[RegExp, string, string]> = [
      [/Minimum volume/i, '1000', 'volume_min=1000'],
      [/Minimum liquidity/i, '2500', 'liquidity_min=2500'],
      [/Maximum spread/i, '300', 'spread_max_bps=300'],
      [/YES minimum/i, '0.2', 'yes_min=0.2'],
      [/YES maximum/i, '0.8', 'yes_max=0.8'],
      [/Resolves within days/i, '14', 'resolves_within_days=14'],
      [/Subscription/i, 'only', 'subscribed=only']
    ];

    for (const [label, value, expectedQuery] of cases) {
      const control = screen.getByLabelText(label);
      fireEvent.change(control, { target: { value } });
      expect(replaceMock).toHaveBeenLastCalledWith(`/markets?${expectedQuery}`, {
        scroll: false
      });
    }
  });
});
