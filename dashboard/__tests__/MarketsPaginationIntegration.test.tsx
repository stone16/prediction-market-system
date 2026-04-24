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

function PaginationHarness() {
  const { marketPath, pagination, setFilter, setPage, setPageSize } = useMarketsFilters();
  return (
    <>
      <span data-testid="page">{pagination.page}</span>
      <span data-testid="page-size">{pagination.pageSize}</span>
      <span data-testid="market-path">{marketPath}</span>
      <button onClick={() => setPage(3)} type="button">
        Go page 3
      </button>
      <button onClick={() => setPageSize(100)} type="button">
        Set size 100
      </button>
      <button onClick={() => setFilter('volumeMin', '100000')} type="button">
        Set filter
      </button>
    </>
  );
}

describe('markets pagination integration', () => {
  beforeEach(() => {
    currentSearch = '';
    replaceMock.mockReset();
  });

  test('derives limit and offset from URL page state', () => {
    currentSearch = 'page=3&limit=50';

    render(<PaginationHarness />);

    expect(screen.getByTestId('page')).toHaveTextContent('3');
    expect(screen.getByTestId('page-size')).toHaveTextContent('50');
    expect(screen.getByTestId('market-path')).toHaveTextContent('/markets?limit=50&offset=100');
  });

  test('changing page, page size, and filters write the expected URL state', () => {
    currentSearch = 'page=3&limit=50';

    render(<PaginationHarness />);

    fireEvent.click(screen.getByRole('button', { name: 'Go page 3' }));
    expect(replaceMock).toHaveBeenCalledWith('/markets?limit=50&page=3', { scroll: false });

    fireEvent.click(screen.getByRole('button', { name: 'Set size 100' }));
    expect(replaceMock).toHaveBeenLastCalledWith('/markets?limit=100', { scroll: false });

    fireEvent.click(screen.getByRole('button', { name: 'Set filter' }));
    expect(replaceMock).toHaveBeenLastCalledWith('/markets?limit=50&volume_min=100000', {
      scroll: false
    });
  });
});
