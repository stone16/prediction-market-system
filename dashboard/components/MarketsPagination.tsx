import type { MarketsPageSize } from '@/lib/useMarketsFilters';

type MarketsPaginationProps = {
  page: number;
  pageSize: MarketsPageSize;
  total: number;
  onPageChange: (page: number) => void;
  onPageSizeChange: (pageSize: MarketsPageSize) => void;
};

const PAGE_SIZE_OPTIONS: MarketsPageSize[] = [20, 50, 100];

function totalPages(total: number, pageSize: number) {
  return Math.max(1, Math.ceil(total / pageSize));
}

function clampPage(page: number, maxPage: number) {
  return Math.min(Math.max(1, page), maxPage);
}

function pageWindow(page: number, maxPage: number): Array<number | 'ellipsis-left' | 'ellipsis-right'> {
  if (maxPage <= 7) {
    return Array.from({ length: maxPage }, (_, index) => index + 1);
  }
  if (page <= 4) {
    return [1, 2, 3, 4, 'ellipsis-right', maxPage];
  }
  if (page >= maxPage - 3) {
    return [1, 'ellipsis-left', maxPage - 3, maxPage - 2, maxPage - 1, maxPage];
  }
  return [1, 'ellipsis-left', page - 1, page, page + 1, 'ellipsis-right', maxPage];
}

function formatRange(page: number, pageSize: number, total: number) {
  if (total === 0) {
    return '0 of 0';
  }
  const start = (page - 1) * pageSize + 1;
  const end = Math.min(page * pageSize, total);
  return `${start}-${end} of ${total}`;
}

function parsePageInput(value: string, maxPage: number) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed)) {
    return null;
  }
  return clampPage(parsed, maxPage);
}

function parsePageSize(value: string): MarketsPageSize {
  const parsed = Number(value);
  return PAGE_SIZE_OPTIONS.includes(parsed as MarketsPageSize)
    ? (parsed as MarketsPageSize)
    : 50;
}

export function MarketsPagination({
  page,
  pageSize,
  total,
  onPageChange,
  onPageSizeChange
}: MarketsPaginationProps) {
  const maxPage = totalPages(total, pageSize);
  const currentPage = clampPage(page, maxPage);

  return (
    <nav aria-label="Markets pagination" className="markets-pagination">
      <div className="markets-pagination__summary">
        <strong>{formatRange(currentPage, pageSize, total)}</strong>
        <span>Page {currentPage} of {maxPage}</span>
      </div>
      <div className="markets-pagination__controls">
        <button
          aria-label="Previous page"
          disabled={currentPage === 1}
          onClick={() => onPageChange(currentPage - 1)}
          type="button"
        >
          «
        </button>
        {pageWindow(currentPage, maxPage).map((item) =>
          typeof item === 'number' ? (
            <button
              aria-current={item === currentPage ? 'page' : undefined}
              aria-label={item === currentPage ? `Page ${item}` : `Go to page ${item}`}
              key={item}
              onClick={() => onPageChange(item)}
              type="button"
            >
              {item}
            </button>
          ) : (
            <span aria-hidden="true" className="markets-pagination__ellipsis" key={item}>
              …
            </span>
          )
        )}
        <button
          aria-label="Next page"
          disabled={currentPage === maxPage}
          onClick={() => onPageChange(currentPage + 1)}
          type="button"
        >
          »
        </button>
      </div>
      <div className="markets-pagination__inputs">
        <label>
          <span>Page number</span>
          <input
            inputMode="numeric"
            max={maxPage}
            min={1}
            onChange={(event) => {
              const nextPage = parsePageInput(event.target.value, maxPage);
              if (nextPage !== null) {
                onPageChange(nextPage);
              }
            }}
            type="number"
            value={currentPage}
          />
        </label>
        <label>
          <span>Page size</span>
          <select
            onChange={(event) => onPageSizeChange(parsePageSize(event.target.value))}
            value={pageSize}
          >
            {PAGE_SIZE_OPTIONS.map((option) => (
              <option key={option} value={option}>
                {option}
              </option>
            ))}
          </select>
        </label>
      </div>
    </nav>
  );
}
