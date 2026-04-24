import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, test, vi } from 'vitest';
import { MarketsPagination } from '@/components/MarketsPagination';

describe('MarketsPagination', () => {
  test('renders first-page boundaries and page controls', () => {
    const onPageChange = vi.fn();
    const onPageSizeChange = vi.fn();

    render(
      <MarketsPagination
        onPageChange={onPageChange}
        onPageSizeChange={onPageSizeChange}
        page={1}
        pageSize={50}
        total={485}
      />
    );

    expect(screen.getByText('1-50 of 485')).toBeInTheDocument();
    expect(screen.getByText('Page 1 of 10')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Previous page' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Next page' })).toBeEnabled();
    expect(screen.getByRole('button', { name: 'Page 1' })).toHaveAttribute(
      'aria-current',
      'page'
    );
    expect(screen.getByRole('button', { name: 'Go to page 2' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Go to page 3' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Go to page 10' })).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Next page' }));
    expect(onPageChange).toHaveBeenCalledWith(2);
  });

  test('renders middle and last-page boundaries', () => {
    const onPageChange = vi.fn();
    const onPageSizeChange = vi.fn();
    const { rerender } = render(
      <MarketsPagination
        onPageChange={onPageChange}
        onPageSizeChange={onPageSizeChange}
        page={5}
        pageSize={50}
        total={485}
      />
    );

    expect(screen.getByText('201-250 of 485')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Go to page 4' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Page 5' })).toHaveAttribute(
      'aria-current',
      'page'
    );
    expect(screen.getByRole('button', { name: 'Go to page 6' })).toBeInTheDocument();

    rerender(
      <MarketsPagination
        onPageChange={onPageChange}
        onPageSizeChange={onPageSizeChange}
        page={10}
        pageSize={50}
        total={485}
      />
    );

    expect(screen.getByText('451-485 of 485')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Previous page' })).toBeEnabled();
    expect(screen.getByRole('button', { name: 'Next page' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Page 10' })).toHaveAttribute(
      'aria-current',
      'page'
    );
  });

  test('page number input and size selector dispatch bounded changes', () => {
    const onPageChange = vi.fn();
    const onPageSizeChange = vi.fn();

    render(
      <MarketsPagination
        onPageChange={onPageChange}
        onPageSizeChange={onPageSizeChange}
        page={1}
        pageSize={50}
        total={485}
      />
    );

    fireEvent.change(screen.getByLabelText('Page number'), { target: { value: '10' } });
    expect(onPageChange).toHaveBeenCalledWith(10);

    fireEvent.change(screen.getByLabelText('Page size'), { target: { value: '100' } });
    expect(onPageSizeChange).toHaveBeenCalledWith(100);
  });
});
