import { render, screen } from '@testing-library/react';
import { describe, expect, test } from 'vitest';
import { FreshnessDot } from '@/components/FreshnessDot';

const now = new Date('2026-04-24T12:00:00+00:00');

describe('FreshnessDot', () => {
  test('renders green for prices updated less than 60 seconds ago', () => {
    render(<FreshnessDot priceUpdatedAt="2026-04-24T11:59:30+00:00" now={now} />);

    expect(screen.getByLabelText('Fresh price')).toHaveClass('freshness-dot--green');
  });

  test('renders amber for prices updated between 60 seconds and 5 minutes ago', () => {
    render(<FreshnessDot priceUpdatedAt="2026-04-24T11:58:00+00:00" now={now} />);

    expect(screen.getByLabelText('Aging price')).toHaveClass('freshness-dot--amber');
  });

  test('renders gray for prices updated more than 5 minutes ago', () => {
    render(<FreshnessDot priceUpdatedAt="2026-04-24T11:54:59+00:00" now={now} />);

    expect(screen.getByLabelText('Stale price')).toHaveClass('freshness-dot--gray');
  });
});
