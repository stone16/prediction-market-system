import { render, screen } from '@testing-library/react';
import { EmptyState } from '@/components/EmptyState';
import { expect, test } from 'vitest';

test('EmptyState renders title, body, and CTA when provided', () => {
  render(
    <EmptyState
      body="Browse a few markets to start building your watchlist."
      cta={{ href: '/markets', label: 'Browse markets' }}
      title="Your watchlist is empty"
    />
  );

  expect(screen.getByText('Your watchlist is empty')).toBeInTheDocument();
  expect(
    screen.getByText('Browse a few markets to start building your watchlist.')
  ).toBeInTheDocument();
  expect(screen.getByRole('link', { name: 'Browse markets' })).toHaveAttribute(
    'href',
    '/markets'
  );
});

test('EmptyState omits the CTA section when no action exists', () => {
  render(<EmptyState body="No action needed yet." title="Quiet market period" />);

  expect(screen.getByText('Quiet market period')).toBeInTheDocument();
  expect(screen.queryByRole('link')).toBeNull();
});
