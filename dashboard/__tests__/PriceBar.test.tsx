import { render, screen } from '@testing-library/react';
import { describe, expect, test } from 'vitest';
import { PriceBar } from '@/components/PriceBar';

describe('PriceBar', () => {
  test('renders a percentage label and matching bar width', () => {
    render(<PriceBar label="YES price" tone="yes" value={0.525} />);

    expect(screen.getByText('52.5%')).toBeInTheDocument();
    expect(screen.getByTestId('price-bar-fill')).toHaveStyle({ width: '52.5%' });
  });
});
