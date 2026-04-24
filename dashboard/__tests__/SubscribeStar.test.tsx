import { render, screen } from '@testing-library/react';
import { describe, expect, test } from 'vitest';
import { SubscribeStar } from '@/components/SubscribeStar';

describe('SubscribeStar', () => {
  test('renders filled gold for user subscriptions', () => {
    render(<SubscribeStar subscribed subscriptionSource="user" />);

    expect(screen.getByLabelText('User subscription')).toHaveClass('subscribe-star--user');
  });

  test('renders filled blue for strategy-driven subscriptions', () => {
    render(<SubscribeStar subscribed subscriptionSource={null} />);

    expect(screen.getByLabelText('Strategy subscription')).toHaveClass(
      'subscribe-star--strategy'
    );
  });

  test('renders outline for idle markets', () => {
    render(<SubscribeStar subscribed={false} subscriptionSource={null} />);

    expect(screen.getByLabelText('Not subscribed')).toHaveClass('subscribe-star--idle');
  });
});
