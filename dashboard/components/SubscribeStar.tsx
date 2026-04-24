type SubscribeStarProps = {
  subscribed: boolean;
  subscriptionSource: 'user' | null;
};

export function SubscribeStar({ subscribed, subscriptionSource }: SubscribeStarProps) {
  if (subscriptionSource === 'user') {
    return (
      <span
        aria-label="User subscription"
        className="subscribe-star subscribe-star--user"
        role="img"
      >
        ★
      </span>
    );
  }
  if (subscribed) {
    return (
      <span
        aria-label="Strategy subscription"
        className="subscribe-star subscribe-star--strategy"
        role="img"
      >
        ★
      </span>
    );
  }
  return (
    <span
      aria-label="Not subscribed"
      className="subscribe-star subscribe-star--idle"
      role="img"
    >
      ☆
    </span>
  );
}
