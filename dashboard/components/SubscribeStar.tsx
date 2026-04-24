type SubscribeStarProps = {
  subscribed: boolean;
  subscriptionSource: 'user' | null;
  title?: string;
};

export function SubscribeStar({ subscribed, subscriptionSource, title }: SubscribeStarProps) {
  if (subscriptionSource === 'user') {
    return (
      <span
        aria-label="User subscription"
        className="subscribe-star subscribe-star--user"
        role="img"
        title={title}
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
        title={title}
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
      title={title}
    >
      ☆
    </span>
  );
}
