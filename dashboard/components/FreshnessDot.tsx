type FreshnessDotProps = {
  priceUpdatedAt: string | null;
  now?: Date;
};

type FreshnessState = 'green' | 'amber' | 'gray';

function freshnessState(priceUpdatedAt: string | null, now: Date): FreshnessState {
  if (priceUpdatedAt === null) {
    return 'gray';
  }
  const updatedAt = new Date(priceUpdatedAt);
  const ageMs = now.getTime() - updatedAt.getTime();
  if (!Number.isFinite(ageMs) || ageMs < 0) {
    return 'gray';
  }
  if (ageMs < 60_000) {
    return 'green';
  }
  if (ageMs <= 300_000) {
    return 'amber';
  }
  return 'gray';
}

function freshnessLabel(state: FreshnessState) {
  if (state === 'green') {
    return 'Fresh price';
  }
  if (state === 'amber') {
    return 'Aging price';
  }
  return 'Stale price';
}

export function FreshnessDot({ priceUpdatedAt, now = new Date() }: FreshnessDotProps) {
  const state = freshnessState(priceUpdatedAt, now);
  return (
    <span
      aria-label={freshnessLabel(state)}
      className={`freshness-dot freshness-dot--${state}`}
      data-freshness={state}
      role="status"
    />
  );
}
