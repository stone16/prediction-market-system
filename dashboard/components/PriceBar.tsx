type PriceBarProps = {
  label: string;
  tone: 'yes' | 'no';
  value: number | null;
};

function clampPercent(value: number) {
  return Math.min(100, Math.max(0, value * 100));
}

function formatPercent(value: number) {
  return new Intl.NumberFormat('en-US', {
    maximumFractionDigits: 1,
    minimumFractionDigits: 1
  }).format(value);
}

export function PriceBar({ label, tone, value }: PriceBarProps) {
  if (value === null) {
    return <span className="muted">—</span>;
  }
  const percent = clampPercent(value);
  const percentText = `${formatPercent(percent)}%`;
  return (
    <div
      aria-label={label}
      aria-valuemax={100}
      aria-valuemin={0}
      aria-valuenow={percent}
      className={`price-bar price-bar--${tone}`}
      role="meter"
    >
      <span className="price-bar__label">{percentText}</span>
      <span className="price-bar__track" aria-hidden="true">
        <span
          className="price-bar__fill"
          data-testid="price-bar-fill"
          style={{ width: percentText }}
        />
      </span>
    </div>
  );
}
