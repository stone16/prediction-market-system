import type { MarketsFilterChip, MarketsFilterKey } from '@/lib/useMarketsFilters';

type MarketsFilterChipsProps = {
  chips: MarketsFilterChip[];
  onClearFilter: (key: MarketsFilterKey) => void;
};

export function MarketsFilterChips({ chips, onClearFilter }: MarketsFilterChipsProps) {
  if (chips.length === 0) {
    return null;
  }

  return (
    <div aria-label="Active market filters" className="markets-filter-chips">
      {chips.map((chip) => (
        <span className="markets-filter-chip" key={chip.key}>
          <span>{chip.label}</span>
          <button
            aria-label={`Remove ${chip.label} filter`}
            onClick={() => onClearFilter(chip.key)}
            type="button"
          >
            ×
          </button>
        </span>
      ))}
    </div>
  );
}
