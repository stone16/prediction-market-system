'use client';

import { useState } from 'react';
import type { MarketsFilterKey, MarketsFilterState } from '@/lib/useMarketsFilters';

type MarketsFilterPopoverProps = {
  filters: MarketsFilterState;
  onFilterChange: (key: MarketsFilterKey, value: string) => void;
};

const numberControls: Array<{
  key: MarketsFilterKey;
  label: string;
  placeholder: string;
  step: string;
}> = [
  { key: 'volumeMin', label: 'Minimum volume', placeholder: '100000', step: '1' },
  { key: 'liquidityMin', label: 'Minimum liquidity', placeholder: '25000', step: '1' },
  { key: 'spreadMaxBps', label: 'Maximum spread (bps)', placeholder: '300', step: '1' },
  { key: 'yesMin', label: 'YES minimum', placeholder: '0.25', step: '0.01' },
  { key: 'yesMax', label: 'YES maximum', placeholder: '0.75', step: '0.01' },
  { key: 'resolvesWithinDays', label: 'Resolves within days', placeholder: '14', step: '1' }
];

export function MarketsFilterPopover({ filters, onFilterChange }: MarketsFilterPopoverProps) {
  const [open, setOpen] = useState(false);

  return (
    <div className="markets-filter-popover">
      <button
        aria-controls="markets-filter-panel"
        aria-expanded={open}
        className="markets-filter-button"
        onClick={() => setOpen((current) => !current)}
        type="button"
      >
        Filters
      </button>
      {open ? (
        <div className="markets-filter-panel" id="markets-filter-panel">
          <div className="markets-filter-grid">
            {numberControls.map((control) => (
              <label className="markets-filter-control" key={control.key}>
                <span>{control.label}</span>
                <input
                  inputMode="decimal"
                  onChange={(event) => onFilterChange(control.key, event.target.value)}
                  placeholder={control.placeholder}
                  step={control.step}
                  type="number"
                  value={filters[control.key]}
                />
              </label>
            ))}
            <label className="markets-filter-control">
              <span>Subscription</span>
              <select
                onChange={(event) => onFilterChange('subscribed', event.target.value)}
                value={filters.subscribed}
              >
                <option value="all">All markets</option>
                <option value="only">Subscribed only</option>
                <option value="idle">Idle only</option>
              </select>
            </label>
          </div>
        </div>
      ) : null}
    </div>
  );
}
