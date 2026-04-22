'use client';

import type { DashboardSource } from '@/lib/dashboard-source';
import { useDashboardSource } from './SourceProvider';

type SourceBadgeProps = {
  source?: DashboardSource;
};

export function SourceBadge({ source: sourceOverride }: SourceBadgeProps) {
  const source = sourceOverride ?? useDashboardSource();

  if (source !== 'mock') {
    return null;
  }

  return (
    <span className="source-badge" data-testid="source-badge">
      MOCK
    </span>
  );
}
