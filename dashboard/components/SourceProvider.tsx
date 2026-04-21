'use client';

import { createContext, useContext } from 'react';
import type { DashboardSource } from '@/lib/dashboard-source';

const SourceContext = createContext<DashboardSource>('mock');

type SourceProviderProps = {
  children: React.ReactNode;
  source: DashboardSource;
};

export function SourceProvider({ children, source }: SourceProviderProps) {
  return <SourceContext.Provider value={source}>{children}</SourceContext.Provider>;
}

export function useDashboardSource(): DashboardSource {
  return useContext(SourceContext);
}
