'use client';

import dynamic from 'next/dynamic';

export const FactorSeriesChartNoSsr = dynamic(
  () => import('@/components/FactorSeriesChart').then((module) => module.FactorSeriesChart),
  { ssr: false }
);
