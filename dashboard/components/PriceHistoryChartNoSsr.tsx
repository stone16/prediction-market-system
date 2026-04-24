'use client';

import dynamic from 'next/dynamic';

export const PriceHistoryChartNoSsr = dynamic(
  () => import('@/components/PriceHistoryChart').then((module) => module.PriceHistoryChart),
  { ssr: false }
);
