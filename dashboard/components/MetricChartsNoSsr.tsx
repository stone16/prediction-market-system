'use client';

import dynamic from 'next/dynamic';

export const MetricChartsNoSsr = dynamic(
  () => import('@/components/MetricCharts').then((module) => module.MetricCharts),
  { ssr: false }
);
