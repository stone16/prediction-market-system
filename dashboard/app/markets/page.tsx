import { Suspense } from 'react';
import { MarketsPageClient } from '@/components/MarketsPageClient';

export default function MarketsPage() {
  return (
    <Suspense fallback={<main className="shell" aria-busy="true" />}>
      <MarketsPageClient />
    </Suspense>
  );
}
