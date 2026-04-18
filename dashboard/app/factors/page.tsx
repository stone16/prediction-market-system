import { FactorsPageClient } from './FactorsPageClient';

type FactorsPageProps = {
  searchParams?: Promise<{
    factor_id?: string;
    market_id?: string;
    param?: string;
  }>;
};

export default async function FactorsPage({ searchParams }: FactorsPageProps) {
  const resolvedSearchParams = searchParams ? await searchParams : undefined;
  return (
    <FactorsPageClient
      initialFactorId={resolvedSearchParams?.factor_id ?? 'orderbook_imbalance'}
      initialMarketId={resolvedSearchParams?.market_id ?? 'factor-depth'}
      initialParam={resolvedSearchParams?.param ?? ''}
    />
  );
}
