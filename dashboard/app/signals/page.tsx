import { SignalsPageClient } from '@/components/SignalsPageClient';

type SignalsPageProps = {
  searchParams: Promise<{ market_id?: string | string[] }>;
};

export default async function SignalsPage({ searchParams }: SignalsPageProps) {
  const params = await searchParams;
  const marketIdValue = params.market_id;
  const marketId =
    typeof marketIdValue === 'string'
      ? marketIdValue
      : Array.isArray(marketIdValue)
        ? marketIdValue[0] ?? null
        : null;

  return <SignalsPageClient marketId={marketId} />;
}
