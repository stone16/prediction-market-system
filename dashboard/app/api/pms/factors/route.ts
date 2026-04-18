import { NextRequest, NextResponse } from 'next/server';
import { mockFactorSeries } from '@/lib/mock-store';
import { upstreamResponse } from '@/lib/upstream';

function parseLimit(rawLimit: string | null): number {
  const parsed = Number.parseInt(rawLimit ?? '500', 10);
  if (!Number.isFinite(parsed) || Number.isNaN(parsed)) {
    return 500;
  }
  return Math.min(Math.max(parsed, 1), 2000);
}

export async function GET(request: NextRequest) {
  const query = request.nextUrl.searchParams.toString();
  const upstream = await upstreamResponse(query ? `/factors?${query}` : '/factors');
  if (upstream) return upstream;
  return NextResponse.json(
    mockFactorSeries({
      factorId: request.nextUrl.searchParams.get('factor_id') ?? 'orderbook_imbalance',
      marketId: request.nextUrl.searchParams.get('market_id') ?? 'factor-depth',
      param: request.nextUrl.searchParams.get('param') ?? '',
      since: request.nextUrl.searchParams.get('since'),
      limit: parseLimit(request.nextUrl.searchParams.get('limit'))
    })
  );
}
