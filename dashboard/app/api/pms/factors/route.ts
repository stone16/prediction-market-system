import { NextRequest, NextResponse } from 'next/server';
import { mockFactorSeries } from '@/lib/mock-store';
import { upstreamResponse } from '@/lib/upstream';

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
      limit: Number(request.nextUrl.searchParams.get('limit') ?? '500')
    })
  );
}
