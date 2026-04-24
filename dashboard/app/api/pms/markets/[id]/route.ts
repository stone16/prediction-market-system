import { NextRequest, NextResponse } from 'next/server';
import { mockMarkets } from '@/lib/mock-store';
import { upstreamResponse } from '@/lib/upstream';

type RouteContext = {
  params: Promise<{ id: string }>;
};

export async function GET(request: NextRequest, context: RouteContext) {
  const { id } = await context.params;
  const upstream = await upstreamResponse(
    `/markets/${encodeURIComponent(id)}${request.nextUrl.search}`
  );
  if (upstream) return upstream;

  const market = mockMarkets().find((item) => item.market_id === id);
  if (!market) {
    return NextResponse.json({ detail: 'Market not found' }, { status: 404 });
  }
  return NextResponse.json(market);
}
