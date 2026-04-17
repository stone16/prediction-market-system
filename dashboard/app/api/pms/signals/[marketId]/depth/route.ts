import { NextRequest, NextResponse } from 'next/server';
import { mockSignalDepth } from '@/lib/mock-store';
import { upstreamResponse } from '@/lib/upstream';

type RouteContext = {
  params: Promise<{ marketId: string }>;
};

export async function GET(request: NextRequest, context: RouteContext) {
  const { marketId } = await context.params;
  const upstream = await upstreamResponse(
    `/signals/${marketId}/depth${request.nextUrl.search}`
  );
  if (upstream) return upstream;
  return NextResponse.json(mockSignalDepth(marketId));
}
