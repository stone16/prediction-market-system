import { NextRequest, NextResponse } from 'next/server';
import { upstreamResponse } from '@/lib/upstream';

type RouteContext = {
  params: Promise<{ id: string }>;
};

export async function GET(request: NextRequest, context: RouteContext) {
  const { id } = await context.params;
  const upstream = await upstreamResponse(
    `/markets/${encodeURIComponent(id)}/price-history${request.nextUrl.search}`
  );
  if (upstream) return upstream;
  return NextResponse.json({ condition_id: id, snapshots: [] });
}
