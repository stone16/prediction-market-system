import { NextRequest, NextResponse } from 'next/server';
import { mockDecisions } from '@/lib/mock-store';
import { upstreamResponse } from '@/lib/upstream';

export async function GET(request: NextRequest) {
  const upstream = await upstreamResponse(`/decisions${request.nextUrl.search}`);
  if (upstream) return upstream;
  const limit = Number(request.nextUrl.searchParams.get('limit') ?? 50);
  return NextResponse.json(mockDecisions().slice(-Math.max(limit, 0)));
}
