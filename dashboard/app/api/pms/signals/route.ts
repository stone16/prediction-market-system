import { NextRequest, NextResponse } from 'next/server';
import { mockSignals } from '@/lib/mock-store';
import { upstreamResponse } from '@/lib/upstream';

export async function GET(request: NextRequest) {
  const upstream = await upstreamResponse(`/signals${request.nextUrl.search}`);
  if (upstream) return upstream;
  const limit = Number(request.nextUrl.searchParams.get('limit') ?? 50);
  return NextResponse.json(mockSignals().slice(-Math.max(limit, 0)));
}
