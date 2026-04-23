import { NextRequest, NextResponse } from 'next/server';
import { mockTrades } from '@/lib/mock-store';
import { upstreamResponse } from '@/lib/upstream';

function parsePositiveInt(value: string | null, fallback: number) {
  if (value === null) {
    return fallback;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.max(parsed, 1) : fallback;
}

export async function GET(request: NextRequest) {
  const upstream = await upstreamResponse(`/trades${request.nextUrl.search}`);
  if (upstream) return upstream;

  const limit = parsePositiveInt(request.nextUrl.searchParams.get('limit'), 20);
  return NextResponse.json(mockTrades(limit));
}
