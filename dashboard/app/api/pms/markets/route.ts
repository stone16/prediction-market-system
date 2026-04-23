import { NextRequest, NextResponse } from 'next/server';
import { mockMarkets } from '@/lib/mock-store';
import { upstreamResponse } from '@/lib/upstream';

function parseNonNegativeInt(value: string | null, fallback: number) {
  if (value === null) {
    return fallback;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.max(parsed, 0) : fallback;
}

export async function GET(request: NextRequest) {
  const upstream = await upstreamResponse(`/markets${request.nextUrl.search}`);
  if (upstream) return upstream;

  const limit = parseNonNegativeInt(request.nextUrl.searchParams.get('limit'), 20);
  const offset = parseNonNegativeInt(request.nextUrl.searchParams.get('offset'), 0);
  const markets = mockMarkets();

  return NextResponse.json({
    markets: markets.slice(offset, offset + limit),
    limit,
    offset,
    total: markets.length
  });
}
