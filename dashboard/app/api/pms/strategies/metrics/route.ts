import { NextResponse } from 'next/server';
import { mockStrategyMetrics } from '@/lib/mock-store';
import { upstreamResponse } from '@/lib/upstream';

export async function GET() {
  const upstream = await upstreamResponse('/strategies/metrics');
  if (upstream) return upstream;
  return NextResponse.json(mockStrategyMetrics());
}
