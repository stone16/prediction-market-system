import { NextResponse } from 'next/server';
import { mockStrategies } from '@/lib/mock-store';
import { upstreamResponse } from '@/lib/upstream';

export async function GET() {
  const upstream = await upstreamResponse('/strategies');
  if (upstream) return upstream;
  return NextResponse.json(mockStrategies());
}
