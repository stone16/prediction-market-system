import { NextResponse } from 'next/server';
import { mockMetrics } from '@/lib/mock-store';
import { upstreamResponse } from '@/lib/upstream';

export async function GET() {
  const upstream = await upstreamResponse('/metrics');
  if (upstream) return upstream;
  return NextResponse.json(mockMetrics());
}
