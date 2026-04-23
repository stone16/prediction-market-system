import { NextResponse } from 'next/server';
import { mockPositions } from '@/lib/mock-store';
import { upstreamResponse } from '@/lib/upstream';

export async function GET() {
  const upstream = await upstreamResponse('/positions');
  if (upstream) return upstream;

  return NextResponse.json(mockPositions());
}
