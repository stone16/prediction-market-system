import { NextResponse } from 'next/server';
import { mockFactorsCatalog } from '@/lib/mock-store';
import { upstreamResponse } from '@/lib/upstream';

export async function GET() {
  const upstream = await upstreamResponse('/factors/catalog');
  if (upstream) return upstream;
  return NextResponse.json(mockFactorsCatalog());
}
