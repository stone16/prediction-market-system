import { NextResponse } from 'next/server';
import { mockStatus } from '@/lib/mock-store';
import { upstreamResponse } from '@/lib/upstream';

export async function GET() {
  const upstream = await upstreamResponse('/status');
  if (upstream) return upstream;
  return NextResponse.json(mockStatus());
}
