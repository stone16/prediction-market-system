import { NextResponse } from 'next/server';
import { upstreamResponse } from '@/lib/upstream';

export async function GET() {
  const upstream = await upstreamResponse('/strategies');
  if (upstream) return upstream;
  return NextResponse.json(
    { detail: 'PMS upstream unavailable for /strategies' },
    { status: 503 }
  );
}
