import { NextResponse } from 'next/server';
import { upstreamResponse } from '@/lib/upstream';

export async function POST() {
  const upstream = await upstreamResponse('/run/stop', { method: 'POST' });
  if (upstream) return upstream;
  return NextResponse.json(
    {
      detail:
        'PMS_API_BASE_URL is not configured — runner control requires a live backend.'
    },
    { status: 503 }
  );
}
