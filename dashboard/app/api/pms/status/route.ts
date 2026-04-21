import { NextResponse } from 'next/server';
import { getDashboardSource } from '@/lib/dashboard-source';
import { mockStatus } from '@/lib/mock-store';
import type { StatusResponse } from '@/lib/types';
import { upstreamResponse } from '@/lib/upstream';

export async function GET() {
  const source = getDashboardSource();
  const upstream = await upstreamResponse('/status');
  if (upstream) {
    if (!upstream.ok) {
      return upstream;
    }
    const payload = (await upstream.json()) as Omit<StatusResponse, 'source'>;
    return NextResponse.json<StatusResponse>({
      ...payload,
      source
    });
  }
  return NextResponse.json<StatusResponse>({
    ...mockStatus(),
    source
  });
}
