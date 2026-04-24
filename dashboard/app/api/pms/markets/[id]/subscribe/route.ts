import { NextResponse } from 'next/server';
import { upstreamResponse } from '@/lib/upstream';

type RouteContext = {
  params: Promise<{ id: string }>;
};

function liveBackendRequired() {
  return NextResponse.json(
    {
      detail:
        'PMS_API_BASE_URL is not configured — market subscriptions require a live backend.'
    },
    { status: 503 }
  );
}

export async function POST(_request: Request, context: RouteContext) {
  const { id } = await context.params;
  const upstream = await upstreamResponse(`/markets/${encodeURIComponent(id)}/subscribe`, {
    method: 'POST'
  });
  if (upstream) return upstream;
  return liveBackendRequired();
}

export async function DELETE(_request: Request, context: RouteContext) {
  const { id } = await context.params;
  const upstream = await upstreamResponse(`/markets/${encodeURIComponent(id)}/subscribe`, {
    method: 'DELETE'
  });
  if (upstream) return upstream;
  return liveBackendRequired();
}
