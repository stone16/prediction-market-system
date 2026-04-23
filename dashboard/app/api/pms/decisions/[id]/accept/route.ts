import { NextResponse } from 'next/server';
import { upstreamResponse } from '@/lib/upstream';

type RouteContext = {
  params: Promise<{ id: string }>;
};

export async function POST(request: Request, context: RouteContext) {
  const { id } = await context.params;
  const body = await request.text();
  const upstream = await upstreamResponse(`/decisions/${id}/accept`, {
    method: 'POST',
    headers: {
      'content-type': request.headers.get('content-type') ?? 'application/json'
    },
    body
  });
  if (upstream) return upstream;
  return NextResponse.json(
    { detail: 'PMS_API_BASE_URL is not configured — idea acceptance requires a live backend.' },
    { status: 503 }
  );
}
