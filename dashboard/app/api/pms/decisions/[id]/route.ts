import { NextRequest, NextResponse } from 'next/server';
import { mockDecisions } from '@/lib/mock-store';
import { upstreamResponse } from '@/lib/upstream';

type RouteContext = {
  params: Promise<{ id: string }>;
};

export async function GET(request: NextRequest, context: RouteContext) {
  const { id } = await context.params;
  const upstream = await upstreamResponse(`/decisions/${id}${request.nextUrl.search}`);
  if (upstream) return upstream;
  const decision = mockDecisions().find((item) => item.decision_id === id);
  if (!decision) {
    return NextResponse.json({ detail: 'Decision not found' }, { status: 404 });
  }
  return NextResponse.json(decision);
}
