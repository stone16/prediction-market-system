import { NextResponse } from 'next/server';
import { mockBacktestComparison } from '@/lib/mock-store';
import { upstreamResponse } from '@/lib/upstream';

type RouteContext = {
  params: Promise<{ runId: string }>;
};

export async function POST(request: Request, context: RouteContext) {
  const { runId } = await context.params;
  const body = await request.text();
  const upstream = await upstreamResponse(`/research/backtest/${runId}/compare`, {
    method: 'POST',
    headers: {
      'content-type': request.headers.get('content-type') ?? 'application/json'
    },
    body
  });
  if (upstream) return upstream;
  return NextResponse.json(mockBacktestComparison(runId));
}
