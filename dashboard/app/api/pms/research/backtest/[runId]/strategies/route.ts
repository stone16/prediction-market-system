import { NextResponse } from 'next/server';
import { mockBacktestStrategyRuns } from '@/lib/mock-store';
import { upstreamResponse } from '@/lib/upstream';

type RouteContext = {
  params: Promise<{ runId: string }>;
};

export async function GET(_request: Request, context: RouteContext) {
  const { runId } = await context.params;
  const upstream = await upstreamResponse(`/research/backtest/${runId}/strategies`);
  if (upstream) return upstream;
  return NextResponse.json(mockBacktestStrategyRuns(runId));
}
