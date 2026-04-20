import { NextResponse } from 'next/server';
import { mockBacktestRun } from '@/lib/mock-store';
import { upstreamResponse } from '@/lib/upstream';

type RouteContext = {
  params: Promise<{ runId: string }>;
};

export async function GET(_request: Request, context: RouteContext) {
  const { runId } = await context.params;
  const upstream = await upstreamResponse(`/research/backtest/${runId}`);
  if (upstream) return upstream;
  const run = mockBacktestRun(runId);
  if (!run) {
    return NextResponse.json({ detail: 'Backtest run not found' }, { status: 404 });
  }
  return NextResponse.json(run);
}
