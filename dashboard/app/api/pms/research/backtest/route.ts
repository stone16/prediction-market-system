import { NextResponse } from 'next/server';
import { mockBacktestRuns, mockEnqueueBacktestRun } from '@/lib/mock-store';
import { upstreamResponse } from '@/lib/upstream';

export async function GET(request: Request) {
  const search = new URL(request.url).search;
  const upstream = await upstreamResponse(`/research/backtest${search}`);
  if (upstream) return upstream;
  return NextResponse.json(mockBacktestRuns());
}

export async function POST(request: Request) {
  const body = await request.text();
  const upstream = await upstreamResponse('/research/backtest', {
    method: 'POST',
    headers: {
      'content-type': request.headers.get('content-type') ?? 'application/x-yaml'
    },
    body
  });
  if (upstream) return upstream;
  return NextResponse.json(mockEnqueueBacktestRun());
}
