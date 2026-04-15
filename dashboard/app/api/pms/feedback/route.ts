import { NextRequest, NextResponse } from 'next/server';
import { readFeedback } from '@/lib/mock-store';
import { upstreamResponse } from '@/lib/upstream';

export async function GET(request: NextRequest) {
  const upstream = await upstreamResponse(`/feedback${request.nextUrl.search}`);
  if (upstream) return upstream;
  const resolved = request.nextUrl.searchParams.get('resolved');
  const limit = Number.parseInt(request.nextUrl.searchParams.get('limit') ?? '50', 10);
  const items = readFeedback().filter((item) => {
    if (resolved === null) return true;
    return item.resolved === (resolved === 'true');
  });
  const boundedLimit = Number.isNaN(limit) ? 50 : Math.max(limit, 0);
  return NextResponse.json(boundedLimit === 0 ? [] : items.slice(-boundedLimit));
}
