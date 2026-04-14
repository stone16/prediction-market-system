import { NextRequest, NextResponse } from 'next/server';
import { readFeedback } from '@/lib/mock-store';
import { upstreamResponse } from '@/lib/upstream';

export async function GET(request: NextRequest) {
  const upstream = await upstreamResponse(`/feedback${request.nextUrl.search}`);
  if (upstream) return upstream;
  const resolved = request.nextUrl.searchParams.get('resolved');
  const items = readFeedback().filter((item) => {
    if (resolved === null) return true;
    return item.resolved === (resolved === 'true');
  });
  return NextResponse.json(items);
}
