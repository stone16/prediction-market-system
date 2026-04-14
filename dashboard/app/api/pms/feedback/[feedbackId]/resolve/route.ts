import { NextResponse } from 'next/server';
import { resolveFeedback } from '@/lib/mock-store';
import { upstreamResponse } from '@/lib/upstream';

type Context = { params: Promise<{ feedbackId: string }> };

export async function POST(_request: Request, context: Context) {
  const { feedbackId } = await context.params;
  const upstream = await upstreamResponse(`/feedback/${feedbackId}/resolve`, {
    method: 'POST'
  });
  if (upstream) return upstream;
  const feedback = resolveFeedback(feedbackId);
  if (!feedback) {
    return NextResponse.json({ detail: 'Feedback not found' }, { status: 404 });
  }
  return NextResponse.json(feedback);
}
