import { NextRequest, NextResponse } from 'next/server';
import { upstreamResponse } from '@/lib/upstream';

export async function POST(request: NextRequest) {
  const rawBody = await request.text();
  const upstream = await upstreamResponse('/config', {
    method: 'POST',
    body: rawBody,
    headers: { 'content-type': 'application/json' }
  });
  if (upstream) return upstream;
  const body = JSON.parse(rawBody) as { mode?: string };
  if (body.mode === 'live') {
    return NextResponse.json(
      { detail: 'Live trading is disabled. Set live_trading_enabled=true in config.' },
      { status: 400 }
    );
  }
  return NextResponse.json({ mode: body.mode ?? 'backtest' });
}
