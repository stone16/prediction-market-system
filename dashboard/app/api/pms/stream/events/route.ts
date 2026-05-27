import { NextRequest, NextResponse } from 'next/server';
import { MISSING_PRODUCTION_BACKEND_DETAIL } from '@/lib/upstream';

export const dynamic = 'force-dynamic';

const encoder = new TextEncoder();

function streamHeaders() {
  return {
    'cache-control': 'no-cache, no-transform',
    connection: 'keep-alive',
    'content-type': 'text/event-stream'
  };
}

function mockStream() {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(encoder.encode(': mock stream\n\n'));
    }
  });
}

export async function GET(request: NextRequest) {
  const baseUrl = process.env.PMS_API_BASE_URL;
  if (!baseUrl) {
    if (process.env.NODE_ENV === 'production') {
      return NextResponse.json(
        { detail: MISSING_PRODUCTION_BACKEND_DETAIL },
        { status: 503 }
      );
    }
    return new Response(mockStream(), {
      headers: streamHeaders()
    });
  }

  const url = new URL('/stream/events', baseUrl);
  for (const [key, value] of request.nextUrl.searchParams.entries()) {
    url.searchParams.set(key, value);
  }

  const lastEventId = request.headers.get('last-event-id');
  if (lastEventId && !url.searchParams.has('last_event_id')) {
    url.searchParams.set('last_event_id', lastEventId);
  }

  const headers = new Headers();
  const apiToken = process.env.PMS_API_TOKEN;
  if (apiToken) {
    headers.set('Authorization', `Bearer ${apiToken}`);
  }

  let upstream: Response;
  try {
    upstream = await fetch(url, {
      cache: 'no-store',
      headers
    });
  } catch (error) {
    console.warn(`PMS upstream unavailable at ${url.toString()}`, error);
    return NextResponse.json({ detail: 'Research backend unavailable' }, { status: 503 });
  }

  if (!upstream.ok || upstream.body === null) {
    const body = await upstream.text();
    return new NextResponse(body, {
      status: upstream.status,
      headers: {
        'content-type': upstream.headers.get('content-type') ?? 'application/json'
      }
    });
  }

  return new Response(upstream.body, {
    status: upstream.status,
    headers: streamHeaders()
  });
}
