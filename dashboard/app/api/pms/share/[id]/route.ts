import { NextResponse } from 'next/server';

type RouteContext = {
  params: Promise<{ id: string }>;
};

export const dynamic = 'force-dynamic';

export async function GET(_: Request, context: RouteContext) {
  const { id } = await context.params;
  const baseUrl = process.env.PMS_API_BASE_URL;

  if (!baseUrl) {
    return NextResponse.json({ detail: 'Research backend unavailable' }, { status: 503 });
  }

  let upstream: Response;
  try {
    upstream = await fetch(new URL(`/share/${id}`, baseUrl), {
      cache: 'no-store'
    });
  } catch (error) {
    console.warn(`PMS upstream unavailable at ${baseUrl}/share/${id}`, error);
    return NextResponse.json({ detail: 'Research backend unavailable' }, { status: 503 });
  }

  const body = await upstream.text();
  return new NextResponse(body, {
    status: upstream.status,
    headers: {
      'content-type': upstream.headers.get('content-type') ?? 'application/json'
    }
  });
}
