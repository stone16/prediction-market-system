import { NextResponse } from 'next/server';

export const MISSING_PRODUCTION_BACKEND_DETAIL =
  'PMS_API_BASE_URL is not configured; production dashboard requires a live backend';

/**
 * Returns:
 * - `null` when PMS_API_BASE_URL is unset outside production — caller should
 *   fall back to mocks (dev mode, no backend configured).
 * - A 503 NextResponse when PMS_API_BASE_URL is unset in production — caller
 *   should fail closed rather than serve mock trading data.
 * - A 5xx NextResponse when the backend URL is set but unreachable — caller
 *   should propagate the outage, not hide it behind mock data.
 * - The upstream Response proxied through when the backend replies.
 */
export async function upstreamResponse(pathname: string, init?: RequestInit) {
  const baseUrl = process.env.PMS_API_BASE_URL;
  if (!baseUrl) {
    if (process.env.NODE_ENV === 'production') {
      return NextResponse.json(
        { detail: MISSING_PRODUCTION_BACKEND_DETAIL },
        { status: 503 }
      );
    }
    return null;
  }
  const url = new URL(pathname, baseUrl);
  const headers = new Headers(init?.headers);
  const apiToken = process.env.PMS_API_TOKEN;
  if (apiToken) {
    headers.set('Authorization', `Bearer ${apiToken}`);
  }
  let response: Response;
  let body: string;
  try {
    response = await fetch(url, {
      cache: 'no-store',
      ...init,
      headers
    });
    body = await response.text();
  } catch (error) {
    console.warn(`PMS upstream unavailable at ${url.toString()}`, error);
    return NextResponse.json(
      { detail: 'Research backend unavailable' },
      { status: 503 }
    );
  }
  return new NextResponse(body, {
    status: response.status,
    headers: {
      'content-type': response.headers.get('content-type') ?? 'application/json'
    }
  });
}
