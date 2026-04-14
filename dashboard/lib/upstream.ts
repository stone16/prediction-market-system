import { NextResponse } from 'next/server';

export async function upstreamResponse(pathname: string, init?: RequestInit) {
  const baseUrl = process.env.PMS_API_BASE_URL;
  if (!baseUrl) return null;
  const url = new URL(pathname, baseUrl);
  let response: Response;
  try {
    response = await fetch(url, {
      cache: 'no-store',
      ...init
    });
  } catch (error) {
    console.warn(`PMS upstream unavailable at ${url.toString()}`, error);
    return null;
  }
  const body = await response.text();
  return new NextResponse(body, {
    status: response.status,
    headers: {
      'content-type': response.headers.get('content-type') ?? 'application/json'
    }
  });
}
