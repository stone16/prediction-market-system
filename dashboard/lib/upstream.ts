import { NextResponse } from 'next/server';

export async function upstreamResponse(pathname: string, init?: RequestInit) {
  const baseUrl = process.env.PMS_API_BASE_URL;
  if (!baseUrl) return null;
  const url = new URL(pathname, baseUrl);
  let response: Response;
  let body: string;
  try {
    response = await fetch(url, {
      cache: 'no-store',
      ...init
    });
    body = await response.text();
  } catch (error) {
    console.warn(`PMS upstream unavailable at ${url.toString()}`, error);
    return null;
  }
  return new NextResponse(body, {
    status: response.status,
    headers: {
      'content-type': response.headers.get('content-type') ?? 'application/json'
    }
  });
}
