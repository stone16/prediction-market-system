import { NextRequest } from 'next/server';
import { afterEach, describe, expect, test, vi } from 'vitest';
import { GET } from '@/app/api/pms/markets/[id]/route';

const originalApiBaseUrl = process.env.PMS_API_BASE_URL;
const originalApiToken = process.env.PMS_API_TOKEN;

function restoreEnv() {
  if (originalApiBaseUrl === undefined) {
    delete process.env.PMS_API_BASE_URL;
  } else {
    process.env.PMS_API_BASE_URL = originalApiBaseUrl;
  }

  if (originalApiToken === undefined) {
    delete process.env.PMS_API_TOKEN;
  } else {
    process.env.PMS_API_TOKEN = originalApiToken;
  }
}

function requestFor(id: string) {
  return new NextRequest(`http://127.0.0.1:3100/api/pms/markets/${id}`);
}

function contextFor(id: string) {
  return {
    params: Promise.resolve({ id })
  };
}

afterEach(() => {
  restoreEnv();
  vi.restoreAllMocks();
});

describe('market detail route', () => {
  test('returns a mock market when PMS_API_BASE_URL is unset', async () => {
    delete process.env.PMS_API_BASE_URL;
    delete process.env.PMS_API_TOKEN;
    const fetchSpy = vi.spyOn(global, 'fetch');

    const response = await GET(requestFor('market-001'), contextFor('market-001'));

    expect(fetchSpy).not.toHaveBeenCalled();
    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      market_id: 'market-001',
      resolves_at: expect.any(String)
    });
  });

  test('returns 404 when an unconfigured dev backend has no matching mock market', async () => {
    delete process.env.PMS_API_BASE_URL;
    delete process.env.PMS_API_TOKEN;

    const response = await GET(requestFor('missing-market'), contextFor('missing-market'));

    expect(response.status).toBe(404);
    await expect(response.json()).resolves.toEqual({ detail: 'Market not found' });
  });

  test('proxies the detail request when PMS_API_BASE_URL is configured', async () => {
    process.env.PMS_API_BASE_URL = 'http://127.0.0.1:8001';
    delete process.env.PMS_API_TOKEN;
    const fetchSpy = vi.spyOn(global, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ market_id: 'market-001' }), {
        status: 200,
        headers: { 'content-type': 'application/json' }
      })
    );

    const response = await GET(requestFor('market-001'), contextFor('market-001'));

    expect(response.status).toBe(200);
    expect(fetchSpy).toHaveBeenCalledTimes(1);
    const [url, init] = fetchSpy.mock.calls[0] ?? [];
    expect(String(url)).toBe('http://127.0.0.1:8001/markets/market-001');
    expect(init).toMatchObject({ cache: 'no-store' });
  });
});
