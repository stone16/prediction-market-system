import { NextRequest } from 'next/server';
import { afterEach, describe, expect, test, vi } from 'vitest';
import { GET } from '@/app/api/pms/decisions/[id]/route';

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
  return new NextRequest(
    `http://127.0.0.1:3100/api/pms/decisions/${encodeURIComponent(id)}?include=opportunity`
  );
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

describe('decision detail route', () => {
  test('encodes decision ids before proxying upstream', async () => {
    process.env.PMS_API_BASE_URL = 'http://127.0.0.1:8001';
    delete process.env.PMS_API_TOKEN;
    const fetchSpy = vi.spyOn(global, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ decision_id: 'decision/../status' }), {
        status: 200,
        headers: { 'content-type': 'application/json' }
      })
    );

    const response = await GET(
      requestFor('decision/../status'),
      contextFor('decision/../status')
    );

    expect(response.status).toBe(200);
    expect(fetchSpy).toHaveBeenCalledTimes(1);
    const [url] = fetchSpy.mock.calls[0] ?? [];
    expect(String(url)).toBe(
      'http://127.0.0.1:8001/decisions/decision%2F..%2Fstatus?include=opportunity'
    );
  });
});
