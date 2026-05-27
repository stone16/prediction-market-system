import { afterEach, describe, expect, test, vi } from 'vitest';
import { GET } from '@/app/api/pms/share/[id]/route';

const originalApiBaseUrl = process.env.PMS_API_BASE_URL;

function restoreEnv() {
  if (originalApiBaseUrl === undefined) {
    delete process.env.PMS_API_BASE_URL;
  } else {
    process.env.PMS_API_BASE_URL = originalApiBaseUrl;
  }
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

describe('share route', () => {
  test('encodes share ids before proxying upstream', async () => {
    process.env.PMS_API_BASE_URL = 'http://127.0.0.1:8001';
    const fetchSpy = vi.spyOn(global, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ strategy_id: 'alpha/../status' }), {
        status: 200,
        headers: { 'content-type': 'application/json' }
      })
    );

    const response = await GET(
      new Request('http://127.0.0.1:3100'),
      contextFor('alpha/../status')
    );

    expect(response.status).toBe(200);
    expect(fetchSpy).toHaveBeenCalledTimes(1);
    const [url] = fetchSpy.mock.calls[0] ?? [];
    expect(String(url)).toBe(
      'http://127.0.0.1:8001/share/alpha%2F..%2Fstatus'
    );
  });
});
