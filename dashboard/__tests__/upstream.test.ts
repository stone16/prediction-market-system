import { afterEach, describe, expect, test, vi } from 'vitest';
import { upstreamResponse } from '@/lib/upstream';

const originalApiBaseUrl = process.env.PMS_API_BASE_URL;
const originalApiToken = process.env.PMS_API_TOKEN;

afterEach(() => {
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
});

describe('upstreamResponse', () => {
  test('injects Authorization when PMS_API_TOKEN is configured', async () => {
    process.env.PMS_API_BASE_URL = 'http://127.0.0.1:8001';
    process.env.PMS_API_TOKEN = 'testtoken';

    const fetchSpy = vi.spyOn(global, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { 'content-type': 'application/json' }
      })
    );

    await upstreamResponse('/status');

    expect(fetchSpy).toHaveBeenCalledTimes(1);
    const [, init] = fetchSpy.mock.calls[0] ?? [];
    const headers = new Headers(init?.headers);
    expect(headers.get('Authorization')).toBe('Bearer testtoken');
  });

  test('does not inject Authorization when PMS_API_TOKEN is unset', async () => {
    process.env.PMS_API_BASE_URL = 'http://127.0.0.1:8001';
    delete process.env.PMS_API_TOKEN;

    const fetchSpy = vi.spyOn(global, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { 'content-type': 'application/json' }
      })
    );

    await upstreamResponse('/status');

    expect(fetchSpy).toHaveBeenCalledTimes(1);
    const [, init] = fetchSpy.mock.calls[0] ?? [];
    const headers = new Headers(init?.headers);
    expect(headers.has('Authorization')).toBe(false);
  });
});
