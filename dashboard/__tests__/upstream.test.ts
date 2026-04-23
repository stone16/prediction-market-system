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
  test('returns null when PMS_API_BASE_URL is unset', async () => {
    delete process.env.PMS_API_BASE_URL;
    delete process.env.PMS_API_TOKEN;

    const fetchSpy = vi.spyOn(global, 'fetch');

    const response = await upstreamResponse('/status');

    expect(response).toBeNull();
    expect(fetchSpy).not.toHaveBeenCalled();
  });

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

  test('preserves caller headers while injecting Authorization', async () => {
    process.env.PMS_API_BASE_URL = 'http://127.0.0.1:8001';
    process.env.PMS_API_TOKEN = 'testtoken';

    const fetchSpy = vi.spyOn(global, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { 'content-type': 'application/json' }
      })
    );

    await upstreamResponse('/status', {
      headers: {
        'X-Trace-Id': 'trace-123'
      }
    });

    const [, init] = fetchSpy.mock.calls[0] ?? [];
    const headers = new Headers(init?.headers);
    expect(headers.get('Authorization')).toBe('Bearer testtoken');
    expect(headers.get('X-Trace-Id')).toBe('trace-123');
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

  test('returns a 503 response when the upstream fetch fails', async () => {
    process.env.PMS_API_BASE_URL = 'http://127.0.0.1:8001';
    process.env.PMS_API_TOKEN = 'testtoken';

    vi.spyOn(global, 'fetch').mockRejectedValue(new TypeError('Failed to fetch'));

    const response = await upstreamResponse('/status');

    expect(response?.status).toBe(503);
    await expect(response?.json()).resolves.toEqual({
      detail: 'Research backend unavailable'
    });
  });

  test('defaults content-type when the upstream response omits it', async () => {
    process.env.PMS_API_BASE_URL = 'http://127.0.0.1:8001';
    delete process.env.PMS_API_TOKEN;

    const fetchSpy = vi.spyOn(global, 'fetch').mockResolvedValue({
      status: 200,
      headers: {
        get: () => null
      },
      text: async () => JSON.stringify({ ok: true })
    } as unknown as Response);

    const response = await upstreamResponse('/status');

    expect(fetchSpy).toHaveBeenCalledTimes(1);
    expect(response?.headers.get('content-type')).toBe('application/json');
  });
});
