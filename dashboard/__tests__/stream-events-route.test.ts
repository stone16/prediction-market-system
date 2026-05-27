import { NextRequest } from 'next/server';
import { afterEach, describe, expect, test, vi } from 'vitest';
import { GET } from '@/app/api/pms/stream/events/route';

const originalApiBaseUrl = process.env.PMS_API_BASE_URL;
const originalApiToken = process.env.PMS_API_TOKEN;
const originalNodeEnv = process.env.NODE_ENV;

function setNodeEnv(value: string | undefined) {
  const env = process.env as Record<string, string | undefined>;
  if (value === undefined) {
    delete env.NODE_ENV;
  } else {
    env.NODE_ENV = value;
  }
}

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

  setNodeEnv(originalNodeEnv);
}

function requestForStream() {
  return new NextRequest('http://127.0.0.1:3100/api/pms/stream/events');
}

afterEach(() => {
  restoreEnv();
  vi.restoreAllMocks();
});

describe('stream events route', () => {
  test('fails closed instead of serving a mock stream in production', async () => {
    setNodeEnv('production');
    delete process.env.PMS_API_BASE_URL;
    delete process.env.PMS_API_TOKEN;
    const fetchSpy = vi.spyOn(global, 'fetch');

    const response = await GET(requestForStream());

    expect(fetchSpy).not.toHaveBeenCalled();
    expect(response.status).toBe(503);
    await expect(response.json()).resolves.toEqual({
      detail: 'PMS_API_BASE_URL is not configured; production dashboard requires a live backend'
    });
  });
});
