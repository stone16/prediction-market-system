import 'server-only';

import type { ShareProjection } from '@/lib/types';

const DEFAULT_REVALIDATE_SECONDS = 60;
const debugReadCounts = new Map<string, number>();
const shareCache = new Map<
  string,
  {
    expiresAt: number;
    projection: ShareProjection;
    debugReadCount: number;
  }
>();

export class ShareProjectionNotFoundError extends Error {}

function shareRevalidateSeconds() {
  const raw = Number(process.env.PMS_SHARE_REVALIDATE_SECONDS ?? DEFAULT_REVALIDATE_SECONDS);
  return Number.isFinite(raw) && raw > 0 ? Math.floor(raw) : DEFAULT_REVALIDATE_SECONDS;
}

async function fetchShareProjection(strategyId: string): Promise<ShareProjection> {
  const baseUrl = process.env.PMS_API_BASE_URL;
  if (!baseUrl) {
    throw new Error('PMS_API_BASE_URL is not configured');
  }

  const response = await fetch(new URL(`/share/${strategyId}`, baseUrl), {
    cache: 'no-store'
  });

  if (response.status === 404) {
    throw new ShareProjectionNotFoundError();
  }
  if (!response.ok) {
    throw new Error(`Share projection upstream failed with status ${response.status}`);
  }

  const nextCount = (debugReadCounts.get(strategyId) ?? 0) + 1;
  debugReadCounts.set(strategyId, nextCount);

  return (await response.json()) as ShareProjection;
}

export async function getSharePageData(strategyId: string) {
  const now = Date.now();
  const cached = shareCache.get(strategyId);
  if (cached && cached.expiresAt > now) {
    return {
      projection: cached.projection,
      debugReadCount: cached.debugReadCount
    };
  }

  const projection = await fetchShareProjection(strategyId);
  const payload = {
    projection,
    debugReadCount: debugReadCounts.get(strategyId) ?? 0
  };

  shareCache.set(strategyId, {
    expiresAt: now + shareRevalidateSeconds() * 1000,
    projection,
    debugReadCount: payload.debugReadCount
  });

  return payload;
}
