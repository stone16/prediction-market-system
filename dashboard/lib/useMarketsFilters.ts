'use client';

import { usePathname, useRouter, useSearchParams } from 'next/navigation';

export type SubscribedFilter = 'all' | 'only' | 'idle';

export type MarketsFilterState = {
  q: string;
  volumeMin: string;
  liquidityMin: string;
  spreadMaxBps: string;
  yesMin: string;
  yesMax: string;
  resolvesWithinDays: string;
  subscribed: SubscribedFilter;
};

export type MarketsFilterKey = keyof MarketsFilterState;

export type MarketsFilterChip = {
  key: MarketsFilterKey;
  label: string;
};

const FILTER_PARAMS: Record<MarketsFilterKey, string> = {
  q: 'q',
  volumeMin: 'volume_min',
  liquidityMin: 'liquidity_min',
  spreadMaxBps: 'spread_max_bps',
  yesMin: 'yes_min',
  yesMax: 'yes_max',
  resolvesWithinDays: 'resolves_within_days',
  subscribed: 'subscribed'
};

const DEFAULT_FILTERS: MarketsFilterState = {
  q: '',
  volumeMin: '',
  liquidityMin: '',
  spreadMaxBps: '',
  yesMin: '',
  yesMax: '',
  resolvesWithinDays: '',
  subscribed: 'all'
};

function normalizeSubscribed(value: string | null): SubscribedFilter {
  if (value === 'only' || value === 'idle') {
    return value;
  }
  return 'all';
}

function readFilters(params: URLSearchParams): MarketsFilterState {
  return {
    q: params.get(FILTER_PARAMS.q) ?? DEFAULT_FILTERS.q,
    volumeMin: params.get(FILTER_PARAMS.volumeMin) ?? DEFAULT_FILTERS.volumeMin,
    liquidityMin: params.get(FILTER_PARAMS.liquidityMin) ?? DEFAULT_FILTERS.liquidityMin,
    spreadMaxBps: params.get(FILTER_PARAMS.spreadMaxBps) ?? DEFAULT_FILTERS.spreadMaxBps,
    yesMin: params.get(FILTER_PARAMS.yesMin) ?? DEFAULT_FILTERS.yesMin,
    yesMax: params.get(FILTER_PARAMS.yesMax) ?? DEFAULT_FILTERS.yesMax,
    resolvesWithinDays:
      params.get(FILTER_PARAMS.resolvesWithinDays) ?? DEFAULT_FILTERS.resolvesWithinDays,
    subscribed: normalizeSubscribed(params.get(FILTER_PARAMS.subscribed))
  };
}

function hasActiveValue(key: MarketsFilterKey, value: string) {
  if (key === 'subscribed') {
    return value !== 'all';
  }
  return value.trim() !== '';
}

function writeFilter(params: URLSearchParams, key: MarketsFilterKey, value: string) {
  const param = FILTER_PARAMS[key];
  const normalizedValue = key === 'subscribed' ? normalizeSubscribed(value) : value.trim();
  if (!hasActiveValue(key, normalizedValue)) {
    params.delete(param);
    return;
  }
  params.set(param, normalizedValue);
}

function buildUrl(pathname: string, params: URLSearchParams) {
  const query = params.toString();
  return `${pathname}${query ? `?${query}` : ''}`;
}

function buildMarketPath(filters: MarketsFilterState) {
  const params = new URLSearchParams({ limit: '20' });
  for (const key of Object.keys(FILTER_PARAMS) as MarketsFilterKey[]) {
    writeFilter(params, key, filters[key]);
  }
  return `/markets?${params.toString()}`;
}

function buildActiveChips(filters: MarketsFilterState): MarketsFilterChip[] {
  const chips: MarketsFilterChip[] = [];
  if (filters.q.trim() !== '') {
    chips.push({ key: 'q', label: `Search: ${filters.q}` });
  }
  if (filters.volumeMin !== '') {
    chips.push({ key: 'volumeMin', label: `Volume >= ${filters.volumeMin}` });
  }
  if (filters.liquidityMin !== '') {
    chips.push({ key: 'liquidityMin', label: `Liquidity >= ${filters.liquidityMin}` });
  }
  if (filters.spreadMaxBps !== '') {
    chips.push({ key: 'spreadMaxBps', label: `Spread <= ${filters.spreadMaxBps} bps` });
  }
  if (filters.yesMin !== '') {
    chips.push({ key: 'yesMin', label: `YES >= ${filters.yesMin}` });
  }
  if (filters.yesMax !== '') {
    chips.push({ key: 'yesMax', label: `YES <= ${filters.yesMax}` });
  }
  if (filters.resolvesWithinDays !== '') {
    chips.push({
      key: 'resolvesWithinDays',
      label: `Resolves <= ${filters.resolvesWithinDays}d`
    });
  }
  if (filters.subscribed === 'only') {
    chips.push({ key: 'subscribed', label: 'Subscribed only' });
  }
  if (filters.subscribed === 'idle') {
    chips.push({ key: 'subscribed', label: 'Idle only' });
  }
  return chips;
}

export function useMarketsFilters() {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const currentParams = new URLSearchParams(searchParams.toString());
  const filters = readFilters(currentParams);

  function setFilter(key: MarketsFilterKey, value: string) {
    const nextParams = new URLSearchParams(searchParams.toString());
    writeFilter(nextParams, key, value);
    router.replace(buildUrl(pathname, nextParams), { scroll: false });
  }

  function clearFilter(key: MarketsFilterKey) {
    const nextParams = new URLSearchParams(searchParams.toString());
    nextParams.delete(FILTER_PARAMS[key]);
    router.replace(buildUrl(pathname, nextParams), { scroll: false });
  }

  return {
    filters,
    activeChips: buildActiveChips(filters),
    marketPath: buildMarketPath(filters),
    setFilter,
    clearFilter
  };
}
