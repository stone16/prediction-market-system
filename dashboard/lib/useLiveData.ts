'use client';

import { useEffect, useState } from 'react';
import { apiGet } from './api';

export type LiveData<T> = {
  data: T | null;
  loading: boolean;
  disconnected: boolean;
  error: string | null;
};

export function useLiveData<T>(path: string | null, intervalMs = 5000): LiveData<T> {
  const [state, setState] = useState<LiveData<T>>({
    data: null,
    loading: true,
    disconnected: false,
    error: null
  });

  useEffect(() => {
    if (path === null) {
      setState({
        data: null,
        loading: false,
        disconnected: false,
        error: null
      });
      return undefined;
    }

    const activePath = path;
    let cancelled = false;
    async function load() {
      try {
        const result = await apiGet<T>(activePath);
        if (!cancelled) {
          setState({ data: result, loading: false, disconnected: false, error: null });
        }
      } catch (err) {
        if (!cancelled) {
          setState((prev) => ({
            ...prev,
            loading: false,
            disconnected: true,
            error: err instanceof Error ? err.message : 'Unknown error'
          }));
        }
      }
    }
    void load();
    const timer = window.setInterval(load, intervalMs);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [path, intervalMs]);

  return state;
}
