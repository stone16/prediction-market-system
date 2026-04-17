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
    let timer: number | null = null;
    let loadGeneration = 0;
    async function load() {
      const generation = ++loadGeneration;
      try {
        const result = await apiGet<T>(activePath);
        if (!cancelled && generation === loadGeneration) {
          setState({ data: result, loading: false, disconnected: false, error: null });
        }
      } catch (err) {
        if (!cancelled && generation === loadGeneration) {
          setState((prev) => ({
            ...prev,
            loading: false,
            disconnected: true,
            error: err instanceof Error ? err.message : 'Unknown error'
          }));
        }
      }
    }

    function stopPolling() {
      if (timer !== null) {
        window.clearInterval(timer);
        timer = null;
      }
    }

    function startPolling() {
      if (timer !== null) {
        return;
      }
      timer = window.setInterval(() => {
        if (document.visibilityState === 'visible') {
          void load();
        }
      }, intervalMs);
    }

    function handleVisibilityChange() {
      if (document.visibilityState === 'hidden') {
        loadGeneration += 1;
        stopPolling();
        return;
      }
      void load();
      startPolling();
    }

    void load();
    startPolling();
    document.addEventListener('visibilitychange', handleVisibilityChange);
    return () => {
      cancelled = true;
      loadGeneration += 1;
      document.removeEventListener('visibilitychange', handleVisibilityChange);
      stopPolling();
    };
  }, [path, intervalMs]);

  return state;
}
