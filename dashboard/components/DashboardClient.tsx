'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import { FeedbackPanel } from './FeedbackPanel';
import { RunControls } from './RunControls';
import { Today } from './Today';
import { useConnection } from '@/lib/ConnectionContext';
import { apiGet } from '@/lib/api';
import type { Feedback, MetricsResponse, StatusResponse } from '@/lib/types';

type DashboardData = {
  status: StatusResponse | null;
  metrics: MetricsResponse | null;
  feedback: Feedback[];
};

const initialData: DashboardData = {
  status: null,
  metrics: null,
  feedback: []
};

type ApiGetter = typeof apiGet;

export async function loadDashboardData(get: ApiGetter = apiGet) {
  const [status, metrics, feedback] = await Promise.all([
    get<StatusResponse>('/status'),
    get<MetricsResponse>('/metrics'),
    get<Feedback[]>('/feedback?resolved=false')
  ]);

  return { feedback, metrics, status };
}

function isRecoverableDashboardLoadError(error: unknown) {
  if (error instanceof Error && error.name === 'AbortError') {
    return true;
  }

  return (
    error instanceof TypeError &&
    (error.message === 'Failed to fetch' ||
      error.message === 'NetworkError when attempting to fetch resource')
  );
}

export function DashboardClient() {
  const [data, setData] = useState<DashboardData>(initialData);
  const cancelledRef = useRef(false);
  const loadGenerationRef = useRef(0);
  const { markConnected, markDisconnected } = useConnection();

  const load = useCallback(async () => {
    const generation = ++loadGenerationRef.current;

    try {
      const nextData = await loadDashboardData();
      if (!cancelledRef.current && generation === loadGenerationRef.current) {
        setData(nextData);
        markConnected();
      }

      return;
    } catch (error) {
      if (!isRecoverableDashboardLoadError(error)) {
        throw error;
      }

      if (!cancelledRef.current && generation === loadGenerationRef.current) {
        markDisconnected();
      }
    }
  }, [markConnected, markDisconnected]);

  useEffect(() => {
    cancelledRef.current = false;
    let timer: number | null = null;

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
      }, 5000);
    }

    function handleVisibilityChange() {
      if (document.visibilityState === 'hidden') {
        loadGenerationRef.current += 1;
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
      cancelledRef.current = true;
      loadGenerationRef.current += 1;
      document.removeEventListener('visibilitychange', handleVisibilityChange);
      stopPolling();
    };
  }, [load]);

  const status = data.status;

  return (
    <>
      <Today feedback={data.feedback} metrics={data.metrics} status={status} />

      <section className="section-grid">
        <RunControls
          running={status?.running ?? false}
          mode={status?.mode ?? null}
          onChange={() => void load()}
        />
        <FeedbackPanel
          items={data.feedback}
          onResolved={(feedbackId) =>
            setData((current) => ({
              ...current,
              feedback: current.feedback.filter((item) => item.feedback_id !== feedbackId)
            }))
          }
        />
      </section>
    </>
  );
}
