'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import { FeedbackPanel } from './FeedbackPanel';
import { LayerCard } from './LayerCard';
import { RunControls } from './RunControls';
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
  const metrics = data.metrics;
  const sensorStatus = status?.sensors[0]?.status ?? 'unknown';

  return (
    <>
      <section className="hero">
        <div>
          <p className="eyebrow">Cybernetic trading loop</p>
          <h1>Cybernetic Console</h1>
          <p className="lede">
            Live state for sensor intake, controller decisions, actuator fills, and evaluator feedback.
          </p>
        </div>
        <div className="status-strip" aria-label="run summary">
          <div>
            <span>Mode</span>
            {status?.mode ?? 'loading'}
          </div>
          <div>
            <span>Started</span>
            {status?.runner_started_at ?? 'not started'}
          </div>
          <div>
            <span>Brier</span>
            {metrics?.brier_overall?.toFixed(3) ?? 'n/a'}
          </div>
        </div>
      </section>

      <RunControls
        running={status?.running ?? false}
        mode={status?.mode ?? null}
        onChange={() => void load()}
      />

      <section className="grid-four" aria-label="layer status">
        <LayerCard
          disconnected={false}
          label="last signal heartbeat"
          metric={status?.sensors[0]?.last_signal_at ? 'fresh' : 'none'}
          name="Sensor"
          status={sensorStatus}
        />
        <LayerCard
          disconnected={false}
          label="decisions total"
          metric={String(status?.controller.decisions_total ?? 0)}
          name="Controller"
          status="ready"
        />
        <LayerCard
          disconnected={false}
          label="fills total"
          metric={String(status?.actuator.fills_total ?? 0)}
          name="Actuator"
          status={status?.actuator.mode ?? 'unknown'}
        />
        <LayerCard
          disconnected={false}
          label="eval records"
          metric={String(status?.evaluator.eval_records_total ?? 0)}
          name="Evaluator"
          status="scoring"
        />
      </section>

      <section className="section-grid">
        <div className="card summary-card">
          <h2>Loop Health</h2>
          <p className="lede">
            Pending feedback stays visible until a human resolves it. The panel updates in place after each
            action.
          </p>
        </div>
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
