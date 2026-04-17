'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import { FeedbackPanel } from './FeedbackPanel';
import { LayerCard } from './LayerCard';
import { RunControls } from './RunControls';
import { apiGet } from '@/lib/api';
import type { Feedback, MetricsResponse, StatusResponse } from '@/lib/types';

type DashboardData = {
  status: StatusResponse | null;
  metrics: MetricsResponse | null;
  feedback: Feedback[];
  disconnected: boolean;
};

const initialData: DashboardData = {
  status: null,
  metrics: null,
  feedback: [],
  disconnected: false
};

export function DashboardClient() {
  const [data, setData] = useState<DashboardData>(initialData);
  const cancelledRef = useRef(false);

  const load = useCallback(async () => {
    try {
      const [status, metrics, feedback] = await Promise.all([
        apiGet<StatusResponse>('/status'),
        apiGet<MetricsResponse>('/metrics'),
        apiGet<Feedback[]>('/feedback?resolved=false')
      ]);
      if (!cancelledRef.current) setData({ status, metrics, feedback, disconnected: false });
    } catch {
      if (!cancelledRef.current) {
        setData((current) => ({ ...current, disconnected: true }));
      }
    }
  }, []);

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
          disconnected={data.disconnected}
          label="last signal heartbeat"
          metric={status?.sensors[0]?.last_signal_at ? 'fresh' : 'none'}
          name="Sensor"
          status={sensorStatus}
        />
        <LayerCard
          disconnected={data.disconnected}
          label="decisions total"
          metric={String(status?.controller.decisions_total ?? 0)}
          name="Controller"
          status="ready"
        />
        <LayerCard
          disconnected={data.disconnected}
          label="fills total"
          metric={String(status?.actuator.fills_total ?? 0)}
          name="Actuator"
          status={status?.actuator.mode ?? 'unknown'}
        />
        <LayerCard
          disconnected={data.disconnected}
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
