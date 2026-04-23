'use client';

import { useEffect, useRef, useState } from 'react';
import { EmptyState } from '@/components/EmptyState';
import { IdeaCard } from '@/components/IdeaCard';
import { Nav } from '@/components/Nav';
import { ToastStack, type ToastMessage } from '@/components/Toast';
import { useLiveData } from '@/lib/useLiveData';
import type { Decision } from '@/lib/types';

export function IdeasPageClient() {
  const { data, loading, disconnected } = useLiveData<Decision[]>(
    '/decisions?status=pending&include=opportunity&limit=50',
    5000
  );
  const [ideas, setIdeas] = useState<Decision[]>([]);
  const [toasts, setToasts] = useState<ToastMessage[]>([]);
  const toastCounterRef = useRef(0);

  useEffect(() => {
    if (data !== null) {
      setIdeas(data);
    }
  }, [data]);

  function updateIdea(refetched: Decision) {
    setIdeas((current) =>
      current.map((item) =>
        item.decision_id === refetched.decision_id ? { ...item, ...refetched } : item
      )
    );
  }

  function pushToast(toast: Omit<ToastMessage, 'id'>) {
    const id = `toast-${toastCounterRef.current++}`;
    setToasts((current) => [...current.slice(-2), { id, ...toast }]);
  }

  return (
    <main className="shell">
      <Nav />
      <section className="page ideas-page">
        <header className="hero ideas-hero">
          <div>
            <p className="eyebrow">Ideas</p>
            <h1>Ideas</h1>
            <p className="lede">
              Pending trade ideas with factor context, stale-market protection, and one-click paper
              execution.
            </p>
          </div>
          <div className="ideas-hero__panel">
            <span className="badge info">{ideas.length} pending</span>
            {disconnected ? <span className="badge disconnected">disconnected</span> : null}
          </div>
        </header>

        {loading && ideas.length === 0 ? (
          <p className="muted">Loading ideas…</p>
        ) : disconnected && ideas.length === 0 ? (
          <EmptyState
            title="Ideas unavailable."
            body="The backend connection dropped before pending ideas could load."
            cta={{ href: '/markets', label: 'Browse markets' }}
          />
        ) : ideas.length === 0 ? (
          <EmptyState
            title="No ideas waiting."
            body="Start the runner or browse markets to generate the next candidate set."
            cta={{ href: '/markets', label: 'Browse markets' }}
          />
        ) : (
          <div className="ideas-grid">
            {ideas.map((idea) => (
              <IdeaCard
                decision={idea}
                key={idea.decision_id}
                onDecisionRefetched={updateIdea}
                onToast={pushToast}
              />
            ))}
          </div>
        )}
      </section>
      <ToastStack
        items={toasts}
        onDismiss={(id) => setToasts((current) => current.filter((toast) => toast.id !== id))}
      />
    </main>
  );
}
