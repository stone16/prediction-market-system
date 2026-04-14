'use client';

import { apiPost } from '@/lib/api';
import type { Feedback } from '@/lib/types';

type FeedbackPanelProps = {
  items: Feedback[];
  onResolved: (feedbackId: string) => void;
};

export function FeedbackPanel({ items, onResolved }: FeedbackPanelProps) {
  return (
    <section className="feedback-panel">
      <h2>Feedback Queue</h2>
      <p className="muted">Human review items waiting for resolution.</p>
      <div className="feedback-list">
        {items.length === 0 ? <p className="muted">No pending feedback.</p> : null}
        {items.map((item) => (
          <article className="feedback-item" data-testid="feedback-item" key={item.feedback_id}>
            <strong>{item.category ?? item.source}</strong>
            <p>{item.message}</p>
            <button
              className="resolve-button"
              onClick={async () => {
                await apiPost<Feedback>(`/feedback/${item.feedback_id}/resolve`);
                onResolved(item.feedback_id);
              }}
              type="button"
            >
              Mark Resolved
            </button>
          </article>
        ))}
      </div>
    </section>
  );
}
