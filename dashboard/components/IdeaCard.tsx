'use client';

import { AcceptIdeaButton } from '@/components/AcceptIdeaButton';
import type { ToastMessage } from '@/components/Toast';
import { WhyPopover } from '@/components/WhyPopover';
import type { Decision } from '@/lib/types';

type IdeaCardProps = {
  decision: Decision;
  onDecisionRefetched: (decision: Decision) => void;
  onToast: (toast: Omit<ToastMessage, 'id'>) => void;
};

function formatPercent(value: number) {
  return `${(value * 100).toFixed(1)}%`;
}

function formatPrice(value: number | undefined) {
  if (typeof value !== 'number') {
    return '--';
  }
  return `${(value * 100).toFixed(1)}¢`;
}

function titleFor(decision: Decision) {
  return decision.opportunity?.rationale
    ? `${decision.market_id}: ${decision.opportunity.rationale}`
    : decision.market_id;
}

export function IdeaCard({ decision, onDecisionRefetched, onToast }: IdeaCardProps) {
  const side = (decision.action ?? decision.side ?? 'BUY').toUpperCase();
  const price = decision.limit_price ?? decision.price;
  const rationale =
    decision.opportunity?.rationale ??
    `${decision.forecaster} estimates an edge of ${formatPercent(decision.expected_edge)}.`;

  return (
    <article className="idea-card" data-testid="idea-card">
      <div className="idea-card__top">
        <h3>{titleFor(decision)}</h3>
        <span className={`idea-side idea-side--${side === 'SELL' ? 'sell' : 'buy'}`}>
          {side === 'SELL' ? 'NO' : 'YES'}
        </span>
      </div>
      <div className="idea-card__meta">
        <span>Limit {formatPrice(price)}</span>
        <span>Edge {formatPercent(decision.expected_edge)}</span>
        <span>Size {(decision.kelly_size ?? decision.notional_usdc ?? 0).toFixed(2)} USDC</span>
      </div>
      <p className="idea-card__rationale">{rationale}</p>
      <div className="idea-card__actions">
        <WhyPopover decision={decision} />
        <AcceptIdeaButton
          decision={decision}
          onDecisionRefetched={onDecisionRefetched}
          onToast={onToast}
        />
      </div>
    </article>
  );
}
