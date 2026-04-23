'use client';

import { useEffect, useRef, useState } from 'react';
import type { ToastMessage } from '@/components/Toast';
import type { Decision } from '@/lib/types';

type AcceptIdeaButtonProps = {
  decision: Decision;
  onDecisionRefetched?: (decision: Decision) => void;
  onToast: (toast: Omit<ToastMessage, 'id'>) => void;
};

type ButtonPhase = 'idle' | 'pending' | 'success';

async function readJson(response: Response): Promise<unknown> {
  try {
    return await response.json();
  } catch {
    return null;
  }
}

function riskErrorMessage(payload: unknown): string {
  if (payload && typeof payload === 'object' && 'detail' in payload) {
    return `Risk blocked: ${String(payload.detail)}`;
  }
  return 'Risk blocked this idea.';
}

export function AcceptIdeaButton({
  decision,
  onDecisionRefetched,
  onToast
}: AcceptIdeaButtonProps) {
  const [phase, setPhase] = useState<ButtonPhase>('idle');
  const [coolingDown, setCoolingDown] = useState(false);
  const inflightRef = useRef(false);
  const cooldownRef = useRef<number | null>(null);
  const successRef = useRef<number | null>(null);
  const disabled = phase === 'pending' || coolingDown || decision.status === 'accepted';

  useEffect(() => {
    return () => {
      if (cooldownRef.current !== null) {
        window.clearTimeout(cooldownRef.current);
      }
      if (successRef.current !== null) {
        window.clearTimeout(successRef.current);
      }
    };
  }, []);

  async function refetchDecision() {
    const refetch = await fetch(`/api/pms/decisions/${decision.decision_id}?include=opportunity`);
    if (!refetch.ok) {
      return;
    }
    const payload = await readJson(refetch);
    if (payload !== null) {
      onDecisionRefetched?.(payload as Decision);
    }
  }

  async function handleAccept() {
    if (inflightRef.current || disabled) {
      return;
    }

    inflightRef.current = true;
    setPhase('pending');
    try {
      const response = await fetch(`/api/pms/decisions/${decision.decision_id}/accept`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          factor_snapshot_hash: decision.factor_snapshot_hash ?? ''
        })
      });

      if (response.ok) {
        setPhase('success');
        onToast({
          tone: 'success',
          message: 'First trade placed',
          href: '/trades'
        });
        successRef.current = window.setTimeout(() => {
          setPhase('idle');
        }, 150);
        return;
      }

      if (response.status === 409) {
        setCoolingDown(true);
        onToast({
          tone: 'error',
          message: 'Market changed... refresh loaded'
        });
        cooldownRef.current = window.setTimeout(() => {
          setCoolingDown(false);
        }, 500);
        void refetchDecision();
        return;
      }

      onToast({
        tone: 'error',
        message:
          response.status === 422
            ? riskErrorMessage(await readJson(response))
            : 'Unable to accept this idea.'
      });
    } finally {
      inflightRef.current = false;
      setPhase((current) => (current === 'pending' ? 'idle' : current));
    }
  }

  const label =
    phase === 'pending' ? 'Accepting...' : phase === 'success' ? 'Accepted' : 'Accept';

  return (
    <button
      aria-busy={phase === 'pending' ? 'true' : undefined}
      className={`accept-idea-button accept-idea-button--${phase}`}
      disabled={disabled}
      onClick={handleAccept}
      type="button"
    >
      {phase === 'pending' ? <span aria-hidden="true" className="accept-spinner" /> : null}
      {label}
    </button>
  );
}
