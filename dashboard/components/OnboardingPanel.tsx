'use client';

import Link from 'next/link';
import { useEffect, useRef } from 'react';
import { useOnboarding } from '@/lib/OnboardingContext';
import { useLiveData } from '@/lib/useLiveData';
import type { StatusResponse } from '@/lib/types';

function focusableElements(root: HTMLElement) {
  return Array.from(
    root.querySelectorAll<HTMLElement>('a[href], button:not([disabled]), [tabindex]:not([tabindex="-1"])')
  ).filter((element) => !element.hasAttribute('disabled'));
}

export function OnboardingPanel() {
  const { dismissOnboarding, open, ready } = useOnboarding();
  const closeButtonRef = useRef<HTMLButtonElement | null>(null);
  const dialogRef = useRef<HTMLElement | null>(null);
  const lastFocusedRef = useRef<HTMLElement | null>(null);
  const statusState = useLiveData<StatusResponse>('/status');
  const runnerLabel = statusState.data?.running ? 'running' : 'paused';

  useEffect(() => {
    if (!ready || !open) {
      return undefined;
    }

    lastFocusedRef.current =
      document.activeElement instanceof HTMLElement ? document.activeElement : null;
    closeButtonRef.current?.focus();

    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === 'Escape') {
        event.preventDefault();
        dismissOnboarding();
        return;
      }

      if (event.key !== 'Tab' || dialogRef.current === null) {
        return;
      }

      const focusable = focusableElements(dialogRef.current);
      if (focusable.length === 0) {
        return;
      }

      const first = focusable[0];
      const last = focusable[focusable.length - 1];

      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    }

    document.addEventListener('keydown', handleKeyDown);
    return () => {
      document.removeEventListener('keydown', handleKeyDown);
      lastFocusedRef.current?.focus();
    };
  }, [dismissOnboarding, open, ready]);

  if (!ready || !open) {
    return null;
  }

  return (
    <div className="onboarding-backdrop">
      <section
        aria-labelledby="onboarding-title"
        aria-modal="true"
        className="onboarding-panel"
        ref={dialogRef}
        role="dialog"
      >
        <div className="onboarding-panel__header">
          <div>
            <p className="eyebrow">First loop</p>
            <h2 id="onboarding-title">Quick start</h2>
          </div>
          <button
            aria-label="Dismiss onboarding"
            className="onboarding-panel__close"
            onClick={dismissOnboarding}
            ref={closeButtonRef}
            type="button"
          >
            ×
          </button>
        </div>
        <p className="lede">
          Clear the three blockers that keep first-time users from ever seeing positions and trades.
        </p>

        <ol className="onboarding-steps">
          <li className="onboarding-step">
            <div className="onboarding-step__meta">
              <span className={runnerLabel === 'running' ? 'badge ok' : 'badge muted-badge'}>
                runner {runnerLabel}
              </span>
            </div>
            <div className="onboarding-step__body">
              <strong>Runner running?</strong>
              <p className="muted">
                Use the home-page controls to start paper execution before expecting fills to appear.
              </p>
              <Link className="run-link" href="/" onClick={dismissOnboarding}>
                Return home
              </Link>
            </div>
          </li>
          <li className="onboarding-step">
            <div className="onboarding-step__body">
              <strong>Activate default strategy</strong>
              <p className="muted">
                Without an active strategy, the controller stays idle and the trades/positions pages stay empty.
              </p>
              <Link className="run-link" href="/strategies" onClick={dismissOnboarding}>
                Open strategies
              </Link>
            </div>
          </li>
          <li className="onboarding-step">
            <div className="onboarding-step__body">
              <strong>Browse markets</strong>
              <p className="muted">
                Inspect the candidate set, confirm subscriptions, and jump from a market into the live depth view.
              </p>
              <Link className="run-link" href="/markets" onClick={dismissOnboarding}>
                Browse markets
              </Link>
            </div>
          </li>
        </ol>
      </section>
    </div>
  );
}
