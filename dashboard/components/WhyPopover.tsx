'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
import type { Decision } from '@/lib/types';

type OpenMode = 'dialog' | 'tooltip';

function focusableElements(root: HTMLElement) {
  return Array.from(
    root.querySelectorAll<HTMLElement>('button:not([disabled]), a[href], [tabindex]:not([tabindex="-1"])')
  ).filter((element) => !element.hasAttribute('disabled'));
}

function factorEntries(decision: Decision) {
  const values = decision.opportunity?.selected_factor_values ?? {
    edge: decision.expected_edge
  };
  return Object.entries(values).sort(([, left], [, right]) => Math.abs(right) - Math.abs(left));
}

function factorWidth(value: number, max: number) {
  if (max <= 0) {
    return '4%';
  }
  return `${Math.max(8, Math.min(100, (Math.abs(value) / max) * 100)).toFixed(1)}%`;
}

export function WhyPopover({ decision }: { decision: Decision }) {
  const [open, setOpen] = useState(false);
  const [mode, setMode] = useState<OpenMode>('dialog');
  const panelRef = useRef<HTMLDivElement | null>(null);
  const triggerRef = useRef<HTMLButtonElement | null>(null);
  const closeRef = useRef<HTMLButtonElement | null>(null);
  const hoverTimerRef = useRef<number | null>(null);
  const lastFocusedRef = useRef<HTMLElement | null>(null);
  const factors = useMemo(() => factorEntries(decision), [decision]);
  const maxFactor = Math.max(...factors.map(([, value]) => Math.abs(value)), 0);

  function close() {
    setOpen(false);
    lastFocusedRef.current?.focus();
  }

  function openAs(nextMode: OpenMode) {
    lastFocusedRef.current =
      triggerRef.current ??
      (document.activeElement instanceof HTMLElement ? document.activeElement : null);
    setMode(nextMode);
    setOpen(true);
  }

  useEffect(() => {
    if (!open) {
      return undefined;
    }

    if (mode === 'dialog') {
      window.setTimeout(() => closeRef.current?.focus(), 0);
    }

    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === 'Escape') {
        event.preventDefault();
        close();
        return;
      }
      if (event.key !== 'Tab' || mode !== 'dialog' || panelRef.current === null) {
        return;
      }

      const focusable = focusableElements(panelRef.current);
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

    function handlePointerDown(event: PointerEvent) {
      const target = event.target;
      if (!(target instanceof Node)) {
        return;
      }
      if (panelRef.current?.contains(target) || triggerRef.current?.contains(target)) {
        return;
      }
      close();
    }

    document.addEventListener('keydown', handleKeyDown);
    document.addEventListener('pointerdown', handlePointerDown);
    return () => {
      document.removeEventListener('keydown', handleKeyDown);
      document.removeEventListener('pointerdown', handlePointerDown);
    };
  }, [mode, open]);

  return (
    <span className="why-popover-shell">
      <button
        className="why-trigger"
        onClick={() => openAs('dialog')}
        onMouseEnter={() => {
          hoverTimerRef.current = window.setTimeout(() => openAs('tooltip'), 400);
        }}
        onMouseLeave={() => {
          if (hoverTimerRef.current !== null) {
            window.clearTimeout(hoverTimerRef.current);
            hoverTimerRef.current = null;
          }
          if (mode === 'tooltip') {
            setOpen(false);
          }
        }}
        ref={triggerRef}
        type="button"
      >
        Why
      </button>
      {open ? (
        <div
          aria-label="Why this idea?"
          aria-modal={mode === 'dialog' ? 'true' : undefined}
          className="why-popover"
          ref={panelRef}
          role={mode === 'dialog' ? 'dialog' : 'tooltip'}
        >
          <div className="why-popover__header">
            <div>
              <p className="eyebrow">Reason</p>
              <h3>Why this idea?</h3>
            </div>
            <button
              aria-label="Close why panel"
              className="why-popover__close"
              onClick={close}
              ref={closeRef}
              type="button"
            >
              ×
            </button>
          </div>
          <div className="why-factor-list">
            {factors.map(([name, value]) => (
              <div className="why-factor" key={name}>
                <div className="why-factor__label">
                  <span>{name}</span>
                  <strong>{value.toFixed(3)}</strong>
                </div>
                <div className="why-factor__track">
                  <span style={{ width: factorWidth(value, maxFactor) }} />
                </div>
              </div>
            ))}
          </div>
          <p className="why-rationale">
            {decision.opportunity?.rationale ?? 'No rationale was attached to this idea.'}
          </p>
          <button className="why-reasoning-toggle" type="button">
            Show reasoning
          </button>
        </div>
      ) : null}
    </span>
  );
}
