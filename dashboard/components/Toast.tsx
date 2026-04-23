'use client';

import Link from 'next/link';

export type ToastMessage = {
  id: string;
  tone: 'success' | 'error';
  message: string;
  href?: string;
};

type ToastStackProps = {
  items: ToastMessage[];
  onDismiss: (id: string) => void;
};

export function ToastStack({ items, onDismiss }: ToastStackProps) {
  if (items.length === 0) {
    return null;
  }

  return (
    <div aria-live="polite" className="toast-stack">
      {items.map((toast) => (
        <div className={`toast toast--${toast.tone}`} key={toast.id} role="status">
          {toast.href ? (
            <Link className="toast__link" href={toast.href}>
              {toast.message} · View in /trades
            </Link>
          ) : (
            <span>{toast.message}</span>
          )}
          <button
            aria-label="Dismiss notification"
            className="toast__dismiss"
            onClick={() => onDismiss(toast.id)}
            type="button"
          >
            ×
          </button>
        </div>
      ))}
    </div>
  );
}
