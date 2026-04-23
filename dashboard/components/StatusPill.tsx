import type { ReactNode } from 'react';

type StatusPillProps = {
  label: string;
  variant?: 'live' | 'muted' | 'error';
  icon?: ReactNode;
  onClick?: () => void;
};

export function StatusPill({
  label,
  variant = 'muted',
  icon,
  onClick
}: StatusPillProps) {
  const className = [
    'status-pill',
    `status-pill--${variant}`,
    onClick ? 'status-pill--clickable' : ''
  ]
    .filter(Boolean)
    .join(' ');

  if (onClick) {
    return (
      <button className={className} onClick={onClick} type="button">
        {icon ? <span className="status-pill__icon">{icon}</span> : null}
        <span className="status-pill__label">{label}</span>
      </button>
    );
  }

  return (
    <span className={className} role="status">
      {icon ? <span className="status-pill__icon">{icon}</span> : null}
      <span className="status-pill__label">{label}</span>
    </span>
  );
}
