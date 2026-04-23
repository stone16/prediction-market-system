import Link from 'next/link';
import type { ReactNode } from 'react';

type EmptyStateProps = {
  title: string;
  body: string;
  cta?: {
    href: string;
    label: string;
  };
  icon?: ReactNode;
};

export function EmptyState({ title, body, cta, icon }: EmptyStateProps) {
  return (
    <section className="foundation-empty-state" role="status">
      {icon ? <div className="foundation-empty-state__icon">{icon}</div> : null}
      <div className="foundation-empty-state__title">{title}</div>
      <p className="foundation-empty-state__body">{body}</p>
      {cta ? (
        <Link className="foundation-empty-state__cta" href={cta.href}>
          {cta.label}
        </Link>
      ) : null}
    </section>
  );
}
