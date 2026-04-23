'use client';

import Link from 'next/link';
import { useOnboarding } from '@/lib/OnboardingContext';

const navItems = [
  { href: '/markets', label: 'Markets' },
  { href: '/overview', label: 'Watchlist' },
  { href: '/ideas', label: 'Ideas' },
  { href: '/trades', label: 'Trades' },
  { href: '/positions', label: 'Positions' },
  { href: '/metrics', label: 'Performance' },
  { href: '/strategies', label: 'Strategies' },
  { href: '/backtest', label: 'Backtest' }
] as const;

export function Nav() {
  const { openOnboarding } = useOnboarding();

  return (
    <nav className="nav" aria-label="Dashboard navigation">
      <Link href="/" className="brand">
        <span className="brand-mark">P</span>
        PMS Console
      </Link>
      <div className="nav-links">
        {navItems.map((item) => (
          <Link href={item.href} key={item.label}>
            {item.label}
          </Link>
        ))}
        <button
          aria-label="Redo tour"
          className="nav-tour-button"
          onClick={openOnboarding}
          type="button"
        >
          ?
        </button>
      </div>
    </nav>
  );
}
