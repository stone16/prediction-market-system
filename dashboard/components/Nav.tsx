import Link from 'next/link';

const navItems = [
  { href: '/markets', label: 'Markets' },
  { href: '/overview', label: 'Watchlist' },
  { href: '/decisions', label: 'Ideas' },
  { href: '/backtest', label: 'Trades' },
  { href: '/strategies', label: 'Positions' },
  { href: '/metrics', label: 'Performance' },
  { href: '/strategies', label: 'Strategies' },
  { href: '/backtest', label: 'Backtest' }
] as const;

export function Nav() {
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
      </div>
    </nav>
  );
}
