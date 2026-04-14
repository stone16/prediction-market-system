import Link from 'next/link';

export function Nav() {
  return (
    <nav className="nav" aria-label="Dashboard navigation">
      <Link href="/" className="brand">
        <span className="brand-mark">P</span>
        PMS Console
      </Link>
      <div className="nav-links">
        <Link href="/">Overview</Link>
        <Link href="/signals">Signals</Link>
        <Link href="/decisions">Decisions</Link>
        <Link href="/metrics">Metrics</Link>
        <Link href="/backtest">Backtest</Link>
      </div>
    </nav>
  );
}
