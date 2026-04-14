import { DashboardClient } from '@/components/DashboardClient';
import { Nav } from '@/components/Nav';

export default function HomePage() {
  return (
    <main className="shell">
      <Nav />
      <div className="page">
        <DashboardClient />
      </div>
    </main>
  );
}
