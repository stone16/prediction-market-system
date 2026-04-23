import type { Metadata } from 'next';
import { ConnectionBanner } from '@/components/ConnectionBanner';
import { SourceBadgePortals } from '@/components/SourceBadgePortals';
import { SourceBanner } from '@/components/SourceBanner';
import { ConnectionProvider } from '@/lib/ConnectionContext';
import { SourceProvider } from '@/components/SourceProvider';
import { getDashboardSource } from '@/lib/dashboard-source';
import './globals.css';

export const metadata: Metadata = {
  title: 'PMS Cybernetic Console',
  description: 'Prediction market system dashboard'
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  const source = getDashboardSource();

  return (
    <html lang="en">
      <body data-dashboard-source={source}>
        <ConnectionProvider>
          <SourceProvider source={source}>
            <SourceBanner />
            <ConnectionBanner />
            <SourceBadgePortals />
            {children}
          </SourceProvider>
        </ConnectionProvider>
      </body>
    </html>
  );
}
