import type { Metadata } from 'next';
import { GlobalChrome } from '@/components/GlobalChrome';
import { ConnectionProvider } from '@/lib/ConnectionContext';
import { OnboardingProvider } from '@/lib/OnboardingContext';
import { SourceProvider } from '@/components/SourceProvider';
import { getDashboardSource } from '@/lib/dashboard-source';
import './globals.css';

export const metadata: Metadata = {
  title: 'PMS Today',
  description: 'Prediction market system dashboard'
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  const source = getDashboardSource();

  return (
    <html lang="en">
      <body data-dashboard-source={source}>
        <ConnectionProvider>
          <SourceProvider source={source}>
            <OnboardingProvider>
              <GlobalChrome />
              {children}
            </OnboardingProvider>
          </SourceProvider>
        </ConnectionProvider>
      </body>
    </html>
  );
}
