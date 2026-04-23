'use client';

import { usePathname } from 'next/navigation';
import { ConnectionBanner } from '@/components/ConnectionBanner';
import { EventLogDrawer } from '@/components/EventLogDrawer';
import { OnboardingPanel } from '@/components/OnboardingPanel';
import { SourceBadgePortals } from '@/components/SourceBadgePortals';
import { SourceBanner } from '@/components/SourceBanner';

function isPublicShareRoute(pathname: string) {
  return pathname.startsWith('/share/');
}

export function GlobalChrome() {
  const pathname = usePathname();

  if (isPublicShareRoute(pathname)) {
    return null;
  }

  return (
    <>
      <SourceBanner />
      <ConnectionBanner />
      <SourceBadgePortals />
      <OnboardingPanel />
      <EventLogDrawer />
    </>
  );
}
