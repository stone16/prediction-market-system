'use client';

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState
} from 'react';
import { usePathname } from 'next/navigation';

const ONBOARDED_STORAGE_KEY = 'pms.onboarded';

type OnboardingContextValue = {
  dismissOnboarding: () => void;
  open: boolean;
  openOnboarding: () => void;
  ready: boolean;
};

const OnboardingContext = createContext<OnboardingContextValue | null>(null);

export function OnboardingProvider({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const [open, setOpen] = useState(false);
  const [ready, setReady] = useState(false);

  useEffect(() => {
    const onboarded = window.localStorage.getItem(ONBOARDED_STORAGE_KEY) === 'true';
    setOpen(pathname === '/' && !onboarded);
    setReady(true);
  }, [pathname]);

  const dismissOnboarding = useCallback(() => {
    window.localStorage.setItem(ONBOARDED_STORAGE_KEY, 'true');
    setOpen(false);
  }, []);

  const openOnboarding = useCallback(() => {
    setOpen(true);
  }, []);

  const value = useMemo<OnboardingContextValue>(
    () => ({
      dismissOnboarding,
      open,
      openOnboarding,
      ready
    }),
    [dismissOnboarding, open, openOnboarding, ready]
  );

  return <OnboardingContext.Provider value={value}>{children}</OnboardingContext.Provider>;
}

export function useOnboarding() {
  const context = useContext(OnboardingContext);
  if (context === null) {
    throw new Error('useOnboarding must be used within an OnboardingProvider');
  }
  return context;
}
