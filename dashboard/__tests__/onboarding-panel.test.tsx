import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, test, vi } from 'vitest';
import { Nav } from '@/components/Nav';
import { OnboardingPanel } from '@/components/OnboardingPanel';
import { OnboardingProvider } from '@/lib/OnboardingContext';

vi.mock('next/link', () => ({
  default: ({
    children,
    href,
    ...props
  }: {
    children: React.ReactNode;
    href: string;
  }) => (
    <a href={href} {...props}>
      {children}
    </a>
  )
}));

vi.mock('next/navigation', () => ({
  usePathname: () => '/'
}));

function renderOnboardingShell() {
  return render(
    <OnboardingProvider>
      <Nav />
      <OnboardingPanel />
    </OnboardingProvider>
  );
}

describe('OnboardingPanel', () => {
  beforeEach(() => {
    window.localStorage.clear();
  });

  test('renders on first mount and includes the activate default strategy step', async () => {
    renderOnboardingShell();

    await waitFor(() => {
      expect(screen.getByRole('dialog', { name: 'Quick start' })).toBeInTheDocument();
    });

    expect(screen.getByText('Activate default strategy')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Open strategies' })).toHaveAttribute(
      'href',
      '/strategies'
    );
  });

  test('stays hidden when the onboarding key is already set', async () => {
    window.localStorage.setItem('pms.onboarded', 'true');

    renderOnboardingShell();

    await waitFor(() => {
      expect(screen.queryByRole('dialog', { name: 'Quick start' })).not.toBeInTheDocument();
    });
  });

  test('dismiss button persists the onboarding key', async () => {
    renderOnboardingShell();

    const closeButton = await screen.findByRole('button', { name: 'Dismiss onboarding' });
    fireEvent.click(closeButton);

    await waitFor(() => {
      expect(window.localStorage.getItem('pms.onboarded')).toBe('true');
      expect(screen.queryByRole('dialog', { name: 'Quick start' })).not.toBeInTheDocument();
    });
  });

  test('redo tour icon re-opens the panel after dismissal', async () => {
    renderOnboardingShell();

    fireEvent.click(await screen.findByRole('button', { name: 'Dismiss onboarding' }));

    await waitFor(() => {
      expect(screen.queryByRole('dialog', { name: 'Quick start' })).not.toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole('button', { name: 'Redo tour' }));

    await waitFor(() => {
      expect(screen.getByRole('dialog', { name: 'Quick start' })).toBeInTheDocument();
    });
  });
});
