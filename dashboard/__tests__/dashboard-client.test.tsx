import { render, screen, waitFor } from '@testing-library/react';
import { describe, expect, test, vi } from 'vitest';
import { ConnectionBanner } from '@/components/ConnectionBanner';
import { DashboardClient, loadDashboardData } from '@/components/DashboardClient';
import { ConnectionProvider } from '@/lib/ConnectionContext';

function renderDashboardWithConnection() {
  return render(
    <ConnectionProvider>
      <ConnectionBanner />
      <DashboardClient />
    </ConnectionProvider>
  );
}

describe('DashboardClient error handling', () => {
  test('loadDashboardData rethrows non-network TypeError values', async () => {
    const failingGet = vi.fn(async () => {
      throw new TypeError('forced for test');
    });

    await expect(loadDashboardData(failingGet)).rejects.toThrow('forced for test');
  });

  test('network-style TypeError marks the connection disconnected', async () => {
    vi.spyOn(global, 'fetch').mockRejectedValue(new TypeError('Failed to fetch'));

    renderDashboardWithConnection();

    await waitFor(() => {
      expect(screen.getByText('Backend disconnected')).toBeInTheDocument();
      expect(screen.getByText('offline')).toBeInTheDocument();
    });
  });

  test('safari network TypeError also marks the connection disconnected', async () => {
    vi
      .spyOn(global, 'fetch')
      .mockRejectedValue(new TypeError('NetworkError when attempting to fetch resource'));

    renderDashboardWithConnection();

    await waitFor(() => {
      expect(screen.getByText('Backend disconnected')).toBeInTheDocument();
      expect(screen.getByText('offline')).toBeInTheDocument();
    });
  });
});
