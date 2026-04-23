import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { describe, expect, test, vi } from 'vitest';
import { AcceptIdeaButton } from '@/components/AcceptIdeaButton';
import type { Decision } from '@/lib/types';

function decision(overrides: Partial<Decision> = {}) {
  return {
    decision_id: 'decision-cp09',
    market_id: 'market-cp09',
    forecaster: 'model-cp09',
    prob_estimate: 0.67,
    expected_edge: 0.18,
    kelly_size: 25,
    side: 'BUY',
    limit_price: 0.41,
    factor_snapshot_hash: 'snapshot-cp09',
    status: 'pending',
    opportunity: {
      opportunity_id: 'opportunity-cp09',
      market_id: 'market-cp09',
      token_id: 'token-cp09-yes',
      side: 'yes',
      selected_factor_values: { edge: 0.18, liquidity: 0.04 },
      expected_edge: 0.18,
      rationale: 'cp09 rationale',
      target_size_usdc: 25,
      expiry: '2026-04-23T10:15:00+00:00',
      staleness_policy: 'cp09',
      strategy_id: 'default',
      strategy_version_id: 'default-v1',
      created_at: '2026-04-23T10:00:00+00:00',
      factor_snapshot_hash: 'snapshot-cp09',
      composition_trace: { kind: 'unit' }
    },
    ...overrides
  } as unknown as Decision;
}

function jsonResponse(payload: unknown, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { 'content-type': 'application/json' }
  });
}

describe('AcceptIdeaButton', () => {
  test('disables and marks aria-busy while the accept request is in flight', async () => {
    let resolveFetch!: (response: Response) => void;
    vi.spyOn(global, 'fetch').mockReturnValue(
      new Promise<Response>((resolve) => {
        resolveFetch = resolve;
      })
    );
    const onToast = vi.fn();

    render(<AcceptIdeaButton decision={decision()} onToast={onToast} />);
    const button = screen.getByRole('button', { name: 'Accept' });

    fireEvent.click(button);

    expect(button).toBeDisabled();
    expect(button).toHaveAttribute('aria-busy', 'true');
    expect(screen.getByText('Accepting...')).toBeInTheDocument();

    resolveFetch(jsonResponse({ decision_id: 'decision-cp09', status: 'accepted', fill_id: null }));

    await waitFor(() => {
      expect(onToast).toHaveBeenCalledWith(
        expect.objectContaining({
          tone: 'success',
          message: 'First trade placed',
          href: '/trades'
        })
      );
    });
  });

  test('double-clicking sends exactly one accept request', async () => {
    const fetchSpy = vi
      .spyOn(global, 'fetch')
      .mockResolvedValue(
        jsonResponse({ decision_id: 'decision-cp09', status: 'accepted', fill_id: null })
      );

    render(<AcceptIdeaButton decision={decision()} onToast={vi.fn()} />);
    const button = screen.getByRole('button', { name: 'Accept' });

    fireEvent.click(button);
    fireEvent.click(button);

    await waitFor(() => {
      expect(fetchSpy).toHaveBeenCalledTimes(1);
    });
  });

  test('409 disables briefly and refetches the idea by id', async () => {
    vi.useFakeTimers();
    const refetched = decision({ factor_snapshot_hash: 'snapshot-fresh' });
    const fetchSpy = vi
      .spyOn(global, 'fetch')
      .mockResolvedValueOnce(
        jsonResponse(
          {
            detail: 'market_changed',
            current_factor_snapshot_hash: 'snapshot-fresh'
          },
          409
        )
      )
      .mockResolvedValueOnce(jsonResponse(refetched));
    const onRefetched = vi.fn();
    const onToast = vi.fn();

    render(
      <AcceptIdeaButton
        decision={decision()}
        onDecisionRefetched={onRefetched}
        onToast={onToast}
      />
    );

    const button = screen.getByRole('button', { name: 'Accept' });
    fireEvent.click(button);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });

    expect(fetchSpy).toHaveBeenCalledTimes(2);
    expect(fetchSpy).toHaveBeenLastCalledWith('/api/pms/decisions/decision-cp09?include=opportunity');
    expect(onRefetched).toHaveBeenCalledWith(refetched);
    expect(onToast).toHaveBeenCalledWith(
      expect.objectContaining({
        tone: 'error',
        message: 'Market changed... refresh loaded'
      })
    );

    expect(button).toBeDisabled();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(500);
    });
    expect(button).not.toBeDisabled();
  });
});
