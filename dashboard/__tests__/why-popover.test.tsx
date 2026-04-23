import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { describe, expect, test } from 'vitest';
import { WhyPopover } from '@/components/WhyPopover';
import type { Decision } from '@/lib/types';

const decision = {
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
    rationale: 'Edge is high while liquidity remains deep.',
    target_size_usdc: 25,
    expiry: '2026-04-23T10:15:00+00:00',
    staleness_policy: 'cp09',
    strategy_id: 'default',
    strategy_version_id: 'default-v1',
    created_at: '2026-04-23T10:00:00+00:00',
    factor_snapshot_hash: 'snapshot-cp09',
    composition_trace: { kind: 'unit', model: 'trace' }
  }
} as unknown as Decision;

describe('WhyPopover', () => {
  test('renders factor contributions and dismisses on Escape and click outside', async () => {
    render(
      <div>
        <WhyPopover decision={decision} />
        <button type="button">Outside target</button>
      </div>
    );

    fireEvent.click(screen.getByRole('button', { name: 'Why' }));

    expect(screen.getByRole('dialog', { name: 'Why this idea?' })).toBeInTheDocument();
    expect(screen.getByText('edge')).toBeInTheDocument();
    expect(screen.getByText('liquidity')).toBeInTheDocument();
    expect(screen.getByText('Edge is high while liquidity remains deep.')).toBeInTheDocument();

    fireEvent.keyDown(document, { key: 'Escape' });
    await waitFor(() => {
      expect(screen.queryByRole('dialog', { name: 'Why this idea?' })).toBeNull();
    });

    fireEvent.click(screen.getByRole('button', { name: 'Why' }));
    expect(screen.getByRole('dialog', { name: 'Why this idea?' })).toBeInTheDocument();
    fireEvent.pointerDown(screen.getByRole('button', { name: 'Outside target' }));

    await waitFor(() => {
      expect(screen.queryByRole('dialog', { name: 'Why this idea?' })).toBeNull();
    });
  });

  test('traps focus inside the dialog and restores focus to the trigger', async () => {
    render(
      <div>
        <button type="button">Before</button>
        <WhyPopover decision={decision} />
      </div>
    );
    const trigger = screen.getByRole('button', { name: 'Why' });

    fireEvent.click(trigger);

    const closeButton = await screen.findByRole('button', { name: 'Close why panel' });
    await waitFor(() => {
      expect(closeButton).toHaveFocus();
    });

    fireEvent.keyDown(document, { key: 'Tab', shiftKey: true });
    expect(screen.getByRole('button', { name: 'Show reasoning' })).toHaveFocus();

    fireEvent.keyDown(document, { key: 'Escape' });
    await waitFor(() => {
      expect(trigger).toHaveFocus();
    });
  });
});
