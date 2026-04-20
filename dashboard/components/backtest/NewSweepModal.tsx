'use client';

import { useMemo, useState } from 'react';
import { apiPost } from '@/lib/api';
import {
  buildSweepYaml,
  DEFAULT_SWEEP_RISK_POLICY,
  defaultParameterRows,
  ensureRunId,
  type SweepParameterRow
} from '@/lib/backtest';
import type { BacktestEnqueueResponse, StrategyRow } from '@/lib/types';

type NewSweepModalProps = {
  strategies: StrategyRow[];
  onClose: () => void;
  onSubmitted: (runId: string) => void;
};

function parsePositiveNumber(raw: string, fallback: number): number {
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}

export function NewSweepModal({ strategies, onClose, onSubmitted }: NewSweepModalProps) {
  const [selectedStrategyIds, setSelectedStrategyIds] = useState<string[]>([]);
  const [startDate, setStartDate] = useState('2026-04-01');
  const [endDate, setEndDate] = useState('2026-04-30');
  const [profile, setProfile] = useState<'polymarket_paper' | 'polymarket_live_estimate'>(
    'polymarket_paper'
  );
  const [chunkDays, setChunkDays] = useState('7');
  const [timeBudget, setTimeBudget] = useState('1800');
  const [maxPositionNotional, setMaxPositionNotional] = useState(
    String(DEFAULT_SWEEP_RISK_POLICY.max_position_notional_usdc)
  );
  const [maxDailyDrawdownPct, setMaxDailyDrawdownPct] = useState(
    String(DEFAULT_SWEEP_RISK_POLICY.max_daily_drawdown_pct)
  );
  const [minOrderSize, setMinOrderSize] = useState(
    String(DEFAULT_SWEEP_RISK_POLICY.min_order_size_usdc)
  );
  const [parameterRows, setParameterRows] = useState<SweepParameterRow[]>(defaultParameterRows());
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const selectedStrategies = useMemo(
    () => strategies.filter((strategy) => selectedStrategyIds.includes(strategy.strategy_id)),
    [selectedStrategyIds, strategies]
  );
  const yamlPreview = useMemo(
    () =>
      buildSweepYaml({
        selectedStrategies,
        startDate,
        endDate,
        profile,
        chunkDays: parseInt(chunkDays || '7', 10),
        timeBudget: parseInt(timeBudget || '1800', 10),
        parameterRows,
        riskPolicy: {
          max_position_notional_usdc: parsePositiveNumber(
            maxPositionNotional,
            DEFAULT_SWEEP_RISK_POLICY.max_position_notional_usdc
          ),
          max_daily_drawdown_pct: parsePositiveNumber(
            maxDailyDrawdownPct,
            DEFAULT_SWEEP_RISK_POLICY.max_daily_drawdown_pct
          ),
          min_order_size_usdc: parsePositiveNumber(
            minOrderSize,
            DEFAULT_SWEEP_RISK_POLICY.min_order_size_usdc
          )
        }
      }),
    [
      chunkDays,
      endDate,
      maxDailyDrawdownPct,
      maxPositionNotional,
      minOrderSize,
      parameterRows,
      profile,
      selectedStrategies,
      startDate,
      timeBudget
    ]
  );
  const canSubmit = selectedStrategies.length > 0 && startDate !== '' && endDate !== '';

  function toggleStrategy(strategyId: string) {
    setSelectedStrategyIds((current) =>
      current.includes(strategyId)
        ? current.filter((value) => value !== strategyId)
        : [...current, strategyId]
    );
  }

  function updateParameterRow(id: string, patch: Partial<SweepParameterRow>) {
    setParameterRows((current) =>
      current.map((row) => (row.id === id ? { ...row, ...patch } : row))
    );
  }

  function addParameterRow() {
    setParameterRows((current) => [
      ...current,
      { id: `parameter-row-${current.length + 1}`, field: '', values: '' }
    ]);
  }

  function removeParameterRow(id: string) {
    setParameterRows((current) => {
      const next = current.filter((row) => row.id !== id);
      return next.length > 0 ? next : defaultParameterRows();
    });
  }

  function exportYaml() {
    const blob = new Blob([yamlPreview], { type: 'application/x-yaml' });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = `backtest-sweep-${Date.now()}.yaml`;
    anchor.click();
    window.setTimeout(() => URL.revokeObjectURL(url), 0);
  }

  async function runHere() {
    if (!canSubmit) return;
    setSubmitting(true);
    setError(null);
    try {
      const response = await apiPost<BacktestEnqueueResponse>('/research/backtest', {
        headers: { 'content-type': 'application/x-yaml' },
        body: yamlPreview
      });
      onSubmitted(ensureRunId(response));
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unable to enqueue backtest run');
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="modal-scrim" role="presentation">
      <div className="modal card" data-testid="new-sweep-modal" role="dialog" aria-modal="true">
        <div className="modal-header">
          <div>
            <p className="eyebrow">Research</p>
            <h2>New sweep</h2>
          </div>
          <button type="button" className="ghost-button" onClick={onClose}>
            Close
          </button>
        </div>

        <div className="modal-grid">
          <section className="modal-section">
            <h3>Strategies</h3>
            <div className="checkbox-grid">
              {strategies.map((strategy) => (
                <label key={strategy.strategy_id} className="check-option">
                  <input
                    type="checkbox"
                    aria-label={strategy.strategy_id}
                    checked={selectedStrategyIds.includes(strategy.strategy_id)}
                    onChange={() => toggleStrategy(strategy.strategy_id)}
                  />
                  <span>{strategy.strategy_id}</span>
                  <small>{strategy.active_version_id ?? 'no active version'}</small>
                </label>
              ))}
            </div>
          </section>

          <section className="modal-section">
            <h3>Window</h3>
            <div className="control-grid compact-grid">
              <label className="field-group">
                <span>Start date</span>
                <input type="date" value={startDate} onChange={(event) => setStartDate(event.target.value)} />
              </label>
              <label className="field-group">
                <span>End date</span>
                <input type="date" value={endDate} onChange={(event) => setEndDate(event.target.value)} />
              </label>
              <label className="field-group">
                <span>Execution profile</span>
                <select value={profile} onChange={(event) => setProfile(event.target.value as typeof profile)}>
                  <option value="polymarket_paper">Polymarket paper</option>
                  <option value="polymarket_live_estimate">Polymarket live estimate</option>
                </select>
              </label>
              <label className="field-group">
                <span>Chunk days</span>
                <input value={chunkDays} onChange={(event) => setChunkDays(event.target.value)} />
              </label>
              <label className="field-group">
                <span>Time budget</span>
                <input value={timeBudget} onChange={(event) => setTimeBudget(event.target.value)} />
              </label>
              <label className="field-group">
                <span>Max position notional (USDC)</span>
                <input
                  value={maxPositionNotional}
                  data-testid="risk-max-position-notional"
                  onChange={(event) => setMaxPositionNotional(event.target.value)}
                />
              </label>
              <label className="field-group">
                <span>Max daily drawdown (%)</span>
                <input
                  value={maxDailyDrawdownPct}
                  data-testid="risk-max-daily-drawdown-pct"
                  onChange={(event) => setMaxDailyDrawdownPct(event.target.value)}
                />
              </label>
              <label className="field-group">
                <span>Min order size (USDC)</span>
                <input
                  value={minOrderSize}
                  data-testid="risk-min-order-size"
                  onChange={(event) => setMinOrderSize(event.target.value)}
                />
              </label>
            </div>
          </section>
        </div>

        <section className="modal-section">
          <div className="section-heading">
            <h3>Parameter grid</h3>
            <button type="button" className="ghost-button" onClick={addParameterRow}>
              Add parameter
            </button>
          </div>
          <div className="parameter-grid-list">
            {parameterRows.map((row) => (
              <div key={row.id} className="parameter-grid-row">
                <label className="field-group">
                  <span>Field</span>
                  <input
                    value={row.field}
                    placeholder="risk_policy.max_position_notional_usdc"
                    onChange={(event) => updateParameterRow(row.id, { field: event.target.value })}
                  />
                </label>
                <label className="field-group">
                  <span>Values</span>
                  <input
                    value={row.values}
                    placeholder="50, 75, 100"
                    onChange={(event) => updateParameterRow(row.id, { values: event.target.value })}
                  />
                </label>
                <button
                  type="button"
                  className="ghost-button destructive"
                  onClick={() => removeParameterRow(row.id)}
                >
                  Remove
                </button>
              </div>
            ))}
          </div>
        </section>

        <section className="modal-section">
          <div className="section-heading">
            <h3>YAML preview</h3>
            <span className="muted">Generated from the current form state.</span>
          </div>
          <pre className="yaml-preview">{yamlPreview}</pre>
        </section>

        {error ? <p className="error">{error}</p> : null}

        <div className="modal-actions">
          <button
            type="button"
            className="ghost-button"
            data-testid="export-sweep-yaml"
            disabled={!canSubmit}
            onClick={exportYaml}
          >
            Export YAML
          </button>
          <button
            type="button"
            className="primary-button"
            data-testid="run-sweep-here"
            disabled={!canSubmit || submitting}
            onClick={() => void runHere()}
          >
            {submitting ? 'Queueing…' : 'Run here'}
          </button>
        </div>
      </div>
    </div>
  );
}
