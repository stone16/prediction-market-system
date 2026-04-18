'use client';

import { useEffect, useMemo, useState } from 'react';
import { FactorSeriesChartNoSsr } from '@/components/FactorSeriesChartNoSsr';
import { Nav } from '@/components/Nav';
import { useLiveData } from '@/lib/useLiveData';
import type {
  FactorCatalogEntry,
  FactorCatalogResponse,
  FactorSeriesResponse
} from '@/lib/types';

const timestampFormatter = new Intl.DateTimeFormat(undefined, {
  dateStyle: 'medium',
  timeStyle: 'short'
});

function formatTimestamp(ts: string): string {
  const parsed = new Date(ts);
  if (Number.isNaN(parsed.getTime())) {
    return ts;
  }
  return timestampFormatter.format(parsed);
}

type FactorsPageClientProps = {
  initialFactorId: string;
  initialMarketId: string;
  initialParam: string;
};

export function FactorsPageClient({
  initialFactorId,
  initialMarketId,
  initialParam
}: FactorsPageClientProps) {
  const [factorId, setFactorId] = useState(initialFactorId);
  const [marketId, setMarketId] = useState(initialMarketId);
  const [param, setParam] = useState(initialParam);

  const {
    data: catalogData,
    loading: catalogLoading,
    disconnected: catalogDisconnected
  } = useLiveData<FactorCatalogResponse>('/factors/catalog', 30_000);
  const catalog = catalogData?.catalog ?? [];

  useEffect(() => {
    if (catalog.length === 0) {
      return;
    }
    const knownFactor = catalog.some((entry) => entry.factor_id === factorId);
    if (!knownFactor) {
      setFactorId(catalog[0].factor_id);
    }
  }, [catalog, factorId]);

  const seriesPath = useMemo(() => {
    if (factorId === '' || marketId === '') {
      return null;
    }
    const query = new URLSearchParams({
      factor_id: factorId,
      market_id: marketId,
      param,
      limit: '120'
    });
    return `/factors?${query.toString()}`;
  }, [factorId, marketId, param]);

  const {
    data: seriesData,
    loading: seriesLoading,
    disconnected: seriesDisconnected
  } = useLiveData<FactorSeriesResponse>(seriesPath, 15_000);

  const selectedFactor: FactorCatalogEntry | undefined = catalog.find(
    (entry) => entry.factor_id === factorId
  );
  const points = seriesData?.points ?? [];
  const latestPoint = points.at(-1);
  const disconnected = catalogDisconnected || seriesDisconnected;

  return (
    <main className="shell">
      <Nav />
      <section className="page">
        <div className="hero">
          <div>
            <p className="eyebrow">Middle Ring</p>
            <h1>Factors</h1>
            <p className="lede">
              Inspect persisted factor values for a specific factor, market, and param tuple.
            </p>
          </div>
          {disconnected ? <span className="badge disconnected">disconnected</span> : null}
        </div>

        <section className="card controls-panel">
          <div className="controls-topline">
            <div>
              <h2>Selector</h2>
              <p className="muted">
                Choose a factor from the catalog, then point the chart at the market tuple you
                want to inspect.
              </p>
            </div>
            <div className="factor-chip">
              <span>Rows loaded</span>
              <strong>{points.length}</strong>
            </div>
          </div>
          <div className="control-grid">
            <label className="field-group">
              <span>Factor</span>
              <select
                aria-label="Factor"
                value={factorId}
                onChange={(event) => setFactorId(event.target.value)}
              >
                {catalog.map((entry) => (
                  <option key={entry.factor_id} value={entry.factor_id}>
                    {entry.name}
                  </option>
                ))}
              </select>
            </label>
            <label className="field-group">
              <span>Market ID</span>
              <input
                aria-label="Market ID"
                value={marketId}
                onChange={(event) => setMarketId(event.target.value)}
                placeholder="factor-depth"
                spellCheck={false}
              />
            </label>
            <label className="field-group">
              <span>Param</span>
              <input
                aria-label="Param"
                value={param}
                onChange={(event) => setParam(event.target.value)}
                placeholder="(blank)"
                spellCheck={false}
              />
            </label>
          </div>
          <div className="factor-meta">
            <p className="muted">
              {selectedFactor?.description ?? 'Loading factor catalog…'}
            </p>
            {selectedFactor ? (
              <div className="depth-badges">
                <span className="badge">{selectedFactor.output_type}</span>
                <span className="badge">{selectedFactor.direction}</span>
                <span className="badge">{selectedFactor.factor_id}</span>
              </div>
            ) : null}
          </div>
        </section>

        <div className="summary-grid">
          <section className="card summary-card">
            <span className="muted">Latest value</span>
            <div className="metric">{latestPoint?.value.toFixed(4) ?? 'n/a'}</div>
          </section>
          <section className="card summary-card">
            <span className="muted">Latest timestamp</span>
            <div className="metric compact">
              {latestPoint ? formatTimestamp(latestPoint.ts) : 'n/a'}
            </div>
          </section>
          <section className="card summary-card">
            <span className="muted">Market</span>
            <div className="metric compact mono">{marketId || 'n/a'}</div>
          </section>
          <section className="card summary-card">
            <span className="muted">Param</span>
            <div className="metric compact mono">{param || '∅'}</div>
          </section>
        </div>

        {catalogLoading && catalog.length === 0 ? (
          <p className="muted">Loading factor catalog…</p>
        ) : seriesLoading && points.length === 0 ? (
          <p className="muted">Loading factor series…</p>
        ) : points.length === 0 ? (
          <p className="muted" data-testid="factor-empty-state">
            No rows returned for this factor tuple yet.
          </p>
        ) : (
          <>
            <FactorSeriesChartNoSsr series={seriesData as FactorSeriesResponse} />
            <section className="table-wrap factor-table">
              <table>
                <thead>
                  <tr>
                    <th>Timestamp</th>
                    <th>Value</th>
                  </tr>
                </thead>
                <tbody>
                  {points.map((point, index) => (
                    <tr data-testid="factor-row" key={`${point.ts}-${point.value}-${index}`}>
                      <td>{point.ts}</td>
                      <td>{point.value.toFixed(4)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </section>
          </>
        )}
      </section>
    </main>
  );
}
