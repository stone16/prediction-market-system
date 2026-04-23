import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { ShareProjectionNotFoundError, getSharePageData } from '@/lib/share';

export const dynamic = 'force-dynamic';

type SharePageProps = {
  params: Promise<{ strategy_id: string }>;
};

export async function generateMetadata({ params }: SharePageProps): Promise<Metadata> {
  const { strategy_id: strategyId } = await params;

  try {
    const { projection } = await getSharePageData(strategyId);
    return {
      title: `${projection.title ?? projection.strategy_id} | PMS Share`,
      description: projection.description ?? 'Public PMS strategy summary'
    };
  } catch {
    return {
      title: 'Strategy Share | PMS',
      description: 'Public PMS strategy summary'
    };
  }
}

export default async function SharePage({ params }: SharePageProps) {
  const { strategy_id: strategyId } = await params;

  try {
    const { projection, debugReadCount } = await getSharePageData(strategyId);

    return (
      <main className="share-shell">
        <section className="share-hero" data-testid="share-hero">
          <p className="eyebrow">Public strategy brief</p>
          <h1>{projection.title ?? projection.strategy_id}</h1>
          <p className="lede">{projection.description ?? 'No public description available yet.'}</p>
          <div className="share-meta">
            <span className="badge info">Strategy {projection.strategy_id}</span>
            <span className="badge">{projection.version_id_short ?? 'unversioned'}</span>
          </div>
          {process.env.PMS_SHARE_DEBUG_RENDER === '1' ? (
            <p className="share-debug" data-testid="share-debug-read-count">
              {debugReadCount}
            </p>
          ) : null}
        </section>

        <section className="share-grid">
          <article className="card share-card">
            <h2>Theory</h2>
            <p>
              {projection.description ??
                'This strategy has been shared publicly without a longer theory note.'}
            </p>
          </article>

          <article className="card share-card">
            <h2>Performance</h2>
            <dl className="share-stats">
              <div>
                <dt>Brier overall</dt>
                <dd>
                  {projection.brier_overall === null
                    ? 'Not enough settled trades yet'
                    : projection.brier_overall.toFixed(3)}
                </dd>
              </div>
              <div>
                <dt>Trade count</dt>
                <dd>{projection.trade_count}</dd>
              </div>
            </dl>
          </article>

          <article className="card share-card">
            <h2>Calibration</h2>
            <p>
              Public shares expose only headline calibration and trade counts. Runtime credentials,
              registry metadata, and private notes stay inside the authenticated dashboard.
            </p>
          </article>
        </section>
      </main>
    );
  } catch (error) {
    if (error instanceof ShareProjectionNotFoundError) {
      notFound();
    }
    throw error;
  }
}
