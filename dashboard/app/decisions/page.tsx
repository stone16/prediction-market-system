import { Nav } from '@/components/Nav';
import { mockDecisions } from '@/lib/mock-store';

export default function DecisionsPage() {
  const decisions = mockDecisions();
  return (
    <main className="shell">
      <Nav />
      <section className="page">
        <div className="hero">
          <div>
            <p className="eyebrow">Controller</p>
            <h1>Decision Ledger</h1>
            <p className="lede">Recent trade decisions with forecaster attribution and Kelly sizing.</p>
          </div>
        </div>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Market</th>
                <th>Forecaster</th>
                <th>Prob</th>
                <th>Edge</th>
                <th>Kelly size</th>
                <th>Resolved</th>
              </tr>
            </thead>
            <tbody>
              {decisions.map((decision) => (
                <tr key={decision.decision_id}>
                  <td>{decision.market_id}</td>
                  <td>{decision.forecaster}</td>
                  <td>{decision.prob_estimate.toFixed(2)}</td>
                  <td>{decision.expected_edge.toFixed(2)}</td>
                  <td>{decision.kelly_size.toFixed(2)}</td>
                  <td>{decision.resolved_outcome ?? 'pending'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </main>
  );
}
