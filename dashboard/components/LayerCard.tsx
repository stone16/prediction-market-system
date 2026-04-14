type LayerCardProps = {
  name: string;
  status: string;
  metric: string;
  label: string;
  disconnected: boolean;
};

export function LayerCard({ name, status, metric, label, disconnected }: LayerCardProps) {
  return (
    <section className="card layer-card" data-testid="layer-card">
      <div className="layer-top">
        <h3>{name}</h3>
        <span className={`badge ${disconnected ? 'disconnected' : ''}`}>
          {disconnected ? 'disconnected' : status}
        </span>
      </div>
      <div className="metric">{metric}</div>
      <div className="muted">{label}</div>
    </section>
  );
}
