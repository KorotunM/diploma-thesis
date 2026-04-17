export function EvidenceDrawer() {
  return (
    <section className="panel accent-panel">
      <h2>EvidenceDrawer</h2>
      <p>Designed for raw URL, parser version, claim set and resolution rationale.</p>
      <div className="evidence-card">
        <span className="chip">official-site</span>
        <code>https://example.edu</code>
        <small>parser.stub.0.1.0</small>
      </div>
    </section>
  );
}
