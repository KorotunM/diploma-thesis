import { useHomeOverview } from "../features/home-overview";
import type { FreshnessState } from "../features/home-overview";
import { useFrontendRuntime } from "../shared/runtime";

export function HomePage() {
  const { config } = useFrontendRuntime();
  const { snapshot, error, loading, refreshing } = useHomeOverview();
  const pipeline = snapshot?.pipeline;
  const freshness = snapshot?.freshness;

  return (
    <section className="panel home-panel">
      <div className="home-heading">
        <div>
          <p className="section-kicker">Control Tower</p>
          <h2>Live pipeline and source freshness summary</h2>
          <p className="section-copy">
            Home page now reads service health and source registry state directly from the
            running platform instead of showing static placeholders.
          </p>
        </div>
        <div className="live-status-cluster">
          <span className={`live-pill ${refreshing ? "live-pill-refreshing" : ""}`}>
            {loading ? "Bootstrapping" : `${pipeline?.liveServices ?? 0}/${pipeline?.totalServices ?? 4} live`}
          </span>
          <small>
            refresh {Math.round(config.overviewRefreshIntervalMs / 1000)}s
          </small>
        </div>
      </div>

      {error ? <p className="panel-alert">{error}</p> : null}
      {freshness?.error ? <p className="panel-alert">{freshness.error}</p> : null}

      <div className="summary-grid">
        <article className="summary-tile">
          <span>environment</span>
          <strong>{config.appEnvironment}</strong>
          <small>{config.backendBaseUrl}</small>
        </article>
        <article className="summary-tile">
          <span>pipeline</span>
          <strong>{pipeline ? `${pipeline.liveServices}/${pipeline.totalServices}` : "..."}</strong>
          <small>{pipeline?.degradedServices ?? 0} degraded</small>
        </article>
        <article className="summary-tile">
          <span>sources</span>
          <strong>{freshness?.activeSources ?? 0}</strong>
          <small>{freshness?.scheduledSources ?? 0} scheduled</small>
        </article>
        <article className="summary-tile">
          <span>freshness</span>
          <strong>{freshness?.freshSources ?? 0}</strong>
          <small>{freshness?.staleSources ?? 0} stale</small>
        </article>
      </div>

      <div className="home-columns">
        <section className="home-subpanel">
          <div className="subpanel-heading">
            <h3>Pipeline health</h3>
            <small>{snapshot ? formatTimestamp(snapshot.capturedAt) : "waiting for first snapshot"}</small>
          </div>
          <div className="service-grid">
            {(pipeline?.services ?? []).map((service) => (
              <article key={service.key} className={`service-card service-${service.state}`}>
                <div className="service-card-header">
                  <strong>{service.label}</strong>
                  <span className={`service-state service-state-${service.state}`}>
                    {service.state}
                  </span>
                </div>
                <p>{service.description}</p>
                <code>{service.baseUrl}</code>
                <div className="service-meta">
                  <span>{service.environment ?? "env unknown"}</span>
                  <span>{service.version ?? "version unknown"}</span>
                </div>
                <div className="service-dependencies">
                  {Object.entries(service.dependencies).map(([name, status]) => (
                    <span key={name} className="dependency-chip">
                      {name}: {status}
                    </span>
                  ))}
                </div>
                {service.error ? <small className="service-error">{service.error}</small> : null}
              </article>
            ))}
          </div>
        </section>

        <section className="home-subpanel">
          <div className="subpanel-heading">
            <h3>Source freshness</h3>
            <small>
              {freshness
                ? `${freshness.policyOnlySources} policy-only, ${freshness.inactiveSources} inactive`
                : "waiting for registry"}
            </small>
          </div>
          <div className="freshness-list">
            {(freshness?.sources ?? []).map((source) => (
              <article key={source.sourceKey} className="freshness-row">
                <div className="freshness-main">
                  <div className="freshness-title-row">
                    <strong>{source.sourceKey}</strong>
                    <span className={`freshness-pill freshness-${source.freshnessState}`}>
                      {formatFreshnessState(source.freshnessState)}
                    </span>
                  </div>
                  <p>{source.freshnessReason}</p>
                </div>
                <div className="freshness-metrics">
                  <span>{source.trustTier}</span>
                  <span>{source.sourceType}</span>
                  <span>{source.endpointCount} endpoints</span>
                  <span>{source.scheduledEndpointCount} scheduled</span>
                  <span>
                    {source.lastObservedAt ? formatTimestamp(source.lastObservedAt) : "no observed crawl"}
                  </span>
                </div>
              </article>
            ))}
            {!loading && (freshness?.sources.length ?? 0) === 0 ? (
              <p className="empty-state">
                No sources registered yet. Once scheduler registry is populated, this panel will
                summarize freshness pressure automatically.
              </p>
            ) : null}
          </div>
        </section>
      </div>
    </section>
  );
}

function formatFreshnessState(state: FreshnessState): string {
  switch (state) {
    case "fresh":
      return "Fresh";
    case "aging":
      return "Aging";
    case "stale":
      return "Stale";
    case "scheduled":
      return "Scheduled";
    case "manual":
      return "Manual";
    case "inactive":
      return "Inactive";
    default:
      return "Unknown";
  }
}

function formatTimestamp(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat("en-GB", {
    day: "2-digit",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}
