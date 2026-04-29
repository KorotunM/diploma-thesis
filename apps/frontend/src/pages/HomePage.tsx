import { useHomeOverview } from "../features/home-overview";
import type { FreshnessState } from "../features/home-overview";
import { useFrontendRuntime } from "../shared/runtime";
import { ViewState } from "../shared/ui/view-state";

export function HomePage() {
  const { config } = useFrontendRuntime();
  const { snapshot, error, loading, refreshing } = useHomeOverview();
  const pipeline = snapshot?.pipeline;
  const freshness = snapshot?.freshness;
  const hasPipelineServices = (pipeline?.services.length ?? 0) > 0;
  const hasRegisteredSources = (freshness?.sources.length ?? 0) > 0;

  return (
    <section className="panel home-panel">
      <div className="home-heading">
        <div>
          <p className="section-kicker">Control Tower</p>
          <h2>Live pipeline and source freshness summary</h2>
          <p className="section-copy">
            Home page reads service health and source registry state directly from the running
            platform and keeps the current crawl posture visible in one place.
          </p>
        </div>
        <div className="live-status-cluster">
          <span className={`live-pill ${refreshing ? "live-pill-refreshing" : ""}`}>
            {loading && !snapshot
              ? "Bootstrapping"
              : pipeline
                ? `${pipeline.liveServices}/${pipeline.totalServices} live`
                : error
                  ? "Unavailable"
                  : "Awaiting snapshot"}
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
          <strong>{pipeline ? `${pipeline.liveServices}/${pipeline.totalServices}` : "Waiting"}</strong>
          <small>{pipeline ? `${pipeline.degradedServices} degraded` : "first probe pending"}</small>
        </article>
        <article className="summary-tile">
          <span>sources</span>
          <strong>{freshness ? freshness.activeSources : "Waiting"}</strong>
          <small>{freshness ? `${freshness.scheduledSources} scheduled` : "registry sync pending"}</small>
        </article>
        <article className="summary-tile">
          <span>freshness</span>
          <strong>{freshness ? freshness.freshSources : "Waiting"}</strong>
          <small>{freshness ? `${freshness.staleSources} stale` : "no source signal yet"}</small>
        </article>
      </div>

      <div className="home-columns">
        <section className="home-subpanel">
          <div className="subpanel-heading">
            <h3>Pipeline health</h3>
            <small>
              {snapshot
                ? formatTimestamp(snapshot.capturedAt)
                : loading
                  ? "collecting first snapshot"
                  : "snapshot unavailable"}
            </small>
          </div>
          {loading && !hasPipelineServices ? (
            <ViewState
              kind="loading"
              title="Checking pipeline services"
              message="Polling scheduler, parser, normalizer and backend health endpoints."
              detail="The first snapshot appears as soon as the platform answers."
            />
          ) : null}
          {!loading && !hasPipelineServices && error ? (
            <ViewState
              kind="error"
              title="Pipeline snapshot unavailable"
              message={error}
              detail="Service health cards return automatically on the next successful refresh."
            />
          ) : null}
          {!loading && !error && !hasPipelineServices ? (
            <ViewState
              kind="empty"
              title="No pipeline services reported yet"
              message="The page is reachable, but no service health entries were returned."
              detail="Check runtime URLs and service startup order if this persists."
            />
          ) : null}
          {hasPipelineServices ? (
            <div className="service-grid">
              {pipeline?.services.map((service) => (
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
          ) : null}
        </section>

        <section className="home-subpanel">
          <div className="subpanel-heading">
            <h3>Source freshness</h3>
            <small>
              {freshness
                ? `${freshness.policyOnlySources} policy-only, ${freshness.inactiveSources} inactive`
                : loading
                  ? "reading scheduler registry"
                  : "registry unavailable"}
            </small>
          </div>
          {loading && !hasRegisteredSources ? (
            <ViewState
              kind="loading"
              title="Scanning source registry"
              message="Collecting freshness counters and the latest observed crawl timestamps."
            />
          ) : null}
          {!loading && !hasRegisteredSources && freshness?.error ? (
            <ViewState
              kind="error"
              title="Freshness registry unavailable"
              message={freshness.error}
              detail="This affects freshness counters only. Other panels continue to refresh independently."
            />
          ) : null}
          {!loading && !freshness?.error && !hasRegisteredSources ? (
            <ViewState
              kind="empty"
              title="No sources registered yet"
              message="Once source records and endpoints are created, this panel will show freshness pressure automatically."
            />
          ) : null}
          {hasRegisteredSources ? (
            <div className="freshness-list">
              {freshness?.sources.map((source) => (
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
            </div>
          ) : null}
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
