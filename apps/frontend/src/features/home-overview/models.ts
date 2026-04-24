export type PipelineServiceState = "live" | "degraded";

export type FreshnessState =
  | "fresh"
  | "aging"
  | "stale"
  | "scheduled"
  | "manual"
  | "inactive"
  | "unknown";

export interface PipelineServiceSnapshot {
  key: string;
  label: string;
  description: string;
  baseUrl: string;
  state: PipelineServiceState;
  environment: string | null;
  version: string | null;
  dependencies: Record<string, string>;
  error: string | null;
}

export interface SourceFreshnessSnapshot {
  sourceKey: string;
  sourceType: string;
  trustTier: string;
  isActive: boolean;
  endpointCount: number;
  scheduledEndpointCount: number;
  freshnessState: FreshnessState;
  freshnessReason: string;
  refreshIntervalSeconds: number | null;
  lastObservedAt: string | null;
}

export interface PipelineOverviewSnapshot {
  totalServices: number;
  liveServices: number;
  degradedServices: number;
  services: PipelineServiceSnapshot[];
}

export interface FreshnessOverviewSnapshot {
  totalSources: number;
  activeSources: number;
  scheduledSources: number;
  freshSources: number;
  agingSources: number;
  staleSources: number;
  policyOnlySources: number;
  inactiveSources: number;
  error: string | null;
  sources: SourceFreshnessSnapshot[];
}

export interface HomeOverviewSnapshot {
  capturedAt: string;
  pipeline: PipelineOverviewSnapshot;
  freshness: FreshnessOverviewSnapshot;
}
