import type { FrontendRuntime } from "../../shared/runtime";
import { describeRequestError } from "../../shared/http";
import type {
  SourceEndpointResponseDto,
  SourceResponseDto,
} from "../../shared/scheduler-api";

import type {
  FreshnessOverviewSnapshot,
  FreshnessState,
  HomeOverviewSnapshot,
  PipelineOverviewSnapshot,
  PipelineServiceSnapshot,
  SourceFreshnessSnapshot,
} from "./models";

interface LoadHomeOverviewOptions {
  runtime: FrontendRuntime;
  signal?: AbortSignal;
}

const DEFAULT_FRESHNESS_INTERVAL_SECONDS = 24 * 60 * 60;

export async function loadHomeOverview(
  options: LoadHomeOverviewOptions,
): Promise<HomeOverviewSnapshot> {
  const capturedAt = new Date().toISOString();
  const [pipeline, freshness] = await Promise.all([
    loadPipelineOverview(options),
    loadFreshnessOverview(options),
  ]);

  return {
    capturedAt,
    pipeline,
    freshness,
  };
}

async function loadPipelineOverview(
  options: LoadHomeOverviewOptions,
): Promise<PipelineOverviewSnapshot> {
  const services = await Promise.all(
    [
      {
        key: "scheduler",
        label: "Scheduler",
        description: "Планирует обходы и хранит реестр источников.",
        baseUrl: options.runtime.config.schedulerBaseUrl,
        request: () => options.runtime.schedulerApi.getHealth({ signal: options.signal }),
      },
      {
        key: "parser",
        label: "Parser",
        description: "Забирает raw artifacts и публикует parsed payloads.",
        baseUrl: options.runtime.config.parserBaseUrl,
        request: () => options.runtime.parserApi.getHealth({ signal: options.signal }),
      },
      {
        key: "normalizer",
        label: "Normalizer",
        description: "Разрешает claims в канонические delivery projection.",
        baseUrl: options.runtime.config.normalizerBaseUrl,
        request: () => options.runtime.normalizerApi.getHealth({ signal: options.signal }),
      },
      {
        key: "backend",
        label: "Backend",
        description: "Отдает UI delivery-карточки и собранный provenance.",
        baseUrl: options.runtime.config.backendBaseUrl,
        request: () => options.runtime.backendApi.getHealth({ signal: options.signal }),
      },
    ].map(async (service): Promise<PipelineServiceSnapshot> => {
      try {
        const health = await service.request();
        return {
          key: service.key,
          label: service.label,
          description: service.description,
          baseUrl: service.baseUrl,
          state: "live",
          environment: health.environment,
          version: health.version,
          dependencies: health.dependencies,
          error: null,
        };
      } catch (error) {
        return {
          key: service.key,
          label: service.label,
          description: service.description,
          baseUrl: service.baseUrl,
          state: "degraded",
          environment: null,
          version: null,
          dependencies: {},
          error: describeRequestError(error),
        };
      }
    }),
  );

  const liveServices = services.filter((service) => service.state === "live").length;
  return {
    totalServices: services.length,
    liveServices,
    degradedServices: services.length - liveServices,
    services,
  };
}

async function loadFreshnessOverview(
  options: LoadHomeOverviewOptions,
): Promise<FreshnessOverviewSnapshot> {
  try {
    const sources = await options.runtime.schedulerApi.listSources(
      {
        includeInactive: true,
        limit: 200,
        offset: 0,
      },
      { signal: options.signal },
    );
    const endpointResponses = await Promise.all(
      sources.items.map(async (source) => {
        try {
          const endpoints = await options.runtime.schedulerApi.listSourceEndpoints(
            source.source_key,
            {
              limit: 200,
              offset: 0,
            },
            { signal: options.signal },
          );
          return {
            sourceKey: source.source_key,
            items: endpoints.items,
          };
        } catch {
          return {
            sourceKey: source.source_key,
            items: [] as SourceEndpointResponseDto[],
          };
        }
      }),
    );
    const endpointsBySource = new Map(
      endpointResponses.map((entry) => [entry.sourceKey, entry.items]),
    );
    const summaries = sources.items
      .map((source) =>
        summarizeSourceFreshness(source, endpointsBySource.get(source.source_key) ?? []),
      )
      .sort(compareSourceFreshness);

    return {
      totalSources: sources.total,
      activeSources: summaries.filter((source) => source.isActive).length,
      scheduledSources: summaries.filter((source) => source.scheduledEndpointCount > 0).length,
      freshSources: summaries.filter((source) => source.freshnessState === "fresh").length,
      agingSources: summaries.filter((source) => source.freshnessState === "aging").length,
      staleSources: summaries.filter((source) => source.freshnessState === "stale").length,
      policyOnlySources: summaries.filter((source) =>
        ["scheduled", "manual", "unknown"].includes(source.freshnessState),
      ).length,
      inactiveSources: summaries.filter((source) => source.freshnessState === "inactive").length,
      error: null,
      sources: summaries,
    };
  } catch (error) {
    return {
      totalSources: 0,
      activeSources: 0,
      scheduledSources: 0,
      freshSources: 0,
      agingSources: 0,
      staleSources: 0,
      policyOnlySources: 0,
      inactiveSources: 0,
      error: describeRequestError(error),
      sources: [],
    };
  }
}

function summarizeSourceFreshness(
  source: SourceResponseDto,
  endpoints: SourceEndpointResponseDto[],
): SourceFreshnessSnapshot {
  const scheduledEndpoints = endpoints.filter((endpoint) => endpoint.crawl_policy.schedule_enabled);
  const refreshIntervalSeconds = minIntervalSeconds(scheduledEndpoints);
  const lastObservedAt = findObservedTimestamp(source);
  const freshnessState = resolveFreshnessState(
    source,
    endpoints,
    refreshIntervalSeconds,
    lastObservedAt,
  );

  return {
    sourceKey: source.source_key,
    sourceType: source.source_type,
    trustTier: source.trust_tier,
    isActive: source.is_active,
    endpointCount: endpoints.length,
    scheduledEndpointCount: scheduledEndpoints.length,
    freshnessState,
    freshnessReason: describeFreshnessReason(
      freshnessState,
      refreshIntervalSeconds,
      lastObservedAt,
    ),
    refreshIntervalSeconds,
    lastObservedAt,
  };
}

function resolveFreshnessState(
  source: SourceResponseDto,
  endpoints: SourceEndpointResponseDto[],
  refreshIntervalSeconds: number | null,
  lastObservedAt: string | null,
): FreshnessState {
  if (!source.is_active) {
    return "inactive";
  }

  if (lastObservedAt !== null) {
    const observedAt = Date.parse(lastObservedAt);
    if (Number.isFinite(observedAt)) {
      const ageMs = Date.now() - observedAt;
      const intervalMs = (refreshIntervalSeconds ?? DEFAULT_FRESHNESS_INTERVAL_SECONDS) * 1000;
      if (ageMs <= intervalMs * 1.25) {
        return "fresh";
      }
      if (ageMs <= intervalMs * 2) {
        return "aging";
      }
      return "stale";
    }
  }

  if (endpoints.some((endpoint) => endpoint.crawl_policy.schedule_enabled)) {
    return "scheduled";
  }
  if (endpoints.length > 0) {
    return "manual";
  }
  return "unknown";
}

function describeFreshnessReason(
  state: FreshnessState,
  refreshIntervalSeconds: number | null,
  lastObservedAt: string | null,
): string {
  if (state === "fresh" || state === "aging" || state === "stale") {
    if (lastObservedAt === null) {
      return "Отсутствует observed timestamp.";
    }
    const intervalLabel =
      refreshIntervalSeconds === null
        ? "базовое окно 24ч"
        : `policy ${formatIntervalSeconds(refreshIntervalSeconds)}`;
    return `Последний наблюдавшийся обход был ${formatAge(lastObservedAt)} при окне ${intervalLabel}.`;
  }
  if (state === "scheduled") {
    return refreshIntervalSeconds === null
      ? "Расписание есть, но observed crawl timestamp еще не появился."
      : `Расписание: каждые ${formatIntervalSeconds(refreshIntervalSeconds)}, но observed crawl timestamp еще не появился.`;
  }
  if (state === "manual") {
    return "Источник работает только вручную и не имеет окна актуальности по расписанию.";
  }
  if (state === "inactive") {
    return "Источник отключен и исключен из проверок актуальности.";
  }
  return "Пока нет endpoint'ов или observed timestamp.";
}

function minIntervalSeconds(endpoints: SourceEndpointResponseDto[]): number | null {
  const values = endpoints
    .map((endpoint) => endpoint.crawl_policy.interval_seconds)
    .filter((value): value is number => typeof value === "number" && value > 0);
  if (values.length === 0) {
    return null;
  }
  return Math.min(...values);
}

function findObservedTimestamp(source: SourceResponseDto): string | null {
  const metadata = source.metadata;
  const candidates = [
    metadata.last_success_at,
    metadata.last_crawled_at,
    metadata.last_fetched_at,
    metadata.freshness_checked_at,
  ];
  for (const candidate of candidates) {
    if (typeof candidate === "string" && !Number.isNaN(Date.parse(candidate))) {
      return candidate;
    }
  }
  return null;
}

function compareSourceFreshness(
  left: SourceFreshnessSnapshot,
  right: SourceFreshnessSnapshot,
): number {
  const severity = (state: FreshnessState): number =>
    ({
      stale: 0,
      aging: 1,
      unknown: 2,
      manual: 3,
      scheduled: 4,
      fresh: 5,
      inactive: 6,
    })[state];

  return (
    severity(left.freshnessState) - severity(right.freshnessState) ||
    left.sourceKey.localeCompare(right.sourceKey)
  );
}

function formatAge(value: string): string {
  const ageMs = Date.now() - Date.parse(value);
  if (!Number.isFinite(ageMs)) {
    return "в неизвестный момент";
  }
  const totalMinutes = Math.max(1, Math.round(ageMs / 60_000));
  if (totalMinutes < 60) {
    return `${totalMinutes} мин назад`;
  }
  const totalHours = Math.round(totalMinutes / 60);
  if (totalHours < 48) {
    return `${totalHours} ч назад`;
  }
  const totalDays = Math.round(totalHours / 24);
  return `${totalDays} дн назад`;
}

function formatIntervalSeconds(value: number): string {
  if (value < 3600) {
    return `${Math.round(value / 60)} мин`;
  }
  if (value < 86_400) {
    return `${Math.round(value / 3600)} ч`;
  }
  return `${Math.round(value / 86_400)} дн`;
}
