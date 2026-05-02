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
    <section className="panel panel--overview overview-panel">
      <div className="panel__header">
        <div>
          <p className="panel__kicker">Мониторинг</p>
          <h2 className="panel__title">Состояние пайплайна и актуальность источников</h2>
          <p className="panel__copy">
            Отдельное представление для runtime-наблюдения: health сервисов, деградации,
            свежесть источников и текущая нагрузка на контур обхода.
          </p>
        </div>
        <div className="overview-panel__status">
          <span className={`panel__badge ${refreshing ? "panel__badge--refreshing" : ""}`}>
            {loading && !snapshot
              ? "Инициализация"
              : pipeline
                ? `${pipeline.liveServices}/${pipeline.totalServices} в строю`
                : error
                  ? "Недоступно"
                  : "Ожидание снимка"}
          </span>
          <small>Обновление каждые {Math.round(config.overviewRefreshIntervalMs / 1000)}с</small>
        </div>
      </div>

      {error ? <p className="panel-alert">{error}</p> : null}
      {freshness?.error ? <p className="panel-alert">{freshness.error}</p> : null}

      <div className="overview-panel__summary">
        <article className="summary-card">
          <span>Среда</span>
          <strong>{config.appEnvironment}</strong>
          <small>{config.backendBaseUrl}</small>
        </article>
        <article className="summary-card">
          <span>Пайплайн</span>
          <strong>{pipeline ? `${pipeline.liveServices}/${pipeline.totalServices}` : "Ожидание"}</strong>
          <small>{pipeline ? `${pipeline.degradedServices} с деградацией` : "первый опрос еще не завершен"}</small>
        </article>
        <article className="summary-card">
          <span>Источники</span>
          <strong>{freshness ? freshness.activeSources : "Ожидание"}</strong>
          <small>{freshness ? `${freshness.scheduledSources} по расписанию` : "ожидается синхронизация реестра"}</small>
        </article>
        <article className="summary-card">
          <span>Актуальность</span>
          <strong>{freshness ? freshness.freshSources : "Ожидание"}</strong>
          <small>{freshness ? `${freshness.staleSources} устарели` : "пока нет сигналов от источников"}</small>
        </article>
      </div>

      <div className="overview-panel__columns">
        <section className="overview-panel__section">
          <div className="overview-panel__section-header">
            <h3>Состояние пайплайна</h3>
            <small>
              {snapshot
                ? formatTimestamp(snapshot.capturedAt)
                : loading
                  ? "собираем первый снимок"
                  : "снимок недоступен"}
            </small>
          </div>
          {loading && !hasPipelineServices ? (
            <ViewState
              kind="loading"
              title="Проверяем сервисы пайплайна"
              message="Опрашиваем health endpoint'ы scheduler, parser, normalizer и backend."
              detail="Первый снимок появится, как только сервисы ответят."
            />
          ) : null}
          {!loading && !hasPipelineServices && error ? (
            <ViewState
              kind="error"
              title="Снимок пайплайна недоступен"
              message={error}
              detail="Карточки сервисов вернутся автоматически после следующего успешного обновления."
            />
          ) : null}
          {!loading && !error && !hasPipelineServices ? (
            <ViewState
              kind="empty"
              title="Сервисы пока не отчитались"
              message="Страница доступна, но список сервисов еще не вернулся."
              detail="Если это не исчезнет, проверь runtime URL и порядок запуска сервисов."
            />
          ) : null}
          {hasPipelineServices ? (
            <div className="overview-panel__service-grid">
              {pipeline?.services.map((service) => (
                <article
                  key={service.key}
                  className={`overview-panel__service-card overview-panel__service-card--${service.state}`}
                >
                  <div className="overview-panel__service-header">
                    <strong>{service.label}</strong>
                    <span className={`status-pill status-pill--${service.state}`}>
                      {service.state === "live" ? "в строю" : "с деградацией"}
                    </span>
                  </div>
                  <p>{service.description}</p>
                  <code>{service.baseUrl}</code>
                  <div className="overview-panel__service-meta">
                    <span>{service.environment ?? "среда неизвестна"}</span>
                    <span>{service.version ?? "версия неизвестна"}</span>
                  </div>
                  <div className="overview-panel__service-dependencies">
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

        <section className="overview-panel__section">
          <div className="overview-panel__section-header">
            <h3>Актуальность источников</h3>
            <small>
              {freshness
                ? `${freshness.policyOnlySources} без факта обхода, ${freshness.inactiveSources} неактивны`
                : loading
                  ? "читаем реестр scheduler"
                  : "реестр недоступен"}
            </small>
          </div>
          {loading && !hasRegisteredSources ? (
            <ViewState
              kind="loading"
              title="Сканируем реестр источников"
              message="Собираем счетчики актуальности и последние отметки наблюдавшихся обходов."
            />
          ) : null}
          {!loading && !hasRegisteredSources && freshness?.error ? (
            <ViewState
              kind="error"
              title="Реестр актуальности недоступен"
              message={freshness.error}
              detail="Это влияет только на счетчики актуальности. Остальные панели продолжают обновляться независимо."
            />
          ) : null}
          {!loading && !freshness?.error && !hasRegisteredSources ? (
            <ViewState
              kind="empty"
              title="Источники еще не зарегистрированы"
              message="Как только появятся source records и endpoint'ы, панель начнет показывать их состояние автоматически."
            />
          ) : null}
          {hasRegisteredSources ? (
            <div className="overview-panel__freshness-list">
              {freshness?.sources.map((source) => (
                <article key={source.sourceKey} className="overview-panel__freshness-card">
                  <div className="overview-panel__freshness-main">
                    <div className="overview-panel__freshness-header">
                      <strong>{source.sourceKey}</strong>
                      <span className={`status-pill status-pill--${source.freshnessState}`}>
                        {formatFreshnessState(source.freshnessState)}
                      </span>
                    </div>
                    <p>{source.freshnessReason}</p>
                  </div>
                  <div className="overview-panel__freshness-metrics">
                    <span>{source.trustTier}</span>
                    <span>{source.sourceType}</span>
                    <span>{source.endpointCount} endpoint'ов</span>
                    <span>{source.scheduledEndpointCount} по расписанию</span>
                    <span>
                      {source.lastObservedAt ? formatTimestamp(source.lastObservedAt) : "обход еще не наблюдался"}
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
      return "Актуально";
    case "aging":
      return "Стареет";
    case "stale":
      return "Устарело";
    case "scheduled":
      return "По расписанию";
    case "manual":
      return "Только вручную";
    case "inactive":
      return "Неактивно";
    default:
      return "Неизвестно";
  }
}

function formatTimestamp(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat("ru-RU", {
    day: "2-digit",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}
