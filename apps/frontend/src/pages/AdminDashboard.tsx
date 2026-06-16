import { useCallback, useEffect, useState } from "react";

import { HomePage } from "./HomePage";
import type {
  PipelineRunItemDto,
  PipelineSourceItemDto,
  ReviewCaseItemDto,
  ReviewCaseResolveDto,
} from "../shared/backend-api";
import { describeRequestError } from "../shared/http";
import { useFrontendRuntime } from "../shared/runtime";
import { ViewState } from "../shared/ui/view-state";
import { EvidenceDrawer } from "../widgets/evidence-drawer/EvidenceDrawer";

type AdminTab = "monitoring" | "control" | "review" | "evidence";

const TABS: Array<{ id: AdminTab; label: string }> = [
  { id: "monitoring", label: "Мониторинг пайплайна" },
  { id: "control", label: "Управление пайплайном" },
  { id: "review", label: "Разбор фактов" },
  { id: "evidence", label: "Доказательства" },
];

interface AdminDashboardProps {
  onLogout: () => void;
}

export function AdminDashboard({ onLogout }: AdminDashboardProps) {
  const [tab, setTab] = useState<AdminTab>("monitoring");

  return (
    <div className="admin-dashboard">
      <div className="admin-dashboard__topbar">
        <div className="admin-dashboard__topbar-inner">
          <div className="admin-dashboard__tabs">
            {TABS.map((t) => (
              <button
                key={t.id}
                className={`app__nav-link ${tab === t.id ? "app__nav-link--active" : ""}`}
                type="button"
                onClick={() => setTab(t.id)}
              >
                {t.label}
              </button>
            ))}
          </div>
          <button className="button button--ghost" type="button" onClick={onLogout}>
            Выйти
          </button>
        </div>
      </div>

      <div className="admin-dashboard__content">
        {tab === "monitoring" && <HomePage />}
        {tab === "control" && <PipelineControl />}
        {tab === "review" && <ReviewInbox />}
        {tab === "evidence" && <EvidenceDrawer />}
      </div>
    </div>
  );
}

function PipelineControl() {
  const { backendApi } = useFrontendRuntime();
  const [sources, setSources] = useState<PipelineSourceItemDto[]>([]);
  const [runs, setRuns] = useState<PipelineRunItemDto[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [busyKey, setBusyKey] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const loadRuns = useCallback(() => {
    return backendApi
      .getPipelineRuns({ limit: 20 })
      .then((response) => setRuns(response.items))
      .catch(() => {
        /* runs are best-effort; surface only the fatal load error below */
      });
  }, [backendApi]);

  const loadAll = useCallback(() => {
    setLoading(true);
    Promise.all([backendApi.getPipelineSources(), backendApi.getPipelineRuns({ limit: 20 })])
      .then(([sourcesResponse, runsResponse]) => {
        setSources(sourcesResponse.items);
        setRuns(runsResponse.items);
        setError(null);
      })
      .catch((requestError: unknown) => setError(describeRequestError(requestError)))
      .finally(() => setLoading(false));
  }, [backendApi]);

  useEffect(() => {
    loadAll();
  }, [loadAll]);

  const rerun = async (sourceKey: string | null) => {
    const key = sourceKey ?? "__all__";
    setBusyKey(key);
    setNotice(null);
    try {
      const result = await backendApi.rerunPipeline({ source_key: sourceKey, priority: "high" });
      const scope = result.scope === "all" ? "весь пайплайн" : `источник «${sourceKey}»`;
      setNotice(
        `Перезапущен ${scope}: поставлено задач — ${result.triggered}` +
          (result.failed > 0 ? `, с ошибкой — ${result.failed}` : ""),
      );
      await loadRuns();
    } catch (requestError: unknown) {
      setNotice(`Ошибка перезапуска: ${describeRequestError(requestError)}`);
    } finally {
      setBusyKey(null);
    }
  };

  if (loading) {
    return (
      <ViewState
        kind="loading"
        title="Загружаем источники"
        message="Получаем список источников и историю запусков пайплайна."
      />
    );
  }

  if (error) {
    return (
      <ViewState
        kind="error"
        title="Управление недоступно"
        message={error}
        actions={
          <button className="button button--primary" type="button" onClick={loadAll}>
            Повторить
          </button>
        }
      />
    );
  }

  return (
    <section className="admin-pipeline">
      <div className="admin-pipeline__header">
        <div>
          <h2>Управление пайплайном</h2>
          <p>
            Перезапуск повторно публикует crawl-запросы, которые проходят весь конвейер:
            сбор → парсинг → нормализация → построение карточки.
          </p>
        </div>
        <div className="admin-pipeline__header-actions">
          <button
            className="button button--primary"
            type="button"
            disabled={busyKey !== null}
            onClick={() => void rerun(null)}
          >
            {busyKey === "__all__" ? "Запускаем…" : "Перезапустить весь пайплайн"}
          </button>
          <button className="button button--ghost" type="button" onClick={loadAll}>
            Обновить
          </button>
        </div>
      </div>

      {notice && <p className="admin-pipeline__notice">{notice}</p>}

      <div className="admin-pipeline__sources">
        <h3>Источники</h3>
        {sources.length === 0 && (
          <p className="admin-pipeline__empty">Источники не зарегистрированы.</p>
        )}
        {sources.map((source) => (
          <div className="admin-pipeline__source" key={source.source_key}>
            <div className="admin-pipeline__source-main">
              <strong>{source.source_key}</strong>
              <span className="admin-pipeline__source-meta">
                {source.source_type} · {source.trust_tier}
                {!source.is_active && " · неактивен"}
              </span>
            </div>
            <button
              className="button button--secondary"
              type="button"
              disabled={busyKey !== null}
              onClick={() => void rerun(source.source_key)}
            >
              {busyKey === source.source_key ? "Запускаем…" : "Перезапустить"}
            </button>
          </div>
        ))}
      </div>

      <div className="admin-pipeline__runs">
        <h3>Недавние запуски</h3>
        {runs.length === 0 && (
          <p className="admin-pipeline__empty">Запусков пока нет.</p>
        )}
        {runs.length > 0 && (
          <table className="admin-pipeline__table">
            <thead>
              <tr>
                <th>Тип</th>
                <th>Источник</th>
                <th>Статус</th>
                <th>Триггер</th>
                <th>Начат</th>
              </tr>
            </thead>
            <tbody>
              {runs.map((run) => (
                <tr key={run.run_id}>
                  <td>{run.run_type}</td>
                  <td>{run.source_key ?? "—"}</td>
                  <td>
                    <span className={`admin-pipeline__status admin-pipeline__status--${run.status}`}>
                      {run.status}
                    </span>
                  </td>
                  <td>{run.trigger_type}</td>
                  <td>{formatDateTime(run.started_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </section>
  );
}

function ReviewInbox() {
  const { backendApi } = useFrontendRuntime();
  const [items, setItems] = useState<ReviewCaseItemDto[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [busyId, setBusyId] = useState<string | null>(null);

  const loadCases = useCallback(() => {
    setLoading(true);
    backendApi
      .getReviewCases({ status: "open", limit: 50 })
      .then((response) => {
        setItems(response.items);
        setError(null);
      })
      .catch((requestError: unknown) => setError(describeRequestError(requestError)))
      .finally(() => setLoading(false));
  }, [backendApi]);

  useEffect(() => {
    loadCases();
  }, [loadCases]);

  const resolve = async (
    item: ReviewCaseItemDto,
    resolution: ReviewCaseResolveDto["resolution"],
  ) => {
    setBusyId(item.review_case_id);
    try {
      await backendApi.resolveReviewCase(item.review_case_id, { resolution });
      setItems((current) =>
        current.filter((caseItem) => caseItem.review_case_id !== item.review_case_id),
      );
    } catch (requestError: unknown) {
      setError(describeRequestError(requestError));
    } finally {
      setBusyId(null);
    }
  };

  if (loading) {
    return (
      <ViewState
        kind="loading"
        title="Загружаем очередь"
        message="Получаем кейсы, требующие ручной проверки."
      />
    );
  }

  if (error) {
    return (
      <ViewState
        kind="error"
        title="Очередь недоступна"
        message={error}
        actions={
          <button className="button button--primary" type="button" onClick={loadCases}>
            Повторить
          </button>
        }
      />
    );
  }

  if (items.length === 0) {
    return (
      <ViewState
        kind="empty"
        title="Нет открытых кейсов"
        message="Все события review.required обработаны или ещё не поступали."
      />
    );
  }

  return (
    <section className="admin-review">
      <div className="admin-review__header">
        <div>
          <h2>Очередь ручной проверки</h2>
          <p>Открытые события review.required из пайплайна нормализации.</p>
        </div>
        <button className="button button--ghost" type="button" onClick={loadCases}>
          Обновить
        </button>
      </div>

      <div className="admin-review__list">
        {items.map((item) => (
          <article className="admin-review__case" key={item.review_case_id}>
            <div className="admin-review__case-main">
              <div className="admin-review__case-title">
                <span className={`admin-review__priority admin-review__priority--${item.priority}`}>
                  {item.priority === "high" ? "Высокий" : "Обычный"}
                </span>
                <strong>{caseTitle(item)}</strong>
              </div>
              <p>{caseSummary(item)}</p>
              <dl className="admin-review__facts">
                <div>
                  <dt>Причина</dt>
                  <dd>{item.reason}</dd>
                </div>
                <div>
                  <dt>Вуз</dt>
                  <dd>{item.canonical_name ?? item.university_id ?? "Не указан"}</dd>
                </div>
                <div>
                  <dt>Доказательства</dt>
                  <dd>{item.evidence_ids.length}</dd>
                </div>
                <div>
                  <dt>Создано</dt>
                  <dd>{formatDateTime(item.created_at)}</dd>
                </div>
              </dl>
            </div>
            <div className="admin-review__actions">
              <button
                className="button button--primary"
                type="button"
                disabled={busyId === item.review_case_id}
                onClick={() => void resolve(item, "accepted")}
              >
                Принять
              </button>
              <button
                className="button button--ghost"
                type="button"
                disabled={busyId === item.review_case_id}
                onClick={() => void resolve(item, "rejected")}
              >
                Отклонить
              </button>
              <button
                className="button button--ghost"
                type="button"
                disabled={busyId === item.review_case_id}
                onClick={() => void resolve(item, "ignored")}
              >
                Игнорировать
              </button>
            </div>
          </article>
        ))}
      </div>
    </section>
  );
}

function caseTitle(item: ReviewCaseItemDto): string {
  const value = item.metadata.title;
  if (typeof value === "string" && value.trim()) return value;
  return item.canonical_name ?? "Кейс ручной проверки";
}

function caseSummary(item: ReviewCaseItemDto): string {
  const value = item.metadata.summary;
  if (typeof value === "string" && value.trim()) return value;
  return "Проверьте спорные данные и выберите решение для пайплайна.";
}

function formatDateTime(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("ru-RU", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}
