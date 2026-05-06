import { useEffect, useMemo, useState } from "react";

import { useAuth } from "../shared/auth";
import { useFrontendRuntime } from "../shared/runtime";
import { useSelectedUniversity } from "../shared/selected-university";
import type { AdmissionProgramDto, UniversityCardDto, UniversityProvenanceDto } from "../shared/backend-api/types";

// ── Direction name mapping (Russian UGNS) ──────────────────────────────────────
const UGNS: Record<string, string> = {
  "01": "Математика и механика",
  "02": "Компьютерные и информационные науки",
  "03": "Физика и астрономия",
  "04": "Химия",
  "05": "Науки о Земле",
  "06": "Биологические науки",
  "07": "Архитектура",
  "08": "Техника и технологии строительства",
  "09": "Информатика и вычислительная техника",
  "10": "Информационная безопасность",
  "11": "Электроника, радиотехника и системы связи",
  "12": "Фотоника, приборостроение, оптические системы",
  "13": "Электро- и теплоэнергетика",
  "14": "Ядерная энергетика и технологии",
  "15": "Машиностроение",
  "16": "Физико-технические науки и технологии",
  "18": "Химические технологии",
  "19": "Промышленная экология и биотехнологии",
  "20": "Техносферная безопасность",
  "21": "Прикладная геология и горное дело",
  "22": "Технологии материалов",
  "23": "Наземный транспорт",
  "24": "Авиационная и ракетно-космическая техника",
  "27": "Управление в технических системах",
  "38": "Экономика и управление",
  "39": "Социология и социальная работа",
  "40": "Юриспруденция",
  "41": "Политические науки и регионоведение",
  "42": "СМИ и библиотечное дело",
  "43": "Сервис и туризм",
  "44": "Образование и педагогические науки",
  "45": "Языкознание и литературоведение",
  "46": "История и археология",
  "47": "Философия, этика и религиоведение",
  "49": "Физическая культура и спорт",
  "51": "Культуроведение",
  "53": "Музыкальное искусство",
  "54": "Изобразительные и прикладные искусства",
};

function getDirectionGroup(code: string | null): string {
  if (!code) return "00";
  return code.split(".")[0].padStart(2, "0");
}

function getLevel(code: string | null): string {
  if (!code) return "Бакалавриат";
  const parts = code.split(".");
  if (parts.length < 2) return "Бакалавриат";
  const sub = parts[1];
  if (sub === "04") return "Магистратура";
  if (sub === "05") return "Специалитет";
  if (sub === "06") return "Аспирантура";
  return "Бакалавриат";
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function strVal(v: unknown): string | null {
  if (typeof v === "string" && v.trim()) return v.trim();
  if (typeof v === "number") return String(v);
  return null;
}

function computeMetrics(programs: AdmissionProgramDto[]) {
  const budgetTotal = programs.reduce((s, p) => s + (p.budget_places ?? 0), 0);
  const withScore = programs.filter((p) => p.passing_score != null);
  const avgScore =
    withScore.length > 0
      ? Math.round(
          (withScore.reduce((s, p) => s + (p.passing_score ?? 0), 0) / withScore.length) * 10,
        ) / 10
      : null;
  const directions = new Set(programs.map((p) => getDirectionGroup(p.code)));
  directions.delete("00");
  return {
    budgetPlaces: budgetTotal,
    programCount: programs.length,
    directionCount: directions.size,
    avgScore,
  };
}

function formatNum(n: number): string {
  return n.toLocaleString("ru-RU");
}

// ── Tab type ──────────────────────────────────────────────────────────────────

type Tab = "about" | "programs" | "admission" | "students" | "reviews";

const TABS: Array<{ id: Tab; label: string }> = [
  { id: "about", label: "О вузе" },
  { id: "programs", label: "Направления и программы" },
  { id: "admission", label: "Поступление" },
  { id: "students", label: "Студенты" },
  { id: "reviews", label: "Отзывы" },
];

// ── Provenance dot ────────────────────────────────────────────────────────────

function ProvenanceDot({ url }: { url: string }) {
  let hostname = url;
  try { hostname = new URL(url).hostname.replace(/^www\./, ""); } catch { /* */ }
  return (
    <a
      href={url}
      target="_blank"
      rel="noopener noreferrer"
      className="prov-dot"
      title={`Источник: ${hostname}`}
      onClick={(e) => e.stopPropagation()}
    />
  );
}

// ── Skeleton ──────────────────────────────────────────────────────────────────

function Skeleton({ h = 16, w = "100%" }: { h?: number; w?: string | number }) {
  return (
    <div
      className="skeleton"
      style={{ height: h, width: w, borderRadius: 6 }}
    />
  );
}

// ── Favorite / Compare buttons ────────────────────────────────────────────────

function AuthPrompt({ onLogin }: { onLogin: () => void }) {
  return (
    <div className="auth-prompt">
      <p>Войдите, чтобы использовать эту функцию</p>
      <button className="btn btn--primary btn--sm" type="button" onClick={onLogin}>
        Войти
      </button>
    </div>
  );
}

// ── Direction accordion ───────────────────────────────────────────────────────

function DirectionAccordion({
  code,
  name,
  programs,
}: {
  code: string;
  name: string;
  programs: AdmissionProgramDto[];
}) {
  const [open, setOpen] = useState(false);
  return (
    <div className="direction-accordion">
      <button
        className="direction-accordion__header"
        type="button"
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
      >
        <span className={`direction-accordion__toggle${open ? " direction-accordion__toggle--open" : ""}`}>
          ▶
        </span>
        <span className="direction-accordion__name">
          {code !== "00" ? `${code} ` : ""}
          {name}
        </span>
        <span className="direction-accordion__badge">{programs.length} программ</span>
      </button>

      {open && (
        <div className="direction-accordion__body">
          <div className="program-table-header">
            <span>Код</span>
            <span>Название</span>
            <span>Форма</span>
            <span>Уровень</span>
            <span>Бюджет</span>
            <span>Балл</span>
          </div>
          {programs.map((p, i) => (
            <div className="program-row" key={p.field_name ?? i}>
              <span className="program-row__code">{p.code ?? "—"}</span>
              <span className="program-row__name">{p.name ?? "—"}</span>
              <span className="program-row__form">
                {p.study_form === "full_time" ? "Очная"
                  : p.study_form === "evening" ? "Вечерняя"
                  : p.study_form === "distance" ? "Заочная"
                  : p.study_form === "mixed" ? "Очно-заочная"
                  : "—"}
              </span>
              <span className="program-row__level">{p.level ?? getLevel(p.code)}</span>
              <span className="program-row__budget">
                {p.budget_places != null ? p.budget_places : "—"}
              </span>
              <span className="program-row__score">
                {p.passing_score != null ? p.passing_score : "—"}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ── Main page ─────────────────────────────────────────────────────────────────

export function UniversityDetailPage({
  onShowLogin,
}: {
  onShowLogin: () => void;
}) {
  const { activeUniversityId } = useSelectedUniversity();
  const { backendApi } = useFrontendRuntime();
  const { user, token } = useAuth();

  const [card, setCard] = useState<UniversityCardDto | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [provenance, setProvenance] = useState<UniversityProvenanceDto | null>(null);

  const [tab, setTab] = useState<Tab>("about");
  const [programSearch, setProgramSearch] = useState("");
  const [showAll, setShowAll] = useState(false);

  const [isFavorite, setIsFavorite] = useState(false);
  const [isCompared, setIsCompared] = useState(false);
  const [actionLoading, setActionLoading] = useState(false);

  // Load card
  useEffect(() => {
    if (!activeUniversityId) return;
    setLoading(true);
    setError(null);
    setCard(null);
    setProvenance(null);
    backendApi
      .getUniversityCard(activeUniversityId)
      .then((c) => {
        setCard(c);
        setIsFavorite(c.is_favorite);
        setIsCompared(c.is_compared);
        // Load provenance lazily after card is shown
        backendApi.getUniversityProvenance(activeUniversityId).then(setProvenance).catch(() => {});
      })
      .catch((e: unknown) => setError(e instanceof Error ? e.message : "Ошибка загрузки."))
      .finally(() => setLoading(false));
  }, [activeUniversityId, token]);

  // ── Derived data ────────────────────────────────────────────────────────────

  const programs = card?.admission?.programs ?? [];
  const metrics = useMemo(() => computeMetrics(programs), [programs]);

  const name = strVal(card?.canonical_name?.value) ?? "Вуз";
  const shortName = card?.aliases?.[0] ?? null;
  const city = card?.location?.city ?? null;
  const country = card?.location?.country ?? null;
  const website = card?.contacts?.website ?? null;
  const logoUrl = card?.contacts?.logo_url ?? null;
  const phone = card?.contacts?.phones?.[0] ?? null;
  const address = card?.location?.address ?? null;
  const foundedYear = card?.institutional?.founded_year ?? null;
  const instType = card?.institutional?.type ?? null;
  const category = card?.institutional?.category ?? null;
  const isFlagship = card?.institutional?.is_flagship ?? false;
  const description = card?.description ?? card?.reviews?.summary ?? null;
  const reviewRating = card?.reviews?.rating ?? null;
  const reviewRatingCount = card?.reviews?.rating_count ?? null;
  const reviewItems = card?.reviews?.items ?? [];

  const rating = reviewRating != null
    ? String(reviewRating)
    : (card?.ratings?.[0]?.value ?? null);

  // ── Program groups ──────────────────────────────────────────────────────────

  const filteredPrograms = useMemo(() => {
    const q = programSearch.trim().toLowerCase();
    if (!q) return programs;
    return programs.filter(
      (p) =>
        (p.name ?? "").toLowerCase().includes(q) ||
        (p.code ?? "").toLowerCase().includes(q),
    );
  }, [programs, programSearch]);

  const directionGroups = useMemo(() => {
    const groups = new Map<string, AdmissionProgramDto[]>();
    for (const p of filteredPrograms) {
      const g = getDirectionGroup(p.code);
      if (!groups.has(g)) groups.set(g, []);
      groups.get(g)!.push(p);
    }
    return Array.from(groups.entries()).sort(([a], [b]) => a.localeCompare(b));
  }, [filteredPrograms]);

  const visibleGroups = showAll ? directionGroups : directionGroups.slice(0, 8);

  // ── Favorite / Compare handlers ─────────────────────────────────────────────

  const handleToggleFavorite = async () => {
    if (!user) { onShowLogin(); return; }
    if (!card) return;
    setActionLoading(true);
    try {
      if (isFavorite) {
        await backendApi.removeFavorite(card.university_id);
        setIsFavorite(false);
      } else {
        await backendApi.addFavorite(card.university_id);
        setIsFavorite(true);
      }
    } finally {
      setActionLoading(false);
    }
  };

  const handleToggleCompare = async () => {
    if (!user) { onShowLogin(); return; }
    if (!card) return;
    setActionLoading(true);
    try {
      if (isCompared) {
        await backendApi.removeComparison(card.university_id);
        setIsCompared(false);
      } else {
        await backendApi.addComparison(card.university_id);
        setIsCompared(true);
      }
    } finally {
      setActionLoading(false);
    }
  };

  // ── Initials for logo placeholder ───────────────────────────────────────────

  const initials = useMemo(() => {
    const n = shortName ?? name;
    return n
      .split(/\s+/)
      .slice(0, 2)
      .map((w) => w[0]?.toUpperCase() ?? "")
      .join("");
  }, [name, shortName]);

  // ── Field → source URL map (from provenance) ────────────────────────────────

  const fieldSourceMap = useMemo(() => {
    if (!provenance) return new Map<string, string>();
    const claimToUrl = new Map<string, string>();
    for (const ev of provenance.claim_evidence) {
      if (ev.source_url) claimToUrl.set(ev.claim_id, ev.source_url);
    }
    const map = new Map<string, string>();
    for (const fact of provenance.resolved_facts) {
      for (const claimId of fact.selected_claim_ids) {
        const url = claimToUrl.get(claimId);
        if (url) { map.set(fact.field_name, url); break; }
      }
    }
    return map;
  }, [provenance]);

  // ── Render guards ────────────────────────────────────────────────────────────

  if (!activeUniversityId) {
    return (
      <div className="ud-empty">
        <p>Вуз не выбран. <button type="button" onClick={() => (window.location.hash = "search")}>← Вернуться в поиск</button></p>
      </div>
    );
  }

  if (loading) {
    return (
      <div className="ud-page">
        <div className="ud-hero ud-hero--skeleton">
          <Skeleton h={80} w={80} />
          <div style={{ flex: 1 }}>
            <Skeleton h={28} w="60%" />
            <div style={{ marginTop: 8 }}><Skeleton h={16} w="40%" /></div>
            <div style={{ marginTop: 12 }}><Skeleton h={14} w="90%" /></div>
          </div>
        </div>
        <div className="ud-metrics">
          {[1, 2, 3, 4, 5].map((i) => (
            <div key={i} className="ud-metric"><Skeleton h={32} w={60} /><Skeleton h={12} w={80} /></div>
          ))}
        </div>
      </div>
    );
  }

  if (error || !card) {
    return (
      <div className="ud-empty">
        <p>{error ?? "Карточка вуза не найдена."}</p>
        <button
          className="btn btn--ghost"
          type="button"
          onClick={() => (window.location.hash = "search")}
        >
          ← Назад
        </button>
      </div>
    );
  }

  // ── Full render ──────────────────────────────────────────────────────────────

  return (
    <div className="ud-page">
      {/* Back button */}
      <button
        className="ud-back"
        type="button"
        onClick={() => (window.location.hash = "search")}
      >
        ← Назад к поиску
      </button>

      {/* ── Hero card ── */}
      <div className="ud-hero">
        <div className="ud-hero__logo">
          {logoUrl ? (
            <img src={logoUrl} alt={shortName ?? name} className="ud-hero__logo-img" />
          ) : (
            initials
          )}
        </div>

        <div className="ud-hero__content">
          <h1 className="ud-hero__name">{shortName ?? name}</h1>
          {shortName && <p className="ud-hero__full-name">{name}</p>}

          <div className="ud-hero__tags">
            {instType && (
              <span className="ud-tag ud-tag--green">{instType}</span>
            )}
            {isFlagship && (
              <span className="ud-tag ud-tag--blue">Головной</span>
            )}
            {category && (
              <span className="ud-tag ud-tag--orange">Категория {category}</span>
            )}
            {city && <span className="ud-tag ud-tag--gray">{city}</span>}
            {country && country !== "Russia" && (
              <span className="ud-tag ud-tag--gray">{country}</span>
            )}
          </div>

          {description && <p className="ud-hero__description">{description}</p>}
        </div>

        <div className="ud-hero__right">
          {rating && (
            <div className="ud-hero__rating">
              {rating}
              {reviewRatingCount && (
                <span className="ud-hero__rating-count">
                  {reviewRatingCount.toLocaleString("ru-RU")} отзывов
                </span>
              )}
            </div>
          )}

          <div className="ud-hero__score">
            <span className="ud-hero__score-value">
              {metrics.avgScore != null ? metrics.avgScore : "—"}
            </span>
            <span className="ud-hero__score-label">средний балл</span>
          </div>

          <div className="ud-hero__actions">
            <button
              className={`ud-icon-btn${isFavorite ? " ud-icon-btn--active" : ""}`}
              type="button"
              title={isFavorite ? "Убрать из избранного" : "Добавить в избранное"}
              onClick={handleToggleFavorite}
              disabled={actionLoading}
              aria-label="Избранное"
            >
              {isFavorite ? "♥" : "♡"}
            </button>
            <button
              className={`ud-icon-btn${isCompared ? " ud-icon-btn--compare-active" : ""}`}
              type="button"
              title={isCompared ? "Убрать из сравнения" : "Добавить к сравнению"}
              onClick={handleToggleCompare}
              disabled={actionLoading}
              aria-label="Сравнить"
            >
              ⚖
            </button>
          </div>
        </div>
      </div>

      {/* ── Metrics row ── */}
      <div className="ud-metrics">
        <div className="ud-metric">
          <span className="ud-metric__value">{formatNum(metrics.budgetPlaces)}</span>
          <span className="ud-metric__label">Бюджетные места</span>
        </div>
        <div className="ud-metric">
          <span className="ud-metric__value">{metrics.programCount}</span>
          <span className="ud-metric__label">Программы</span>
        </div>
        <div className="ud-metric">
          <span className="ud-metric__value">{metrics.directionCount}</span>
          <span className="ud-metric__label">Направления</span>
        </div>
        <div className="ud-metric">
          <span className="ud-metric__value">
            {metrics.avgScore != null ? metrics.avgScore : "—"}
          </span>
          <span className="ud-metric__label">Средний проходной балл</span>
        </div>
        <div className="ud-metric">
          <span className="ud-metric__value">—</span>
          <span className="ud-metric__label">Средняя стоимость обучения</span>
        </div>
      </div>

      {/* ── Tabs ── */}
      <div className="ud-tabs" role="tablist">
        {TABS.map((t) => (
          <button
            key={t.id}
            className={`ud-tabs__tab${tab === t.id ? " ud-tabs__tab--active" : ""}`}
            role="tab"
            type="button"
            aria-selected={tab === t.id}
            onClick={() => setTab(t.id)}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* ── Tab content ── */}

      {tab === "about" && (
        <div className="ud-section">
          {description && (
            <div className="ud-about__description">{description}</div>
          )}
          <div className="ud-about__grid">
            <div className="ud-about__field">
              <span className="ud-about__label">
                Год основания
                {fieldSourceMap.has("institutional.founded_year") && (
                  <ProvenanceDot url={fieldSourceMap.get("institutional.founded_year")!} />
                )}
              </span>
              <span className="ud-about__value">{foundedYear ?? "—"}</span>
            </div>
            <div className="ud-about__field">
              <span className="ud-about__label">
                Телефон
                {fieldSourceMap.has("contacts.phones") && (
                  <ProvenanceDot url={fieldSourceMap.get("contacts.phones")!} />
                )}
              </span>
              <span className="ud-about__value">{phone ?? "—"}</span>
            </div>
            <div className="ud-about__field">
              <span className="ud-about__label">
                Сайт
                {fieldSourceMap.has("contacts.website") && (
                  <ProvenanceDot url={fieldSourceMap.get("contacts.website")!} />
                )}
              </span>
              <span className="ud-about__value">
                {website ? (
                  <a href={website} target="_blank" rel="noopener noreferrer">
                    {website}
                  </a>
                ) : (
                  "—"
                )}
              </span>
            </div>
            <div className="ud-about__field">
              <span className="ud-about__label">
                Город
                {fieldSourceMap.has("location.city") && (
                  <ProvenanceDot url={fieldSourceMap.get("location.city")!} />
                )}
              </span>
              <span className="ud-about__value">{address ?? city ?? "—"}</span>
            </div>
            <div className="ud-about__field">
              <span className="ud-about__label">
                Тип вуза
                {fieldSourceMap.has("institutional.type") && (
                  <ProvenanceDot url={fieldSourceMap.get("institutional.type")!} />
                )}
              </span>
              <span className="ud-about__value">{instType ?? "—"}</span>
            </div>
            <div className="ud-about__field">
              <span className="ud-about__label">
                Название
                {fieldSourceMap.has("canonical_name") && (
                  <ProvenanceDot url={fieldSourceMap.get("canonical_name")!} />
                )}
              </span>
              <span className="ud-about__value">{name}</span>
            </div>
          </div>
        </div>
      )}

      {tab === "programs" && (
        <div className="ud-section">
          <div className="ud-programs__header">
            <h2 className="ud-programs__title">Направления подготовки бакалавриата и специалиста</h2>
            <input
              className="ud-programs__search"
              type="search"
              placeholder="Поиск по направлениям..."
              value={programSearch}
              onChange={(e) => {
                setProgramSearch(e.target.value);
                setShowAll(false);
              }}
            />
          </div>

          {directionGroups.length === 0 && (
            <p className="ud-empty-text">Программы не найдены.</p>
          )}

          {visibleGroups.map(([code, group]) => (
            <DirectionAccordion
              key={code}
              code={code}
              name={UGNS[code] ?? `Группа ${code}`}
              programs={group}
            />
          ))}

          {!showAll && directionGroups.length > 8 && (
            <button
              className="ud-show-more"
              type="button"
              onClick={() => setShowAll(true)}
            >
              Показать ещё ({directionGroups.length - 8})
            </button>
          )}
        </div>
      )}

      {tab === "admission" && (
        <div className="ud-section ud-coming-soon">
          <p>Раздел «Поступление» находится в разработке.</p>
        </div>
      )}

      {tab === "students" && (
        <div className="ud-section ud-coming-soon">
          <p>Раздел «Студенты» находится в разработке.</p>
        </div>
      )}

      {tab === "reviews" && (
        <div className="ud-section">
          {reviewRating != null && (
            <div className="ud-reviews-summary">
              <span className="ud-reviews-summary__score">{reviewRating}</span>
              <span className="ud-reviews-summary__max">/10</span>
              {reviewRatingCount != null && (
                <span className="ud-reviews-summary__count">
                  {reviewRatingCount.toLocaleString("ru-RU")} оценок
                </span>
              )}
              <span className="ud-reviews-summary__source">по данным Табитуриент</span>
            </div>
          )}

          {reviewItems.length === 0 ? (
            <p className="ud-empty-text">Отзывы ещё не загружены.</p>
          ) : (
            <div className="ud-reviews-list">
              {reviewItems.map((review, i) => (
                <div className="ud-review-card" key={i}>
                  <div className="ud-review-card__meta">
                    {review.author_type && (
                      <span className="ud-review-card__author">{review.author_type}</span>
                    )}
                    {review.date && (
                      <span className="ud-review-card__date">{review.date}</span>
                    )}
                  </div>
                  <p className="ud-review-card__text">{review.text}</p>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
