import { useEffect, useMemo, useState } from "react";

import { useAuth } from "../shared/auth";
import { useFrontendRuntime } from "../shared/runtime";
import type { UniversityCardDto, AdmissionProgramDto } from "../shared/backend-api/types";

// ── Helpers ───────────────────────────────────────────────────────────────────

function strVal(v: unknown): string | null {
  if (typeof v === "string" && v.trim()) return v.trim();
  if (typeof v === "number") return String(v);
  return null;
}

function computeMetrics(card: UniversityCardDto) {
  const programs = card.admission?.programs ?? [];
  const budgetTotal = programs.reduce((s: number, p: AdmissionProgramDto) => s + (p.budget_places ?? 0), 0);
  const withScore = programs.filter((p: AdmissionProgramDto) => p.passing_score != null);
  const avgScore =
    withScore.length > 0
      ? Math.round((withScore.reduce((s: number, p: AdmissionProgramDto) => s + (p.passing_score ?? 0), 0) / withScore.length) * 10) / 10
      : null;
  const directions = new Set(
    programs
      .map((p: AdmissionProgramDto) => (p.code ?? "").split(".")[0])
      .filter(Boolean),
  );
  return {
    budgetPlaces: budgetTotal,
    programCount: programs.length,
    directionCount: directions.size,
    avgScore,
  };
}

function initials(card: UniversityCardDto): string {
  const n = card.aliases?.[0] ?? strVal(card.canonical_name?.value) ?? "";
  return n.split(/\s+/).slice(0, 2).map((w: string) => w[0]?.toUpperCase() ?? "").join("");
}

// ── Comparison bar ────────────────────────────────────────────────────────────

function ComparisonBar({
  label,
  leftVal,
  rightVal,
  leftLabel,
  rightLabel,
  format = (n: number) => n.toLocaleString("ru-RU"),
}: {
  label: string;
  leftVal: number | null;
  rightVal: number | null;
  leftLabel: string;
  rightLabel: string;
  format?: (n: number) => string;
}) {
  const max = Math.max(leftVal ?? 0, rightVal ?? 0, 1);
  const leftPct = leftVal != null ? Math.round((leftVal / max) * 100) : 0;
  const rightPct = rightVal != null ? Math.round((rightVal / max) * 100) : 0;

  return (
    <div className="cmp2-bar-row">
      <div className="cmp2-bar-row__label">{label}</div>
      <div className="cmp2-bar-row__cols">
        <div className="cmp2-bar-row__left">
          <span className="cmp2-bar-row__val cmp2-bar-row__val--left">
            {leftVal != null ? format(leftVal) : "—"}
          </span>
          <div className="cmp2-bar-row__track">
            <div
              className="cmp2-bar-row__fill cmp2-bar-row__fill--left"
              style={{ width: `${leftPct}%` }}
            />
          </div>
        </div>

        <div className="cmp2-bar-row__divider">
          <span className="cmp2-bar-row__uni cmp2-bar-row__uni--left">{leftLabel}</span>
          <span className="cmp2-bar-row__vs">VS</span>
          <span className="cmp2-bar-row__uni cmp2-bar-row__uni--right">{rightLabel}</span>
        </div>

        <div className="cmp2-bar-row__right">
          <div className="cmp2-bar-row__track">
            <div
              className="cmp2-bar-row__fill cmp2-bar-row__fill--right"
              style={{ width: `${rightPct}%` }}
            />
          </div>
          <span className="cmp2-bar-row__val cmp2-bar-row__val--right">
            {rightVal != null ? format(rightVal) : "—"}
          </span>
        </div>
      </div>
    </div>
  );
}

// ── University hero card ──────────────────────────────────────────────────────

function UniversityHeroCard({
  item,
  onRemove,
  onOpen,
}: {
  item: { id: string; card: UniversityCardDto | null; loading: boolean; error: string | null };
  onRemove: () => void;
  onOpen: () => void;
}) {
  const { card, loading, error } = item;

  if (loading) {
    return (
      <div className="cmp2-ucard cmp2-ucard--loading">
        <div className="cmp2-ucard__skeleton" />
      </div>
    );
  }

  if (error || !card) {
    return (
      <div className="cmp2-ucard cmp2-ucard--error">
        <p>Не удалось загрузить</p>
        <button className="cmp2-ucard__remove" type="button" onClick={onRemove}>✕</button>
      </div>
    );
  }

  const logoUrl = card.contacts?.logo_url ?? null;
  const shortName = card.aliases?.[0] ?? null;
  const fullName = strVal(card.canonical_name?.value) ?? "Вуз";
  const city = card.location?.city ?? null;
  const instType = card.institutional?.type ?? null;
  const rating = card.reviews?.rating ?? null;
  const ratingCount = card.reviews?.rating_count ?? null;
  const metrics = computeMetrics(card);
  const abbr = initials(card);

  return (
    <div className="cmp2-ucard">
      <button className="cmp2-ucard__remove" type="button" title="Убрать из сравнения" onClick={onRemove}>✕</button>

      <div className="cmp2-ucard__top">
        <div className="cmp2-ucard__logo">
          {logoUrl ? (
            <img src={logoUrl} alt={shortName ?? fullName} className="cmp2-ucard__logo-img" />
          ) : (
            <span className="cmp2-ucard__logo-abbr">{abbr}</span>
          )}
        </div>

        <div className="cmp2-ucard__info">
          <button className="cmp2-ucard__name" type="button" onClick={onOpen}>
            {shortName ?? fullName}
          </button>
          {shortName && <p className="cmp2-ucard__full-name">{fullName}</p>}
          <div className="cmp2-ucard__meta">
            {instType && <span className="cmp2-ucard__tag">{instType}</span>}
            {city && <span className="cmp2-ucard__tag cmp2-ucard__tag--muted">{city}</span>}
          </div>
        </div>

        {rating != null && (
          <div className="cmp2-ucard__rating">
            <span className="cmp2-ucard__rating-val">{rating}</span>
            <span className="cmp2-ucard__rating-max">/10</span>
            {ratingCount != null && (
              <span className="cmp2-ucard__rating-count">
                {ratingCount.toLocaleString("ru-RU")} отзывов
              </span>
            )}
          </div>
        )}
      </div>

      <div className="cmp2-ucard__stats">
        <div className="cmp2-ucard__stat">
          <span className="cmp2-ucard__stat-val">{metrics.budgetPlaces.toLocaleString("ru-RU")}</span>
          <span className="cmp2-ucard__stat-lbl">Бюджет</span>
        </div>
        <div className="cmp2-ucard__stat">
          <span className="cmp2-ucard__stat-val">{metrics.directionCount}</span>
          <span className="cmp2-ucard__stat-lbl">Направления</span>
        </div>
        <div className="cmp2-ucard__stat">
          <span className="cmp2-ucard__stat-val">
            {metrics.avgScore != null ? metrics.avgScore : "—"}
          </span>
          <span className="cmp2-ucard__stat-lbl">Ср. балл</span>
        </div>
        <div className="cmp2-ucard__stat">
          <span className="cmp2-ucard__stat-val">{metrics.programCount}</span>
          <span className="cmp2-ucard__stat-lbl">Программы</span>
        </div>
      </div>
    </div>
  );
}

// ── Base parameters table ─────────────────────────────────────────────────────

const BASE_ROWS: Array<{ label: string; get: (c: UniversityCardDto) => string }> = [
  { label: "Город", get: (c) => c.location?.city ?? "—" },
  { label: "Тип вуза", get: (c) => c.institutional?.type ?? "—" },
  { label: "Категория", get: (c) => c.institutional?.category ? `Категория ${c.institutional.category}` : "—" },
  { label: "Год основания", get: (c) => c.institutional?.founded_year ? String(c.institutional.founded_year) : "—" },
  { label: "Головной", get: (c) => c.institutional?.is_flagship ? "Да" : "—" },
  { label: "Сайт", get: (c) => c.contacts?.website ?? "—" },
  { label: "Программы", get: (c) => String(c.admission?.programs?.length ?? 0) },
];

// ── Programs comparison ───────────────────────────────────────────────────────

function ProgramsComparison({
  leftCard,
  rightCard,
  leftLabel,
  rightLabel,
}: {
  leftCard: UniversityCardDto;
  rightCard: UniversityCardDto;
  leftLabel: string;
  rightLabel: string;
}) {
  const leftCodes = new Set(
    (leftCard.admission?.programs ?? []).map((p: AdmissionProgramDto) => p.code).filter(Boolean),
  );
  const rightCodes = new Set(
    (rightCard.admission?.programs ?? []).map((p: AdmissionProgramDto) => p.code).filter(Boolean),
  );
  const allCodes = [...new Set([...leftCodes, ...rightCodes])].sort();

  if (allCodes.length === 0) return <p className="cmp2-empty-text">Программы не найдены.</p>;

  return (
    <div className="cmp2-programs">
      <div className="cmp2-programs__header">
        <span className="cmp2-programs__col-code">Код</span>
        <span className="cmp2-programs__col-label">{leftLabel}</span>
        <span className="cmp2-programs__col-label">{rightLabel}</span>
      </div>
      {allCodes.slice(0, 30).map((code) => {
        const lp = (leftCard.admission?.programs ?? []).find((p: AdmissionProgramDto) => p.code === code);
        const rp = (rightCard.admission?.programs ?? []).find((p: AdmissionProgramDto) => p.code === code);
        return (
          <div key={code} className="cmp2-programs__row">
            <span className="cmp2-programs__code">{code}</span>
            <span className={`cmp2-programs__cell ${lp ? "cmp2-programs__cell--has" : "cmp2-programs__cell--no"}`}>
              {lp ? (lp.budget_places != null ? `${lp.budget_places} бюдж.` : "Есть") : "—"}
            </span>
            <span className={`cmp2-programs__cell ${rp ? "cmp2-programs__cell--has" : "cmp2-programs__cell--no"}`}>
              {rp ? (rp.budget_places != null ? `${rp.budget_places} бюдж.` : "Есть") : "—"}
            </span>
          </div>
        );
      })}
      {allCodes.length > 30 && (
        <p className="cmp2-programs__more">…и ещё {allCodes.length - 30} программ</p>
      )}
    </div>
  );
}

// ── Main page ─────────────────────────────────────────────────────────────────

interface ComparedUniversity {
  id: string;
  card: UniversityCardDto | null;
  loading: boolean;
  error: string | null;
}

type CmpTab = "overview" | "admission" | "programs";

const CMP_TABS: Array<{ id: CmpTab; label: string }> = [
  { id: "overview", label: "Обзор" },
  { id: "admission", label: "Статистика приёма" },
  { id: "programs", label: "Направления подготовки" },
];

export function ComparisonPage({ onShowLogin }: { onShowLogin: () => void }) {
  const { user, token } = useAuth();
  const { backendApi } = useFrontendRuntime();

  const [ids, setIds] = useState<string[]>([]);
  const [items, setItems] = useState<ComparedUniversity[]>([]);
  const [listLoading, setListLoading] = useState(false);
  const [tab, setTab] = useState<CmpTab>("overview");

  // Load comparison list
  useEffect(() => {
    if (!user) return;
    setListLoading(true);
    backendApi
      .getComparisons()
      .then((res) => {
        const newIds = res.items.map((i) => i.university_id);
        setIds(newIds);
        setItems(newIds.map((id) => ({ id, card: null, loading: true, error: null })));
      })
      .catch(() => {})
      .finally(() => setListLoading(false));
  }, [user, token]);

  // Load each card
  useEffect(() => {
    if (ids.length === 0) return;
    ids.forEach((id) => {
      backendApi
        .getUniversityCard(id)
        .then((card) =>
          setItems((prev) => prev.map((it) => (it.id === id ? { ...it, card, loading: false } : it))),
        )
        .catch((e: unknown) =>
          setItems((prev) =>
            prev.map((it) =>
              it.id === id ? { ...it, loading: false, error: e instanceof Error ? e.message : "Ошибка" } : it,
            ),
          ),
        );
    });
  }, [ids]);

  const handleRemove = async (id: string) => {
    await backendApi.removeComparison(id).catch(() => {});
    setIds((prev) => prev.filter((x) => x !== id));
    setItems((prev) => prev.filter((it) => it.id !== id));
  };

  const openUniversity = (id: string) => {
    const url = new URL(window.location.href);
    url.searchParams.set("university_id", id);
    window.history.replaceState({}, "", url.toString());
    window.location.hash = "university";
  };

  // ── Derived ────────────────────────────────────────────────────────────────

  const loadedItems = useMemo(() => items.filter((it) => it.card != null), [items]);
  const [left, right] = loadedItems;
  const canCompare = loadedItems.length >= 2 && left?.card && right?.card;

  const leftLabel = useMemo(
    () => left?.card?.aliases?.[0] ?? strVal(left?.card?.canonical_name?.value) ?? "Вуз 1",
    [left],
  );
  const rightLabel = useMemo(
    () => right?.card?.aliases?.[0] ?? strVal(right?.card?.canonical_name?.value) ?? "Вуз 2",
    [right],
  );

  const pageTitle = useMemo(() => {
    if (loadedItems.length === 0) return "Сравнение вузов";
    if (loadedItems.length === 1) return `${leftLabel} — сравнение`;
    return `${leftLabel} или ${rightLabel} — сравнение вузов`;
  }, [loadedItems.length, leftLabel, rightLabel]);

  // ── Empty states ───────────────────────────────────────────────────────────

  if (!user) {
    return (
      <div className="cmp2-page">
        <h1 className="cmp2-page__title">Сравнение вузов</h1>
        <div className="cmp2-empty">
          <div className="cmp2-empty__icon">⚖</div>
          <p className="cmp2-empty__text">Войдите, чтобы добавлять вузы в сравнение.</p>
          <button className="btn btn--primary" type="button" onClick={onShowLogin}>Войти</button>
        </div>
      </div>
    );
  }

  if (listLoading) {
    return (
      <div className="cmp2-page">
        <h1 className="cmp2-page__title">Сравнение вузов</h1>
        <p className="cmp2-loading">Загружаем список сравнения…</p>
      </div>
    );
  }

  if (items.length === 0) {
    return (
      <div className="cmp2-page">
        <h1 className="cmp2-page__title">Сравнение вузов</h1>
        <div className="cmp2-empty">
          <div className="cmp2-empty__icon">⚖</div>
          <p className="cmp2-empty__text">Список сравнения пуст</p>
          <p className="cmp2-empty__hint">Нажмите ⚖ на странице вуза, чтобы добавить его в сравнение.</p>
          <button className="btn btn--primary" type="button" onClick={() => (window.location.hash = "search")}>
            Перейти к поиску
          </button>
        </div>
      </div>
    );
  }

  // ── Main render ─────────────────────────────────────────────────────────────

  return (
    <div className="cmp2-page">
      {/* Page header */}
      <div className="cmp2-page__header">
        <h1 className="cmp2-page__title">{pageTitle}</h1>
        {canCompare && (
          <p className="cmp2-page__subtitle">
            Сравните основные параметры университетов и выберите лучший вариант для поступления
          </p>
        )}
      </div>

      {/* University hero cards */}
      <div className="cmp2-heroes">
        {items.map((it, idx) => (
          <div key={it.id} className="cmp2-heroes__slot">
            <UniversityHeroCard
              item={it}
              onRemove={() => handleRemove(it.id)}
              onOpen={() => openUniversity(it.id)}
            />
            {idx < items.length - 1 && items.length === 2 && (
              <div className="cmp2-heroes__vs">VS</div>
            )}
          </div>
        ))}
      </div>

      {/* Only show detailed comparison if we have 2 loaded cards */}
      {!canCompare && (
        <div className="cmp2-hint">
          <p>Добавьте ещё {2 - loadedItems.length} вуз(а) для сравнения.</p>
          <button className="btn btn--ghost" type="button" onClick={() => (window.location.hash = "search")}>
            Перейти к поиску
          </button>
        </div>
      )}

      {canCompare && left.card && right.card && (
        <>
          {/* Tabs */}
          <div className="cmp2-tabs" role="tablist">
            {CMP_TABS.map((t) => (
              <button
                key={t.id}
                className={`cmp2-tabs__tab${tab === t.id ? " cmp2-tabs__tab--active" : ""}`}
                role="tab"
                type="button"
                onClick={() => setTab(t.id)}
              >
                {t.label}
              </button>
            ))}
          </div>

          {/* Overview */}
          {tab === "overview" && (
            <>
              {/* Ключевые различия */}
              <div className="cmp2-section">
                <h2 className="cmp2-section__title">Ключевые различия</h2>
                <div className="cmp2-bars">
                  <ComparisonBar
                    label="Бюджетные места"
                    leftVal={computeMetrics(left.card).budgetPlaces}
                    rightVal={computeMetrics(right.card).budgetPlaces}
                    leftLabel={leftLabel}
                    rightLabel={rightLabel}
                  />
                  <ComparisonBar
                    label="Количество программ"
                    leftVal={computeMetrics(left.card).programCount}
                    rightVal={computeMetrics(right.card).programCount}
                    leftLabel={leftLabel}
                    rightLabel={rightLabel}
                  />
                  <ComparisonBar
                    label="Направления"
                    leftVal={computeMetrics(left.card).directionCount}
                    rightVal={computeMetrics(right.card).directionCount}
                    leftLabel={leftLabel}
                    rightLabel={rightLabel}
                  />
                  {(computeMetrics(left.card).avgScore != null || computeMetrics(right.card).avgScore != null) && (
                    <ComparisonBar
                      label="Средний проходной балл"
                      leftVal={computeMetrics(left.card).avgScore}
                      rightVal={computeMetrics(right.card).avgScore}
                      leftLabel={leftLabel}
                      rightLabel={rightLabel}
                    />
                  )}
                </div>
              </div>

              {/* Базовые параметры */}
              <div className="cmp2-section">
                <h2 className="cmp2-section__title">Базовые параметры</h2>
                <div className="cmp2-params">
                  <div className="cmp2-params__header">
                    <span className="cmp2-params__label-col">Параметр</span>
                    <span className="cmp2-params__val-col">{leftLabel}</span>
                    <span className="cmp2-params__val-col">{rightLabel}</span>
                  </div>
                  {BASE_ROWS.map((row) => {
                    const lv = left.card ? row.get(left.card) : "—";
                    const rv = right.card ? row.get(right.card) : "—";
                    const differs = lv !== rv && lv !== "—" && rv !== "—";
                    return (
                      <div key={row.label} className={`cmp2-params__row${differs ? " cmp2-params__row--diff" : ""}`}>
                        <span className="cmp2-params__label">{row.label}</span>
                        <span className="cmp2-params__val">
                          {row.label === "Сайт" && lv !== "—" ? (
                            <a href={lv} target="_blank" rel="noopener noreferrer" className="cmp2-link">{lv}</a>
                          ) : lv}
                        </span>
                        <span className="cmp2-params__val">
                          {row.label === "Сайт" && rv !== "—" ? (
                            <a href={rv} target="_blank" rel="noopener noreferrer" className="cmp2-link">{rv}</a>
                          ) : rv}
                        </span>
                      </div>
                    );
                  })}
                </div>
              </div>
            </>
          )}

          {/* Admission stats */}
          {tab === "admission" && (
            <div className="cmp2-section">
              <h2 className="cmp2-section__title">Статистика приёма</h2>
              <div className="cmp2-bars">
                {[
                  { label: "Бюджетных мест всего", key: "budgetPlaces" as const },
                  { label: "Программ всего", key: "programCount" as const },
                  { label: "Направлений подготовки", key: "directionCount" as const },
                ].map(({ label, key }) => (
                  <ComparisonBar
                    key={key}
                    label={label}
                    leftVal={computeMetrics(left.card!)[key] as number}
                    rightVal={computeMetrics(right.card!)[key] as number}
                    leftLabel={leftLabel}
                    rightLabel={rightLabel}
                  />
                ))}
                <ComparisonBar
                  label="Средний проходной балл"
                  leftVal={computeMetrics(left.card).avgScore}
                  rightVal={computeMetrics(right.card).avgScore}
                  leftLabel={leftLabel}
                  rightLabel={rightLabel}
                />
              </div>
            </div>
          )}

          {/* Programs */}
          {tab === "programs" && (
            <div className="cmp2-section">
              <h2 className="cmp2-section__title">Направления подготовки</h2>
              <ProgramsComparison
                leftCard={left.card}
                rightCard={right.card}
                leftLabel={leftLabel}
                rightLabel={rightLabel}
              />
            </div>
          )}
        </>
      )}
    </div>
  );
}
