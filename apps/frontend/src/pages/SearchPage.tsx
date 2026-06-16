import { useEffect, useState } from "react";

import { useUniversitySearch } from "../features/search";
import type { SearchSortBy } from "../features/search";
import { useAuth } from "../shared/auth";
import type { EgeSubjectDto } from "../shared/backend-api";
import { describeRequestError } from "../shared/http";
import { useFrontendRuntime } from "../shared/runtime";
import { GeoPickerDropdown } from "../shared/ui/GeoPickerDropdown";
import { ViewState } from "../shared/ui/view-state";

const popularDirections = {
  it: ["01.03.02", "02.03.02", "02.03.03", "09.03.01", "09.03.02", "09.03.03", "09.03.04", "10.03.01"],
  engineering: ["08.03.01", "11.03.01", "11.03.02", "12.03.01", "13.03.01", "13.03.02", "15.03.01", "15.03.04", "15.03.06", "27.03.04"],
  economy: ["38.03.01", "38.03.05", "38.03.06", "38.03.07"],
  medicine: ["31.05.01", "31.05.02", "31.05.03", "32.05.01", "33.05.01", "34.03.01"],
  management: ["38.03.02", "38.03.03", "38.03.04", "38.03.05", "27.03.05"],
  humanities: ["37.03.01", "39.03.01", "39.03.02", "40.03.01", "41.03.01", "41.03.05", "42.03.01", "42.03.02", "44.03.01", "45.03.01", "45.03.02", "46.03.01"],
} as const;

const DIRECTIONS: Array<{ key: keyof typeof popularDirections; label: string }> = [
  { key: "it", label: "IT и цифровые технологии" },
  { key: "engineering", label: "Инженерия" },
  { key: "economy", label: "Экономика" },
  { key: "medicine", label: "Медицина" },
  { key: "management", label: "Управление" },
  { key: "humanities", label: "Гуманитарные науки" },
];

type EgeScores = Record<string, string>;

const SORT_OPTIONS: Array<{ value: SearchSortBy; label: string }> = [
  { value: "rating", label: "По рейтингу" },
  { value: "budget_places", label: "По бюджетным местам" },
  { value: "avg_passing_score", label: "По проходному баллу" },
];

// ── EGE panel ─────────────────────────────────────────────────────────────────

function EgePanel({
  subjects,
  scores,
  checked,
  onToggle,
  onScore,
}: {
  subjects: EgeSubjectDto[];
  scores: EgeScores;
  checked: Set<string>;
  onToggle: (id: string) => void;
  onScore: (id: string, val: string) => void;
}) {
  return (
    <div className="ege-panel__grid">
        {subjects.map((s) => {
          const active = checked.has(s.id);
          return (
            <div
              key={s.id}
              className={`ege-card${active ? " ege-card--active" : ""}`}
              role="button"
              tabIndex={0}
              onClick={() => onToggle(s.id)}
              onKeyDown={(e) => { if (e.key === " " || e.key === "Enter") { e.preventDefault(); onToggle(s.id); } }}
            >
              <div className="ege-card__top">
                <span className={`ege-card__checkbox${active ? " ege-card__checkbox--checked" : ""}`} aria-hidden>
                  {active ? "✓" : ""}
                </span>
                <span className="ege-card__label">{s.label}</span>
              </div>
              <input
                className="ege-card__score"
                type="number"
                min={0}
                max={100}
                placeholder="Баллы"
                value={scores[s.id] ?? ""}
                onClick={(e) => e.stopPropagation()}
                onChange={(e) => { onScore(s.id, e.target.value); if (!checked.has(s.id)) onToggle(s.id); }}
              />
            </div>
          );
        })}
      </div>
  );
}

// ── Main page ─────────────────────────────────────────────────────────────────

export function SearchPage({ onShowLogin }: { onShowLogin?: () => void }) {
  const { backendApi } = useFrontendRuntime();
  const { user } = useAuth();
  const {
    query,
    setQuery,
    city,
    setCity,
    region,
    setRegion,
    page,
    setPage,
    pageSize,
    egeSubjects,
    setEgeSubjects,
    egeScores,
    setEgeScores,
    programCodes,
    setProgramCodes,
    dormitory,
    setDormitory,
    militaryDepartment,
    setMilitaryDepartment,
    sortBy,
    setSortBy,
    resetFilters,
    snapshot,
    error,
    loading,
  } = useUniversitySearch();

  const hasGeoFilter = region.trim().length > 0 || city.trim().length > 0;

  const [localQuery, setLocalQuery] = useState(query);
  const [showEge, setShowEge] = useState(false);
  const [showSortMenu, setShowSortMenu] = useState(false);
  const [egeSubjectsList, setEgeSubjectsList] = useState<EgeSubjectDto[]>([]);
  const [egeChecked, setEgeChecked] = useState<Set<string>>(new Set(egeSubjects));
  const [localEgeScores, setLocalEgeScores] = useState<EgeScores>(
    Object.fromEntries(Object.entries(egeScores).map(([subject, score]) => [subject, String(score)])),
  );

  useEffect(() => {
    backendApi.getEgeSubjects().then((res) => setEgeSubjectsList(res.subjects)).catch(() => {});
  }, [backendApi]);
  const [savingSearch, setSavingSearch] = useState(false);
  const [saveMessage, setSaveMessage] = useState<string | null>(null);
  const [showSaveModal, setShowSaveModal] = useState(false);
  const [saveName, setSaveName] = useState("");

  useEffect(() => {
    setLocalQuery(query);
  }, [query]);

  useEffect(() => {
    setEgeChecked(new Set(egeSubjects));
    setLocalEgeScores(
      Object.fromEntries(
        Object.entries(egeScores).map(([subject, score]) => [subject, String(score)]),
      ),
    );
  }, [egeScores, egeSubjects]);

  useEffect(() => {
    if (!showEge) return;
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") setShowEge(false); };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [showEge]);

  useEffect(() => {
    if (!showSortMenu) return;
    const onClick = () => setShowSortMenu(false);
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") setShowSortMenu(false); };
    document.addEventListener("click", onClick);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("click", onClick);
      document.removeEventListener("keydown", onKey);
    };
  }, [showSortMenu]);

  const hasResults = (snapshot?.items.length ?? 0) > 0;
  const hasQueryState =
    query.trim().length > 0 ||
    hasGeoFilter ||
    egeSubjects.length > 0 ||
    programCodes.length > 0 ||
    dormitory ||
    militaryDepartment;

  const handleSearch = () => {
    setProgramCodes(resolvePopularDirectionCodes(localQuery));
    setQuery(localQuery);
  };
  const handleKeyDown = (e: React.KeyboardEvent) => { if (e.key === "Enter") handleSearch(); };
  const handleDirectionClick = (direction: { key: keyof typeof popularDirections; label: string }) => {
    setLocalQuery(direction.label);
    setProgramCodes([...popularDirections[direction.key]]);
    setQuery(direction.label);
  };

  const handleOpenSaveModal = () => {
    if (!user) {
      onShowLogin?.();
      return;
    }
    setSaveName(query.trim().slice(0, 25));
    setSaveMessage(null);
    setShowSaveModal(true);
  };

  const handleSaveSearch = async () => {
    setSavingSearch(true);
    setSaveMessage(null);
    try {
      const existing = await backendApi.getSavedSearches();
      const isDuplicate = existing.items.some(
        (s) => s.query.trim() === query.trim() && (s.city ?? "") === (city ?? ""),
      );
      if (isDuplicate) {
        setSaveMessage("Такой поиск уже сохранён");
        setShowSaveModal(false);
        setSavingSearch(false);
        return;
      }
      await backendApi.createSavedSearch({
        name: saveName.trim() || query.trim().slice(0, 25),
        query,
        city: city || null,
        page_size: pageSize,
      });
      setSaveMessage("Поиск сохранён ✓");
      setShowSaveModal(false);
    } catch (e: unknown) {
      setSaveMessage(describeRequestError(e));
    } finally {
      setSavingSearch(false);
    }
  };

  const handleEgeToggle = (id: string) => {
    setEgeChecked((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id); else next.add(id);
      setEgeSubjects([...next]);
      if (!next.has(id)) {
        setLocalEgeScores((scores) => {
          const copy = { ...scores };
          delete copy[id];
          setEgeScores(parseEgeScores(copy));
          return copy;
        });
      }
      return next;
    });
  };

  const handleEgeScore = (id: string, val: string) => {
    setLocalEgeScores((prev) => {
      const next = { ...prev, [id]: val };
      setEgeScores(parseEgeScores(next));
      return next;
    });
  };

  return (
    <>
      {/* Hero */}
      <section className="hero">
        <div className="hero__inner">
          <h1 className="hero__heading">Поступи в вуз мечты</h1>
          <p className="hero__sub">
            Найдите лучшие вузы, программы и возможности для вашего будущего
          </p>

          <div className="hero__search">
            <input
              className="hero__search-input"
              type="search"
              value={localQuery}
              onChange={(e) => {
                setLocalQuery(e.target.value);
                setProgramCodes([]);
              }}
              onKeyDown={handleKeyDown}
              placeholder="Введите название вуза, города или направления..."
            />
            <button className="hero__search-btn" type="button" onClick={handleSearch}>
              Подобрать
            </button>
          </div>

          <div className="hero__filters">
            <div className="hero__filter-wrap">
              <span className="hero__filter-label">Город / регион</span>
              <GeoPickerDropdown
                region={region}
                city={city}
                onChangeRegion={setRegion}
                onChangeCity={setCity}
              />
            </div>
            <label className={`hero__check-filter${dormitory ? " hero__check-filter--active" : ""}`}>
              <input
                type="checkbox"
                checked={dormitory}
                onChange={(event) => setDormitory(event.target.checked)}
              />
              <span className="hero__check-box" aria-hidden>{dormitory ? "✓" : ""}</span>
              <span>Общежитие</span>
            </label>
            <label className={`hero__check-filter${militaryDepartment ? " hero__check-filter--active" : ""}`}>
              <input
                type="checkbox"
                checked={militaryDepartment}
                onChange={(event) => setMilitaryDepartment(event.target.checked)}
              />
              <span className="hero__check-box" aria-hidden>{militaryDepartment ? "✓" : ""}</span>
              <span>Военная кафедра</span>
            </label>
            <div className="hero__filter-wrap">
              <span className="hero__filter-label">Баллы ЕГЭ</span>
              <button
                className={`hero__filter hero__filter--ege-btn${showEge ? " hero__filter--ege-btn--active" : ""}`}
                type="button"
                onClick={() => setShowEge((v) => !v)}
              >
                {egeChecked.size > 0 ? `${egeChecked.size} предмет${egeChecked.size === 1 ? "" : egeChecked.size < 5 ? "а" : "ов"}` : "Указать баллы"}
                <span className="hero__filter-ege-arrow">{showEge ? "▲" : "▼"}</span>
              </button>
            </div>
          </div>

          <div className="hero__directions">
            <span className="hero__directions-label">Популярные направления</span>
            {DIRECTIONS.map((d) => (
              <button
                key={d.key}
                className="hero__direction-chip"
                type="button"
                onClick={() => handleDirectionClick(d)}
              >
                {d.label}
              </button>
            ))}
          </div>
        </div>
      </section>

      {/* EGE modal */}
      {showEge && (
        <div className="modal-overlay" onClick={() => setShowEge(false)}>
          <div className="modal ege-modal" onClick={(e) => e.stopPropagation()}>
            <div className="modal__header">
              <h2 className="modal__title">
                Баллы ЕГЭ
                {egeChecked.size > 0 && (
                  <span className="ege-panel__badge" style={{ marginLeft: 10 }}>{egeChecked.size} выбрано</span>
                )}
              </h2>
              <button className="modal__close" type="button" onClick={() => setShowEge(false)}>✕</button>
            </div>
            <div className="modal__body ege-modal__body">
              <p className="ege-panel__hint">
                Отметьте предметы и укажите баллы — подберём подходящие программы.
              </p>
              <EgePanel
                subjects={egeSubjectsList}
                scores={localEgeScores}
                checked={egeChecked}
                onToggle={handleEgeToggle}
                onScore={handleEgeScore}
              />
              <button
                className="modal__submit"
                type="button"
                onClick={() => setShowEge(false)}
              >
                Применить
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Results */}
      <div className="search-page">
        <div className="section-header">
          <h2 className="section-header__title">
            {hasQueryState ? "Результаты поиска" : "Популярные вузы"}
          </h2>
          {snapshot && (
            <span className="section-header__count">{snapshot.total} вузов</span>
          )}
          <div className="sort-dropdown" onClick={(e) => e.stopPropagation()}>
            <button
              className="sort-dropdown__button"
              type="button"
              aria-haspopup="listbox"
              aria-expanded={showSortMenu}
              onClick={() => setShowSortMenu((value) => !value)}
            >
              <span>{SORT_OPTIONS.find((option) => option.value === sortBy)?.label}</span>
              <span className={`sort-dropdown__chevron${showSortMenu ? " sort-dropdown__chevron--open" : ""}`}>
                ▾
              </span>
            </button>
            {showSortMenu && (
              <div className="sort-dropdown__menu" role="listbox">
                {SORT_OPTIONS.map((option) => (
                  <button
                    key={option.value}
                    className={`sort-dropdown__option${sortBy === option.value ? " sort-dropdown__option--active" : ""}`}
                    type="button"
                    role="option"
                    aria-selected={sortBy === option.value}
                    onClick={() => {
                      setSortBy(option.value);
                      setShowSortMenu(false);
                    }}
                  >
                    {option.label}
                  </button>
                ))}
              </div>
            )}
          </div>
          <button
            className="section-header__link"
            type="button"
              onClick={() => {
                resetFilters();
                setQuery("");
                setLocalQuery("");
                setEgeChecked(new Set());
                setEgeSubjects([]);
                setLocalEgeScores({});
                setEgeScores({});
              }}
          >
            Сбросить фильтры ✕
          </button>
          {hasQueryState && (
            <button
              className="section-header__save-btn"
              type="button"
              disabled={savingSearch}
              onClick={handleOpenSaveModal}
            >
              <span className="section-header__save-icon">☆</span> Сохранить поиск
            </button>
          )}
        </div>

        {saveMessage && <p className="search-page__save-message">{saveMessage}</p>}

        {error && !loading && (
          <ViewState
            kind="error"
            title="Поиск недоступен"
            message={error}
            detail="Проверьте, что backend сервис запущен."
          />
        )}

        {loading && !snapshot && (
          <ViewState kind="loading" title="Загружаем вузы" message="Ждём ответ от сервера..." />
        )}

        {!loading && snapshot && !hasResults && (
          <ViewState
            kind="empty"
            title="Вузы не найдены"
            message="Попробуйте изменить запрос или сбросить фильтры."
          />
        )}

        {hasResults && (
          <div className="uni-list">
            {snapshot!.items.map((item) => (
              <div
                key={item.university_id}
                className="uni-card"
                onClick={() => openUniversityCard(item.university_id)}
                role="button"
                tabIndex={0}
                onKeyDown={(e) => { if (e.key === "Enter") openUniversityCard(item.university_id); }}
              >
                <div className="uni-card__logo">
                  {item.logo_url ? (
                    <img
                      src={item.logo_url}
                      alt={item.canonical_name}
                      style={{ width: "100%", height: "100%", objectFit: "contain", borderRadius: 6 }}
                      onError={(e) => {
                        (e.currentTarget as HTMLImageElement).style.display = "none";
                        (e.currentTarget.parentElement as HTMLElement).textContent = item.canonical_name.charAt(0);
                      }}
                    />
                  ) : (
                    item.canonical_name.charAt(0)
                  )}
                </div>

                <div className="uni-card__info">
                  <p className="uni-card__name">{item.canonical_name}</p>
                  <p className="uni-card__fullname">
                    {item.aliases.length > 0 ? item.aliases[0] : item.website ?? "сайт не указан"}
                  </p>
                  <div className="uni-card__tags">
                    <span className="uni-card__tag uni-card__tag--state">Государственный</span>
                    {item.city && (
                      <span className="uni-card__tag uni-card__tag--city">📍 {item.city}</span>
                    )}
                    {item.country_code && item.country_code !== "RU" && (
                      <span className="uni-card__tag uni-card__tag--city">{item.country_code}</span>
                    )}
                  </div>
                </div>

                <div className="uni-card__stats">
                  <div className="uni-card__stat">
                    <span className={`uni-card__stat-value uni-card__category ${categoryClass(item.rating_category)}`}>
                      {formatRatingCategory(item.rating_category)}
                    </span>
                    <span className="uni-card__stat-label">Категория</span>
                  </div>
                  <div className="uni-card__stat">
                    <span className="uni-card__stat-value">{formatIntegerMetric(item.budget_places)}</span>
                    <span className="uni-card__stat-label">Бюджетных мест</span>
                  </div>
                  <div className="uni-card__stat">
                    <span className="uni-card__stat-value">{formatDecimalMetric(item.avg_passing_score)}</span>
                    <span className="uni-card__stat-label">Проходной балл</span>
                  </div>
                </div>

                <div className="uni-card__arrow">›</div>
              </div>
            ))}
          </div>
        )}

        {snapshot && (
          <div className="search-pagination">
            <button
              className="button button--secondary"
              type="button"
              disabled={loading || page <= 1}
              onClick={() => setPage(page - 1)}
            >
              Назад
            </button>
            <div className="search-pagination__status">
              <strong>Страница {snapshot.page}</strong>
              <span>{snapshot.total} совпадений</span>
            </div>
            <button
              className="button button--secondary"
              type="button"
              disabled={loading || !snapshot.hasMore}
              onClick={() => setPage(page + 1)}
            >
              Далее
            </button>
          </div>
        )}
      </div>

      {/* Save search modal */}
      {showSaveModal && (
        <div className="modal-overlay" onClick={() => setShowSaveModal(false)}>
          <div className="modal save-search-modal" onClick={(e) => e.stopPropagation()}>
            <div className="modal__header">
              <h2 className="modal__title">Сохранить поиск</h2>
              <button className="modal__close" type="button" onClick={() => setShowSaveModal(false)}>✕</button>
            </div>
            <div className="modal__body">
              <p className="save-search-modal__hint">
                Дайте название этому поиску, чтобы легко найти его в профиле.
              </p>
              <input
                className="save-search-modal__input"
                type="text"
                maxLength={25}
                placeholder="Например: IT в Москве"
                value={saveName}
                autoFocus
                onChange={(e) => setSaveName(e.target.value)}
                onKeyDown={(e) => { if (e.key === "Enter") void handleSaveSearch(); }}
              />
              <p className="save-search-modal__chars">{25 - saveName.length} символов осталось</p>
              <button
                className="modal__submit"
                type="button"
                disabled={savingSearch}
                onClick={() => void handleSaveSearch()}
              >
                {savingSearch ? "Сохраняем..." : "Сохранить"}
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}

function formatRatingCategory(value: string | null): string {
  return value?.trim() || "—";
}

function categoryClass(value: string | null): string {
  const normalized = normalizeCategory(value);
  return normalized ? `uni-card__category--${normalized}` : "uni-card__category--empty";
}

function normalizeCategory(value: string | null): string | null {
  const normalized = value?.trim().toUpperCase().replace("А", "A");
  if (normalized === "A+") return "aplus";
  if (normalized === "A") return "a";
  if (normalized === "A-") return "aminus";
  if (normalized === "B+") return "bplus";
  if (normalized === "B") return "b";
  if (normalized === "C") return "c";
  return null;
}

function formatIntegerMetric(value: number | null): string {
  return typeof value === "number" && Number.isFinite(value)
    ? value.toLocaleString("ru-RU")
    : "—";
}

function formatDecimalMetric(value: number | null): string {
  return typeof value === "number" && Number.isFinite(value)
    ? value.toLocaleString("ru-RU", { maximumFractionDigits: 1 })
    : "—";
}

function parseEgeScores(scores: EgeScores): Record<string, number> {
  const result: Record<string, number> = {};
  for (const [subject, value] of Object.entries(scores)) {
    const score = Number.parseInt(value, 10);
    if (Number.isFinite(score) && score >= 0 && score <= 100) {
      result[subject] = score;
    }
  }
  return result;
}

function resolvePopularDirectionCodes(value: string): string[] {
  const normalized = value.trim().toLowerCase();
  const match = DIRECTIONS.find((direction) => direction.label.toLowerCase() === normalized);
  return match ? [...popularDirections[match.key]] : [];
}

function openUniversityCard(universityId: string): void {
  const url = new URL(window.location.href);
  url.searchParams.set("university_id", universityId);
  window.history.replaceState({}, "", url);
  window.location.hash = "university";
}
