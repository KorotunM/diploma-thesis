import { useState } from "react";

import { useUniversitySearch } from "../features/search";
import { ViewState } from "../shared/ui/view-state";

const DIRECTIONS = [
  "IT и цифровые технологии",
  "Инженерия",
  "Экономика",
  "Медицина",
  "Управление",
  "Гуманитарные науки",
];

const FEATURE_CARDS = [
  {
    icon: "🎓",
    iconClass: "feature-card__icon--blue",
    title: "Подбор вуза",
    desc: "Найдём вузы под ваши баллы и предпочтения",
  },
  {
    icon: "🔢",
    iconClass: "feature-card__icon--green",
    title: "Калькулятор ЕГЭ",
    desc: "Рассчитайте шансы на поступление",
  },
  {
    icon: "📖",
    iconClass: "feature-card__icon--purple",
    title: "Специальности",
    desc: "Изучите направления и профили обучения",
  },
  {
    icon: "📊",
    iconClass: "feature-card__icon--orange",
    title: "Рейтинги",
    desc: "Сравнивайте вузы по различным критериям",
  },
];

export function SearchPage() {
  const {
    query,
    setQuery,
    city,
    setCity,
    page,
    setPage,
    pageSize,
    resetFilters,
    snapshot,
    error,
    loading,
  } = useUniversitySearch();

  const [localQuery, setLocalQuery] = useState(query);
  const hasResults = (snapshot?.items.length ?? 0) > 0;
  const hasQueryState = query.trim().length > 0 || city.trim().length > 0;

  const handleSearch = () => {
    setQuery(localQuery);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") handleSearch();
  };

  const handleDirectionClick = (direction: string) => {
    setLocalQuery(direction);
    setQuery(direction);
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
              onChange={(e) => setLocalQuery(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Введите название вуза, города или направления..."
            />
            <button className="hero__search-btn" type="button" onClick={handleSearch}>
              Подобрать
            </button>
          </div>

          <div className="hero__filters">
            <div className="hero__filter-wrap">
              <span className="hero__filter-label">Направление подготовки</span>
              <select className="hero__filter" defaultValue="">
                <option value="">Любое направление</option>
                {DIRECTIONS.map((d) => (
                  <option key={d} value={d}>
                    {d}
                  </option>
                ))}
              </select>
            </div>
            <div className="hero__filter-wrap">
              <span className="hero__filter-label">Регион</span>
              <input
                className="hero__filter"
                type="text"
                value={city}
                onChange={(e) => setCity(e.target.value)}
                placeholder="Вся Россия"
              />
            </div>
            <div className="hero__filter-wrap">
              <span className="hero__filter-label">Форма обучения</span>
              <select className="hero__filter" defaultValue="full">
                <option value="full">Очная</option>
                <option value="part">Заочная</option>
                <option value="mixed">Очно-заочная</option>
              </select>
            </div>
            <div className="hero__filter-wrap">
              <span className="hero__filter-label">Бюджет/платно</span>
              <select className="hero__filter" defaultValue="">
                <option value="">Любой вариант</option>
                <option value="budget">Бюджет</option>
                <option value="paid">Платно</option>
              </select>
            </div>
            <div className="hero__filter-wrap">
              <span className="hero__filter-label">Баллы ЕГЭ</span>
              <select className="hero__filter" defaultValue="0">
                <option value="0">От 0 баллов</option>
                <option value="60">От 60 баллов</option>
                <option value="70">От 70 баллов</option>
                <option value="80">От 80 баллов</option>
              </select>
            </div>
          </div>

          <div className="hero__directions">
            <span className="hero__directions-label">Популярные направления</span>
            {DIRECTIONS.map((d) => (
              <button
                key={d}
                className="hero__direction-chip"
                type="button"
                onClick={() => handleDirectionClick(d)}
              >
                {d}
              </button>
            ))}
          </div>
        </div>
      </section>

      {/* Feature cards */}
      <div className="feature-cards">
        {FEATURE_CARDS.map((card) => (
          <div key={card.title} className="feature-card">
            <div className={`feature-card__icon ${card.iconClass}`}>{card.icon}</div>
            <div className="feature-card__text">
              <div className="feature-card__title">{card.title}</div>
              <div className="feature-card__desc">{card.desc}</div>
            </div>
            <span className="feature-card__arrow">›</span>
          </div>
        ))}
      </div>

      {/* Results */}
      <div className="search-page">
        <div className="section-header">
          <h2 className="section-header__title">
            {hasQueryState ? "Результаты поиска" : "Популярные вузы"}
          </h2>
          {snapshot && (
            <span className="section-header__count">{snapshot.total} вузов</span>
          )}
          {hasQueryState && (
            <button
              className="section-header__link"
              type="button"
              onClick={() => { resetFilters(); setLocalQuery(""); }}
            >
              Сбросить фильтры ✕
            </button>
          )}
        </div>

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
                onKeyDown={(e) => {
                  if (e.key === "Enter") openUniversityCard(item.university_id);
                }}
              >
                <div className="uni-card__logo">{item.canonical_name.charAt(0)}</div>

                <div className="uni-card__info">
                  <p className="uni-card__name">{item.canonical_name}</p>
                  <p className="uni-card__fullname">
                    {item.aliases.length > 0 ? item.aliases[0] : item.website ?? "сайт не указан"}
                  </p>
                  <div className="uni-card__tags">
                    <span className="uni-card__tag uni-card__tag--state">Государственный</span>
                    {item.city && (
                      <span className="uni-card__tag uni-card__tag--city">
                        📍 {item.city}
                      </span>
                    )}
                    {item.country_code && item.country_code !== "RU" && (
                      <span className="uni-card__tag uni-card__tag--city">
                        {item.country_code}
                      </span>
                    )}
                  </div>
                </div>

                <div className="uni-card__stats">
                  <div className="uni-card__stat">
                    <span className="uni-card__stat-value uni-card__stat-value--rating">
                      ★ {item.score.toFixed(1)}
                    </span>
                    <span className="uni-card__stat-label">Рейтинг</span>
                  </div>
                  <div className="uni-card__stat">
                    <span className="uni-card__stat-value">—</span>
                    <span className="uni-card__stat-label">Бюджетных мест</span>
                  </div>
                  <div className="uni-card__stat">
                    <span className="uni-card__stat-value">—</span>
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
    </>
  );
}

function openUniversityCard(universityId: string): void {
  const url = new URL(window.location.href);
  url.searchParams.set("university_id", universityId);
  window.history.replaceState({}, "", url);
  window.location.hash = "university";
}
