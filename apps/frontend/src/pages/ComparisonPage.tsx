import { useEffect, useState } from "react";

import { useAuth } from "../shared/auth";
import { useFrontendRuntime } from "../shared/runtime";
import type { UniversityCardDto } from "../shared/backend-api/types";

interface ComparedUniversity {
  id: string;
  card: UniversityCardDto | null;
  loading: boolean;
  error: string | null;
}

function strVal(v: unknown): string | null {
  if (typeof v === "string" && v.trim()) return v.trim();
  if (typeof v === "number") return String(v);
  return null;
}

function computeBudget(card: UniversityCardDto): string {
  const total = (card.admission?.programs ?? []).reduce(
    (s, p) => s + (p.budget_places ?? 0),
    0,
  );
  return total > 0 ? total.toLocaleString("ru-RU") : "—";
}

function computeAvgScore(card: UniversityCardDto): string {
  const scores = (card.admission?.programs ?? [])
    .map((p) => p.passing_score)
    .filter((s): s is number => s != null);
  if (scores.length === 0) return "—";
  return (scores.reduce((a, b) => a + b, 0) / scores.length).toFixed(1);
}

const ROWS: Array<{ label: string; get: (c: UniversityCardDto) => string }> = [
  { label: "Название", get: (c) => strVal(c.canonical_name?.value) ?? "—" },
  { label: "Город", get: (c) => c.location?.city ?? "—" },
  { label: "Тип", get: (c) => c.institutional?.type ?? "—" },
  { label: "Год основания", get: (c) => c.institutional?.founded_year ? String(c.institutional.founded_year) : "—" },
  { label: "Бюджетные места", get: computeBudget },
  { label: "Программы", get: (c) => String(c.admission?.programs?.length ?? 0) },
  { label: "Направления", get: (c) => {
    const codes = new Set(
      (c.admission?.programs ?? [])
        .map((p) => (p.code ?? "").split(".")[0])
        .filter(Boolean),
    );
    return String(codes.size);
  }},
  { label: "Средний проходной балл", get: computeAvgScore },
  { label: "Сайт", get: (c) => c.contacts?.website ?? "—" },
];

export function ComparisonPage({ onShowLogin }: { onShowLogin: () => void }) {
  const { user, token } = useAuth();
  const { backendApi } = useFrontendRuntime();

  const [ids, setIds] = useState<string[]>([]);
  const [items, setItems] = useState<ComparedUniversity[]>([]);
  const [listLoading, setListLoading] = useState(false);

  // Load comparison list
  useEffect(() => {
    if (!user) return;
    setListLoading(true);
    backendApi
      .getComparisons()
      .then((res) => {
        const newIds = res.items.map((i) => i.university_id);
        setIds(newIds);
        setItems(
          newIds.map((id) => ({ id, card: null, loading: true, error: null })),
        );
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
        .then((card) => {
          setItems((prev) =>
            prev.map((it) =>
              it.id === id ? { ...it, card, loading: false } : it,
            ),
          );
        })
        .catch((e: unknown) => {
          setItems((prev) =>
            prev.map((it) =>
              it.id === id
                ? { ...it, loading: false, error: e instanceof Error ? e.message : "Ошибка" }
                : it,
            ),
          );
        });
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
    window.history.replaceState({}, "", url);
    window.location.hash = "university";
  };

  if (!user) {
    return (
      <div className="cmp-page">
        <h1 className="cmp-page__title">Сравнение вузов</h1>
        <div className="cmp-empty">
          <p>Войдите, чтобы добавлять вузы в сравнение.</p>
          <button className="btn btn--primary" type="button" onClick={onShowLogin}>
            Войти
          </button>
        </div>
      </div>
    );
  }

  if (listLoading) {
    return (
      <div className="cmp-page">
        <h1 className="cmp-page__title">Сравнение вузов</h1>
        <p className="cmp-loading">Загружаем список сравнения…</p>
      </div>
    );
  }

  if (items.length === 0) {
    return (
      <div className="cmp-page">
        <h1 className="cmp-page__title">Сравнение вузов</h1>
        <div className="cmp-empty">
          <div className="cmp-empty__icon">⚖</div>
          <p className="cmp-empty__text">Список сравнения пуст</p>
          <p className="cmp-empty__hint">
            Нажмите ⚖ на странице вуза, чтобы добавить его в сравнение.
          </p>
          <button
            className="btn btn--primary"
            type="button"
            onClick={() => (window.location.hash = "search")}
          >
            Перейти к поиску
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="cmp-page">
      <h1 className="cmp-page__title">Сравнение вузов</h1>

      <div className="cmp-table-wrap">
        <table className="cmp-table">
          <thead>
            <tr>
              <th className="cmp-table__row-label" />
              {items.map((it) => (
                <th key={it.id} className="cmp-table__col-header">
                  <div className="cmp-col-header">
                    <button
                      className="cmp-col-header__name"
                      type="button"
                      onClick={() => openUniversity(it.id)}
                    >
                      {it.card
                        ? (it.card.aliases?.[0] ?? strVal(it.card.canonical_name?.value) ?? it.id)
                        : it.loading
                        ? "Загружаем…"
                        : "Ошибка"}
                    </button>
                    <button
                      className="cmp-col-header__remove"
                      type="button"
                      title="Убрать из сравнения"
                      onClick={() => handleRemove(it.id)}
                    >
                      ×
                    </button>
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {ROWS.map((row) => (
              <tr key={row.label} className="cmp-table__row">
                <td className="cmp-table__row-label">{row.label}</td>
                {items.map((it) => (
                  <td key={it.id} className="cmp-table__cell">
                    {it.loading ? (
                      <span className="cmp-loading-cell">…</span>
                    ) : it.error ? (
                      <span className="cmp-error-cell">—</span>
                    ) : it.card ? (
                      row.label === "Сайт" && it.card.contacts?.website ? (
                        <a
                          href={it.card.contacts.website}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="cmp-link"
                        >
                          {it.card.contacts.website}
                        </a>
                      ) : (
                        row.get(it.card)
                      )
                    ) : (
                      "—"
                    )}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
