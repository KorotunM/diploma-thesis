import { useEffect, useRef, useState } from "react";

import { useFrontendRuntime } from "../runtime";

interface Props {
  region: string;
  city: string;
  onChangeRegion: (region: string) => void;
  onChangeCity: (city: string) => void;
  placeholder?: string;
  className?: string;
}

const DEBOUNCE_MS = 250;

interface SuggestionItem {
  label: string;
  kind: "city" | "region";
}

export function GeoPickerDropdown({
  region,
  city,
  onChangeRegion,
  onChangeCity,
  placeholder = "Вся Россия",
  className,
}: Props) {
  const { backendApi } = useFrontendRuntime();
  const [open, setOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [suggestions, setSuggestions] = useState<SuggestionItem[]>([]);
  const [loading, setLoading] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const displayValue = city || region || "";

  useEffect(() => {
    const onOutside = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", onOutside);
    return () => document.removeEventListener("mousedown", onOutside);
  }, []);

  useEffect(() => {
    if (open) {
      const t = setTimeout(() => inputRef.current?.focus(), 50);
      return () => clearTimeout(t);
    } else {
      setSearchQuery("");
      setSuggestions([]);
    }
  }, [open]);

  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current);
    const q = searchQuery.trim();
    if (!q) {
      setSuggestions([]);
      setLoading(false);
      return;
    }
    setLoading(true);
    debounceRef.current = setTimeout(async () => {
      try {
        const [citiesRes, regionsRes] = await Promise.all([
          backendApi.suggestCities(q),
          backendApi.suggestRegions(q),
        ]);
        const regionNames = new Set(regionsRes.items);
        const regionItems: SuggestionItem[] = regionsRes.items.slice(0, 5).map((r) => ({ label: r, kind: "region" }));
        const cityItems: SuggestionItem[] = citiesRes.items.filter((c) => !regionNames.has(c)).slice(0, 15).map((c) => ({ label: c, kind: "city" }));
        setSuggestions([...regionItems, ...cityItems]);
      } catch {
        setSuggestions([]);
      } finally {
        setLoading(false);
      }
    }, DEBOUNCE_MS);
    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
    };
  }, [searchQuery, backendApi]);

  const selectItem = (item: SuggestionItem) => {
    if (item.kind === "region") {
      onChangeRegion(item.label);
      onChangeCity("");
    } else {
      onChangeCity(item.label);
      onChangeRegion("");
    }
    setOpen(false);
  };

  const clear = () => {
    onChangeRegion("");
    onChangeCity("");
    setOpen(false);
  };

  return (
    <div ref={containerRef} className={`geo-picker${className ? ` ${className}` : ""}`}>
      <button
        type="button"
        className={`geo-picker__trigger${displayValue ? " geo-picker__trigger--active" : ""}`}
        onClick={() => setOpen((v) => !v)}
      >
        <span className="geo-picker__value">{displayValue || placeholder}</span>
        {displayValue ? (
          <span
            className="geo-picker__clear"
            role="button"
            tabIndex={0}
            onClick={(e) => { e.stopPropagation(); clear(); }}
            onKeyDown={(e) => { if (e.key === "Enter") { e.stopPropagation(); clear(); } }}
          >
            ✕
          </span>
        ) : (
          <span className="geo-picker__arrow">{open ? "▲" : "▼"}</span>
        )}
      </button>

      {open && (
        <div className="geo-picker__dropdown">
          <div className="geo-picker__search-wrap">
            <input
              ref={inputRef}
              className="geo-picker__search-input"
              type="text"
              placeholder="Город или регион..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Escape") setOpen(false);
                if (e.key === "Enter" && suggestions.length > 0) selectItem(suggestions[0]!);
              }}
            />
          </div>

          {displayValue && (
            <div
              className="geo-picker__clear-option"
              role="button"
              tabIndex={0}
              onClick={clear}
              onKeyDown={(e) => { if (e.key === "Enter") clear(); }}
            >
              <span className="geo-picker__clear-icon">✕</span>
              Вся Россия (сбросить)
            </div>
          )}

          {loading && (
            <div className="geo-picker__loading">Поиск…</div>
          )}

          {!loading && searchQuery.trim() && suggestions.length === 0 && (
            <div className="geo-picker__empty">Нет совпадений</div>
          )}

          {!searchQuery.trim() && !loading && (
            <div className="geo-picker__hint">Введите название города или региона</div>
          )}

          {suggestions.map((item) => (
            <div
              key={`${item.kind}:${item.label}`}
              className={`geo-picker__suggestion-row geo-picker__suggestion-row--${item.kind}`}
              role="button"
              tabIndex={0}
              onClick={() => selectItem(item)}
              onKeyDown={(e) => { if (e.key === "Enter") selectItem(item); }}
            >
              {item.kind === "region" && <span className="geo-picker__suggestion-badge">регион</span>}
              {item.label}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
