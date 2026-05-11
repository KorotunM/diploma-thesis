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

interface RegionNode {
  name: string;
  cities: string[] | null; // null = not loaded yet
  expanded: boolean;
  loading: boolean;
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
  const [regions, setRegions] = useState<RegionNode[]>([]);
  const [regionsLoaded, setRegionsLoaded] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  const displayValue = city || region || "";

  // Load all regions once when dropdown is first opened
  useEffect(() => {
    if (!open || regionsLoaded) return;
    backendApi.getAllRegions().then((res) => {
      setRegions(
        res.items.map((name) => ({
          name,
          cities: null,
          expanded: false,
          loading: false,
        })),
      );
      setRegionsLoaded(true);
    }).catch(() => setRegionsLoaded(true));
  }, [open, regionsLoaded, backendApi]);

  // Close on outside click
  useEffect(() => {
    const onOutside = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", onOutside);
    return () => document.removeEventListener("mousedown", onOutside);
  }, []);

  const toggleRegion = async (idx: number) => {
    const node = regions[idx];
    const willExpand = !node.expanded;

    setRegions((prev) =>
      prev.map((r, i) =>
        i === idx ? { ...r, expanded: willExpand, loading: willExpand && r.cities === null } : r,
      ),
    );

    if (willExpand && node.cities === null) {
      try {
        const res = await backendApi.getCitiesByRegion(node.name);
        setRegions((prev) =>
          prev.map((r, i) =>
            i === idx ? { ...r, cities: res.items, loading: false } : r,
          ),
        );
      } catch {
        setRegions((prev) =>
          prev.map((r, i) => (i === idx ? { ...r, cities: [], loading: false } : r)),
        );
      }
    }
  };

  const selectRegion = (name: string) => {
    onChangeRegion(name);
    onChangeCity("");
    setOpen(false);
  };

  const selectCity = (cityName: string, regionName: string) => {
    onChangeCity(cityName);
    onChangeRegion(regionName);
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
          {!regionsLoaded && (
            <div className="geo-picker__loading">Загружаем регионы…</div>
          )}
          {regionsLoaded && regions.length === 0 && (
            <div className="geo-picker__empty">Нет данных</div>
          )}
          {regions.map((node, idx) => (
            <div key={node.name} className="geo-picker__region-group">
              <div
                className={`geo-picker__region-row${region === node.name && !city ? " geo-picker__row--selected" : ""}`}
              >
                <button
                  type="button"
                  className="geo-picker__chevron"
                  onClick={() => toggleRegion(idx)}
                  aria-label={node.expanded ? "Свернуть" : "Развернуть"}
                >
                  {node.loading ? "…" : node.expanded ? "∨" : "›"}
                </button>
                <span
                  className="geo-picker__region-name"
                  role="button"
                  tabIndex={0}
                  onClick={() => selectRegion(node.name)}
                  onKeyDown={(e) => { if (e.key === "Enter") selectRegion(node.name); }}
                >
                  {node.name}
                </span>
                <span
                  className={`geo-picker__check${region === node.name && !city ? " geo-picker__check--on" : ""}`}
                />
              </div>

              {node.expanded && node.cities !== null && node.cities.map((c) => (
                <div
                  key={c}
                  className={`geo-picker__city-row${city === c ? " geo-picker__row--selected" : ""}`}
                  role="button"
                  tabIndex={0}
                  onClick={() => selectCity(c, node.name)}
                  onKeyDown={(e) => { if (e.key === "Enter") selectCity(c, node.name); }}
                >
                  <span className="geo-picker__city-name">{c}</span>
                  <span className={`geo-picker__check${city === c ? " geo-picker__check--on" : ""}`} />
                </div>
              ))}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
