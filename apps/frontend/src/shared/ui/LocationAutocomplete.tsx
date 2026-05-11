import { useEffect, useRef, useState } from "react";

import { useFrontendRuntime } from "../runtime";

interface Props {
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  className?: string;
  mode: "region" | "city";
}

const DEBOUNCE_MS = 250;

export function LocationAutocomplete({ value, onChange, placeholder, className, mode }: Props) {
  const { backendApi } = useFrontendRuntime();
  const [inputValue, setInputValue] = useState(value);
  const [suggestions, setSuggestions] = useState<string[]>([]);
  const [open, setOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState(-1);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    setInputValue(value);
  }, [value]);

  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current);
    const q = inputValue.trim();
    if (!q) {
      setSuggestions([]);
      setOpen(false);
      return;
    }
    debounceRef.current = setTimeout(async () => {
      try {
        const res =
          mode === "region"
            ? await backendApi.suggestRegions(q)
            : await backendApi.suggestCities(q);
        setSuggestions(res.items);
        setOpen(res.items.length > 0);
        setActiveIndex(-1);
      } catch {
        setSuggestions([]);
        setOpen(false);
      }
    }, DEBOUNCE_MS);
    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
    };
  }, [inputValue, mode, backendApi]);

  useEffect(() => {
    const onOutside = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", onOutside);
    return () => document.removeEventListener("mousedown", onOutside);
  }, []);

  const select = (item: string) => {
    setInputValue(item);
    onChange(item);
    setOpen(false);
    setSuggestions([]);
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (!open) return;
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setActiveIndex((i) => Math.min(i + 1, suggestions.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setActiveIndex((i) => Math.max(i - 1, 0));
    } else if (e.key === "Enter" && activeIndex >= 0) {
      e.preventDefault();
      select(suggestions[activeIndex]);
    } else if (e.key === "Escape") {
      setOpen(false);
    }
  };

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const v = e.target.value;
    setInputValue(v);
    if (!v.trim()) onChange("");
  };

  const handleBlur = () => {
    if (!inputValue.trim()) onChange("");
  };

  return (
    <div ref={containerRef} style={{ position: "relative" }}>
      <input
        className={className}
        type="text"
        value={inputValue}
        onChange={handleChange}
        onKeyDown={handleKeyDown}
        onBlur={handleBlur}
        placeholder={placeholder}
        autoComplete="off"
      />
      {open && (
        <ul className="location-autocomplete__list">
          {suggestions.map((item, i) => (
            <li
              key={item}
              className={`location-autocomplete__item${i === activeIndex ? " location-autocomplete__item--active" : ""}`}
              onMouseDown={() => select(item)}
            >
              {item}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
