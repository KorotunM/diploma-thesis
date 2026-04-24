import { createContext, useContext, useEffect, useState } from "react";
import type { ReactNode } from "react";

interface SelectedUniversityContextValue {
  activeUniversityId: string;
  setActiveUniversityId: (universityId: string) => void;
  clearActiveUniversityId: () => void;
}

const SelectedUniversityContext = createContext<SelectedUniversityContextValue | null>(null);

export function SelectedUniversityProvider(props: { children: ReactNode }) {
  const [activeUniversityId, setActiveUniversityIdState] = useState(readUniversityIdFromLocation);

  useEffect(() => {
    const handlePopState = () => {
      setActiveUniversityIdState(readUniversityIdFromLocation());
    };

    window.addEventListener("popstate", handlePopState);
    return () => {
      window.removeEventListener("popstate", handlePopState);
    };
  }, []);

  const setActiveUniversityId = (universityId: string) => {
    const normalized = universityId.trim();
    setActiveUniversityIdState(normalized);
    updateLocationUniversityId(normalized || null);
  };

  const clearActiveUniversityId = () => {
    setActiveUniversityIdState("");
    updateLocationUniversityId(null);
  };

  return (
    <SelectedUniversityContext.Provider
      value={{
        activeUniversityId,
        setActiveUniversityId,
        clearActiveUniversityId,
      }}
    >
      {props.children}
    </SelectedUniversityContext.Provider>
  );
}

export function useSelectedUniversity() {
  const value = useContext(SelectedUniversityContext);
  if (value === null) {
    throw new Error("useSelectedUniversity must be used within SelectedUniversityProvider.");
  }
  return value;
}

function readUniversityIdFromLocation(): string {
  const params = new URLSearchParams(window.location.search);
  return params.get("university_id")?.trim() ?? "";
}

function updateLocationUniversityId(universityId: string | null): void {
  const url = new URL(window.location.href);
  if (universityId) {
    url.searchParams.set("university_id", universityId);
  } else {
    url.searchParams.delete("university_id");
  }
  window.history.replaceState({}, "", url);
}
