import { startTransition, useEffect, useRef, useState } from "react";

import { HttpRequestError, isAbortError } from "../../shared/http";
import { useSelectedUniversity } from "../../shared/selected-university";
import { useFrontendRuntime } from "../../shared/runtime";

import type { EvidenceDrawerSnapshot } from "./models";
import { loadEvidenceDrawer } from "./service";

export function useEvidenceDrawer() {
  const runtime = useFrontendRuntime();
  const { activeUniversityId } = useSelectedUniversity();
  const [snapshot, setSnapshot] = useState<EvidenceDrawerSnapshot | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(activeUniversityId !== "");
  const [refreshing, setRefreshing] = useState(false);
  const lastLoadedUniversityIdRef = useRef<string | null>(null);

  useEffect(() => {
    if (!activeUniversityId) {
      setSnapshot(null);
      setError(null);
      setLoading(false);
      setRefreshing(false);
      lastLoadedUniversityIdRef.current = null;
      return;
    }

    let disposed = false;
    const controller = new AbortController();
    const isInitialLoad = lastLoadedUniversityIdRef.current !== activeUniversityId;

    if (isInitialLoad) {
      setSnapshot(null);
      setLoading(true);
    } else {
      setRefreshing(true);
    }
    setError(null);

    void loadEvidenceDrawer({
      runtime,
      universityId: activeUniversityId,
      signal: controller.signal,
    })
      .then((nextSnapshot) => {
        if (disposed) {
          return;
        }
        startTransition(() => {
          setSnapshot(nextSnapshot);
          setError(null);
        });
        lastLoadedUniversityIdRef.current = nextSnapshot.universityId;
      })
      .catch((nextError) => {
        if (disposed || isAbortError(nextError)) {
          return;
        }
        setSnapshot(null);
        if (nextError instanceof HttpRequestError && nextError.status === 404) {
          setError(`Provenance для вуза ${activeUniversityId} не найден.`);
          return;
        }
        if (nextError instanceof Error) {
          setError(nextError.message);
          return;
        }
        setError("Не удалось загрузить панель доказательств.");
      })
      .finally(() => {
        if (disposed) {
          return;
        }
        setLoading(false);
        setRefreshing(false);
      });

    return () => {
      disposed = true;
      controller.abort();
    };
  }, [activeUniversityId, runtime]);

  return {
    activeUniversityId,
    snapshot,
    error,
    loading,
    refreshing,
  };
}
