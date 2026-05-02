import { startTransition, useEffect, useMemo, useRef, useState } from "react";

import { HttpRequestError, isAbortError } from "../../shared/http";
import { useSelectedUniversity } from "../../shared/selected-university";
import { useFrontendRuntime } from "../../shared/runtime";

import type { UniversityCardSnapshot } from "./models";
import { loadUniversityCard } from "./service";

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export function useUniversityCardLookup() {
  const runtime = useFrontendRuntime();
  const {
    activeUniversityId,
    setActiveUniversityId,
    clearActiveUniversityId,
  } = useSelectedUniversity();
  const [draftUniversityId, setDraftUniversityId] = useState(activeUniversityId);
  const [snapshot, setSnapshot] = useState<UniversityCardSnapshot | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [validationError, setValidationError] = useState<string | null>(null);
  const [loading, setLoading] = useState(activeUniversityId !== "");
  const [refreshing, setRefreshing] = useState(false);
  const lastLoadedUniversityIdRef = useRef<string | null>(null);

  useEffect(() => {
    setDraftUniversityId(activeUniversityId);
  }, [activeUniversityId]);

  useEffect(() => {
    if (!activeUniversityId) {
      setSnapshot(null);
      setError(null);
      setLoading(false);
      setRefreshing(false);
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

    void loadUniversityCard({
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
          setError(`Карточка вуза ${activeUniversityId} не найдена в delivery projection.`);
          return;
        }
        if (nextError instanceof Error) {
          setError(nextError.message);
          return;
        }
        setError("Не удалось загрузить карточку вуза.");
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

  const canSubmit = useMemo(
    () => draftUniversityId.trim().length > 0 && validationError === null,
    [draftUniversityId, validationError],
  );

  return {
    activeUniversityId,
    draftUniversityId,
    snapshot,
    error,
    validationError,
    loading,
    refreshing,
    canSubmit,
    setDraftUniversityId: (value: string) => {
      setDraftUniversityId(value);
      setError(null);
      if (!value.trim()) {
        setValidationError(null);
        return;
      }
      setValidationError(
        UUID_PATTERN.test(value.trim()) ? null : "University ID должен быть корректным UUID.",
      );
    },
    submit: () => {
      const nextUniversityId = draftUniversityId.trim();
      if (!nextUniversityId) {
        setValidationError("Для загрузки живой карточки нужен University ID.");
        return;
      }
      if (!UUID_PATTERN.test(nextUniversityId)) {
        setValidationError("University ID должен быть корректным UUID.");
        return;
      }
      setValidationError(null);
      setError(null);
      setActiveUniversityId(nextUniversityId);
    },
    clear: () => {
      setDraftUniversityId("");
      clearActiveUniversityId();
      setSnapshot(null);
      setError(null);
      setValidationError(null);
      lastLoadedUniversityIdRef.current = null;
    },
  };
}
