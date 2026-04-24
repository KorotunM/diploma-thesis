import {
  startTransition,
  useDeferredValue,
  useEffect,
  useRef,
  useState,
} from "react";

import { describeRequestError, isAbortError } from "../../shared/http";
import { useFrontendRuntime } from "../../shared/runtime";

import type { UniversitySearchSnapshot } from "./models";
import { searchUniversities } from "./service";

export function useUniversitySearch() {
  const runtime = useFrontendRuntime();
  const [query, setQuery] = useState("");
  const deferredQuery = useDeferredValue(query.trim());
  const [snapshot, setSnapshot] = useState<UniversitySearchSnapshot | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const hasLoadedRef = useRef(false);

  useEffect(() => {
    let disposed = false;
    const controller = new AbortController();
    const isInitialLoad = !hasLoadedRef.current;

    if (isInitialLoad) {
      setLoading(true);
    } else {
      setRefreshing(true);
    }

    void searchUniversities({
      runtime,
      query: deferredQuery,
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
        hasLoadedRef.current = true;
      })
      .catch((nextError) => {
        if (disposed || isAbortError(nextError)) {
          return;
        }
        setError(describeRequestError(nextError));
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
  }, [deferredQuery, runtime]);

  return {
    query,
    setQuery,
    snapshot,
    error,
    loading,
    refreshing,
  };
}
