import { useEffect, useState } from "react";

import { isAbortError } from "../../shared/http";
import { useFrontendRuntime } from "../../shared/runtime";

import type { HomeOverviewSnapshot } from "./models";
import { loadHomeOverview } from "./service";

export function useHomeOverview() {
  const runtime = useFrontendRuntime();
  const [snapshot, setSnapshot] = useState<HomeOverviewSnapshot | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);

  useEffect(() => {
    let disposed = false;
    let timeoutId: number | null = null;
    let currentController: AbortController | null = null;

    const scheduleNext = () => {
      if (disposed) {
        return;
      }
      timeoutId = window.setTimeout(() => {
        void run(false);
      }, runtime.config.overviewRefreshIntervalMs);
    };

    const run = async (isInitialLoad: boolean) => {
      currentController?.abort();
      currentController = new AbortController();

      if (isInitialLoad) {
        setLoading(true);
      } else {
        setRefreshing(true);
      }

      try {
        const nextSnapshot = await loadHomeOverview({
          runtime,
          signal: currentController.signal,
        });
        if (disposed) {
          return;
        }
        setSnapshot(nextSnapshot);
        setError(null);
      } catch (nextError) {
        if (disposed || isAbortError(nextError)) {
          return;
        }
        if (nextError instanceof Error) {
          setError(nextError.message);
        } else {
          setError("Home overview refresh failed.");
        }
      } finally {
        if (disposed) {
          return;
        }
        setLoading(false);
        setRefreshing(false);
        scheduleNext();
      }
    };

    void run(true);

    return () => {
      disposed = true;
      if (timeoutId !== null) {
        window.clearTimeout(timeoutId);
      }
      currentController?.abort();
    };
  }, [runtime]);

  return {
    snapshot,
    error,
    loading,
    refreshing,
  };
}
