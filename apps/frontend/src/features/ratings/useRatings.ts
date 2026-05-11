import { useEffect, useRef, useState } from "react";

import { describeRequestError, isAbortError } from "../../shared/http";
import { useFrontendRuntime } from "../../shared/runtime";

import type { RankingsSnapshot } from "./models";
import { fetchRankings } from "./service";

const DEFAULT_PAGE_SIZE = 20;

export function useRatings() {
  const runtime = useFrontendRuntime();
  const [page, setPage] = useState(1);
  const [snapshot, setSnapshot] = useState<RankingsSnapshot | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const hasLoadedRef = useRef(false);

  useEffect(() => {
    let disposed = false;
    const controller = new AbortController();

    if (!hasLoadedRef.current) setLoading(true);

    void fetchRankings({
      runtime,
      page,
      pageSize: DEFAULT_PAGE_SIZE,
      signal: controller.signal,
    })
      .then((next) => {
        if (disposed) return;
        setSnapshot(next);
        setError(null);
        hasLoadedRef.current = true;
      })
      .catch((err) => {
        if (disposed || isAbortError(err)) return;
        setError(describeRequestError(err));
      })
      .finally(() => {
        if (!disposed) setLoading(false);
      });

    return () => {
      disposed = true;
      controller.abort();
    };
  }, [runtime, page]);

  return { snapshot, loading, error, page, setPage };
}
