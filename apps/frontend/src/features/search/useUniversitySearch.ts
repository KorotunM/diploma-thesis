import {
  startTransition,
  useDeferredValue,
  useEffect,
  useRef,
  useState,
} from "react";

import { describeRequestError, isAbortError } from "../../shared/http";
import { useFrontendRuntime } from "../../shared/runtime";

import type { SearchQueryState, UniversitySearchSnapshot } from "./models";
import { searchUniversities } from "./service";
import {
  readSearchQueryStateFromLocation,
  writeSearchQueryStateToLocation,
} from "./urlState";

const DEFAULT_PAGE_SIZE = 20;

export function useUniversitySearch() {
  const runtime = useFrontendRuntime();
  const [state, setState] = useState<SearchQueryState>(readSearchQueryStateFromLocation);
  const deferredQuery = useDeferredValue(state.query.trim());
  const deferredCity = useDeferredValue(state.city.trim());
  const deferredCountry = useDeferredValue(state.country.trim());
  const deferredSourceType = useDeferredValue(state.sourceType.trim());
  const deferredPage = useDeferredValue(state.page);
  const deferredPageSize = useDeferredValue(state.pageSize);
  const [snapshot, setSnapshot] = useState<UniversitySearchSnapshot | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const hasLoadedRef = useRef(false);

  useEffect(() => {
    const handlePopState = () => {
      setState(readSearchQueryStateFromLocation());
    };

    window.addEventListener("popstate", handlePopState);
    return () => {
      window.removeEventListener("popstate", handlePopState);
    };
  }, []);

  useEffect(() => {
    writeSearchQueryStateToLocation(state);
  }, [state]);

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
      state: {
        query: deferredQuery,
        city: deferredCity,
        country: deferredCountry,
        sourceType: deferredSourceType,
        page: deferredPage,
        pageSize: deferredPageSize,
      },
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
  }, [
    deferredCity,
    deferredCountry,
    deferredPage,
    deferredPageSize,
    deferredQuery,
    deferredSourceType,
    runtime,
  ]);

  const setQuery = (query: string) => {
    setState((current) => ({
      ...current,
      query,
      page: 1,
    }));
  };

  const setCity = (city: string) => {
    setState((current) => ({
      ...current,
      city,
      page: 1,
    }));
  };

  const setCountry = (country: string) => {
    setState((current) => ({
      ...current,
      country,
      page: 1,
    }));
  };

  const setSourceType = (sourceType: string) => {
    setState((current) => ({
      ...current,
      sourceType,
      page: 1,
    }));
  };

  const setPage = (page: number) => {
    setState((current) => ({
      ...current,
      page: Math.max(1, page),
    }));
  };

  const setPageSize = (pageSize: number) => {
    setState((current) => ({
      ...current,
      pageSize: pageSize > 0 ? pageSize : DEFAULT_PAGE_SIZE,
      page: 1,
    }));
  };

  const resetFilters = () => {
    setState((current) => ({
      ...current,
      city: "",
      country: "",
      sourceType: "",
      page: 1,
    }));
  };

  return {
    query: state.query,
    setQuery,
    city: state.city,
    setCity,
    country: state.country,
    setCountry,
    sourceType: state.sourceType,
    setSourceType,
    page: state.page,
    setPage,
    pageSize: state.pageSize,
    setPageSize,
    resetFilters,
    snapshot,
    error,
    loading,
    refreshing,
  };
}
