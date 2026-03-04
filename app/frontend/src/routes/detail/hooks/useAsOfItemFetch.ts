import { useEffect, useRef, useState } from "react";
import { api } from "../../../api";

type Params<T> = {
  backendReady: boolean;
  code: string | null | undefined;
  asof: number | null;
  enabled?: boolean;
  endpoint: string;
  timeoutMs?: number;
  requestKeyExtra?: string | null;
  maxRetries?: number;
  retryDelayMs?: number;
  retryOnNull?: boolean;
  negativeCacheTtlMs?: number;
  buildParams?: (code: string, asof: number) => Record<string, string | number>;
  parseItem: (item: unknown) => T | null;
};

type CacheEntry<T> = {
  value: T | null;
  fetchedAt: number;
};

export function useAsOfItemFetch<T>({
  backendReady,
  code,
  asof,
  enabled = true,
  endpoint,
  timeoutMs = 30000,
  requestKeyExtra = null,
  maxRetries = 0,
  retryDelayMs = 1000,
  retryOnNull = false,
  negativeCacheTtlMs = 0,
  buildParams,
  parseItem,
}: Params<T>) {
  const [item, setItem] = useState<T | null>(null);
  const [loading, setLoading] = useState(false);
  const [retryToken, setRetryToken] = useState(0);
  const cacheRef = useRef<Map<string, CacheEntry<T>>>(new Map());
  const requestKeyRef = useRef<string | null>(null);
  const attemptCountRef = useRef<Map<string, number>>(new Map());
  const retryTimerRef = useRef<number | null>(null);
  const parseItemRef = useRef(parseItem);
  const buildParamsRef = useRef(buildParams);

  useEffect(() => {
    parseItemRef.current = parseItem;
  }, [parseItem]);

  useEffect(() => {
    buildParamsRef.current = buildParams;
  }, [buildParams]);

  useEffect(() => {
    setItem(null);
    setLoading(false);
    setRetryToken(0);
    requestKeyRef.current = null;
    attemptCountRef.current.clear();
    if (retryTimerRef.current != null) {
      window.clearTimeout(retryTimerRef.current);
      retryTimerRef.current = null;
    }
    cacheRef.current.clear();
  }, [code]);

  useEffect(() => {
    return () => {
      if (retryTimerRef.current != null) {
        window.clearTimeout(retryTimerRef.current);
        retryTimerRef.current = null;
      }
    };
  }, []);

  useEffect(() => {
    const clearRetryTimer = () => {
      if (retryTimerRef.current != null) {
        window.clearTimeout(retryTimerRef.current);
        retryTimerRef.current = null;
      }
    };
    const scheduleRetry = (requestKey: string, nextAttempt: number) => {
      clearRetryTimer();
      attemptCountRef.current.set(requestKey, nextAttempt);
      retryTimerRef.current = window.setTimeout(() => {
        retryTimerRef.current = null;
        setRetryToken((prev) => prev + 1);
      }, Math.max(0, retryDelayMs));
    };

    if (!enabled || !backendReady || !code) {
      clearRetryTimer();
      requestKeyRef.current = null;
      setLoading(false);
      return;
    }
    if (asof == null) {
      clearRetryTimer();
      requestKeyRef.current = null;
      setLoading(false);
      return;
    }

    const requestKey = requestKeyExtra
      ? `${code}|${asof}|${requestKeyExtra}`
      : `${code}|${asof}`;

    const cached = cacheRef.current.get(requestKey);
    if (cached) {
      const isNegative = cached.value == null;
      const negativeFresh =
        isNegative &&
        negativeCacheTtlMs > 0 &&
        Date.now() - cached.fetchedAt <= negativeCacheTtlMs;
      if (!isNegative || negativeFresh) {
        setItem(cached.value);
        setLoading(false);
        attemptCountRef.current.delete(requestKey);
        return;
      }
      cacheRef.current.delete(requestKey);
    }

    clearRetryTimer();
    setLoading(true);
    requestKeyRef.current = requestKey;
    const attempt = attemptCountRef.current.get(requestKey) ?? 0;
    const params = buildParamsRef.current ? buildParamsRef.current(code, asof) : { code, asof };

    api
      .get(endpoint, { params, timeout: timeoutMs })
      .then((res) => {
        if (requestKeyRef.current !== requestKey) return;
        const parsed = parseItemRef.current(res.data?.item ?? null);
        const shouldRetry =
          parsed == null &&
          retryOnNull &&
          attempt < Math.max(0, Math.floor(maxRetries));
        if (shouldRetry) {
          scheduleRetry(requestKey, attempt + 1);
          return;
        }
        attemptCountRef.current.delete(requestKey);
        cacheRef.current.set(requestKey, { value: parsed, fetchedAt: Date.now() });
        setItem(parsed ?? null);
        setLoading(false);
      })
      .catch(() => {
        if (requestKeyRef.current !== requestKey) return;
        const shouldRetry = attempt < Math.max(0, Math.floor(maxRetries));
        if (shouldRetry) {
          scheduleRetry(requestKey, attempt + 1);
          return;
        }
        attemptCountRef.current.delete(requestKey);
        cacheRef.current.set(requestKey, { value: null, fetchedAt: Date.now() });
        setItem(null);
        setLoading(false);
      });
  }, [
    enabled,
    backendReady,
    code,
    asof,
    endpoint,
    timeoutMs,
    requestKeyExtra,
    maxRetries,
    retryDelayMs,
    retryOnNull,
    negativeCacheTtlMs,
    retryToken,
  ]);

  return { item, loading };
}
