import { useEffect, useRef, useState } from "react";
import { api } from "../../../api";

type Params<T> = {
  backendReady: boolean;
  code: string | null | undefined;
  asof: number | null;
  prefetchAsofs?: number[];
  enabled?: boolean;
  readyToFetch?: boolean;
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

const asOfItemCache = new Map<string, CacheEntry<unknown>>();
const asOfItemInFlight = new Map<string, Promise<unknown>>();

export function useAsOfItemFetch<T>({
  backendReady,
  code,
  asof,
  prefetchAsofs,
  enabled = true,
  readyToFetch = true,
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
      setItem(null);
      setLoading(false);
      return;
    }
    if (asof == null) {
      clearRetryTimer();
      requestKeyRef.current = null;
      setItem(null);
      setLoading(false);
      return;
    }

    const requestKey = requestKeyExtra
      ? `${endpoint}|${code}|${asof}|${requestKeyExtra}`
      : `${endpoint}|${code}|${asof}`;

    const cached = asOfItemCache.get(requestKey) as CacheEntry<T> | undefined;
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
      asOfItemCache.delete(requestKey);
    }

    if (!readyToFetch) {
      clearRetryTimer();
      requestKeyRef.current = requestKey;
      setItem(null);
      setLoading(false);
      return;
    }

    clearRetryTimer();
    setLoading(true);
    requestKeyRef.current = requestKey;
    const attempt = attemptCountRef.current.get(requestKey) ?? 0;
    const params = buildParamsRef.current ? buildParamsRef.current(code, asof) : { code, asof };
    const existingRequest = asOfItemInFlight.get(requestKey) as Promise<T | null> | undefined;
    const request =
      existingRequest ??
      api
        .get(endpoint, { params, timeout: timeoutMs })
        .then((res) => parseItemRef.current(res.data?.item ?? null))
        .finally(() => {
          asOfItemInFlight.delete(requestKey);
        });
    if (!existingRequest) {
      asOfItemInFlight.set(requestKey, request);
    }
    request
      .then((res) => {
        if (requestKeyRef.current !== requestKey) return;
        const parsed = res;
        const shouldRetry =
          parsed == null &&
          retryOnNull &&
          attempt < Math.max(0, Math.floor(maxRetries));
        if (shouldRetry) {
          scheduleRetry(requestKey, attempt + 1);
          return;
        }
        attemptCountRef.current.delete(requestKey);
        asOfItemCache.set(requestKey, { value: parsed, fetchedAt: Date.now() });
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
        asOfItemCache.set(requestKey, { value: null, fetchedAt: Date.now() });
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
    readyToFetch,
    retryToken,
  ]);

  useEffect(() => {
    if (!enabled || !backendReady || !code) return;
    if (!prefetchAsofs || prefetchAsofs.length === 0) return;
    let cancelled = false;
    const prefetchKeys = new Set<string>();
    prefetchAsofs.forEach((candidate) => {
      if (candidate == null) return;
      const requestKey = requestKeyExtra
        ? `${endpoint}|${code}|${candidate}|${requestKeyExtra}`
        : `${endpoint}|${code}|${candidate}`;
      if (prefetchKeys.has(requestKey)) return;
      prefetchKeys.add(requestKey);
      const cached = asOfItemCache.get(requestKey) as CacheEntry<T> | undefined;
      if (cached) {
        const isNegative = cached.value == null;
        const negativeFresh =
          isNegative &&
          negativeCacheTtlMs > 0 &&
          Date.now() - cached.fetchedAt <= negativeCacheTtlMs;
        if (!isNegative || negativeFresh) {
          return;
        }
        asOfItemCache.delete(requestKey);
      }
      if (asOfItemInFlight.has(requestKey)) {
        return;
      }
      const params = buildParamsRef.current
        ? buildParamsRef.current(code, candidate)
        : { code, asof: candidate };
      const request = api
        .get(endpoint, { params, timeout: timeoutMs })
        .then((res) => parseItemRef.current(res.data?.item ?? null))
        .finally(() => {
          asOfItemInFlight.delete(requestKey);
        });
      asOfItemInFlight.set(requestKey, request);
      request
        .then((res) => {
          if (cancelled) return;
          asOfItemCache.set(requestKey, { value: res ?? null, fetchedAt: Date.now() });
        })
        .catch(() => {
          if (cancelled) return;
          asOfItemCache.set(requestKey, { value: null, fetchedAt: Date.now() });
        });
    });
    return () => {
      cancelled = true;
    };
  }, [
    enabled,
    backendReady,
    code,
    endpoint,
    timeoutMs,
    requestKeyExtra,
    negativeCacheTtlMs,
    prefetchAsofs,
  ]);

  return { item, loading };
}
