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
  buildParams?: (code: string, asof: number) => Record<string, string | number>;
  parseItem: (item: unknown) => T | null;
};

export function useAsOfItemFetch<T>({
  backendReady,
  code,
  asof,
  enabled = true,
  endpoint,
  timeoutMs = 30000,
  requestKeyExtra = null,
  buildParams,
  parseItem,
}: Params<T>) {
  const [item, setItem] = useState<T | null>(null);
  const [loading, setLoading] = useState(false);
  const cacheRef = useRef<Map<string, T | null>>(new Map());
  const requestKeyRef = useRef<string | null>(null);
  const lastAttemptKeyRef = useRef<string | null>(null);
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
    requestKeyRef.current = null;
    lastAttemptKeyRef.current = null;
    cacheRef.current.clear();
  }, [code]);

  useEffect(() => {
    if (!enabled || !backendReady || !code) {
      setLoading(false);
      return;
    }
    if (asof == null) {
      setLoading(false);
      return;
    }

    const requestKey = requestKeyExtra
      ? `${code}|${asof}|${requestKeyExtra}`
      : `${code}|${asof}`;
    if (cacheRef.current.has(requestKey)) {
      setItem(cacheRef.current.get(requestKey) ?? null);
      setLoading(false);
      lastAttemptKeyRef.current = requestKey;
      return;
    }
    if (lastAttemptKeyRef.current === requestKey) return;

    setLoading(true);
    lastAttemptKeyRef.current = requestKey;
    requestKeyRef.current = requestKey;
    const params = buildParamsRef.current ? buildParamsRef.current(code, asof) : { code, asof };

    api
      .get(endpoint, { params, timeout: timeoutMs })
      .then((res) => {
        if (requestKeyRef.current !== requestKey) return;
        const parsed = parseItemRef.current(res.data?.item ?? null);
        cacheRef.current.set(requestKey, parsed);
        setItem(parsed);
      })
      .catch(() => {
        if (requestKeyRef.current !== requestKey) return;
        cacheRef.current.set(requestKey, null);
        setItem(null);
      })
      .finally(() => {
        if (requestKeyRef.current !== requestKey) return;
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
  ]);

  return { item, loading };
}
