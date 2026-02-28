import { useEffect, useRef, useState } from "react";
import { api } from "../../../api";

type Params<T> = {
  backendReady: boolean;
  code: string | null | undefined;
  asOf: string | null;
  candlesLength: number;
  enabled: boolean;
  normalizePoint: (value: unknown) => T | null;
  getSortTime: (value: T) => number | null;
};

export function useAnalysisTimeline<T>({
  backendReady,
  code,
  asOf,
  candlesLength,
  enabled,
  normalizePoint,
  getSortTime,
}: Params<T>) {
  const [timeline, setTimeline] = useState<T[]>([]);
  const [loading, setLoading] = useState(false);
  const cacheRef = useRef<Map<string, T[]>>(new Map());
  const requestKeyRef = useRef<string | null>(null);

  useEffect(() => {
    setTimeline([]);
    setLoading(false);
    requestKeyRef.current = null;
    cacheRef.current.clear();
  }, [code]);

  useEffect(() => {
    if (!backendReady || !code) return;
    if (candlesLength === 0) {
      setTimeline([]);
      setLoading(false);
      return;
    }
    if (!enabled) {
      setLoading(false);
      return;
    }

    const requestKey = `${code}|${asOf ?? ""}|${candlesLength}`;
    if (cacheRef.current.has(requestKey)) {
      setTimeline(cacheRef.current.get(requestKey) ?? []);
      setLoading(false);
      return;
    }

    const limit = Math.min(2000, Math.max(400, candlesLength || 0));
    setLoading(true);
    requestKeyRef.current = requestKey;
    const params: Record<string, string | number> = { code, limit };
    if (asOf) {
      params.asof = asOf;
    }

    api
      .get("/ticker/analysis/timeline", { params, timeout: 30000 })
      .then((res) => {
        if (requestKeyRef.current !== requestKey) return;
        const itemsRaw = Array.isArray(res.data?.items) ? res.data.items : [];
        const normalized = itemsRaw
          .map((item) => normalizePoint(item))
          .filter((item): item is T => item != null)
          .sort((a, b) => {
            const ta = getSortTime(a) ?? 0;
            const tb = getSortTime(b) ?? 0;
            return ta - tb;
          });
        cacheRef.current.set(requestKey, normalized);
        setTimeline(normalized);
      })
      .catch(() => {
        if (requestKeyRef.current !== requestKey) return;
        cacheRef.current.set(requestKey, []);
        setTimeline([]);
      })
      .finally(() => {
        if (requestKeyRef.current !== requestKey) return;
        setLoading(false);
      });
  }, [backendReady, code, asOf, candlesLength, enabled, normalizePoint, getSortTime]);

  return { timeline, loading };
}
