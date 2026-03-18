// @ts-nocheck
import { useEffect, useRef, useState } from "react";
import { api } from "../../../api";

export type ExactDecisionTone = "up" | "down" | "neutral";

export type ExactDecisionRangeItem = {
  dtKey: number;
  tone: ExactDecisionTone;
};

type Params = {
  backendReady: boolean;
  code: string | null | undefined;
  startDt: number | null;
  endDt: number | null;
  riskMode: string;
  enabled: boolean;
  readyToFetch?: boolean;
  cacheKeyExtra?: string | number | null;
};

const exactDecisionRangeCache = new Map<string, ExactDecisionRangeItem[]>();
const exactDecisionRangeInFlight = new Map<string, Promise<ExactDecisionRangeItem[]>>();

const normalizeDtKey = (value: unknown): number | null => {
  if (typeof value === "number" && Number.isFinite(value)) {
    const intValue = Math.trunc(value);
    return intValue >= 10_000_000 ? intValue : null;
  }
  if (typeof value !== "string") return null;
  const digits = value.trim();
  if (!/^\d{8}$/.test(digits)) return null;
  return Number.parseInt(digits, 10);
};

const normalizeTone = (value: unknown): ExactDecisionTone | null => {
  if (typeof value !== "string") return null;
  const tone = value.trim().toLowerCase();
  if (tone === "up" || tone === "down" || tone === "neutral") {
    return tone;
  }
  return null;
};

export function useExactDecisionRange({
  backendReady,
  code,
  startDt,
  endDt,
  riskMode,
  enabled,
  readyToFetch = true,
  cacheKeyExtra = null,
}: Params) {
  const [items, setItems] = useState<ExactDecisionRangeItem[]>([]);
  const [loading, setLoading] = useState(false);
  const requestKeyRef = useRef<string | null>(null);

  useEffect(() => {
    setItems([]);
    setLoading(false);
    requestKeyRef.current = null;
  }, [code]);

  useEffect(() => {
    if (!backendReady || !code || !enabled || startDt == null || endDt == null) {
      setItems([]);
      setLoading(false);
      return;
    }

    const orderedStart = Math.min(startDt, endDt);
    const orderedEnd = Math.max(startDt, endDt);
    const requestKey = `${code}|${orderedStart}|${orderedEnd}|${riskMode}|${cacheKeyExtra ?? ""}`;
    if (exactDecisionRangeCache.has(requestKey)) {
      setItems(exactDecisionRangeCache.get(requestKey) ?? []);
      setLoading(false);
      return;
    }
    if (!readyToFetch) {
      setItems([]);
      setLoading(false);
      return;
    }

    setLoading(true);
    requestKeyRef.current = requestKey;
    const existingRequest = exactDecisionRangeInFlight.get(requestKey);
    const request =
      existingRequest ??
      api
        .get("/ticker/analysis/decisions", {
          params: {
            code,
            start_dt: orderedStart,
            end_dt: orderedEnd,
            risk_mode: riskMode,
          },
          timeout: 30000,
        })
        .then((res) => {
          const rawItems = Array.isArray(res.data?.items) ? res.data.items : [];
          return rawItems
            .map((value) => {
              if (!value || typeof value !== "object") return null;
              const payload = value as Record<string, unknown>;
              const dtKey = normalizeDtKey(payload.dt);
              const tone = normalizeTone((payload.decision as Record<string, unknown> | null | undefined)?.tone);
              if (dtKey == null || tone == null) return null;
              return { dtKey, tone } satisfies ExactDecisionRangeItem;
            })
            .filter((value): value is ExactDecisionRangeItem => value != null);
        })
        .finally(() => {
          exactDecisionRangeInFlight.delete(requestKey);
        });
    if (!existingRequest) {
      exactDecisionRangeInFlight.set(requestKey, request);
    }
    request
      .then((res) => {
        if (requestKeyRef.current !== requestKey) return;
        exactDecisionRangeCache.set(requestKey, res);
        setItems(res);
      })
      .catch(() => {
        if (requestKeyRef.current !== requestKey) return;
        exactDecisionRangeCache.set(requestKey, []);
        setItems([]);
      })
      .finally(() => {
        if (requestKeyRef.current !== requestKey) return;
        setLoading(false);
      });
  }, [backendReady, code, startDt, endDt, riskMode, enabled, readyToFetch, cacheKeyExtra]);

  return { items, loading };
}
