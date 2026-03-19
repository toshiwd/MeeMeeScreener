import { useEffect, useMemo, useRef, useState } from "react";
import { api } from "../api";
import { isCanceledRequestError } from "../routes/detail/detailHelpers";
import {
  buildTradexListSummaryKey,
  normalizeTradexListSummaryReadResult,
  type TradexListSummaryItem,
  type TradexListSummaryRequestItem,
} from "../routes/list/tradexSummary";

type UseTradexListSummaryParams = {
  backendReady: boolean;
  enabled: boolean;
  scope: string;
  items: TradexListSummaryRequestItem[];
};

type TradexListSummaryState = {
  loading: boolean;
  reason: string | null;
  itemsByKey: Record<string, TradexListSummaryItem>;
};

const SUMMARY_CACHE_TTL_MS = 45_000;
const summaryCache = new Map<string, { expiresAt: number; item: TradexListSummaryItem }>();

const EMPTY_STATE: TradexListSummaryState = {
  loading: false,
  reason: null,
  itemsByKey: {},
};

const normalizeKey = (code: string, asof: string | number | null | undefined) =>
  buildTradexListSummaryKey(code, asof);

const readCachedItem = (key: string): TradexListSummaryItem | null => {
  const cached = summaryCache.get(key);
  if (!cached) return null;
  if (cached.expiresAt <= Date.now()) {
    summaryCache.delete(key);
    return null;
  }
  return cached.item;
};

const writeCachedItem = (key: string, item: TradexListSummaryItem) => {
  summaryCache.set(key, { expiresAt: Date.now() + SUMMARY_CACHE_TTL_MS, item });
};

const normalizeRequestItems = (items: TradexListSummaryRequestItem[]) => {
  const deduped = new Map<string, TradexListSummaryRequestItem>();
  items.forEach((item) => {
    if (!item?.code) return;
    const key = normalizeKey(item.code, item.asof ?? null);
    if (!deduped.has(key)) {
      deduped.set(key, { code: item.code, asof: item.asof ?? null });
    }
  });
  return Array.from(deduped.entries()).map(([key, item]) => ({ key, item }));
};

export function resetTradexListSummaryCache() {
  summaryCache.clear();
}

export function useTradexListSummary({
  backendReady,
  enabled,
  scope,
  items,
}: UseTradexListSummaryParams): TradexListSummaryState {
  const [state, setState] = useState<TradexListSummaryState>(EMPTY_STATE);
  const requestSeqRef = useRef(0);
  const scopeRef = useRef(scope);

  const normalizedItems = useMemo(() => normalizeRequestItems(items), [items]);
  const requestSignature = useMemo(
    () => normalizedItems.map(({ key }) => key).sort().join("|"),
    [normalizedItems]
  );

  useEffect(() => {
    scopeRef.current = scope;
  }, [scope]);

  useEffect(() => {
    const requestSeq = ++requestSeqRef.current;
    if (!enabled || !backendReady || !normalizedItems.length) {
      setState(EMPTY_STATE);
      return;
    }

    const cachedItems: Record<string, TradexListSummaryItem> = {};
    const missingItems: TradexListSummaryRequestItem[] = [];
    normalizedItems.forEach(({ key, item }) => {
      const cached = readCachedItem(key);
      if (cached) {
        cachedItems[key] = cached;
        return;
      }
      missingItems.push(item);
    });

    if (!missingItems.length) {
      setState({
        loading: false,
        reason: null,
        itemsByKey: cachedItems,
      });
      return;
    }

    const controller = new AbortController();
    let settled = false;
    setState({
      loading: true,
      reason: null,
      itemsByKey: cachedItems,
    });

    const timer = window.setTimeout(() => {
      void api
        .post(
          "/ticker/tradex/summary",
          {
            scope: scopeRef.current,
            items: missingItems,
          },
          {
            timeout: 15000,
            signal: controller.signal,
          }
        )
        .then((response) => {
          if (settled || requestSeq !== requestSeqRef.current) return;
          const payload = normalizeTradexListSummaryReadResult(response.data);
          const merged: Record<string, TradexListSummaryItem> = { ...cachedItems };
          payload.items.forEach((item) => {
            const key = normalizeKey(item.code, item.asof ?? null);
            merged[key] = item;
            if (item.available || item.reason === "analysis unavailable") {
              writeCachedItem(key, item);
            }
          });
          setState({
            loading: false,
            reason: payload.reason,
            itemsByKey: merged,
          });
        })
        .catch((error: unknown) => {
          if (settled || requestSeq !== requestSeqRef.current || isCanceledRequestError(error)) return;
          setState({
            loading: false,
            reason: "analysis unavailable",
            itemsByKey: cachedItems,
          });
        });
    }, 120);

    return () => {
      settled = true;
      controller.abort();
      window.clearTimeout(timer);
    };
  }, [backendReady, enabled, normalizedItems, requestSignature]);

  return state;
}

