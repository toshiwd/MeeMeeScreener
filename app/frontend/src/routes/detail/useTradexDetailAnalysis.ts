import { useEffect, useRef, useState } from "react";
import { api } from "../../api";
import { isCanceledRequestError } from "./detailHelpers";
import { normalizeTradexDetailAnalysisReadResult } from "./tradexAnalysis";
import type { TradexAnalysisReadResult } from "./detailTypes";

export type UseTradexDetailAnalysisParams = {
  backendReady: boolean;
  readyToFetch: boolean;
  enabled: boolean;
  code: string | null;
  asof: number | null;
};

export type UseTradexDetailAnalysisState = TradexAnalysisReadResult & {
  loading: boolean;
};

const EMPTY_STATE: UseTradexDetailAnalysisState = {
  available: false,
  reason: null,
  analysis: null,
  loading: false,
};

const extractReason = (error: unknown) => {
  if (!error || typeof error !== "object") return "analysis unavailable";
  const response = (error as { response?: { data?: unknown } }).response?.data;
  if (response && typeof response === "object") {
    const source = response as Record<string, unknown>;
    if (typeof source.reason === "string" && source.reason.trim()) {
      return source.reason.trim();
    }
    if (typeof source.detail === "string" && source.detail.trim()) {
      return source.detail.trim();
    }
    if (typeof source.message === "string" && source.message.trim()) {
      return source.message.trim();
    }
  }
  return "analysis unavailable";
};

export function useTradexDetailAnalysis({
  backendReady,
  readyToFetch,
  enabled,
  code,
  asof,
}: UseTradexDetailAnalysisParams): UseTradexDetailAnalysisState {
  const [state, setState] = useState<UseTradexDetailAnalysisState>(EMPTY_STATE);
  const requestSeqRef = useRef(0);

  useEffect(() => {
    const requestSeq = ++requestSeqRef.current;
    if (!enabled || !backendReady || !readyToFetch || !code) {
      setState(EMPTY_STATE);
      return;
    }

    const controller = new AbortController();
    let cancelled = false;
    setState({ ...EMPTY_STATE, loading: true });

    void api
      .get("/ticker/tradex/analysis", {
        params: {
          code,
          ...(asof != null ? { asof } : {}),
        },
        timeout: 15000,
        signal: controller.signal,
      })
      .then((response) => {
        if (cancelled || requestSeq !== requestSeqRef.current) return;
        setState({ ...normalizeTradexDetailAnalysisReadResult(response.data), loading: false });
      })
      .catch((error: unknown) => {
        if (cancelled || requestSeq !== requestSeqRef.current || isCanceledRequestError(error)) return;
        setState({
          available: false,
          reason: extractReason(error),
          analysis: null,
          loading: false,
        });
      });

    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [backendReady, readyToFetch, enabled, code, asof]);

  return state;
}
