import type { TradexAnalysisPublishReadiness } from "../detail/detailTypes";
import { toFiniteNumber } from "../detail/detailHelpers";

export const TRADEX_LIST_SUMMARY_FLAG_NAME = "VITE_ENABLE_TRADEX_LIST_SUMMARY";

const truthy = new Set(["1", "true", "yes", "on"]);

const toText = (value: unknown, fallback = "") => {
  const text = typeof value === "string" ? value.trim() : String(value ?? "").trim();
  return text || fallback;
};

export type TradexListSummaryTone = "buy" | "neutral" | "sell";

export type TradexListSummaryRequestItem = {
  code: string;
  asof?: string | number | null;
};

export type TradexListSummaryItem = {
  code: string;
  asof: string | null;
  available: boolean;
  reason: string | null;
  dominantTone: TradexListSummaryTone | null;
  confidence: number | null;
  publishReadiness: TradexAnalysisPublishReadiness | null;
  reasons: string[];
};

export type TradexListSummaryReadResult = {
  available: boolean;
  reason: string | null;
  scope: string | null;
  items: TradexListSummaryItem[];
};

export const formatTradexListSummaryToneLabel = (tone: TradexListSummaryTone | null) => {
  switch (tone) {
    case "buy":
      return "買い";
    case "sell":
      return "売り";
    case "neutral":
      return "中立";
    default:
      return "--";
  }
};

export const formatTradexListSummaryConfidence = (value: number | null | undefined) => {
  if (!Number.isFinite(value ?? NaN)) return "--";
  return `${Math.round((Number(value) || 0) * 100)}%`;
};

export const formatTradexListSummaryReadinessLabel = (
  item: Pick<TradexListSummaryItem, "available" | "publishReadiness" | "reason">
) => {
  if (!item.available) return item.reason ? `analysis unavailable: ${item.reason}` : "analysis unavailable";
  const readiness = item.publishReadiness;
  if (!readiness) return "publish readiness: unknown";
  if (readiness.ready) return "publish readiness: ready";
  return `publish readiness: ${readiness.status || "unknown"}`;
};

export const shouldShowTradexListSummary = (
  flag = import.meta.env.VITE_ENABLE_TRADEX_LIST_SUMMARY
) => {
  const raw = toText(flag, "0").toLowerCase();
  return truthy.has(raw);
};

export const buildTradexListSummaryKey = (code: string, asof: string | number | null | undefined) => {
  const normalizedCode = toText(code);
  const normalizedAsof = asof == null ? "latest" : toText(asof);
  return `${normalizedCode}:${normalizedAsof}`;
};

const normalizeReasons = (value: unknown): string[] => {
  if (!Array.isArray(value)) return [];
  return value
    .map((item) => toText(item))
    .filter(Boolean)
    .slice(0, 2);
};

const normalizePublishReadiness = (value: unknown): TradexAnalysisPublishReadiness | null => {
  if (!value || typeof value !== "object") return null;
  const source = value as Record<string, unknown>;
  return {
    ready: Boolean(source.ready),
    status: toText(source.status, "unknown"),
    reasons: normalizeReasons(source.reasons),
    candidateKey: toText(source.candidate_key ?? source.candidateKey) || null,
    approved: source.approved == null ? null : Boolean(source.approved),
  };
};

const normalizeTone = (value: unknown): TradexListSummaryTone | null => {
  const text = toText(value).toLowerCase();
  if (text === "buy" || text === "neutral" || text === "sell") return text;
  return null;
};

const normalizeAsof = (value: unknown): string | null => {
  if (value == null) return null;
  const text = toText(value);
  return text || null;
};

const normalizeItem = (value: unknown): TradexListSummaryItem => {
  const source = value && typeof value === "object" ? (value as Record<string, unknown>) : {};
  const available = Boolean(source.available);
  const reason = toText(source.reason) || null;
  const publishReadiness = normalizePublishReadiness(
    source.publish_readiness ?? source.publishReadiness
  );
  return {
    code: toText(source.code, "unknown"),
    asof: normalizeAsof(source.asof),
    available,
    reason,
    dominantTone: normalizeTone(source.dominant_tone ?? source.dominantTone),
    confidence: toFiniteNumber(source.confidence),
    publishReadiness,
    reasons: normalizeReasons(source.reasons),
  };
};

export const normalizeTradexListSummaryReadResult = (value: unknown): TradexListSummaryReadResult => {
  if (!value || typeof value !== "object") {
    return { available: false, reason: "analysis unavailable", scope: null, items: [] };
  }
  const source = value as Record<string, unknown>;
  const items = Array.isArray(source.items) ? source.items.map(normalizeItem) : [];
  return {
    available: Boolean(source.available),
    reason: toText(source.reason) || null,
    scope: toText(source.scope) || null,
    items,
  };
};
