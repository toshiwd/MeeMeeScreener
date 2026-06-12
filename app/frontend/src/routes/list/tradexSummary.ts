import type { TradexAnalysisPublishReadiness } from "../detail/detailTypes";
import { toFiniteNumber } from "../detail/detailHelpers";

export const TRADEX_LIST_SUMMARY_FLAG_NAME = "VITE_ENABLE_TRADEX_LIST_SUMMARY";
export const TRADEX_LIST_SUMMARY_WARM_CAPS = {
  visible: 48,
  selected: 48,
  favorites: 24,
} as const;

const truthy = new Set(["1", "true", "yes", "on"]);

const toText = (value: unknown, fallback = "") => {
  const text = typeof value === "string" ? value.trim() : String(value ?? "").trim();
  return text || fallback;
};

export type TradexListSummaryTone = "buy" | "neutral" | "sell";

export type TradexShortLifecycleOverlay = {
  state: string | null;
  rank: number | null;
  signalYmd: string | null;
  expectedDownsidePct: number | null;
  riskRewardToSl8: number | null;
  setupState: string | null;
  continuationStatus: string | null;
  finalReviewStatus: string | null;
  reasons: string[];
  reviewOnly: boolean;
  artifactCreatedAt: string | null;
};

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
  shortLifecycle?: TradexShortLifecycleOverlay | null;
};

export type TradexListSummaryReadResult = {
  available: boolean;
  reason: string | null;
  scope: string | null;
  items: TradexListSummaryItem[];
};

const resolveWarmCap = (scope: string, fallback: number = TRADEX_LIST_SUMMARY_WARM_CAPS.visible) => {
  const normalized = toText(scope, "").toLowerCase();
  if (normalized.includes("favorite")) return TRADEX_LIST_SUMMARY_WARM_CAPS.favorites;
  if (normalized.includes("selected")) return TRADEX_LIST_SUMMARY_WARM_CAPS.selected;
  if (normalized.includes("visible")) return TRADEX_LIST_SUMMARY_WARM_CAPS.visible;
  return fallback;
};

export const buildTradexListSummaryWarmItems = (
  items: TradexListSummaryRequestItem[],
  scope: string,
  fallbackCap: number = TRADEX_LIST_SUMMARY_WARM_CAPS.visible
) => {
  const limit = resolveWarmCap(scope, fallbackCap);
  const capped: TradexListSummaryRequestItem[] = [];
  const seen = new Set<string>();
  for (const item of items) {
    if (!item?.code) continue;
    const normalizedCode = toText(item.code);
    if (!normalizedCode) continue;
    const normalizedAsof = item.asof == null ? null : item.asof;
    const key = buildTradexListSummaryKey(normalizedCode, normalizedAsof);
    if (seen.has(key)) continue;
    seen.add(key);
    capped.push({ code: normalizedCode, asof: normalizedAsof });
    if (capped.length >= limit) break;
  }
  return capped;
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

export const formatTradexListSummaryUnavailableReason = (reason: string | null | undefined) => {
  const text = toText(reason).toLowerCase();
  if (!text) return null;
  if (text.includes("unavailable") || text.includes("not available")) return "分析データ未準備";
  if (text.includes("missing") || text.includes("not_found") || text.includes("not found")) return "データ未取得";
  if (text.includes("timeout")) return "確認に時間がかかっています";
  return "詳細確認中";
};

const formatPublishReadinessStatus = (status?: string | null) => {
  const text = toText(status).toLowerCase();
  if (text === "ready" || text === "ok" || text === "pass") return "確認済み";
  if (text === "blocked" || text === "failed" || text === "error") return "要確認";
  if (text === "pending" || text === "unknown" || !text) return "確認中";
  return "確認中";
};

export const formatTradexListSummaryReadinessLabel = (
  item: Pick<TradexListSummaryItem, "available" | "publishReadiness" | "reason">
) => {
  if (!item.available) {
    const reason = formatTradexListSummaryUnavailableReason(item.reason);
    return reason ? `分析を確認できません: ${reason}` : "分析を確認できません";
  }
  const readiness = item.publishReadiness;
  if (!readiness) return "採用確認: 確認中";
  if (readiness.ready) return "採用確認: 確認済み";
  return `採用確認: ${formatPublishReadinessStatus(readiness.status)}`;
};

export const shouldShowTradexListSummary = (
  flag = import.meta.env.VITE_ENABLE_TRADEX_LIST_SUMMARY,
  lifecycleFlag = import.meta.env.VITE_ENABLE_TRADEX_SHORT_LIFECYCLE_OVERLAY
) => {
  const raw = toText(flag, "0").toLowerCase();
  const lifecycleRaw = toText(lifecycleFlag, "0").toLowerCase();
  return truthy.has(raw) || truthy.has(lifecycleRaw);
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

const normalizeShortLifecycle = (value: unknown): TradexShortLifecycleOverlay | null => {
  if (!value || typeof value !== "object") return null;
  const source = value as Record<string, unknown>;
  return {
    state: toText(source.state) || null,
    rank: toFiniteNumber(source.rank),
    signalYmd: normalizeAsof(source.signal_ymd ?? source.signalYmd),
    expectedDownsidePct: toFiniteNumber(source.expected_downside_pct ?? source.expectedDownsidePct),
    riskRewardToSl8: toFiniteNumber(source.risk_reward_to_sl8 ?? source.riskRewardToSl8),
    setupState: toText(source.setup_state ?? source.setupState) || null,
    continuationStatus: toText(source.continuation_status ?? source.continuationStatus) || null,
    finalReviewStatus: toText(source.final_review_status ?? source.finalReviewStatus) || null,
    reasons: normalizeReasons(source.reasons),
    reviewOnly: Boolean(source.review_only ?? source.reviewOnly),
    artifactCreatedAt: normalizeAsof(source.artifact_created_at ?? source.artifactCreatedAt),
  };
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
    shortLifecycle: normalizeShortLifecycle(source.short_lifecycle ?? source.shortLifecycle),
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
