import type {
  TradexAnalysisCandidateComparison,
  TradexAnalysisOutput,
  TradexAnalysisOverrideState,
  TradexAnalysisPublishReadiness,
  TradexAnalysisReadResult,
  TradexAnalysisSideRatios,
} from "./detailTypes";
import { toFiniteNumber } from "./detailHelpers";

export const TRADEX_DETAIL_ANALYSIS_FLAG_NAME = "VITE_ENABLE_TRADEX_DETAIL_ANALYSIS";

const truthy = new Set(["1", "true", "yes", "on"]);

const toText = (value: unknown, fallback = "") => {
  const text = typeof value === "string" ? value.trim() : String(value ?? "").trim();
  return text || fallback;
};

export type TradexDetailAnalysisWarmRequest = {
  code: string;
  asof: number | null;
};

export const buildTradexDetailAnalysisWarmRequest = (
  code: string | null | undefined,
  asof: number | null | undefined
): TradexDetailAnalysisWarmRequest | null => {
  const normalizedCode = toText(code);
  if (!normalizedCode) return null;
  const normalizedAsof =
    typeof asof === "number" && Number.isFinite(asof) ? asof : null;
  return { code: normalizedCode, asof: normalizedAsof };
};

const normalizeReasons = (value: unknown): string[] => {
  if (!Array.isArray(value)) return [];
  return value.map((item) => toText(item)).filter(Boolean);
};

const normalizeCandidateComparison = (value: unknown): TradexAnalysisCandidateComparison => {
  const source = value && typeof value === "object" ? (value as Record<string, unknown>) : {};
  return {
    candidateKey: toText(source.candidate_key ?? source.candidateKey, "candidate"),
    baselineKey: toText(source.baseline_key ?? source.baselineKey) || null,
    comparisonScope: toText(source.comparison_scope ?? source.comparisonScope, "decision_scenarios"),
    score: toFiniteNumber(source.score),
    scoreDelta: toFiniteNumber(source.score_delta ?? source.scoreDelta),
    rank:
      typeof source.rank === "number" && Number.isFinite(source.rank) ? Math.trunc(source.rank) : null,
    reasons: normalizeReasons(source.reasons),
    publishReady:
      source.publish_ready == null && source.publishReady == null
        ? null
        : Boolean(source.publish_ready ?? source.publishReady),
  };
};

const normalizePublishReadiness = (value: unknown): TradexAnalysisPublishReadiness => {
  const source = value && typeof value === "object" ? (value as Record<string, unknown>) : {};
  return {
    ready: Boolean(source.ready),
    status: toText(source.status, "unknown"),
    reasons: normalizeReasons(source.reasons),
    candidateKey: toText(source.candidate_key ?? source.candidateKey) || null,
    approved: source.approved == null ? null : Boolean(source.approved),
  };
};

const normalizeOverrideState = (value: unknown): TradexAnalysisOverrideState => {
  const source = value && typeof value === "object" ? (value as Record<string, unknown>) : {};
  return {
    present: Boolean(source.present),
    source: toText(source.source) || null,
    logicKey: toText(source.logic_key ?? source.logicKey) || null,
    logicVersion: toText(source.logic_version ?? source.logicVersion) || null,
    reason: toText(source.reason) || null,
  };
};

const normalizeSideRatios = (value: unknown): TradexAnalysisSideRatios => {
  const source = value && typeof value === "object" ? (value as Record<string, unknown>) : {};
  return {
    buy: toFiniteNumber(source.buy) ?? toFiniteNumber(source.buyProb) ?? 0,
    neutral: toFiniteNumber(source.neutral) ?? toFiniteNumber(source.neutralProb) ?? 0,
    sell: toFiniteNumber(source.sell) ?? toFiniteNumber(source.sellProb) ?? 0,
  };
};

const normalizeAnalysis = (value: unknown): TradexAnalysisOutput | null => {
  if (!value || typeof value !== "object") return null;
  const source = value as Record<string, unknown>;
  const comparisonsSource = source.candidate_comparisons ?? source.candidateComparisons;
  return {
    symbol: toText(source.symbol, "unknown"),
    asof: toText(source.asof, "unknown"),
    sideRatios: normalizeSideRatios(source.side_ratios ?? source.sideRatios),
    confidence: toFiniteNumber(source.confidence),
    reasons: normalizeReasons(source.reasons),
    candidateComparisons: Array.isArray(comparisonsSource)
      ? comparisonsSource.map(normalizeCandidateComparison)
      : [],
    publishReadiness: normalizePublishReadiness(source.publish_readiness ?? source.publishReadiness),
    overrideState: normalizeOverrideState(source.override_state ?? source.overrideState),
  };
};

export function shouldShowTradexDetailAnalysis(flag = import.meta.env.VITE_ENABLE_TRADEX_DETAIL_ANALYSIS) {
  const raw = toText(flag, "0").toLowerCase();
  return truthy.has(raw);
}

export function normalizeTradexDetailAnalysisReadResult(value: unknown): TradexAnalysisReadResult {
  if (!value || typeof value !== "object") {
    return { available: false, reason: "analysis unavailable", analysis: null };
  }
  const source = value as Record<string, unknown>;
  const available = Boolean(source.available);
  const reason = toText(source.reason) || null;
  const analysis = normalizeAnalysis(source.analysis ?? source.item);
  if (!available || !analysis) {
    return {
      available: false,
      reason: reason || "analysis unavailable",
      analysis: null,
    };
  }
  return {
    available: true,
    reason: null,
    analysis,
  };
}
