import type {
  TradexAnalysisCandidateComparison,
  TradexAnalysisOutput,
  TradexAnalysisOverrideState,
  TradexAnalysisPublishReadiness,
  TradexAnalysisReadResult,
  TradexAnalysisSideRatios,
  TradexForecastSurface,
  TradexForecastSurfaceRow,
  TradexPromotionDecision,
  TradexPromotionReview,
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
  const normalizedAsof = typeof asof === "number" && Number.isFinite(asof) ? asof : null;
  return { code: normalizedCode, asof: normalizedAsof };
};

const normalizeReasons = (value: unknown): string[] => {
  if (!Array.isArray(value)) return [];
  return value.map((item) => toText(item)).filter(Boolean);
};

const normalizeStringList = (value: unknown): string[] => {
  if (Array.isArray(value)) return value.map((item) => toText(item)).filter(Boolean);
  if (typeof value === "string") {
    const text = value.trim();
    if (!text) return [];
    try {
      return normalizeStringList(JSON.parse(text) as unknown);
    } catch {
      return text
        .split(/[,\n]/)
        .map((item) => item.trim())
        .filter(Boolean);
    }
  }
  return [];
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

const normalizeForecastSurfaceRow = (value: unknown): TradexForecastSurfaceRow => {
  const source = value && typeof value === "object" ? (value as Record<string, unknown>) : {};
  return {
    code: toText(source.code) || null,
    side: toText(source.side) || null,
    actionState: toText(source.action_state ?? source.actionState) || null,
    directionProb: toFiniteNumber(source.direction_prob ?? source.directionProb),
    expectedUpside:
      toFiniteNumber(source.expected_upside) ??
      toFiniteNumber(source.expectedUpside) ??
      toFiniteNumber(source.expected_mfe_20) ??
      toFiniteNumber(source.expectedMfe20) ??
      toFiniteNumber(source.expected_ret_20) ??
      toFiniteNumber(source.expectedRet20),
    expectedDownside:
      toFiniteNumber(source.expected_downside) ??
      toFiniteNumber(source.expectedDownside) ??
      toFiniteNumber(source.expected_mae_20) ??
      toFiniteNumber(source.expectedMae20) ??
      toFiniteNumber(source.expected_ret_20) ??
      toFiniteNumber(source.expectedRet20),
    invalidationPrice: toFiniteNumber(source.invalidation_price ?? source.invalidationPrice),
    reasonCodes: normalizeStringList(source.reason_codes ?? source.reasonCodes),
    setupTags: normalizeStringList(source.setup_tags ?? source.setupTags),
    opportunityScore: toFiniteNumber(source.opportunity_score ?? source.opportunityScore),
    freshnessState: toText(source.freshness_state ?? source.freshnessState) || null,
  };
};

const normalizeForecastSurface = (value: unknown): TradexForecastSurface | null => {
  if (!value) return null;
  if (Array.isArray(value)) {
    const rows = value.map(normalizeForecastSurfaceRow);
    return rows.length ? { code: null, asof: null, rows } : null;
  }
  if (typeof value !== "object") return null;
  const source = value as Record<string, unknown>;
  const rowsSource =
    source.rows ??
    source.items ??
    source.forecast_surface ??
    source.forecastSurface ??
    source.surface ??
    source.item;
  const rows = Array.isArray(rowsSource)
    ? rowsSource.map(normalizeForecastSurfaceRow)
    : source.side || source.action_state || source.actionState
      ? [normalizeForecastSurfaceRow(source)]
      : [];
  if (!rows.length) return null;
  return {
    code: toText(source.code) || null,
    asof: toText(source.asof ?? source.asOf) || null,
    rows,
  };
};

const normalizePromotionDecision = (value: unknown): TradexPromotionDecision | null => {
  const source = value && typeof value === "object" ? (value as Record<string, unknown>) : {};
  const decisionId = toText(source.decision_id ?? source.decisionId);
  const decision = toText(source.decision);
  const note = toText(source.note);
  const actor = toText(source.actor);
  const createdAt = toText(source.created_at ?? source.createdAt);
  if (!decisionId && !decision && !note && !actor && !createdAt) return null;
  return {
    decisionId: decisionId || null,
    decision: decision || null,
    note: note || null,
    actor: actor || null,
    createdAt: createdAt || null,
  };
};

const normalizePromotionReview = (value: unknown): TradexPromotionReview | null => {
  const source = value && typeof value === "object" ? (value as Record<string, unknown>) : {};
  const reasonCodes = normalizeReasons(source.reason_codes ?? source.reasonCodes);
  const approvalDecision = normalizePromotionDecision(source.approval_decision ?? source.approvalDecision);
  const asOfDate = toText(source.as_of_date ?? source.asOfDate);
  const championVersion = toText(source.champion_version ?? source.championVersion);
  const challengerVersion = toText(source.challenger_version ?? source.challengerVersion);
  const sampleCount = toFiniteNumber(source.sample_count ?? source.sampleCount);
  const expectancyDelta = toFiniteNumber(source.expectancy_delta ?? source.expectancyDelta);
  const improvedExpectancy =
    source.improved_expectancy == null && source.improvedExpectancy == null
      ? null
      : Boolean(source.improved_expectancy ?? source.improvedExpectancy);
  const maeNonWorse =
    source.mae_non_worse == null && source.maeNonWorse == null
      ? null
      : Boolean(source.mae_non_worse ?? source.maeNonWorse);
  const adverseMoveNonWorse =
    source.adverse_move_non_worse == null && source.adverseMoveNonWorse == null
      ? null
      : Boolean(source.adverse_move_non_worse ?? source.adverseMoveNonWorse);
  const stableWindow =
    source.stable_window == null && source.stableWindow == null
      ? null
      : Boolean(source.stable_window ?? source.stableWindow);
  const alignmentOk =
    source.alignment_ok == null && source.alignmentOk == null
      ? null
      : Boolean(source.alignment_ok ?? source.alignmentOk);
  const readinessPass =
    source.readiness_pass == null && source.readinessPass == null
      ? null
      : Boolean(source.readiness_pass ?? source.readinessPass);
  if (
    !asOfDate &&
    !championVersion &&
    !challengerVersion &&
    sampleCount == null &&
    expectancyDelta == null &&
    improvedExpectancy == null &&
    maeNonWorse == null &&
    adverseMoveNonWorse == null &&
    stableWindow == null &&
    alignmentOk == null &&
    readinessPass == null &&
    reasonCodes.length === 0 &&
    !approvalDecision
  ) {
    return null;
  }
  return {
    asOfDate: asOfDate || null,
    championVersion: championVersion || null,
    challengerVersion: challengerVersion || null,
    sampleCount,
    expectancyDelta,
    improvedExpectancy,
    maeNonWorse,
    adverseMoveNonWorse,
    stableWindow,
    alignmentOk,
    readinessPass,
    reasonCodes,
    approvalDecision,
  };
};

const normalizeAnalysis = (value: unknown): TradexAnalysisOutput | null => {
  if (!value || typeof value !== "object") return null;
  const source = value as Record<string, unknown>;
  const comparisonsSource = source.candidate_comparisons ?? source.candidateComparisons;
  const promotionReview = normalizePromotionReview(source.promotion_review ?? source.promotionReview);
  const forecastSurface = normalizeForecastSurface(
    source.forecast_surface ??
      source.forecastSurface ??
      source.forecast_surface_daily ??
      source.forecastSurfaceDaily ??
      source.forecast ??
      source.forecast_surface_rows
  );
  return {
    symbol: toText(source.symbol, "unknown"),
    asof: toText(source.asof, "unknown"),
    source: toText(source.source) || null,
    displayLabel: toText(source.display_label ?? source.displayLabel) || null,
    sideRatios: normalizeSideRatios(source.side_ratios ?? source.sideRatios),
    confidence: toFiniteNumber(source.confidence),
    reasons: normalizeReasons(source.reasons),
    candidateComparisons: Array.isArray(comparisonsSource)
      ? comparisonsSource.map(normalizeCandidateComparison)
      : [],
    publishReadiness: normalizePublishReadiness(source.publish_readiness ?? source.publishReadiness),
    overrideState: normalizeOverrideState(source.override_state ?? source.overrideState),
    forecastSurface,
    promotionReview,
  };
};

export function shouldShowTradexDetailAnalysis(flag = import.meta.env.VITE_ENABLE_TRADEX_DETAIL_ANALYSIS) {
  const raw = toText(flag, "0").toLowerCase();
  return truthy.has(raw);
}

export function normalizeTradexDetailAnalysisReadResult(value: unknown): TradexAnalysisReadResult {
  if (!value || typeof value !== "object") {
    return { available: false, reason: "analysis unavailable", analysis: null, forecastSurface: null };
  }
  const source = value as Record<string, unknown>;
  const available = Boolean(source.available);
  const reason = toText(source.reason) || null;
  const analysis = normalizeAnalysis(source.analysis ?? source.item);
  const forecastSurface = normalizeForecastSurface(
    source.forecast_surface ??
      source.forecastSurface ??
      source.forecast_surface_daily ??
      source.forecastSurfaceDaily ??
      source.forecast ??
      source.forecast_surface_rows
  );
  const normalizedAnalysis =
    analysis == null
      ? null
      : {
          ...analysis,
          source: analysis.source ?? (toText(source.source) || null),
          displayLabel: analysis.displayLabel ?? (toText(source.display_label ?? source.displayLabel) || null),
        };
  if (!available || !analysis) {
    return {
      available: false,
      reason: reason || "analysis unavailable",
      analysis: null,
      forecastSurface,
    };
  }
  return {
    available: true,
    reason: null,
    analysis: normalizedAnalysis,
    forecastSurface,
  };
}
