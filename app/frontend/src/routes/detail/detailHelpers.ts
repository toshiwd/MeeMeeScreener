import type { ExactDecisionTone } from "./hooks/useExactDecisionRange";
import type {
  Candle,
  VolumePoint,
  ParseStats,
  AnalysisHorizonKey,
  AnalysisHorizonEntry,
  AnalysisHorizonPayload,
  RankRiskMode,
  AnalysisAdditiveSignals,
  AnalysisEntryPolicySide,
  AnalysisEntryPolicy,
  BuyStagePrecisionEntry,
  BuyStrategyBacktest,
  BuyStagePrecisionPayload,
  SellAnalysisFallback,
  AnalysisResearchPriorSide,
  AnalysisResearchPrior,
  AnalysisEdinetSummary,
  EdinetFinancialSummary,
  EdinetFinancialPoint,
  EdinetFinancialPanel,
  TdnetDisclosureItem,
  TdnetReactionSummary,
  AnalysisSwingPlan,
  AnalysisSwingSetupExpectancy,
  AnalysisSwingDiagnostics,
  AnalysisDecisionTone,
  AnalysisDecisionScenario,
  AnalysisDecision,
  EnvironmentTone,
  EnvironmentScenario,
  EnvironmentSummary,
  EnvironmentComputationInput,
  BarsMeta,
} from "./detailTypes";

export const ANALYSIS_BACKFILL_ACTIVE_STATUSES = new Set(["queued", "running", "cancel_requested"]);
export const EMPTY_EXACT_DECISION_TONE_BY_DATE = new Map<number, ExactDecisionTone>();
export const EXACT_DECISION_TONE_CACHE_BY_SCOPE = new Map<string, Map<number, ExactDecisionTone>>();

export const isCanceledRequestError = (error: unknown) => {
  if (!error || typeof error !== "object") return false;
  const err = error as { name?: string; code?: string };
  return err.name === "CanceledError" || err.code === "ERR_CANCELED";
};

export const DEFAULT_LIMITS = {
  daily: 2000,
  monthly: 240
};

export const LIMIT_STEP = {
  daily: 1000,
  monthly: 120
};

export const RANGE_PRESETS = [
  { label: "3M", months: 3 },
  { label: "6M", months: 6 },
  { label: "1Y", months: 12 },
  { label: "2Y", months: 24 }
];

export const ANALYSIS_HORIZONS = [5, 10, 20] as const;
export const ANALYSIS_DECISION_WINDOW_BARS = 130;
export const RANK_VIEW_STATE_KEY = "rankingViewState";

export const buildMonthBoundaries = (candles: Candle[]) => {
  if (!candles.length) return [];
  const boundaries: number[] = [];
  let prevKey: string | null = null;
  for (const candle of candles) {
    const date = new Date(candle.time * 1000);
    const key = `${date.getUTCFullYear()}-${date.getUTCMonth()}`;
    if (prevKey !== null && key !== prevKey) {
      boundaries.push(candle.time);
    }
    prevKey = key;
  }
  return boundaries;
};

export const buildYearBoundaries = (candles: Candle[]) => {
  if (!candles.length) return [];
  const boundaries: number[] = [];
  let prevYear: number | null = null;
  for (const candle of candles) {
    const year = new Date(candle.time * 1000).getUTCFullYear();
    if (prevYear !== null && year !== prevYear) {
      boundaries.push(candle.time);
    }
    prevYear = year;
  }
  return boundaries;
};

export const DAILY_ROW_RATIO = 12 / 16;
export const DEFAULT_WEEKLY_RATIO = 3 / 4;
export const MIN_WEEKLY_RATIO = 0.2;
export const MIN_MONTHLY_RATIO = 0.1;
export const MAX_EVENT_OFFSET_SEC = 3 * 24 * 60 * 60;

export const normalizeDateParts = (year: number, month: number, day: number) => {
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return null;
  if (year < 1900 || month < 1 || month > 12 || day < 1 || day > 31) return null;
  return Math.floor(Date.UTC(year, month - 1, day) / 1000);
};

export const formatNumber = (value: number | null | undefined, digits = 0) => {
  if (value == null || !Number.isFinite(value)) return "--";
  return value.toLocaleString("ja-JP", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits
  });
};

export const formatSignedNumber = (value: number | null | undefined, digits = 0) => {
  if (value == null || !Number.isFinite(value)) return "--";
  return value.toLocaleString("ja-JP", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
    signDisplay: "always"
  });
};

export const formatPercentLabel = (value: number | null | undefined, digits = 1) => {
  if (value == null || !Number.isFinite(value)) return "--";
  return `${(value * 100).toFixed(digits)}%`;
};

export const formatFinancialAmountLabel = (value: number | null | undefined) => {
  if (value == null || !Number.isFinite(value)) return "--";
  const abs = Math.abs(value);
  if (abs >= 1_000_000_000_000) return `${formatNumber(value / 1_000_000_000_000, 2)}兆円`;
  if (abs >= 100_000_000) return `${formatNumber(value / 100_000_000, 1)}億円`;
  if (abs >= 10_000) return `${formatNumber(value / 10_000, 1)}万円`;
  return `${formatNumber(value, 0)}円`;
};

export const formatPerLabel = (value: number | null | undefined) => {
  if (value == null || !Number.isFinite(value)) return "--";
  return `${formatNumber(value, 1)}倍`;
};

export const formatSignedPercentLabel = (value: number | null | undefined, digits = 1) => {
  if (value == null || !Number.isFinite(value)) return "--";
  const scaled = value * 100;
  const sign = scaled > 0 ? "+" : "";
  return `${sign}${scaled.toFixed(digits)}%`;
};

export const formatBarDate = (time: number | null | undefined) => {
  if (time == null || !Number.isFinite(time)) return "--";
  return new Date(time * 1000).toLocaleDateString("ja-JP");
};

export const buildTdnetReactionSummary = (
  candles: Candle[],
  volume: VolumePoint[],
  disclosure: TdnetDisclosureItem | null
): TdnetReactionSummary | null => {
  if (!disclosure?.publishedAt || candles.length === 0) return null;
  const publishedMs = Date.parse(disclosure.publishedAt);
  if (!Number.isFinite(publishedMs)) return null;
  const eventTime = Math.floor(publishedMs / 1000);
  const eventIndex = findNearestCandleIndex(candles, eventTime);
  if (eventIndex == null) return null;
  const baseCandle = candles[eventIndex];
  if (!baseCandle || Math.abs(baseCandle.time - eventTime) > 5 * 24 * 60 * 60) return null;
  const volumeMap = new Map(volume.map((item) => [item.time, item.value]));
  const currentVolume = volumeMap.get(baseCandle.time) ?? null;
  const priorVolumes = candles
    .slice(Math.max(0, eventIndex - 20), eventIndex)
    .map((candle) => volumeMap.get(candle.time) ?? null)
    .filter((value): value is number => value != null && Number.isFinite(value) && value > 0);
  const averagePriorVolume =
    priorVolumes.length > 0 ? priorVolumes.reduce((sum, value) => sum + value, 0) / priorVolumes.length : null;
  return {
    baseDate: formatBarDate(baseCandle.time),
    baseClose: baseCandle.close,
    volumeRatio:
      currentVolume != null && averagePriorVolume != null && averagePriorVolume > 0
        ? currentVolume / averagePriorVolume
        : null,
    reactions: [1, 5, 20].map((bars) => {
      const targetCandle = candles[eventIndex + bars] ?? null;
      return {
        bars,
        label: `+${bars}営業日`,
        targetDate: targetCandle ? formatBarDate(targetCandle.time) : null,
        returnRatio:
          targetCandle && Number.isFinite(baseCandle.close) && baseCandle.close !== 0
            ? (targetCandle.close - baseCandle.close) / baseCandle.close
            : null,
      };
    }),
  };
};

export const formatResearchPriorRank = (rank: number | null | undefined, universe: number | null | undefined) => {
  if (!Number.isFinite(rank ?? NaN)) return "--";
  const base = `#${Math.max(1, Math.round(rank ?? 0))}`;
  if (!Number.isFinite(universe ?? NaN)) return base;
  return `${base}/${Math.max(1, Math.round(universe ?? 0))}`;
};

export const formatResearchPriorMetaLine = (label: string, side: AnalysisResearchPriorSide | null) => {
  if (!side) return `${label} --`;
  const status = side.aligned ? "一致" : "非一致";
  const rank = formatResearchPriorRank(side.rank, side.universe);
  const bonus = formatSignedPercentLabel(side.bonus);
  return `${label} ${status} / Rank ${rank} / 補正 ${bonus}${side.asOf ? ` / asOf ${side.asOf}` : ""}`;
};

export const formatEdinetStatus = (value: string | null | undefined) => {
  if (!value) return "未判定";
  if (value === "ok") return "OK";
  if (value === "missing_tables") return "テーブル不足";
  if (value === "unmapped") return "未マップ";
  if (value === "no_payload") return "データなし";
  return value;
};

export const isNonEmptyString = (value: unknown): value is string =>
  typeof value === "string" && value.length > 0;

export const joinMetaSegments = (segments: Array<string | null | undefined>) =>
  segments.filter(isNonEmptyString).join(" / ");

export const normalizeTickerName = (value: string | null | undefined) => {
  const cleaned = (value ?? "").replace(/\s*\?\s*$/, "").trim();
  return cleaned === "?" ? "" : cleaned;
};

export const toFiniteNumber = (value: unknown): number | null => {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
};

export const toBoolean = (value: unknown): boolean => {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return Number.isFinite(value) && value !== 0;
  if (typeof value === "string") {
    const trimmed = value.trim().toLowerCase();
    return trimmed === "1" || trimmed === "true" || trimmed === "yes";
  }
  return false;
};

export const resolveSellShortScore = (sellAnalysis: SellAnalysisFallback | null | undefined): number | null => {
  const direct = toFiniteNumber(sellAnalysis?.shortScore);
  if (direct != null) return direct;
  const aScore = toFiniteNumber(sellAnalysis?.aScore);
  const bScore = toFiniteNumber(sellAnalysis?.bScore);
  if (aScore == null && bScore == null) return null;
  return (aScore ?? 0) + (bScore ?? 0);
};

export const normalizeRiskMode = (value: unknown): RankRiskMode => {
  if (value === "defensive" || value === "aggressive" || value === "balanced") {
    return value;
  }
  return "balanced";
};

export const resolveRiskModeFromSession = (): RankRiskMode => {
  if (typeof window === "undefined") return "balanced";
  try {
    const raw = window.sessionStorage.getItem(RANK_VIEW_STATE_KEY);
    if (!raw) return "balanced";
    const parsed = JSON.parse(raw) as { riskMode?: unknown };
    return normalizeRiskMode(parsed?.riskMode);
  } catch {
    return "balanced";
  }
};

export const normalizeEntryPolicySide = (value: unknown): AnalysisEntryPolicySide | null => {
  if (!value || typeof value !== "object") return null;
  const payload = value as Record<string, unknown>;
  return {
    setupType: typeof payload.setupType === "string" ? payload.setupType : null,
    recommendedHoldDays: toFiniteNumber(payload.recommendedHoldDays),
    recommendedHoldMinDays: toFiniteNumber(payload.recommendedHoldMinDays),
    recommendedHoldMaxDays: toFiniteNumber(payload.recommendedHoldMaxDays),
    recommendedHoldReason: typeof payload.recommendedHoldReason === "string" ? payload.recommendedHoldReason : null,
    invalidationTrigger: typeof payload.invalidationTrigger === "string" ? payload.invalidationTrigger : null,
    invalidationConservativeAction:
      typeof payload.invalidationConservativeAction === "string" ? payload.invalidationConservativeAction : null,
    invalidationAggressiveAction:
      typeof payload.invalidationAggressiveAction === "string" ? payload.invalidationAggressiveAction : null,
    invalidationRecommendedAction:
      typeof payload.invalidationRecommendedAction === "string" ? payload.invalidationRecommendedAction : null,
    invalidationDotenRecommended: toBoolean(payload.invalidationDotenRecommended),
    invalidationOppositeHoldDays: toFiniteNumber(payload.invalidationOppositeHoldDays),
    invalidationExpectedDeltaMean: toFiniteNumber(payload.invalidationExpectedDeltaMean),
    invalidationPolicyNote: typeof payload.invalidationPolicyNote === "string" ? payload.invalidationPolicyNote : null,
    playbookScoreBonus: toFiniteNumber(payload.playbookScoreBonus)
  };
};

export const normalizeEntryPolicy = (value: unknown): AnalysisEntryPolicy | null => {
  if (!value || typeof value !== "object") return null;
  const payload = value as Record<string, unknown>;
  return {
    riskMode: normalizeRiskMode(payload.riskMode),
    up: normalizeEntryPolicySide(payload.up),
    down: normalizeEntryPolicySide(payload.down)
  };
};

export const normalizeResearchPriorSide = (value: unknown): AnalysisResearchPriorSide | null => {
  if (!value || typeof value !== "object") return null;
  const payload = value as Record<string, unknown>;
  return {
    aligned: toBoolean(payload.aligned),
    rank: toFiniteNumber(payload.rank),
    universe: toFiniteNumber(payload.universe),
    bonus: toFiniteNumber(payload.bonus),
    asOf: typeof payload.asOf === "string" ? payload.asOf : null
  };
};

export const normalizeResearchPrior = (value: unknown): AnalysisResearchPrior | null => {
  if (!value || typeof value !== "object") return null;
  const payload = value as Record<string, unknown>;
  const runId = typeof payload.runId === "string" ? payload.runId : null;
  const up = normalizeResearchPriorSide(payload.up);
  const down = normalizeResearchPriorSide(payload.down);
  if (!runId && !up && !down) return null;
  return { runId, up, down };
};

export const normalizeEdinetSummary = (value: unknown): AnalysisEdinetSummary | null => {
  if (!value || typeof value !== "object") return null;
  const payload = value as Record<string, unknown>;
  const status = typeof payload.status === "string" ? payload.status : null;
  const mapped = payload.mapped == null ? null : toBoolean(payload.mapped);
  const featureFlagApplied = payload.featureFlagApplied == null ? null : toBoolean(payload.featureFlagApplied);
  const parsed: AnalysisEdinetSummary = {
    status,
    mapped,
    freshnessDays: toFiniteNumber(payload.freshnessDays),
    metricCount: toFiniteNumber(payload.metricCount),
    qualityScore: toFiniteNumber(payload.qualityScore),
    dataScore: toFiniteNumber(payload.dataScore),
    scoreBonus: toFiniteNumber(payload.scoreBonus),
    featureFlagApplied,
    ebitdaMetric: toFiniteNumber(payload.ebitdaMetric),
    roe: toFiniteNumber(payload.roe),
    equityRatio: toFiniteNumber(payload.equityRatio),
    debtRatio: toFiniteNumber(payload.debtRatio),
    operatingCfMargin: toFiniteNumber(payload.operatingCfMargin),
    revenueGrowthYoy: toFiniteNumber(payload.revenueGrowthYoy),
  };
  const hasAny =
    parsed.status != null ||
    parsed.mapped != null ||
    parsed.freshnessDays != null ||
    parsed.metricCount != null ||
    parsed.qualityScore != null ||
    parsed.dataScore != null ||
    parsed.scoreBonus != null ||
    parsed.featureFlagApplied != null ||
    parsed.ebitdaMetric != null ||
    parsed.roe != null ||
    parsed.equityRatio != null ||
    parsed.debtRatio != null ||
    parsed.operatingCfMargin != null ||
    parsed.revenueGrowthYoy != null;
  return hasAny ? parsed : null;
};

export const normalizeSwingSetupExpectancy = (value: unknown): AnalysisSwingSetupExpectancy | null => {
  if (!value || typeof value !== "object") return null;
  const payload = value as Record<string, unknown>;
  const parsed: AnalysisSwingSetupExpectancy = {
    asOfYmd: toFiniteNumber(payload.asOfYmd),
    side:
      payload.side === "long" || payload.side === "short"
        ? payload.side
        : null,
    setupType: typeof payload.setupType === "string" ? payload.setupType : null,
    horizonDays: toFiniteNumber(payload.horizonDays),
    samples: toFiniteNumber(payload.samples),
    winRate: toFiniteNumber(payload.winRate),
    meanRet: toFiniteNumber(payload.meanRet),
    shrunkMeanRet: toFiniteNumber(payload.shrunkMeanRet),
    p25Ret: toFiniteNumber(payload.p25Ret),
    p10Ret: toFiniteNumber(payload.p10Ret),
    maxAdverse: toFiniteNumber(payload.maxAdverse),
    sideMeanRet: toFiniteNumber(payload.sideMeanRet),
  };
  const hasAny =
    parsed.asOfYmd != null ||
    parsed.side != null ||
    parsed.setupType != null ||
    parsed.horizonDays != null ||
    parsed.samples != null ||
    parsed.winRate != null ||
    parsed.meanRet != null ||
    parsed.shrunkMeanRet != null ||
    parsed.p25Ret != null ||
    parsed.p10Ret != null ||
    parsed.maxAdverse != null ||
    parsed.sideMeanRet != null;
  return hasAny ? parsed : null;
};

export const normalizeSwingPlan = (value: unknown): AnalysisSwingPlan | null => {
  if (!value || typeof value !== "object") return null;
  const payload = value as Record<string, unknown>;
  const side = payload.side;
  if (side !== "long" && side !== "short") return null;
  const reasonsRaw = payload.reasons;
  return {
    code: typeof payload.code === "string" ? payload.code : null,
    side,
    score: toFiniteNumber(payload.score),
    horizonDays: toFiniteNumber(payload.horizonDays),
    entry: toFiniteNumber(payload.entry),
    stop: toFiniteNumber(payload.stop),
    tp1: toFiniteNumber(payload.tp1),
    tp2: toFiniteNumber(payload.tp2),
    timeStopDays: toFiniteNumber(payload.timeStopDays),
    reasons: Array.isArray(reasonsRaw) ? reasonsRaw.filter((entry): entry is string => typeof entry === "string") : [],
  };
};

export const normalizeSwingDiagnostics = (value: unknown): AnalysisSwingDiagnostics | null => {
  if (!value || typeof value !== "object") return null;
  const payload = value as Record<string, unknown>;
  const parsed: AnalysisSwingDiagnostics = {
    edge: toFiniteNumber(payload.edge),
    risk: toFiniteNumber(payload.risk),
    setupExpectancy: normalizeSwingSetupExpectancy(payload.setupExpectancy),
    regimeFit: toFiniteNumber(payload.regimeFit),
    atrPct: toFiniteNumber(payload.atrPct),
    liquidity20d: toFiniteNumber(payload.liquidity20d),
  };
  const hasAny =
    parsed.edge != null ||
    parsed.risk != null ||
    parsed.setupExpectancy != null ||
    parsed.regimeFit != null ||
    parsed.atrPct != null ||
    parsed.liquidity20d != null;
  return hasAny ? parsed : null;
};

export const normalizeDecisionTone = (value: unknown): AnalysisDecisionTone => {
  if (value === "up" || value === "down" || value === "neutral") return value;
  return "neutral";
};

export const normalizeAnalysisDecision = (value: unknown): AnalysisDecision | null => {
  if (!value || typeof value !== "object") return null;
  const payload = value as Record<string, unknown>;
  const tone = normalizeDecisionTone(payload.tone);
  const scenariosRaw = Array.isArray(payload.scenarios) ? payload.scenarios : [];
  const scenarios: AnalysisDecisionScenario[] = scenariosRaw
    .map((entry) => {
      if (!entry || typeof entry !== "object") return null;
      const row = entry as Record<string, unknown>;
      const key = row.key === "up" || row.key === "down" || row.key === "range" ? row.key : null;
      if (!key) return null;
      const score = toFiniteNumber(row.score);
      if (score == null) return null;
      return {
        key,
        label: typeof row.label === "string" ? row.label : key,
        tone: normalizeDecisionTone(row.tone),
        score: clamp(score, 0, 1)
      };
    })
    .filter((entry): entry is AnalysisDecisionScenario => entry != null);
  const fallbackScenarios: AnalysisDecisionScenario[] =
    scenarios.length > 0
      ? scenarios
      : [
        {
          key: "up",
          label: "上昇継続（押し目再開）",
          tone: "up",
          score: clamp(toFiniteNumber(payload.buyProb) ?? 0, 0, 1)
        },
        {
          key: "down",
          label: "下落継続（戻り売り優位）",
          tone: "down",
          score: clamp(toFiniteNumber(payload.sellProb) ?? 0, 0, 1)
        },
        {
          key: "range",
          label: "往復レンジ（上下振れ）",
          tone: "neutral",
          score: clamp(toFiniteNumber(payload.neutralProb) ?? 0, 0, 1)
        }
      ];
  return {
    tone,
    sideLabel: typeof payload.sideLabel === "string" ? payload.sideLabel : null,
    patternLabel: typeof payload.patternLabel === "string" ? payload.patternLabel : null,
    environmentLabel: typeof payload.environmentLabel === "string" ? payload.environmentLabel : null,
    confidence: toFiniteNumber(payload.confidence),
    buyProb: toFiniteNumber(payload.buyProb),
    sellProb: toFiniteNumber(payload.sellProb),
    neutralProb: toFiniteNumber(payload.neutralProb),
    version: typeof payload.version === "string" ? payload.version : null,
    scenarios: fallbackScenarios.sort((a, b) => b.score - a.score)
  };
};

export const normalizeAnalysisHorizonEntry = (horizon: AnalysisHorizonKey, value: unknown): AnalysisHorizonEntry => {
  const payload = value && typeof value === "object" ? (value as Record<string, unknown>) : {};
  return {
    horizon,
    pUp: toFiniteNumber(payload.pUp),
    pDown: toFiniteNumber(payload.pDown),
    evNet: toFiniteNumber(payload.evNet),
    pTurnDown: toFiniteNumber(payload.pTurnDown),
    pTurnUp: toFiniteNumber(payload.pTurnUp),
    pUpProjected: payload.pUpProjected === true,
    evProjected: payload.evProjected === true,
    turnProjected: payload.turnProjected === true
  };
};

export const normalizeHorizonAnalysis = (value: unknown): AnalysisHorizonPayload | null => {
  if (!value || typeof value !== "object") return null;
  const payload = value as Record<string, unknown>;
  const defaultHorizonRaw = toFiniteNumber(payload.defaultHorizon);
  const defaultHorizon: AnalysisHorizonKey =
    defaultHorizonRaw === 5 || defaultHorizonRaw === 10 || defaultHorizonRaw === 20
      ? defaultHorizonRaw
      : 20;
  const itemsRaw =
    payload.items && typeof payload.items === "object"
      ? (payload.items as Record<string, unknown>)
      : {};
  const items: Partial<Record<`${AnalysisHorizonKey}`, AnalysisHorizonEntry>> = {};
  ANALYSIS_HORIZONS.forEach((horizon) => {
    const key = String(horizon) as `${AnalysisHorizonKey}`;
    items[key] = normalizeAnalysisHorizonEntry(horizon, itemsRaw[key]);
  });
  return {
    defaultHorizon,
    turnBaseHorizon: toFiniteNumber(payload.turnBaseHorizon),
    projectionMethod: typeof payload.projectionMethod === "string" ? payload.projectionMethod : null,
    items
  };
};

export const normalizeAdditiveSignals = (value: unknown): AnalysisAdditiveSignals | null => {
  if (!value || typeof value !== "object") return null;
  const payload = value as Record<string, unknown>;
  return {
    trendUpStrict: toBoolean(payload.trendUpStrict),
    mtfStrongAligned: toBoolean(payload.mtfStrongAligned),
    boxBottomAligned: toBoolean(payload.boxBottomAligned),
    shootingStarLike: toBoolean(payload.shootingStarLike),
    threeWhiteSoldiers: toBoolean(payload.threeWhiteSoldiers),
    bullEngulfing: toBoolean(payload.bullEngulfing),
    reclaim60: toBoolean(payload.reclaim60),
    v60Core: toBoolean(payload.v60Core),
    v60Strong: toBoolean(payload.v60Strong),
    v60StrongPenalty: toBoolean(payload.v60StrongPenalty),
    candlestickPatternBonus: toFiniteNumber(payload.candlestickPatternBonus),
    bonusEstimate: toFiniteNumber(payload.bonusEstimate),
    weeklyBreakoutUpProb: toFiniteNumber(payload.weeklyBreakoutUpProb),
    monthlyBreakoutUpProb: toFiniteNumber(payload.monthlyBreakoutUpProb),
    monthlyRangeProb: toFiniteNumber(payload.monthlyRangeProb),
    monthlyRangePos: toFiniteNumber(payload.monthlyRangePos)
  };
};

export const normalizeBuyStagePrecisionEntry = (value: unknown): BuyStagePrecisionEntry => {
  const payload = value && typeof value === "object" ? (value as Record<string, unknown>) : {};
  const samplesRaw = toFiniteNumber(payload.samples);
  const winsRaw = toFiniteNumber(payload.wins);
  const samples = Math.max(0, Math.trunc(samplesRaw ?? 0));
  const wins = Math.max(0, Math.min(samples, Math.trunc(winsRaw ?? 0)));
  const precisionRaw = toFiniteNumber(payload.precision);
  const precision =
    precisionRaw != null
      ? clamp(precisionRaw, 0, 1)
      : samples > 0
        ? clamp(wins / samples, 0, 1)
        : null;
  return { precision, samples, wins };
};

export const normalizeBuyStrategyBacktest = (value: unknown): BuyStrategyBacktest | null => {
  if (!value || typeof value !== "object") return null;
  const payload = value as Record<string, unknown>;
  const samplesRaw = toFiniteNumber(payload.samples);
  const winsRaw = toFiniteNumber(payload.wins);
  const samples = Math.max(0, Math.trunc(samplesRaw ?? 0));
  const wins = Math.max(0, Math.min(samples, Math.trunc(winsRaw ?? 0)));
  const precisionRaw = toFiniteNumber(payload.precision);
  const precision =
    precisionRaw != null
      ? clamp(precisionRaw, 0, 1)
      : samples > 0
        ? clamp(wins / samples, 0, 1)
        : null;
  return {
    precision,
    samples,
    wins,
    probeShares: toFiniteNumber(payload.probeShares),
    addShares: toFiniteNumber(payload.addShares),
    coreShares: toFiniteNumber(payload.coreShares),
    topupShares: toFiniteNumber(payload.topupShares),
    targetShares: toFiniteNumber(payload.targetShares),
    takeProfitPct: toFiniteNumber(payload.takeProfitPct)
  };
};

export const normalizeBuyStagePrecision = (value: unknown): BuyStagePrecisionPayload | null => {
  if (!value || typeof value !== "object") return null;
  const payload = value as Record<string, unknown>;
  return {
    horizon: toFiniteNumber(payload.horizon),
    lookbackBars: toFiniteNumber(payload.lookbackBars),
    probe: normalizeBuyStagePrecisionEntry(payload.probe),
    add: normalizeBuyStagePrecisionEntry(payload.add),
    core: normalizeBuyStagePrecisionEntry(payload.core),
    strategy: normalizeBuyStrategyBacktest(payload.strategy)
  };
};

export const formatLedgerDate = (value: string) => {
  const trimmed = value?.trim();
  if (!trimmed) return "--";
  const match = trimmed.match(/^(\d{4})[/-](\d{1,2})[/-](\d{1,2})$/);
  if (!match) return trimmed;
  const year = match[1];
  const month = match[2].padStart(2, "0");
  const day = match[3].padStart(2, "0");
  return `${year}-${month}-${day}`;
};

export const normalizeTime = (value: unknown) => {
  if (typeof value === "number" && Number.isFinite(value)) {
    if (value > 10_000_000_000_000) return Math.floor(value / 1000);
    if (value > 10_000_000_000) return Math.floor(value / 10);
    if (value >= 10_000_000 && value < 100_000_000) {
      const year = Math.floor(value / 10000);
      const month = Math.floor((value % 10000) / 100);
      const day = value % 100;
      return normalizeDateParts(year, month, day);
    }
    if (value >= 100_000 && value < 1_000_000) {
      const year = Math.floor(value / 100);
      const month = value % 100;
      return normalizeDateParts(year, month, 1);
    }
    return Math.floor(value);
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (/^\d{8}$/.test(trimmed)) {
      const year = Number(trimmed.slice(0, 4));
      const month = Number(trimmed.slice(4, 6));
      const day = Number(trimmed.slice(6, 8));
      return normalizeDateParts(year, month, day);
    }
    if (/^\d{6}$/.test(trimmed)) {
      const year = Number(trimmed.slice(0, 4));
      const month = Number(trimmed.slice(4, 6));
      return normalizeDateParts(year, month, 1);
    }
    const match = trimmed.match(/^(\d{4})[/-](\d{1,2})[/-](\d{1,2})$/);
    if (match) {
      const year = Number(match[1]);
      const month = Number(match[2]);
      const day = Number(match[3]);
      return normalizeDateParts(year, month, day);
    }
  }
  return null;
};

export const computeMA = (candles: Candle[], period: number) => {
  if (period <= 1) {
    return candles.map((c) => ({ time: c.time, value: c.close }));
  }
  const data: { time: number; value: number }[] = [];
  let sum = 0;
  for (let i = 0; i < candles.length; i += 1) {
    sum += candles[i].close;
    if (i >= period) {
      sum -= candles[i - period].close;
    }
    if (i >= period - 1) {
      data.push({ time: candles[i].time, value: sum / period });
    }
  }
  return data;
};

export const buildCandlesWithStats = (rows: number[][]) => {
  const entries: Candle[] = [];
  const stats: ParseStats = {
    total: rows.length,
    parsed: 0,
    invalidRow: 0,
    invalidTime: 0,
    invalidValue: 0
  };
  for (const row of rows) {
    if (!Array.isArray(row) || row.length < 5) {
      stats.invalidRow += 1;
      continue;
    }
    const time = normalizeTime(row[0]);
    if (time == null) {
      stats.invalidTime += 1;
      continue;
    }
    const open = Number(row[1]);
    const high = Number(row[2]);
    const low = Number(row[3]);
    const close = Number(row[4]);
    if (![open, high, low, close].every((value) => Number.isFinite(value))) {
      stats.invalidValue += 1;
      continue;
    }
    entries.push({ time, open, high, low, close });
  }
  entries.sort((a, b) => a.time - b.time);
  const deduped: Candle[] = [];
  let lastTime = -1;
  for (const item of entries) {
    if (item.time === lastTime) continue;
    deduped.push(item);
    lastTime = item.time;
  }
  stats.parsed = deduped.length;
  return { candles: deduped, stats };
};

export const buildVolume = (rows: number[][]): VolumePoint[] => {
  const entries: VolumePoint[] = [];
  for (const row of rows) {
    if (!Array.isArray(row) || row.length < 6) continue;
    const time = normalizeTime(row[0]);
    if (time == null) continue;
    if (row[5] == null || row[5] === "") continue;
    const value = Number(row[5]);
    if (!Number.isFinite(value)) continue;
    entries.push({ time, value });
  }
  entries.sort((a, b) => a.time - b.time);
  const deduped: VolumePoint[] = [];
  let lastTime = -1;
  for (const item of entries) {
    if (item.time === lastTime) continue;
    deduped.push(item);
    lastTime = item.time;
  }
  return deduped;
};

export const buildWeekly = (candles: Candle[], volume: VolumePoint[]) => {
  const volumeMap = new Map(volume.map((item) => [item.time, item.value]));
  const groups = new Map<number, { candle: Candle; volume: number }>();

  for (const candle of candles) {
    const date = new Date(candle.time * 1000);
    const day = date.getUTCDay();
    const diff = (day + 6) % 7;
    const weekStart = Date.UTC(
      date.getUTCFullYear(),
      date.getUTCMonth(),
      date.getUTCDate() - diff
    );
    const key = Math.floor(weekStart / 1000);
    const vol = volumeMap.get(candle.time) ?? 0;
    const existing = groups.get(key);
    if (!existing) {
      groups.set(key, {
        candle: { ...candle, time: key },
        volume: vol
      });
    } else {
      existing.candle.high = Math.max(existing.candle.high, candle.high);
      existing.candle.low = Math.min(existing.candle.low, candle.low);
      existing.candle.close = candle.close;
      existing.volume += vol;
    }
  }

  const sorted = [...groups.entries()].sort((a, b) => a[0] - b[0]);
  const weeklyCandles = sorted.map((item) => item[1].candle);
  const weeklyVolume = sorted.map((item) => ({
    time: item[1].candle.time,
    value: item[1].volume
  }));
  return { candles: weeklyCandles, volume: weeklyVolume };
};

export const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value));

export const computeEnvironmentTone = (input: EnvironmentComputationInput): EnvironmentSummary => {
  const includeReasons = input.includeReasons !== false;
  const upProb = clamp(
    input.analysisPUp ?? (input.analysisPDown != null ? 1 - input.analysisPDown : 0.5),
    0,
    1
  );
  const downProb = clamp(
    input.analysisPDown ?? (input.sellAnalysis?.pDown ?? (input.analysisPUp != null ? 1 - input.analysisPUp : 0.5)),
    0,
    1
  );
  const playbookUpBias = clamp((input.playbookUpScoreBonus ?? 0) / 0.04, -0.35, 0.35);
  const playbookDownBias = clamp((input.playbookDownScoreBonus ?? 0) / 0.04, -0.35, 0.35);
  const turnUp = clamp(input.analysisPTurnUp ?? 0.5, 0, 1);
  const turnDown = clamp(
    ((input.analysisPTurnDown ?? 0.5) * 0.5) +
    ((input.sellAnalysis?.pTurnDown ?? input.analysisPTurnDown ?? 0.5) * 0.5),
    0,
    1
  );
  const evBias = input.analysisEvNet == null ? 0 : clamp(input.analysisEvNet / 0.06, -1, 1);
  const additiveBias =
    input.additiveSignals?.bonusEstimate == null
      ? 0
      : clamp(input.additiveSignals.bonusEstimate / 0.06, -1, 1);
  const trendDownPenalty =
    input.sellAnalysis?.trendDownStrict ? 0.08 : input.sellAnalysis?.trendDown ? 0.04 : 0;
  const trendDownBoost =
    input.sellAnalysis?.trendDownStrict ? 1.0 : input.sellAnalysis?.trendDown ? 0.7 : 0.3;
  const trendDownFlag = input.sellAnalysis?.trendDown === true;
  const trendDownStrictFlag = input.sellAnalysis?.trendDownStrict === true;
  const resolvedShortScore = resolveSellShortScore(input.sellAnalysis);
  const shortScoreNorm = clamp(((resolvedShortScore ?? 70) - 70) / 90, 0, 1);
  const bullishStructure = Boolean(
    !trendDownFlag &&
    (input.sellAnalysis?.distMa20Signed ?? 0) > 0 &&
    (input.sellAnalysis?.ma20Slope ?? 0) >= 0 &&
    (input.sellAnalysis?.ma60Slope ?? 0) >= 0
  );
  const probabilisticShortSignal = Boolean(
    downProb >= 0.60 &&
    turnDown >= 0.60 &&
    (input.analysisEvNet ?? 0) <= 0.01
  );
  const shortSignalConfirmed =
    trendDownFlag ||
    trendDownStrictFlag ||
    shortScoreNorm >= 0.34 ||
    probabilisticShortSignal;
  const strongUpContext = Boolean(
    input.additiveSignals?.trendUpStrict &&
    (input.additiveSignals?.monthlyBreakoutUpProb ?? 0) >= 0.8
  );

  const upScore = clamp(
    0.5 * upProb +
    0.18 * turnUp +
    0.17 * (0.5 + evBias * 0.5) +
    0.15 * (0.5 + additiveBias * 0.5) -
    0.06 * playbookDownBias +
    0.08 * playbookUpBias -
    trendDownPenalty,
    0,
    1
  );
  const downScore = clamp(
    0.45 * downProb +
    0.22 * turnDown +
    0.18 * (0.5 - evBias * 0.5) +
    0.1 * trendDownBoost +
    0.08 * playbookDownBias -
    0.06 * playbookUpBias +
    0.05 * (0.5 - additiveBias * 0.5),
    0,
    1
  );
  const sellSignalQuality = clamp(
    0.38 * downProb +
    0.22 * turnDown +
    0.14 * clamp((-(input.analysisEvNet ?? 0) + 0.005) / 0.04, 0, 1) +
    0.16 * (trendDownStrictFlag ? 1.0 : trendDownFlag ? 0.72 : 0.2) +
    0.1 * shortScoreNorm -
    0.12 * (bullishStructure ? 1 : 0),
    0,
    1
  );
  const rangeScore = clamp(
    0.4 * (1 - Math.abs(upProb - downProb)) +
    0.3 * Math.min(turnUp, turnDown) +
    0.3 * (1 - Math.abs(evBias)),
    0,
    1
  );
  const forceUpReclaim = Boolean(
    strongUpContext &&
    input.additiveSignals?.mtfStrongAligned &&
    upScore >= downScore &&
    turnUp >= turnDown - 0.10
  );
  const forceDownConfirm = Boolean(
    (trendDownStrictFlag && downProb >= 0.58 && turnDown >= 0.56 && (input.analysisEvNet ?? 0) <= 0) ||
    (downProb >= 0.70 && turnDown >= 0.66 && (input.analysisEvNet ?? 0) <= -0.01 && shortScoreNorm >= 0.34)
  );
  const downConfirm = Boolean(
    trendDownFlag ||
    trendDownStrictFlag ||
    ((downProb - upProb >= 0.10 || downProb >= 0.62) && shortSignalConfirmed)
  );
  const downThreshold = strongUpContext ? 0.68 : 0.58;

  const scenarios: EnvironmentScenario[] = [
    {
      key: "up",
      label: "上昇継続（押し目再開）",
      tone: "up",
      score: upScore,
      reasons: includeReasons
        ? [
          `上昇 ${formatPercentLabel(upProb)} / 下落 ${formatPercentLabel(downProb)}`,
          `期待値 ${formatSignedPercentLabel(input.analysisEvNet)}`,
          `転換 上 ${formatPercentLabel(turnUp)}`
        ]
        : []
    },
    {
      key: "down",
      label: "下落継続（戻り売り優位）",
      tone: "down",
      score: downScore,
      reasons: includeReasons
        ? [
          `下落 ${formatPercentLabel(downProb)} / 上昇 ${formatPercentLabel(upProb)}`,
          `転換 下 ${formatPercentLabel(turnDown)}`,
          `売り品質 ${formatPercentLabel(sellSignalQuality)}`,
          `下向きトレンド判定 ${input.sellAnalysis?.trendDownStrict ? "強い" : input.sellAnalysis?.trendDown ? "あり" : "弱い"}`
        ]
        : []
    },
    {
      key: "range",
      label: "往復レンジ（上下振れ）",
      tone: "neutral",
      score: rangeScore,
      reasons: includeReasons
        ? [
          `確率差 ${formatPercentLabel(Math.abs(upProb - downProb), 1)}`,
          `転換 上 ${formatPercentLabel(turnUp)} / 下 ${formatPercentLabel(turnDown)}`,
          `方向感弱め ${(1 - Math.abs(evBias)).toFixed(2)}`
        ]
        : []
    }
  ].sort((a, b) => b.score - a.score);

  const top = scenarios[0];
  const preSurgeLongCandidate = Boolean(
    !trendDownStrictFlag &&
    (
      input.additiveSignals?.boxBottomAligned ||
      (
        (input.additiveSignals?.monthlyRangeProb ?? 0) >= 0.6 &&
        (input.additiveSignals?.monthlyRangePos ?? 1) <= 0.45
      )
    ) &&
    turnUp >= turnDown - 0.08 &&
    upScore >= downScore - 0.04
  );
  const preSurgeShortCandidate = Boolean(
    downConfirm &&
    turnDown >= turnUp - 0.08 &&
    downScore >= upScore - 0.04
  );
  let environmentLabel = "方向感拮抗";
  let environmentTone: EnvironmentTone = "neutral";
  if (forceUpReclaim && upScore >= 0.56) {
    environmentLabel = "上昇優位";
    environmentTone = "up";
  } else if (forceDownConfirm) {
    environmentLabel = "下落優位";
    environmentTone = "down";
  } else if (top?.key === "up" && top.score >= 0.56) {
    environmentLabel = "上昇優位";
    environmentTone = "up";
  } else if (
    top?.key === "down" &&
    top.score >= downThreshold &&
    downConfirm &&
    sellSignalQuality >= 0.52
  ) {
    environmentLabel = "下落優位";
    environmentTone = "down";
  } else if (top?.key === "range" && top.score >= 0.56) {
    if (preSurgeLongCandidate && !preSurgeShortCandidate) {
      environmentLabel = "レンジ優位（先回り買い監視）";
    } else if (preSurgeShortCandidate && !preSurgeLongCandidate) {
      environmentLabel = "レンジ優位（戻り売り監視）";
    } else {
      environmentLabel = "レンジ優位";
    }
    environmentTone = "neutral";
  }

  let markerTone: "up" | "down" | null =
    environmentTone === "up" || environmentTone === "down" ? environmentTone : null;
  let markerIsSetup = false;
  if (markerTone == null) {
    const setupBuySignal = Boolean(
      preSurgeLongCandidate &&
      !trendDownFlag &&
      upProb >= 0.58 &&
      downProb <= 0.50 &&
      turnUp >= Math.max(0.58, turnDown + 0.02) &&
      upScore >= downScore - 0.04
    );
    const setupSellSignal = Boolean(
      preSurgeShortCandidate &&
      downProb >= 0.58 &&
      turnDown >= Math.max(0.60, turnUp) &&
      downScore >= upScore - 0.06
    );
    const trendAssistedSellSetup = Boolean(
      trendDownFlag &&
      upProb <= 0.56 &&
      downProb >= 0.32 &&
      turnUp <= 0.40 &&
      turnDown >= 0.24
    );
    if (setupBuySignal || setupSellSignal) {
      markerTone = setupBuySignal && !setupSellSignal
        ? "up"
        : setupSellSignal && !setupBuySignal
          ? "down"
          : upScore >= downScore
            ? "up"
            : "down";
      markerIsSetup = true;
    } else if (trendAssistedSellSetup) {
      markerTone = "down";
      markerIsSetup = true;
    }
  }

  return {
    environmentLabel,
    environmentTone,
    markerTone,
    markerIsSetup,
    scenarios
  };
};

export const normalizeEdinetFinancialSummary = (value: unknown): EdinetFinancialSummary | null => {
  if (!value || typeof value !== "object") return null;
  const source = value as Record<string, unknown>;
  return {
    latestFiscalYear: toFiniteNumber(source.latestFiscalYear),
    equityRatio: toFiniteNumber(source.equityRatio),
    eps: toFiniteNumber(source.eps),
    bps: toFiniteNumber(source.bps),
    dividendPerShare: toFiniteNumber(source.dividendPerShare),
    netInterestBearingDebt: toFiniteNumber(source.netInterestBearingDebt),
  };
};

export const normalizeEdinetFinancialPoint = (value: unknown): EdinetFinancialPoint | null => {
  if (!value || typeof value !== "object") return null;
  const source = value as Record<string, unknown>;
  return {
    fiscalYear: toFiniteNumber(source.fiscalYear),
    label: typeof source.label === "string" ? source.label : "--",
    revenue: toFiniteNumber(source.revenue),
    grossProfit: toFiniteNumber(source.grossProfit),
    operatingIncome: toFiniteNumber(source.operatingIncome),
    netIncome: toFiniteNumber(source.netIncome),
    grossMargin: toFiniteNumber(source.grossMargin),
    operatingMargin: toFiniteNumber(source.operatingMargin),
    netMargin: toFiniteNumber(source.netMargin),
    roe: toFiniteNumber(source.roe),
    roa: toFiniteNumber(source.roa),
    eps: toFiniteNumber(source.eps),
    bps: toFiniteNumber(source.bps),
    dividendPerShare: toFiniteNumber(source.dividendPerShare),
    equityRatio: toFiniteNumber(source.equityRatio),
    netInterestBearingDebt: toFiniteNumber(source.netInterestBearingDebt),
  };
};

export const normalizeEdinetFinancialPanel = (value: unknown): EdinetFinancialPanel | null => {
  if (!value || typeof value !== "object") return null;
  const source = value as Record<string, unknown>;
  const rawSeries = Array.isArray(source.series) ? source.series : [];
  return {
    status: typeof source.status === "string" ? source.status : null,
    mapped: source.mapped == null ? null : Boolean(source.mapped),
    fetchedAt: typeof source.fetchedAt === "string" ? source.fetchedAt : null,
    summary: normalizeEdinetFinancialSummary(source.summary),
    series: rawSeries.map(normalizeEdinetFinancialPoint).filter((item): item is EdinetFinancialPoint => item !== null),
  };
};

export const normalizeTdnetDisclosureItem = (value: unknown): TdnetDisclosureItem | null => {
  if (!value || typeof value !== "object") return null;
  const source = value as Record<string, unknown>;
  return {
    disclosureId: typeof source.disclosureId === "string" ? source.disclosureId : null,
    title: typeof source.title === "string" ? source.title : null,
    publishedAt: typeof source.publishedAt === "string" ? source.publishedAt : null,
    tdnetUrl: typeof source.tdnetUrl === "string" ? source.tdnetUrl : null,
    pdfUrl: typeof source.pdfUrl === "string" ? source.pdfUrl : null,
    xbrlUrl: typeof source.xbrlUrl === "string" ? source.xbrlUrl : null,
    summaryText: typeof source.summaryText === "string" ? source.summaryText : null,
    eventType: typeof source.eventType === "string" ? source.eventType : null,
    sentiment: typeof source.sentiment === "string" ? source.sentiment : null,
    importanceScore: toFiniteNumber(source.importanceScore),
    tags: Array.isArray(source.tags) ? source.tags.map((item) => String(item)) : [],
  };
};

export const buildRange = (candles: Candle[], months: number) => {
  if (!candles.length) return null;
  const end = candles[candles.length - 1].time;
  const endDate = new Date(end * 1000);
  const startDate = new Date(endDate);
  startDate.setMonth(endDate.getMonth() - months);
  return { from: Math.floor(startDate.getTime() / 1000), to: end };
};

export const buildRangeEndingAt = (candles: Candle[], months: number, endTime: number | null) => {
  if (!candles.length) return null;
  if (!endTime) return buildRange(candles, months);
  let nearest = candles[candles.length - 1].time;
  let bestDiff = Number.POSITIVE_INFINITY;
  for (const candle of candles) {
    const diff = Math.abs(candle.time - endTime);
    if (diff < bestDiff) {
      bestDiff = diff;
      nearest = candle.time;
    }
  }
  const endDate = new Date(nearest * 1000);
  const startDate = new Date(endDate);
  startDate.setMonth(endDate.getMonth() - months);
  return { from: Math.floor(startDate.getTime() / 1000), to: nearest };
};

export const buildRangeFromEndTime = (months: number, endTime: number | null) => {
  if (!endTime) return null;
  const endDate = new Date(endTime * 1000);
  const startDate = new Date(endDate);
  startDate.setMonth(endDate.getMonth() - months);
  return { from: Math.floor(startDate.getTime() / 1000), to: endTime };
};

export const RANGE_DRAG_SWITCH_TOLERANCE_SEC = 5 * 24 * 60 * 60;

export const hasSignificantRangeChange = (
  base: { from: number; to: number } | null | undefined,
  next: { from: number; to: number } | null | undefined,
  toleranceSec = RANGE_DRAG_SWITCH_TOLERANCE_SEC
) => {
  if (!base || !next) return false;
  if (
    !Number.isFinite(base.from) ||
    !Number.isFinite(base.to) ||
    !Number.isFinite(next.from) ||
    !Number.isFinite(next.to)
  ) {
    return false;
  }
  return (
    Math.abs(base.from - next.from) > toleranceSec ||
    Math.abs(base.to - next.to) > toleranceSec
  );
};

export const formatDateLabel = (value: number | null) => {
  if (!value) return "";
  const date = new Date(value * 1000);
  if (Number.isNaN(date.getTime())) return "";
  const yyyy = date.getUTCFullYear();
  const mm = String(date.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(date.getUTCDate()).padStart(2, "0");
  return `${yyyy}/${mm}/${dd}`;
};

export const resolveLatestResolvedMetaDate = (...metas: Array<BarsMeta | null | undefined>) => {
  let maxValue: number | null = null;
  metas.forEach((meta) => {
    const normalized = normalizeTime(meta?.latestResolvedDate ?? null);
    if (normalized == null) return;
    if (maxValue == null || normalized > maxValue) {
      maxValue = normalized;
    }
  });
  return maxValue;
};

export const resolveAnalysisBaseAsOfTime = ({
  mainAsOfTime,
  resolvedCursorAsOfTime,
  analysisBaseAsOfTime,
  latestResolvedMetaDate,
  latestDailyAsOfTime,
}: {
  mainAsOfTime: number | null;
  resolvedCursorAsOfTime: number | null;
  analysisBaseAsOfTime: number | null;
  latestResolvedMetaDate: number | null;
  latestDailyAsOfTime: number | null;
}) => {
  if (resolvedCursorAsOfTime != null) return resolvedCursorAsOfTime;
  if (mainAsOfTime != null) return mainAsOfTime;
  if (analysisBaseAsOfTime != null) return analysisBaseAsOfTime;
  if (latestResolvedMetaDate != null) return latestResolvedMetaDate;
  return latestDailyAsOfTime;
};

export const toDateKey = (time: number) => {
  const date = new Date(time * 1000);
  const year = date.getUTCFullYear();
  const month = date.getUTCMonth() + 1;
  const day = date.getUTCDate();
  return year * 10000 + month * 100 + day;
};

export const countInRange = (candles: Candle[], months: number | null) => {
  if (!months) return candles.length;
  const range = buildRange(candles, months);
  if (!range) return 0;
  return candles.filter((c) => c.time >= range.from && c.time <= range.to).length;
};

export const filterCandlesByAsOf = (candles: Candle[], asOf: number | null) => {
  if (!asOf) return candles;
  return candles.filter((candle) => candle.time <= asOf);
};

export const filterVolumeByAsOf = (volume: VolumePoint[], asOf: number | null) => {
  if (!asOf) return volume;
  return volume.filter((point) => point.time <= asOf);
};

export const findNearestCandleIndex = (candles: Candle[], time: number): number | null => {
  if (!candles.length) return null;
  let left = 0;
  let right = candles.length - 1;
  while (left <= right) {
    const mid = Math.floor((left + right) / 2);
    const midTime = candles[mid].time;
    if (midTime === time) return mid;
    if (midTime < time) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  const lowerIndex = Math.max(0, Math.min(candles.length - 1, right));
  const upperIndex = Math.max(0, Math.min(candles.length - 1, left));
  const lower = candles[lowerIndex];
  const upper = candles[upperIndex];
  if (!lower) return upper ? upperIndex : null;
  if (!upper) return lowerIndex;
  return Math.abs(time - lower.time) <= Math.abs(upper.time - time) ? lowerIndex : upperIndex;
};

export const findNearestCandleTime = (candles: Candle[], time: number) => {
  const index = findNearestCandleIndex(candles, time);
  return index == null ? null : candles[index]?.time ?? null;
};

