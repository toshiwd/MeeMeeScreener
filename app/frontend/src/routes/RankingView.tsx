// @ts-nocheck
import { useCallback, useEffect, useMemo, useState } from "react";
import type { CSSProperties } from "react";
import type { AxiosError } from "axios";
import { useLocation, useNavigate } from "react-router-dom";
import { IconHeart, IconHeartFilled } from "@tabler/icons-react";
import { api } from "../api";
import { useBackendReadyState } from "../backendReady";
import ChartListCard from "../components/ChartListCard";
import Toast from "../components/Toast";
import UnifiedListHeader from "../components/UnifiedListHeader";
import { MaSetting, useStore } from "../store";
import { formatEventBadgeDate } from "../utils/events";
import { computeSignalMetrics, getSignalDirectionSummary } from "../utils/signals";
import {
  buildConsultationPack,
  ConsultationSort,
  ConsultationTimeframe
} from "../utils/consultation";
import { useConsultScreenshot } from "../hooks/useConsultScreenshot";

type RankItem = {
  code: string;
  name?: string;
  changePct?: number | null;
  changeAbs?: number | null;
  asOf?: string | null;
  close?: number | null;
  prevClose?: number | null;
  liquidity20d?: number | null;
  series?: number[][];
  is_favorite?: boolean;
  mlPUp?: number | null;
  mlPDown?: number | null;
  mlPAbsBig?: number | null;
  mlPUpBig?: number | null;
  mlPDownBig?: number | null;
  mlScoreUp1M?: number | null;
  mlScoreDown1M?: number | null;
  mlP20Side1MRaw?: number | null;
  mlP20Side1M?: number | null;
  accumulationScore?: number | null;
  breakoutReadiness?: number | null;
  target20Gate?: number | null;
  target20Qualified?: boolean | null;
  setupType?: string | null;
  playbookScoreBonus?: number | null;
  recommendedHoldDays?: number | null;
  recommendedHoldMinDays?: number | null;
  recommendedHoldMaxDays?: number | null;
  recommendedHoldReason?: string | null;
  invalidationPolicyVersion?: string | null;
  invalidationTrigger?: string | null;
  invalidationConservativeAction?: string | null;
  invalidationAggressiveAction?: string | null;
  invalidationRecommendedAction?: string | null;
  invalidationDotenRecommended?: boolean | null;
  invalidationOppositeHoldDays?: number | null;
  invalidationExpectedDeltaMean?: number | null;
  invalidationPolicyNote?: string | null;
  riskMode?: string | null;
  mlPUpShort?: number | null;
  mlPTurnUp?: number | null;
  mlPTurnDown?: number | null;
  mlRetPred20?: number | null;
  mlEv20?: number | null;
  mlEv20Net?: number | null;
  mlRankUp?: number | null;
  mlRankDown?: number | null;
  candleTripletUp?: number | null;
  candleTripletDown?: number | null;
  monthlyBreakoutUpProb?: number | null;
  monthlyBreakoutDownProb?: number | null;
  monthlyRangeProb?: number | null;
  hybridScore?: number | null;
  entryScore?: number | null;
  researchPriorRunId?: string | null;
  researchPriorAsOf?: string | null;
  researchPriorAligned?: boolean | null;
  researchPriorRank?: number | null;
  researchPriorUniverse?: number | null;
  researchPriorBonus?: number | null;
  edinetStatus?: string | null;
  edinetMapped?: boolean | null;
  edinetFreshnessDays?: number | null;
  edinetMetricCount?: number | null;
  edinetQualityScore?: number | null;
  edinetDataScore?: number | null;
  edinetScoreBonus?: number | null;
  edinetFeatureFlagApplied?: boolean | null;
  edinetEbitdaMetric?: number | null;
  edinetRoe?: number | null;
  edinetEquityRatio?: number | null;
  edinetDebtRatio?: number | null;
  edinetOperatingCfMargin?: number | null;
  edinetRevenueGrowthYoy?: number | null;
  entryQualified?: boolean | null;
  entryQualifiedByFallback?: boolean | null;
  entryQualifiedFallbackStage?: string | null;
  evAligned?: boolean | null;
  trendAligned?: boolean | null;
  turnAligned?: boolean | null;
  distOk?: boolean | null;
  counterMoveOk?: boolean | null;
  probSide?: number | null;
  prob5d?: number | null;
  prob10d?: number | null;
  prob20d?: number | null;
  prob5dAligned?: boolean | null;
  probCurveAligned?: boolean | null;
  horizonAligned?: boolean | null;
  modelVersion?: string | null;
  mtfQualifiedCount?: number | null;
  mtfFallbackCount?: number | null;
  mtfCoverage?: number | null;
  winNowScore?: number | null;
  mtfSignalBits?: string | null;
  mtfWinD?: number | null;
  mtfWinW?: number | null;
  mtfWinM?: number | null;
  maStreak60Up?: number | null;
  maStreak100Up?: number | null;
  maStreakAligned?: boolean | null;
  weakEarlyPattern?: boolean | null;
  patternA1MaturedBreakout?: boolean | null;
  patternA2BoxTrend?: boolean | null;
  patternA3CapitulationRebound?: boolean | null;
  patternS1WeakBreakdown?: boolean | null;
  patternS2WeakBox?: boolean | null;
  patternS3LateBreakout?: boolean | null;
  patternD1ShortBreakdown?: boolean | null;
  patternD2ShortMixedFar?: boolean | null;
  patternD3ShortNaBelow?: boolean | null;
  patternD4ShortDoubleTop?: boolean | null;
  patternD5ShortHeadShoulders?: boolean | null;
  patternDTrapStackDownFar?: boolean | null;
  patternDTrapOverheatMomentum?: boolean | null;
  patternDTrapTopFakeout?: boolean | null;
  mtfStrictResolved?: MtfStrictnessResolved | null;
  mtfLiquidity20d?: number | null;
  qualityFlags?: string[] | null;
};

type RankTimeframe = "D" | "W" | "M";
type RankWhich = "latest" | "prev";
type RankMode = "hybrid" | "turn";
type RankRiskMode = "defensive" | "balanced" | "aggressive";
type MtfStrictness = "auto" | "loose" | "normal" | "tight";
type MtfStrictnessResolved = "loose" | "normal" | "tight";
type RankMetricsView = "compact" | "full";
type StoredRankViewState = {
  stateVersion?: number;
  listTimeframe?: "daily" | "weekly" | "monthly";
  dir?: "up" | "down";
  metricsView?: RankMetricsView;
  filterSignalsOnly?: boolean;
  filterDataOnly?: boolean;
  filterBuySignalsOnly?: boolean;
  filterSellSignalsOnly?: boolean;
};
type RankingFetchCacheEntry = {
  cacheVersion: number;
  items: RankItem[];
  errorMessage: string | null;
  useFallback: boolean;
};

const RANK_VIEW_STATE_KEY = "rankingViewState";
const RANK_VIEW_STATE_VERSION = 5;
const RANK_FETCH_CACHE_VERSION = 1;
const RANK_FETCH_CACHE_PREFIX = "rankingFetchCache";
const RANK_LIMIT = 50;
const RANK_FETCH_TIMEOUT_MS = 60000;
const TIMEFRAME_LABELS: Record<RankTimeframe, string> = {
  D: "日足",
  W: "週足",
  M: "月足"
};
const rankingFetchMemoryCache = new Map<string, RankingFetchCacheEntry>();

const RANK_MA_SETTINGS: MaSetting[] = [
  { key: "ma1", label: "MA1", period: 7, visible: true, color: "#ef4444", lineWidth: 1 },
  { key: "ma2", label: "MA2", period: 20, visible: true, color: "#22c55e", lineWidth: 1 },
  { key: "ma3", label: "MA3", period: 60, visible: true, color: "#3b82f6", lineWidth: 1 },
  { key: "ma4", label: "MA4", period: 100, visible: true, color: "#a855f7", lineWidth: 1 },
  { key: "ma5", label: "MA5", period: 200, visible: true, color: "#f59e0b", lineWidth: 1 }
];
const MTF_WEIGHTS: Record<RankTimeframe, number> = { D: 0.5, W: 0.3, M: 0.2 };
const MTF_MIN_QUALIFIED_COUNT_STRICT = 2;
const MTF_SCORE_RELAX_GATE = 0.86;
const MTF_PROB_RELAX_GATE = 0.58;
const MTF_WIN_BASELINE = 0.5;
const MTF_STRICT_GATE_BASE = 0.66;
const MTF_STRICT_GATE_FLOOR = 0.58;
const MTF_STRICT_GATE_CEIL = 0.78;
const MTF_STRICT_PROFILES: Record<MtfStrictnessResolved, { gateBias: number; minQualified: number; label: string }> = {
  loose: { gateBias: -0.04, minQualified: 1, label: "緩" },
  normal: { gateBias: 0, minQualified: 2, label: "標準" },
  tight: { gateBias: 0.04, minQualified: 2, label: "強" }
};
const MTF_STRICTNESS_LABEL: Record<MtfStrictness, string> = {
  auto: "自動",
  loose: "緩",
  normal: "標準",
  tight: "強"
};
const MTF_STRICT_ORDER: MtfStrictnessResolved[] = ["normal", "tight", "loose"];

const readStoredRankViewState = (): StoredRankViewState | null => {
  if (typeof window === "undefined") return null;
  try {
    const stored = window.sessionStorage.getItem(RANK_VIEW_STATE_KEY);
    if (!stored) return null;
    return JSON.parse(stored) as StoredRankViewState;
  } catch {
    return null;
  }
};

const buildRankingFetchCacheKey = (params: {
  which: RankWhich;
  dir: "up" | "down";
  mode: RankMode;
  riskMode: RankRiskMode;
}) => `${RANK_FETCH_CACHE_PREFIX}:${params.which}:${params.dir}:${params.mode}:${params.riskMode}:${RANK_LIMIT}`;

const readRankingFetchCache = (cacheKey: string): RankingFetchCacheEntry | null => {
  const cached = rankingFetchMemoryCache.get(cacheKey);
  if (cached) return cached;
  if (typeof window === "undefined") return null;
  try {
    const stored = window.sessionStorage.getItem(cacheKey);
    if (!stored) return null;
    const parsed = JSON.parse(stored) as Partial<RankingFetchCacheEntry>;
    if (parsed.cacheVersion !== RANK_FETCH_CACHE_VERSION || !Array.isArray(parsed.items) || typeof parsed.useFallback !== "boolean") {
      return null;
    }
    const entry: RankingFetchCacheEntry = {
      cacheVersion: RANK_FETCH_CACHE_VERSION,
      items: parsed.items as RankItem[],
      errorMessage: typeof parsed.errorMessage === "string" ? parsed.errorMessage : null,
      useFallback: parsed.useFallback
    };
    rankingFetchMemoryCache.set(cacheKey, entry);
    return entry;
  } catch {
    return null;
  }
};

const clearRankingFetchCache = (cacheKey: string) => {
  rankingFetchMemoryCache.delete(cacheKey);
  if (typeof window === "undefined") return;
  try {
    window.sessionStorage.removeItem(cacheKey);
  } catch {
    // ignore storage failures
  }
};

const writeRankingFetchCache = (cacheKey: string, entry: RankingFetchCacheEntry) => {
  rankingFetchMemoryCache.set(cacheKey, entry);
  if (typeof window === "undefined") return;
  try {
    window.sessionStorage.setItem(cacheKey, JSON.stringify(entry));
  } catch {
    // ignore storage failures
  }
};

const formatRankingBackendErrors = (errors?: string[] | null) => {
  const messages = (errors ?? [])
    .map((entry) => {
      const text = String(entry ?? "").trim();
      if (!text) return null;
      const match = text.match(/^([DWM]):\s*(.+)$/);
      if (!match) return text;
      const tf = match[1] as RankTimeframe;
      return `${TIMEFRAME_LABELS[tf]}: ${match[2]}`;
    })
    .filter((value): value is string => Boolean(value));
  if (!messages.length) return null;
  return messages.join(" / ");
};

const extractRankingFailureReason = (error: unknown) => {
  const axiosError = error as AxiosError<{
    detail?: string | { message?: string } | null;
    error?: string | null;
    errors?: string[] | null;
  }> | undefined;
  const responseData = axiosError?.response?.data;
  const errorList = Array.isArray(responseData?.errors) ? formatRankingBackendErrors(responseData.errors) : null;
  if (errorList) return errorList;
  if (typeof responseData?.detail === "string" && responseData.detail.trim()) return responseData.detail.trim();
  if (responseData?.detail && typeof responseData.detail === "object" && typeof responseData.detail.message === "string" && responseData.detail.message.trim()) {
    return responseData.detail.message.trim();
  }
  if (typeof responseData?.error === "string" && responseData.error.trim()) return responseData.error.trim();
  if (error instanceof Error && error.message.trim()) return error.message.trim();
  if (typeof axiosError?.message === "string" && axiosError.message.trim()) return axiosError.message.trim();
  return null;
};

const buildRankingFallbackMessage = (reason: string | null) =>
  reason
    ? `ランキングの取得に失敗しました。簡易データを表示しています。理由: ${reason}`
    : "ランキングの取得に失敗しました。簡易データを表示しています。";

const isUsableRankingFetchCache = (entry: RankingFetchCacheEntry | null): entry is RankingFetchCacheEntry =>
  Boolean(entry && !entry.useFallback && entry.items.length > 0);

const finiteNum = (value?: number | null) => {
  if (!Number.isFinite(value ?? NaN)) return null;
  return Number(value);
};

const firstFinite = (...values: Array<number | null | undefined>) => {
  for (const value of values) {
    const resolved = finiteNum(value);
    if (resolved != null) return resolved;
  }
  return null;
};

const resolveProbSide = (item: RankItem, dir: "up" | "down") => {
  if (dir === "up") {
    return firstFinite(item.probSide, item.mlPUpShort, item.mlPUpBig, item.mlPUp);
  }
  const downFromUpShort = finiteNum(item.mlPUpShort);
  const downFromUp = finiteNum(item.mlPUp);
  return firstFinite(
    item.probSide,
    item.mlPDown,
    item.mlPDownBig,
    downFromUpShort != null ? 1 - downFromUpShort : null,
    downFromUp != null ? 1 - downFromUp : null
  );
};

const resolveScoreSide = (item: RankItem) => firstFinite(item.entryScore, item.hybridScore);

const matchesMtfStrictRule = (item: RankItem, minQualified: number, winGate: number) => {
  const mtfQualified = firstFinite(item.mtfQualifiedCount) ?? 0;
  const winNow = firstFinite(item.winNowScore) ?? 0;
  return mtfQualified >= minQualified || winNow >= winGate;
};

const normalizeEvSide = (ev: number | null) => {
  if (ev == null) return null;
  return Math.max(0, Math.min(1, (ev + 0.02) / 0.08));
};

const mergeMultiTimeframeRankings = (
  byTf: Record<RankTimeframe, RankItem[]>,
  options: { dir: "up" | "down"; limit: number }
) => {
  const { dir, limit } = options;
  const byCode = new Map<string, Partial<Record<RankTimeframe, RankItem>>>();
  (Object.keys(byTf) as RankTimeframe[]).forEach((tf) => {
    byTf[tf].forEach((item) => {
      const code = String(item.code ?? "").trim();
      if (!code) return;
      const slot = byCode.get(code) ?? {};
      slot[tf] = item;
      byCode.set(code, slot);
    });
  });

  const merged: RankItem[] = [];
  byCode.forEach((slot, code) => {
    const base = slot.D ?? slot.W ?? slot.M;
    if (!base) return;
    let scoreWeighted = 0;
    let scoreWeight = 0;
    let probWeighted = 0;
    let probWeight = 0;
    let hybridWeighted = 0;
    let hybridWeight = 0;
    let qualifiedCount = 0;
    let fallbackCount = 0;
    let winWeighted = 0;
    let winWeight = 0;
    let liquidityWeighted = 0;
    let liquidityWeight = 0;
    const tfWinByTf: Partial<Record<RankTimeframe, number>> = {};
    (["D", "W", "M"] as RankTimeframe[]).forEach((tf) => {
      const item = slot[tf];
      if (!item) return;
      const weight = MTF_WEIGHTS[tf];
      const scoreSide = resolveScoreSide(item);
      const probSide = resolveProbSide(item, dir);
      const hybrid = finiteNum(item.hybridScore);
      const liq = finiteNum(item.liquidity20d);
      const evRaw = finiteNum(item.mlEv20Net);
      const evSide = evRaw == null ? null : (dir === "up" ? evRaw : -evRaw);
      const evNorm = normalizeEvSide(evSide);
      const tfWinScore = (
        0.55 * (scoreSide ?? MTF_WIN_BASELINE)
        + 0.30 * (probSide ?? MTF_WIN_BASELINE)
        + 0.15 * (evNorm ?? MTF_WIN_BASELINE)
      );
      tfWinByTf[tf] = tfWinScore;
      if (scoreSide != null) {
        scoreWeighted += weight * scoreSide;
        scoreWeight += weight;
      }
      if (probSide != null) {
        probWeighted += weight * probSide;
        probWeight += weight;
      }
      if (hybrid != null) {
        hybridWeighted += weight * hybrid;
        hybridWeight += weight;
      }
      if (liq != null && liq > 0) {
        liquidityWeighted += weight * liq;
        liquidityWeight += weight;
      }
      winWeighted += weight * tfWinScore;
      winWeight += weight;
      if (item.entryQualified === true) qualifiedCount += 1;
      if (item.entryQualifiedByFallback === true) fallbackCount += 1;
    });
    const score = scoreWeight > 0 ? scoreWeighted / scoreWeight : null;
    const prob = probWeight > 0 ? probWeighted / probWeight : null;
    const hybrid = hybridWeight > 0 ? hybridWeighted / hybridWeight : score;
    const liquidity = liquidityWeight > 0 ? liquidityWeighted / liquidityWeight : firstFinite(base.liquidity20d);
    const baseWin = winWeight > 0 ? winWeighted / winWeight : null;
    const isStrictQualified = qualifiedCount >= MTF_MIN_QUALIFIED_COUNT_STRICT;
    const isRelaxedQualified = Boolean(
      qualifiedCount >= 1
      && score != null
      && score >= MTF_SCORE_RELAX_GATE
      && prob != null
      && prob >= MTF_PROB_RELAX_GATE
    );
    const isQualified = isStrictQualified || isRelaxedQualified;
    const isFallbackQualified = !isQualified && (qualifiedCount + fallbackCount) >= 2;
    const coverage = Math.max(0, Math.min(1, scoreWeight));
    const consensusBonus = (qualifiedCount / 3) * 0.10;
    const strictBonus = isStrictQualified ? 0.03 : 0;
    const fallbackPenalty = isFallbackQualified ? 0.04 : 0;
    const lowCoveragePenalty = coverage < 0.7 ? (0.7 - coverage) * 0.10 : 0;
    const winNowScore = baseWin == null
      ? null
      : Math.max(
        0,
        Math.min(1, baseWin + consensusBonus + strictBonus - fallbackPenalty - lowCoveragePenalty)
      );
    const mtfSignalBits = (["D", "W", "M"] as RankTimeframe[])
      .map((tf) => {
        const item = slot[tf];
        if (!item) return `${tf}:--`;
        if (item.entryQualified === true) return `${tf}:OK`;
        if (item.entryQualifiedByFallback === true) return `${tf}:補`;
        return `${tf}:--`;
      })
      .join(" ");
    merged.push({
      ...base,
      code,
      asOf: slot.D?.asOf ?? slot.W?.asOf ?? slot.M?.asOf ?? base.asOf,
      liquidity20d: liquidity,
      mlPUpShort: dir === "up" ? (prob ?? base.mlPUpShort ?? base.mlPUp ?? null) : base.mlPUpShort,
      mlPDown: dir === "down" ? (prob ?? base.mlPDown ?? null) : base.mlPDown,
      entryScore: score,
      hybridScore: hybrid,
      probSide: prob,
      entryQualified: isQualified,
      entryQualifiedByFallback: isFallbackQualified,
      entryQualifiedFallbackStage: isFallbackQualified ? "mtf_consensus" : base.entryQualifiedFallbackStage,
      mtfQualifiedCount: qualifiedCount,
      mtfFallbackCount: fallbackCount,
      mtfCoverage: coverage,
      winNowScore,
      mtfSignalBits,
      mtfWinD: tfWinByTf.D ?? null,
      mtfWinW: tfWinByTf.W ?? null,
      mtfWinM: tfWinByTf.M ?? null,
      mtfLiquidity20d: liquidity
    });
  });

  merged.sort((a, b) => {
    const aq = a.entryQualified === true ? 0 : 1;
    const bq = b.entryQualified === true ? 0 : 1;
    if (aq !== bq) return aq - bq;
    const aw = firstFinite(a.winNowScore) ?? -1;
    const bw = firstFinite(b.winNowScore) ?? -1;
    if (aw !== bw) return bw - aw;
    const as = firstFinite(a.entryScore, a.hybridScore) ?? -1;
    const bs = firstFinite(b.entryScore, b.hybridScore) ?? -1;
    if (as !== bs) return bs - as;
    const ap = resolveProbSide(a, dir) ?? -1;
    const bp = resolveProbSide(b, dir) ?? -1;
    if (ap !== bp) return bp - ap;
    const al = firstFinite(a.mtfLiquidity20d, a.liquidity20d) ?? -1;
    const bl = firstFinite(b.mtfLiquidity20d, b.liquidity20d) ?? -1;
    if (al !== bl) return bl - al;
    return a.code.localeCompare(b.code, "ja");
  });
  return merged.slice(0, limit);
};

export default function RankingView() {
  const location = useLocation();
  const navigate = useNavigate();
  const { ready: backendReady } = useBackendReadyState();
  const setFavoriteLocal = useStore((state) => state.setFavoriteLocal);
  const ensureBarsForVisible = useStore((state) => state.ensureBarsForVisible);
  const barsCache = useStore((state) => state.barsCache);
  const barsStatus = useStore((state) => state.barsStatus);
  const boxesCache = useStore((state) => state.boxesCache);
  const maSettings = useStore((state) => state.maSettings);
  const tickers = useStore((state) => state.tickers);
  const ensureListLoaded = useStore((state) => state.ensureListLoaded);
  const listTimeframe = useStore((state) => state.settings.listTimeframe);
  const listRangeBars = useStore((state) => state.settings.listRangeBars);
  const listColumns = useStore((state) => state.settings.listColumns);
  const listRows = useStore((state) => state.settings.listRows);
  const setListTimeframe = useStore((state) => state.setListTimeframe);
  const setListRangeBars = useStore((state) => state.setListRangeBars);
  const setListColumns = useStore((state) => state.setListColumns);
  const setListRows = useStore((state) => state.setListRows);
  const favoriteCodes = useStore((state) => state.favorites);
  const rankWhich: RankWhich = "latest";
  const rankMode: RankMode = "hybrid";
  const riskMode: RankRiskMode = "balanced";
  const storedViewState = useMemo(() => readStoredRankViewState(), []);
  const initialDir: "up" | "down" = storedViewState?.dir === "down" ? "down" : "up";
  const initialFetchCache = useMemo(
    () => {
      const cached = readRankingFetchCache(
        buildRankingFetchCacheKey({ which: rankWhich, dir: initialDir, mode: rankMode, riskMode })
      );
      return isUsableRankingFetchCache(cached) ? cached : null;
    },
    [initialDir, rankMode, rankWhich, riskMode]
  );

  const [dir, setDir] = useState<"up" | "down">(initialDir);
  const [items, setItems] = useState<RankItem[]>(() => initialFetchCache?.items ?? []);
  const [search, setSearch] = useState("");
  const [filterSignalsOnly, setFilterSignalsOnly] = useState(Boolean(storedViewState?.filterSignalsOnly));
  const [filterDataOnly, setFilterDataOnly] = useState(Boolean(storedViewState?.filterDataOnly));
  const [filterBuySignalsOnly, setFilterBuySignalsOnly] = useState(Boolean(storedViewState?.filterBuySignalsOnly));
  const [filterSellSignalsOnly, setFilterSellSignalsOnly] = useState(Boolean(storedViewState?.filterSellSignalsOnly));
  const filterQualifiedOnly = true;
  const filterMtfStrictOnly = true;
  const mtfStrictness: MtfStrictness = "auto";
  const [metricsView, setMetricsView] = useState<RankMetricsView>(storedViewState?.metricsView === "full" ? "full" : "compact");
  const [loading, setLoading] = useState(() => initialFetchCache == null);
  const [errorMessage, setErrorMessage] = useState<string | null>(() => initialFetchCache?.errorMessage ?? null);
  const [toastMessage, setToastMessage] = useState<string | null>(null);
  const [toastAction, setToastAction] = useState<{ label: string; onClick: () => void } | null>(null);
  const [selectedCodes, setSelectedCodes] = useState<string[]>([]);
  const [consultVisible, setConsultVisible] = useState(false);
  const [consultExpanded, setConsultExpanded] = useState(false);
  const [consultTab, setConsultTab] = useState<"selection" | "position">("selection");
  const [consultText, setConsultText] = useState("");
  const [consultSort, setConsultSort] = useState<ConsultationSort>("score");
  const [consultBusy, setConsultBusy] = useState(false);
  const [consultMeta, setConsultMeta] = useState<{ omitted: number }>({ omitted: 0 });
  const consultTimeframe: ConsultationTimeframe = "monthly";
  const consultBarsCount = 60;
  const consultPaddingClass = consultVisible
    ? consultExpanded
      ? "consult-padding-expanded"
      : "consult-padding-mini"
    : "";
  const [useFallback, setUseFallback] = useState(() => initialFetchCache?.useFallback ?? false);
  const favoriteCodeSet = useMemo(() => new Set(favoriteCodes), [favoriteCodes]);
  const syncFavoriteFlags = useCallback(
    (entries: RankItem[]) => {
      let changed = false;
      const next = entries.map((item) => {
        const isFavorite = favoriteCodeSet.has(item.code);
        if (Boolean(item.is_favorite) === isFavorite) {
          return item;
        }
        changed = true;
        return { ...item, is_favorite: isFavorite };
      });
      return changed ? next : entries;
    },
    [favoriteCodeSet]
  );

  // Use the screenshot hook
  const { generateScreenshots, isProcessing: screenshotBusy } = useConsultScreenshot();

  useEffect(() => {
    if (storedViewState?.listTimeframe) {
      setListTimeframe(storedViewState.listTimeframe);
    }
  }, [setListTimeframe, storedViewState]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    try {
      const payload = {
        stateVersion: RANK_VIEW_STATE_VERSION,
        listTimeframe,
        dir,
        metricsView,
        filterSignalsOnly,
        filterDataOnly,
        filterBuySignalsOnly,
        filterSellSignalsOnly
      };
      window.sessionStorage.setItem(RANK_VIEW_STATE_KEY, JSON.stringify(payload));
    } catch {
      // ignore storage failures
    }
  }, [listTimeframe, dir, metricsView, filterSignalsOnly, filterDataOnly, filterBuySignalsOnly, filterSellSignalsOnly]);

  const listStyles = useMemo(
    () =>
    ({
      "--list-cols": listColumns,
      "--list-rows": listRows
    } as CSSProperties),
    [listColumns, listRows]
  );
  const listMaSettings =
    listTimeframe === "daily"
      ? maSettings.daily
      : listTimeframe === "weekly"
        ? maSettings.weekly
        : maSettings.monthly;

  const resolvedMaSettings = listMaSettings ?? RANK_MA_SETTINGS;

  /*
  const timeframeButtons = useMemo(
    () => [
      { key: "D" as RankTimeframe, label: "日足" },
      { key: "W" as RankTimeframe, label: "週足" },
      { key: "M" as RankTimeframe, label: "月足" }
    ],
    []
  );
  */

  const sortOptions = useMemo(
    () => [
      { value: "up", label: "買い勝ちTop50" },
      { value: "down", label: "売り勝ちTop50" }
    ],
    []
  );
  const filterItems = useMemo(
    () => [
      {
        key: "signals",
        label: "\u30b7\u30b0\u30ca\u30eb\u3042\u308a",
        checked: filterSignalsOnly,
        onToggle: () => setFilterSignalsOnly((prev) => !prev)
      },
      {
        key: "data",
        label: "\u30c7\u30fc\u30bf\u53d6\u5f97\u6e08\u307f",
        checked: filterDataOnly,
        onToggle: () => setFilterDataOnly((prev) => !prev)
      },
      {
        key: "buy-signal",
        label: "\u8cb7\u3044\u5224\u5b9a\u3042\u308a",
        checked: filterBuySignalsOnly,
        onToggle: () => setFilterBuySignalsOnly((prev) => !prev)
      },
      {
        key: "sell-signal",
        label: "\u58f2\u308a\u5224\u5b9a\u3042\u308a",
        checked: filterSellSignalsOnly,
        onToggle: () => setFilterSellSignalsOnly((prev) => !prev)
      }
    ],
    [filterSignalsOnly, filterDataOnly, filterBuySignalsOnly, filterSellSignalsOnly]
  );

  const fallbackItems = useMemo(() => {
    const normalizeBars = (bars: number[][]) => {
      if (bars.length < 2) return bars;
      return Number(bars[0]?.[0]) > Number(bars[bars.length - 1]?.[0]) ? [...bars].reverse() : bars;
    };
    const resolveChange = (bars: number[][]) => {
      const normalized = normalizeBars(bars);
      if (normalized.length < 3 && rankWhich === "prev") return { changePct: null, changeAbs: null };
      if (normalized.length < 2) return { changePct: null, changeAbs: null };
      const tIndex = rankWhich === "latest" ? normalized.length - 1 : normalized.length - 2;
      const prevIndex = rankWhich === "latest" ? normalized.length - 2 : normalized.length - 3;
      const close = Number(normalized[tIndex]?.[4]);
      const prevClose = Number(normalized[prevIndex]?.[4]);
      if (!Number.isFinite(close) || !Number.isFinite(prevClose) || prevClose === 0) {
        return { changePct: null, changeAbs: null };
      }
      const changeAbs = close - prevClose;
      return { changePct: changeAbs / prevClose, changeAbs };
    };
    const list = tickers.map((ticker) => {
      const payload = barsCache[listTimeframe]?.[ticker.code] ?? null;
      const series = payload?.bars ?? [];
      const change = resolveChange(series);
      return {
        code: ticker.code,
        name: ticker.name ?? ticker.code,
        changePct: change.changePct,
        changeAbs: change.changeAbs,
        is_favorite: false
      };
    });
    return list;
  }, [tickers, barsCache, listTimeframe, rankWhich]);

  const searchResults = useMemo(() => {
    const term = search.trim().toLowerCase();
    if (!term) return items;
    return items.filter((item) => {
      const codeMatch = item.code.toLowerCase().includes(term);
      const nameMatch = (item.name ?? "").toLowerCase().includes(term);
      return codeMatch || nameMatch;
    });
  }, [items, search]);

  const signalMetricsMap = useMemo(() => {
    const map = new Map<string, ReturnType<typeof computeSignalMetrics>>();
    searchResults.forEach((item) => {
      const payload = barsCache[listTimeframe]?.[item.code] ?? null;
      const series = payload && payload.bars?.length ? payload.bars : item.series ?? [];
      if (!series.length) return;
      map.set(item.code, computeSignalMetrics(series, 4));
    });
    return map;
  }, [searchResults, barsCache, listTimeframe]);

  const signalMap = useMemo(() => {
    const map = new Map<string, ReturnType<typeof computeSignalMetrics>["signals"]>();
    signalMetricsMap.forEach((metrics, code) => {
      if (metrics.signals.length) {
        map.set(code, metrics.signals);
      }
    });
    return map;
  }, [signalMetricsMap]);

  const baseFilteredItems = useMemo(() => {
    const hasDirectionalFilter = filterBuySignalsOnly || filterSellSignalsOnly;
    if (!filterSignalsOnly && !filterDataOnly && !hasDirectionalFilter) return searchResults;
    return searchResults.filter((item) => {
      const payload = barsCache[listTimeframe]?.[item.code] ?? null;
      const series = payload && payload.bars?.length ? payload.bars : item.series ?? [];
      const hasData = series.length > 0;
      const metrics = signalMetricsMap.get(item.code);
      const summary = metrics ? getSignalDirectionSummary(metrics) : null;
      if (filterDataOnly && !hasData) return false;
      if (filterSignalsOnly && !signalMap.has(item.code)) return false;
      if (hasDirectionalFilter) {
        const matchesBuy = filterBuySignalsOnly && Boolean(summary?.hasBuySignal);
        const matchesSell = filterSellSignalsOnly && Boolean(summary?.hasSellSignal);
        if (!(matchesBuy || matchesSell)) return false;
      }
      return true;
    });
  }, [
    searchResults,
    filterSignalsOnly,
    filterDataOnly,
    filterBuySignalsOnly,
    filterSellSignalsOnly,
    barsCache,
    listTimeframe,
    signalMap,
    signalMetricsMap
  ]);

  const qualifiedFilteredItems = useMemo(() => {
    const hasQualificationSignal = baseFilteredItems.some(
      (item) =>
        typeof item.entryQualified === "boolean" ||
        typeof item.entryQualifiedByFallback === "boolean"
    );
    if (!filterQualifiedOnly || useFallback || !hasQualificationSignal) {
      return baseFilteredItems;
    }
    return baseFilteredItems.filter(
      (item) => item.entryQualified === true || item.entryQualifiedByFallback === true
    );
  }, [baseFilteredItems, filterQualifiedOnly, useFallback]);

  const qualificationFilterRelaxed = useMemo(() => {
    if (!filterQualifiedOnly || useFallback) return false;
    const hasQualificationSignal = baseFilteredItems.some(
      (item) =>
        typeof item.entryQualified === "boolean" ||
        typeof item.entryQualifiedByFallback === "boolean"
    );
    if (!hasQualificationSignal) return false;
    return baseFilteredItems.length > 0 && qualifiedFilteredItems.length === 0;
  }, [baseFilteredItems, qualifiedFilteredItems, filterQualifiedOnly, useFallback]);

  const filteredItems = useMemo(() => {
    if (qualificationFilterRelaxed) return baseFilteredItems;
    return qualifiedFilteredItems;
  }, [qualificationFilterRelaxed, baseFilteredItems, qualifiedFilteredItems]);
  const mtfStrictTarget = useMemo(
    () => Math.max(8, Math.min(20, Math.round(filteredItems.length * 0.28))),
    [filteredItems.length]
  );

  const mtfStrictGate = useMemo(() => {
    const wins = filteredItems
      .map((item) => firstFinite(item.winNowScore))
      .filter((value): value is number => value != null)
      .sort((a, b) => b - a);
    if (wins.length === 0) return MTF_STRICT_GATE_BASE;
    const idx = Math.max(0, Math.min(wins.length - 1, mtfStrictTarget - 1));
    const quantGate = wins[idx];
    return Math.max(MTF_STRICT_GATE_FLOOR, Math.min(MTF_STRICT_GATE_CEIL, quantGate));
  }, [filteredItems, mtfStrictTarget]);
  const mtfStrictResolved = useMemo<MtfStrictnessResolved>(() => {
    if (mtfStrictness !== "auto") return mtfStrictness;
    const evaluated = MTF_STRICT_ORDER.map((key) => {
      const profile = MTF_STRICT_PROFILES[key];
      const gate = Math.max(MTF_STRICT_GATE_FLOOR, Math.min(MTF_STRICT_GATE_CEIL, mtfStrictGate + profile.gateBias));
      const count = filteredItems.reduce(
        (acc, item) => acc + (matchesMtfStrictRule(item, profile.minQualified, gate) ? 1 : 0),
        0
      );
      return { key, count };
    });
    const minCount = Math.max(5, Math.floor(mtfStrictTarget * 0.45));
    const usable = evaluated.filter((row) => row.count >= minCount);
    const pool = usable.length > 0 ? usable : evaluated;
    pool.sort((a, b) => {
      const distA = Math.abs(a.count - mtfStrictTarget);
      const distB = Math.abs(b.count - mtfStrictTarget);
      if (distA !== distB) return distA - distB;
      return MTF_STRICT_ORDER.indexOf(a.key) - MTF_STRICT_ORDER.indexOf(b.key);
    });
    return pool[0]?.key ?? "normal";
  }, [mtfStrictness, filteredItems, mtfStrictGate, mtfStrictTarget]);
  const mtfStrictRule = useMemo(() => MTF_STRICT_PROFILES[mtfStrictResolved], [mtfStrictResolved]);
  const mtfStrictGateApplied = useMemo(
    () => Math.max(MTF_STRICT_GATE_FLOOR, Math.min(MTF_STRICT_GATE_CEIL, mtfStrictGate + mtfStrictRule.gateBias)),
    [mtfStrictGate, mtfStrictRule]
  );
  const mtfStrictFilteredItems = useMemo(() => {
    if (!filterMtfStrictOnly || useFallback) return filteredItems;
    return filteredItems.filter((item) => matchesMtfStrictRule(item, mtfStrictRule.minQualified, mtfStrictGateApplied));
  }, [filterMtfStrictOnly, useFallback, filteredItems, mtfStrictRule, mtfStrictGateApplied]);

  const mtfStrictFilterRelaxed = useMemo(() => {
    if (!filterMtfStrictOnly || useFallback) return false;
    return filteredItems.length > 0 && mtfStrictFilteredItems.length === 0;
  }, [filterMtfStrictOnly, useFallback, filteredItems, mtfStrictFilteredItems]);

  const effectiveItems = useMemo(() => {
    const base = mtfStrictFilterRelaxed ? filteredItems : mtfStrictFilteredItems;
    return base.map((item) => ({ ...item, mtfStrictResolved }));
  }, [mtfStrictFilterRelaxed, filteredItems, mtfStrictFilteredItems, mtfStrictResolved]);
  const mtfStrictCount = mtfStrictFilteredItems.length;

  const sortedItems = useMemo(() => {
    if (!useFallback) {
      return effectiveItems;
    }
    const list = [...effectiveItems];
    const getLiquidity = (item: RankItem) =>
      Number.isFinite(item.liquidity20d ?? NaN) ? (item.liquidity20d as number) : -1;
    list.sort((a, b) => {
      const aChange = Number.isFinite(a.changePct ?? NaN) ? (a.changePct as number) : null;
      const bChange = Number.isFinite(b.changePct ?? NaN) ? (b.changePct as number) : null;
      const aMissing = aChange == null;
      const bMissing = bChange == null;
      if (aMissing && bMissing) return a.code.localeCompare(b.code, "ja");
      if (aMissing) return 1;
      if (bMissing) return -1;
      if (aChange !== bChange) {
        return dir === "up" ? bChange - aChange : aChange - bChange;
      }
      const aLiq = getLiquidity(a);
      const bLiq = getLiquidity(b);
      if (aLiq !== bLiq) return bLiq - aLiq;
      return a.code.localeCompare(b.code, "ja");
    });
    return list;
  }, [effectiveItems, dir, useFallback]);
  const listCodes = useMemo(() => sortedItems.map((item) => item.code), [sortedItems]);
  const densityKey = `${listColumns}x${listRows}`;
  const rankingCacheKey = useMemo(
    () =>
      buildRankingFetchCacheKey({
        which: rankWhich,
        dir,
        mode: rankMode,
        riskMode
      }),
    [dir, rankMode, rankWhich, riskMode]
  );

  useEffect(() => {
    if (!backendReady) return;
    if (tickers.length) return;
    ensureListLoaded().catch(() => { });
  }, [backendReady, ensureListLoaded, tickers.length]);

  const tickerMap = useMemo(() => {
    return new Map(tickers.map((ticker) => [ticker.code, ticker]));
  }, [tickers]);
  const itemByCode = useMemo(() => {
    return new Map(items.map((item) => [item.code, item]));
  }, [items]);
  const itemCodeSet = useMemo(() => new Set(items.map((item) => item.code)), [items]);

  useEffect(() => {
    const cached = readRankingFetchCache(rankingCacheKey);
    if (isUsableRankingFetchCache(cached)) {
      setItems(syncFavoriteFlags(cached.items));
      setUseFallback(cached.useFallback);
      setErrorMessage(cached.errorMessage);
      setLoading(false);
      return;
    }
    if (cached) {
      clearRankingFetchCache(rankingCacheKey);
    }
    setItems([]);
    setUseFallback(false);
    setErrorMessage(null);
  }, [rankingCacheKey, syncFavoriteFlags]);

  useEffect(() => {
    if (!backendReady) return;
    const cached = readRankingFetchCache(rankingCacheKey);
    if (isUsableRankingFetchCache(cached)) {
      setLoading(false);
      return;
    }
    if (cached) {
      clearRankingFetchCache(rankingCacheKey);
    }
    let cancelled = false;
    setLoading(true);
    setErrorMessage(null);
    setUseFallback(false);
    (async () => {
      try {
        const res = await api.get("/rankings/multi", {
          params: { which: rankWhich, dir, mode: rankMode, risk_mode: riskMode, limit: RANK_LIMIT },
          timeout: RANK_FETCH_TIMEOUT_MS
        });
        if (cancelled) return;
        const payload = (res.data ?? {}) as {
          itemsByTf?: Partial<Record<RankTimeframe, RankItem[]>>;
          errors?: string[];
        };
        const itemsByTf = payload.itemsByTf ?? {};
        const dailyItems = Array.isArray(itemsByTf.D) ? itemsByTf.D : [];
        const weeklyItems = Array.isArray(itemsByTf.W) ? itemsByTf.W : [];
        const monthlyItems = Array.isArray(itemsByTf.M) ? itemsByTf.M : [];
        const backendErrors = formatRankingBackendErrors(payload.errors);
        if (!dailyItems.length && !weeklyItems.length && !monthlyItems.length) {
          throw new Error(backendErrors ?? "ランキング計算結果が空でした。");
        }
        const merged = mergeMultiTimeframeRankings(
          {
            D: dailyItems,
            W: weeklyItems,
            M: monthlyItems
          },
          { dir, limit: RANK_LIMIT }
        );
        if (!merged.length) {
          throw new Error(backendErrors ?? "統合ランキングの生成結果が空でした。");
        }
        setItems(syncFavoriteFlags(merged));
        setUseFallback(false);
        setErrorMessage(backendErrors);
        writeRankingFetchCache(rankingCacheKey, {
          cacheVersion: RANK_FETCH_CACHE_VERSION,
          items: syncFavoriteFlags(merged),
          errorMessage: backendErrors,
          useFallback: false
        });
      } catch (error) {
        if (cancelled) return;
        setUseFallback(true);
        setErrorMessage(buildRankingFallbackMessage(extractRankingFailureReason(error)));
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [backendReady, dir, rankWhich, rankMode, rankingCacheKey, riskMode, syncFavoriteFlags]);

  useEffect(() => {
    if (!useFallback) return;
    setItems(syncFavoriteFlags(fallbackItems));
    clearRankingFetchCache(rankingCacheKey);
  }, [fallbackItems, rankingCacheKey, syncFavoriteFlags, useFallback]);

  useEffect(() => {
    setItems((current) => syncFavoriteFlags(current));
  }, [syncFavoriteFlags]);

  useEffect(() => {
    if (!items.length) {
      setSelectedCodes([]);
      return;
    }
    setSelectedCodes((prev) => {
      const next = prev.filter((code) => itemCodeSet.has(code));
      return next.length === prev.length ? prev : next;
    });
  }, [items.length, itemCodeSet]);

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape" && consultVisible) {
        setConsultVisible(false);
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [consultVisible]);

  const selectedSet = useMemo(() => new Set(selectedCodes), [selectedCodes]);

  const toggleSelect = useCallback((code: string) => {
    setSelectedCodes((prev) => {
      if (prev.includes(code)) return prev.filter((item) => item !== code);
      return [...prev, code];
    });
  }, []);

  const handleOpenDetail = useCallback(
    (code: string) => {
      try {
        sessionStorage.setItem("detailListBack", location.pathname);
        sessionStorage.setItem("detailListCodes", JSON.stringify(listCodes));
      } catch {
        // ignore storage failures
      }
      navigate(`/detail/${code}`, { state: { from: location.pathname } });
    },
    [navigate, location.pathname, listCodes]
  );

  const handleEnsureVisibleItem = useCallback(
    (code: string) => {
      if (!backendReady) return;
      void ensureBarsForVisible(listTimeframe, [code], "ranking-visible");
    },
    [backendReady, ensureBarsForVisible, listTimeframe]
  );

  const handleToggleFavorite = useCallback(
    async (code: string, isFavorite: boolean) => {
      setItems((current) =>
        current.map((item) =>
          item.code === code ? { ...item, is_favorite: !isFavorite } : item
        )
      );
      setFavoriteLocal(code, !isFavorite);
      try {
        if (isFavorite) {
          await api.delete(`/favorites/${encodeURIComponent(code)}`);
        } else {
          await api.post(`/favorites/${encodeURIComponent(code)}`);
        }
      } catch {
        setItems((current) =>
          current.map((item) =>
            item.code === code ? { ...item, is_favorite: isFavorite } : item
          )
        );
        setFavoriteLocal(code, isFavorite);
        setToastMessage("お気に入りの更新に失敗しました。");
      }
    },
    [setFavoriteLocal]
  );

  const buildConsultation = useCallback(async () => {
    if (!selectedCodes.length) return;
    setConsultBusy(true);
    try {
      try {
        await ensureBarsForVisible(consultTimeframe, selectedCodes, "consult-pack");
      } catch {
        // Use available cache even if fetch fails.
      }
      const itemsForPack = selectedCodes.map((code) => {
        const rankItem = itemByCode.get(code);
        const payload = barsCache[consultTimeframe]?.[code];
        const boxes = boxesCache[consultTimeframe][code] ?? [];
        const monthlyP20 = Number.isFinite(rankItem?.mlP20Side1M ?? NaN)
          ? ((rankItem?.mlP20Side1M ?? 0) * 100)
          : null;
        const monthlyPBig = Number.isFinite(rankItem?.mlPAbsBig ?? NaN)
          ? ((rankItem?.mlPAbsBig ?? 0) * 100)
          : null;
        const monthlyPSide = dir === "up"
          ? (Number.isFinite(rankItem?.mlPUpBig ?? NaN) ? ((rankItem?.mlPUpBig ?? 0) * 100) : null)
          : (Number.isFinite(rankItem?.mlPDownBig ?? NaN) ? ((rankItem?.mlPDownBig ?? 0) * 100) : null);
        const reasonChunks = [
          `setup=${formatSetupType(rankItem?.setupType)}`,
          `1M±20=${formatPct(rankItem?.mlP20Side1M)}`,
          `${dir === "up" ? "1M上昇" : "1M下落"}=${formatPct(dir === "up" ? rankItem?.mlPUpBig : rankItem?.mlPDownBig)}`,
          `1M変動=${formatPct(rankItem?.mlPAbsBig)}`
        ];
        reasonChunks.push(`勝ちやすさ=${formatPct(rankItem?.winNowScore)}`);
        if (rankItem?.mtfStrictResolved) {
          reasonChunks.push(`厳選=${MTF_STRICTNESS_LABEL[rankItem.mtfStrictResolved]}`);
        }
        reasonChunks.push(`目標=${mtfStrictTarget}件`);
        reasonChunks.push(`ゲート=${(mtfStrictGateApplied * 100).toFixed(1)}%`);
        if (Number.isFinite(rankItem?.mtfLiquidity20d ?? NaN)) {
          reasonChunks.push(`流動=${(rankItem?.mtfLiquidity20d ?? 0).toFixed(0)}`);
        }
        if (rankItem?.mtfSignalBits) {
          reasonChunks.push(`MTF=${rankItem.mtfSignalBits}`);
        }
        const consultationScore = firstFinite(rankItem?.winNowScore, rankItem?.entryScore, rankItem?.hybridScore);
        return {
          code,
          name: rankItem?.name ?? null,
          market: null,
          sector: null,
          bars: payload?.bars ?? null,
          boxes,
          boxState: null,
          hasBox: null,
          buyState: formatSetupType(rankItem?.setupType),
          buyStateScore: consultationScore,
          buyStateReason: reasonChunks.join(" / "),
          buyStateDetails: {
            monthly: monthlyP20,
            weekly: monthlyPSide,
            daily: monthlyPBig
          }
        };
      });
      const result = buildConsultationPack(
        {
          createdAt: new Date(),
          timeframe: consultTimeframe,
          barsCount: consultBarsCount
        },
        itemsForPack,
        consultSort
      );
      setConsultText(result.text);
      setConsultMeta({ omitted: result.omittedCount });
      setConsultVisible(true);
      setConsultExpanded(true);
      setConsultTab("selection");
    } finally {
      setConsultBusy(false);
    }
  }, [
    selectedCodes,
    ensureBarsForVisible,
    consultTimeframe,
    dir,
    mtfStrictTarget,
    mtfStrictGateApplied,
    itemByCode,
    barsCache,
    boxesCache,
    consultSort
  ]);

  const handleCreateScreenshots = useCallback(async () => {
    if (selectedCodes.length === 0) {
      setToastMessage("スクショ対象がありません。");
      return;
    }

    // Check setting for Consult mode (Use new method)
    // The user requirement says "Replace" so we just use the new one.

    setToastMessage("スクショ生成を開始します...");

    const result = await generateScreenshots(selectedCodes);

    if (result.success) {
      setToastMessage(`${result.count}件のスクショを保存しました`);
      if (result.success && window.pywebview?.api?.open_screenshot_dir) {
        setToastAction({
          label: "フォルダを開く",
          onClick: async () => {
            await window.pywebview!.api.open_screenshot_dir();
          }
        });
      }
    } else {
      setToastMessage(`保存失敗: ${result.error || "不明なエラー"}`);
    }
  }, [selectedCodes, generateScreenshots]);

  const handleCopyConsult = useCallback(async () => {
    if (!consultText) {
      setToastMessage("相談パックがまだありません。");
      return;
    }
    try {
      await navigator.clipboard.writeText(consultText);
      setToastMessage("相談パックをコピーしました。");
    } catch {
      setToastMessage("コピーに失敗しました。");
    }
  }, [consultText]);

  const selectedChips = useMemo(() => {
    const limit = 6;
    const visible = selectedCodes.slice(0, limit);
    const extra = Math.max(0, selectedCodes.length - visible.length);
    return { visible, extra };
  }, [selectedCodes]);

  const showSkeleton = backendReady && loading && items.length === 0;
  const emptyLabel =
    !loading && backendReady && sortedItems.length === 0 && !errorMessage
      ? search.trim() ||
        filterSignalsOnly ||
        filterDataOnly ||
        filterBuySignalsOnly ||
        filterSellSignalsOnly ||
        filterQualifiedOnly ||
        filterMtfStrictOnly
        ? "該当する銘柄がありません。"
        : "ランキングがありません。"
      : null;
  const isSingleDensity = listColumns === 1 && listRows === 1;
  const formatPct = (value?: number | null) => {
    if (!Number.isFinite(value ?? NaN)) return "--";
    return `${((value ?? 0) * 100).toFixed(2)}%`;
  };
  const formatDownProb = (downProb?: number | null, upProb?: number | null) => {
    const raw =
      Number.isFinite(downProb ?? NaN) ? downProb : Number.isFinite(upProb ?? NaN) ? 1 - (upProb ?? 0) : null;
    if (!Number.isFinite(raw ?? NaN)) return "--";
    const clipped = Math.min(1, Math.max(0, raw ?? 0));
    return `${(clipped * 100).toFixed(2)}%`;
  };
  const formatRankScore = (value?: number | null) => {
    if (!Number.isFinite(value ?? NaN)) return "--";
    return (value ?? 0).toFixed(3);
  };
  const formatResearchPriorRank = (item: RankItem) => {
    if (!Number.isFinite(item.researchPriorRank ?? NaN)) return "--";
    const rank = Math.max(1, Math.round(item.researchPriorRank ?? 0));
    if (!Number.isFinite(item.researchPriorUniverse ?? NaN)) return `#${rank}`;
    const universe = Math.max(1, Math.round(item.researchPriorUniverse ?? 0));
    return `#${rank}/${universe}`;
  };
  const formatTurnProb = (upTurn?: number | null, downTurn?: number | null) => {
    if (dir === "up") return formatPct(upTurn);
    return formatPct(downTurn);
  };
  const formatAsOf = (value?: string | null) => value ?? "--";
  const formatQualification = (item: RankItem) => {
    if (item.entryQualified === true) {
      const cnt = Number.isFinite(item.mtfQualifiedCount ?? NaN) ? Number(item.mtfQualifiedCount) : null;
      if (cnt != null) return `適格 ${cnt}/3`;
      return "適格 OK";
    }
    if (item.entryQualifiedByFallback === true) {
      if (item.entryQualifiedFallbackStage === "mtf_consensus") return "適格 合意待ち";
      if (item.entryQualifiedFallbackStage === "hybrid_relaxed_score") return "適格 段階1";
      if (item.entryQualifiedFallbackStage === "turn_strict_recovery") return "適格 段階2";
      if (item.entryQualifiedFallbackStage === "short_pattern_recovery") return "適格 売り補完";
      if (item.entryQualifiedFallbackStage === "short_turn_recovery") return "適格 売り再同調";
      return "適格 補完";
    }
    if (item.entryQualified === false) return "適格 要確認";
    return "適格 --";
  };
  const formatSetupType = (value?: string | null) => {
    if (!value) return "--";
    if (value === "target20_breakout" || value === "breakout20") return "20%狙い";
    if (value === "breakout_trend" || value === "breakout") return "ブレイク";
    if (value === "breakdown") return "崩れ継続";
    if (value === "accumulation_break" || value === "accumulation") return "貯め→抜け";
    if (value === "rebound") return "反発狙い";
    if (value === "watchlist" || value === "watch") return "監視";
    return value;
  };
  const formatPctSigned = (value?: number | null) => {
    if (!Number.isFinite(value ?? NaN)) return "--";
    const n = value ?? 0;
    const sign = n > 0 ? "+" : "";
    return `${sign}${(n * 100).toFixed(2)}%`;
  };
  const formatEdinetStatus = (value?: string | null) => {
    if (!value) return "未判定";
    if (value === "ok") return "OK";
    if (value === "missing_tables") return "テーブル不足";
    if (value === "unmapped") return "未マップ";
    if (value === "no_payload") return "データなし";
    return value;
  };
  const formatEdinetNotAppliedReason = (value?: string | null) => {
    if (!value || value === "ok") return "補正条件外";
    if (value === "missing_tables") return "補正未適用: EDINETテーブル未整備";
    if (value === "unmapped") return "補正未適用: EDINETコード未対応";
    if (value === "no_payload") return "補正未適用: 財務ペイロード未取得";
    return `補正未適用: ${value}`;
  };
  const formatInvalidationTrigger = (value?: string | null) => {
    if (!value) return "--";
    if (value === "box_break") return "Box下限割れ";
    if (value === "box_reclaim") return "Box上限回復";
    if (value === "stop3") return "-3%逆行";
    if (value === "stop5") return "-5%逆行";
    if (value === "ma20") return "MA20逆抜け";
    return value;
  };
  const formatInvalidationAction = (value?: string | null) => {
    if (!value) return "--";
    if (value === "exit") return "撤退";
    if (value === "hold") return "継続";
    if (value === "doten_opt") return "ドテン";
    if (value === "doten_remainder") return "残期間ドテン";
    return value;
  };
  const showExtendedMetrics = metricsView === "full";

  return (
    <div className="app-shell list-view">
      <UnifiedListHeader
        timeframe={listTimeframe}
        onTimeframeChange={setListTimeframe}
        rangeBars={listRangeBars}
        onRangeChange={setListRangeBars}
        search={search}
        onSearchChange={setSearch}
        sortValue={dir}
        sortOptions={sortOptions}
        onSortChange={(value) => setDir(value as "up" | "down")}
        columns={listColumns}
        rows={listRows}
        onColumnsChange={setListColumns}
        onRowsChange={setListRows}
        filterItems={filterItems}
        helpLabel="相談"
        onHelpClick={() => {
          setConsultVisible(true);
          setConsultExpanded(false);
          setConsultTab("selection");
        }}
      />
      <div
        style={{
          display: "flex",
          flexWrap: "wrap",
          gap: "8px",
          alignItems: "center",
          padding: "6px 16px",
          borderBottom: "1px solid var(--theme-border)",
          background: "var(--theme-bg-secondary)"
        }}
      >
        <div className="segmented segmented-compact">
          {(["up", "down"] as const).map((key) => (
            <button
              key={key}
              type="button"
              className={dir === key ? "active" : ""}
              onClick={() => setDir(key)}
            >
              {key === "up" ? "買い" : "売り"}
            </button>
          ))}
        </div>
        <div className="segmented segmented-compact">
          {(["compact", "full"] as const).map((key) => (
            <button
              key={key}
              type="button"
              className={metricsView === key ? "active" : ""}
              onClick={() => setMetricsView(key)}
            >
              {key === "compact" ? "要点" : "詳細"}
            </button>
          ))}
        </div>
        <span className="rank-score-badge">
          表示: 統合厳選(自動)
        </span>
        <span className="rank-score-badge">
          運用: 継続 x 中立
        </span>
        <span className="rank-score-badge">
          厳選: {mtfStrictRule.label} / 合意{mtfStrictRule.minQualified}/3+ or 勝ちやすさ{(mtfStrictGateApplied * 100).toFixed(1)}% / 候補{mtfStrictCount}件
        </span>
        {qualificationFilterRelaxed && (
          <div className="rank-top-summary is-warn">
            適格銘柄が0件のため、条件未達を含む候補を表示しています。
          </div>
        )}
        {mtfStrictFilterRelaxed && (
          <div className="rank-top-summary is-warn">
            統合厳選(合意{mtfStrictRule.minQualified}/3+ または 勝ちやすさ{(mtfStrictGateApplied * 100).toFixed(1)}%以上)で0件のため、候補を自動緩和しています。
          </div>
        )}
      </div>
      <div
        className={`rank-shell list-shell${isSingleDensity ? " is-single" : ""} ${consultPaddingClass}`}
        style={listStyles}
      >
        {showSkeleton && (
          <div className="rank-skeleton">
            {Array.from({ length: 4 }).map((_, index) => (
              <div className="tile skeleton-card" key={`rank - skeleton - ${index}`}>
                <div className="skeleton-line wide" />
                <div className="skeleton-line" />
                <div className="skeleton-block tall" />
              </div>
            ))}
          </div>
        )}
        {!showSkeleton && (
          <>
            {errorMessage && <div className="rank-status">{errorMessage}</div>}
            {emptyLabel && <div className="rank-status">{emptyLabel}</div>}
            <div className="rank-grid">
              {sortedItems.map((item, index) => {
                const payload = barsCache[listTimeframe]?.[item.code] ?? null;
                const status = barsStatus[listTimeframe][item.code];
                const isMonthlyList = listTimeframe === "monthly";
                const displayUpProb = Number.isFinite(item.mlPUpShort ?? NaN)
                  ? item.mlPUpShort
                  : item.mlPUp;
                const displayDownProb = Number.isFinite(item.mlPDown ?? NaN)
                  ? item.mlPDown
                  : Number.isFinite(displayUpProb ?? NaN)
                    ? 1 - (displayUpProb ?? 0)
                    : null;
                const displayMonthlyUpProb = Number.isFinite(item.mlPUpBig ?? NaN)
                  ? item.mlPUpBig
                  : item.mlPUp;
                const displayMonthlyDownProb = Number.isFinite(item.mlPDownBig ?? NaN)
                  ? item.mlPDownBig
                  : displayDownProb;
                const displayTripletProb = dir === "up" ? item.candleTripletUp : item.candleTripletDown;
                const displayMonthlyBreakoutProb =
                  dir === "up" ? item.monthlyBreakoutUpProb : item.monthlyBreakoutDownProb;
                const displayMonthlySide20Prob = Number.isFinite(item.mlP20Side1M ?? NaN)
                  ? item.mlP20Side1M
                  : item.mlP20Side1MRaw;
                const setupTypeLabel = formatSetupType(item.setupType);
                const series =
                  payload && payload.bars?.length ? payload.bars : item.series ?? [];
                const ticker = tickerMap.get(item.code);
                const earningsLabel = formatEventBadgeDate(ticker?.eventEarningsDate);
                const rightsLabel = formatEventBadgeDate(ticker?.eventRightsDate);
                return (
                  <ChartListCard
                    key={item.code}
                    code={item.code}
                    name={item.name ?? item.code}
                    payload={payload}
                    fallbackSeries={series}
                    status={status}
                    maSettings={resolvedMaSettings}
                    rangeBars={listRangeBars}
                    densityKey={densityKey}
                    signals={signalMap.get(item.code) ?? []}
                    onOpenDetail={handleOpenDetail}
                    tileClassName={selectedSet.has(item.code) ? "is-selected" : ""}
                    deferUntilInView
                    onEnterView={handleEnsureVisibleItem}
                    maxDate={item.asOf}
                    phaseBody={ticker?.bodyScore ?? null}
                    phaseEarly={ticker?.earlyScore ?? null}
                    phaseLate={ticker?.lateScore ?? null}
                    phaseN={ticker?.phaseN ?? null}
                    headerLeft={
                      <>
                        <span className="rank-badge">{index + 1}</span>
                        <div className="tile-id">
                          <label
                            className="tile-select-toggle"
                            onClick={(event) => event.stopPropagation()}
                            onDoubleClick={(event) => event.stopPropagation()}
                          >
                            <input
                              type="checkbox"
                              checked={selectedSet.has(item.code)}
                              onChange={() => toggleSelect(item.code)}
                              aria-label={`${item.code} を選択`}
                            />
                            <span className="tile-code">{item.code}</span>
                          </label>
                          <span className="tile-name">{item.name ?? item.code}</span>
                          {(rightsLabel || earningsLabel) && (
                            <span className="event-badges">
                              {rightsLabel && (
                                <span className="event-badge event-rights">権利 {rightsLabel}</span>
                              )}
                              {earningsLabel && (
                                <span className="event-badge event-earnings">
                                  決算 {earningsLabel}
                                </span>
                              )}
                            </span>
                          )}
                        </div>
                      </>
                    }
                    headerRight={
                      <>
                        <span className="rank-score-badge">
                          騰落率 {formatPct(item.changePct)}
                        </span>
                        <span className="rank-score-badge">
                          期待値 {formatPct(item.mlEv20Net)}
                        </span>
                        <span className="rank-score-badge">
                          {dir === "up"
                            ? `${isMonthlyList ? "1M上昇確率" : "上昇確率"} ${formatPct(isMonthlyList ? displayMonthlyUpProb : displayUpProb)}`
                            : `${isMonthlyList ? "1M下落確率" : "下落確率"} ${formatDownProb(
                              isMonthlyList ? displayMonthlyDownProb : displayDownProb,
                              isMonthlyList ? displayMonthlyUpProb : displayUpProb
                            )}`}
                        </span>
                        {isMonthlyList && Number.isFinite(displayMonthlySide20Prob ?? NaN) && (
                          <span className="rank-score-badge">
                            1M±20%確率 {formatPct(displayMonthlySide20Prob)}
                          </span>
                        )}
                        {isMonthlyList && (
                          <span className="rank-score-badge">
                            {setupTypeLabel}
                            {item.target20Qualified ? " / 20%狙いOK" : ""}
                          </span>
                        )}
                        {Number.isFinite(item.recommendedHoldDays ?? NaN) && (
                          <span className="rank-score-badge">
                            保有目安 {Math.round(item.recommendedHoldDays ?? 0)}日
                          </span>
                        )}
                        {Number.isFinite(item.recommendedHoldMinDays ?? NaN) &&
                          Number.isFinite(item.recommendedHoldMaxDays ?? NaN) &&
                          Math.round(item.recommendedHoldMinDays ?? 0) !== Math.round(item.recommendedHoldMaxDays ?? 0) && (
                            <span className="rank-score-badge">
                              保有レンジ {Math.round(item.recommendedHoldMinDays ?? 0)}-{Math.round(item.recommendedHoldMaxDays ?? 0)}日
                            </span>
                          )}
                        <span
                          className={`rank-score-badge rank-qualification ${item.entryQualified === true
                            ? "is-ok"
                            : item.entryQualifiedByFallback === true
                              ? "is-warn"
                            : item.entryQualified === false
                              ? "is-warn"
                              : ""
                            }`}
                        >
                          {formatQualification(item)}
                        </span>
                        {item.researchPriorAligned === true && (
                          <span className="rank-score-badge">
                            研究一致 {formatResearchPriorRank(item)}
                            {Number.isFinite(item.researchPriorBonus ?? NaN)
                              ? ` ${formatPctSigned(item.researchPriorBonus)}`
                              : ""}
                          </span>
                        )}
                        {Number.isFinite(item.winNowScore ?? NaN) && (
                          <span className="rank-score-badge">
                            勝ちやすさ {((item.winNowScore ?? 0) * 100).toFixed(1)}%
                          </span>
                        )}
                        {Number.isFinite(item.mtfQualifiedCount ?? NaN) && (
                          <span className="rank-score-badge">
                            MTF適格 {Math.max(0, Math.min(3, Math.round(item.mtfQualifiedCount ?? 0)))}/3
                          </span>
                        )}
                        {Number.isFinite(item.mtfFallbackCount ?? NaN) && (
                          <span className="rank-score-badge">
                            MTF補完 {Math.max(0, Math.min(3, Math.round(item.mtfFallbackCount ?? 0)))}/3
                          </span>
                        )}
                        {item.mtfSignalBits && (
                          <span className="rank-score-badge">
                            {item.mtfSignalBits}
                          </span>
                        )}
                        {showExtendedMetrics && (
                          <>
                            {item.researchPriorRunId && (
                              <span className="rank-score-badge">
                                研究Run {item.researchPriorRunId}
                              </span>
                            )}
                            {item.researchPriorAsOf && (
                              <span className="rank-score-badge">
                                研究asOf {item.researchPriorAsOf}
                              </span>
                            )}
                            {Number.isFinite(item.researchPriorUniverse ?? NaN) && (
                              <span className="rank-score-badge">
                                研究母数 {Math.max(1, Math.round(item.researchPriorUniverse ?? 0))}
                              </span>
                            )}
                            {Number.isFinite(item.researchPriorBonus ?? NaN) &&
                              Math.abs(item.researchPriorBonus ?? 0) > 1e-9 && (
                                <span className="rank-score-badge">
                                  研究補正 {formatPctSigned(item.researchPriorBonus)}
                                </span>
                              )}
                            {item.invalidationTrigger && (
                              <span className="rank-score-badge">
                                否定 {formatInvalidationTrigger(item.invalidationTrigger)} / 推奨 {formatInvalidationAction(item.invalidationRecommendedAction)} / 守 {formatInvalidationAction(item.invalidationConservativeAction)} / 攻 {formatInvalidationAction(item.invalidationAggressiveAction)}
                              </span>
                            )}
                            {item.invalidationDotenRecommended === true && Number.isFinite(item.invalidationOppositeHoldDays ?? NaN) && (
                              <span className="rank-score-badge">
                                否定時ドテン {Math.round(item.invalidationOppositeHoldDays ?? 0)}日
                              </span>
                            )}
                            {Number.isFinite(item.invalidationExpectedDeltaMean ?? NaN) && (
                              <span className="rank-score-badge">
                                否定期待差 {formatPctSigned(item.invalidationExpectedDeltaMean)}
                              </span>
                            )}
                            {Number.isFinite(item.playbookScoreBonus ?? NaN) &&
                              Math.abs(item.playbookScoreBonus ?? 0) > 1e-9 && (
                                <span className="rank-score-badge">
                                  Playbook補正 {formatPctSigned(item.playbookScoreBonus)}
                                </span>
                              )}
                            {item.recommendedHoldReason && (
                              <span className="rank-score-badge">
                                保有根拠 {item.recommendedHoldReason}
                              </span>
                            )}
                            {Number.isFinite(item.mtfWinD ?? NaN) && (
                              <span className="rank-score-badge">D勝 {formatPct(item.mtfWinD)}</span>
                            )}
                            {Number.isFinite(item.mtfWinW ?? NaN) && (
                              <span className="rank-score-badge">W勝 {formatPct(item.mtfWinW)}</span>
                            )}
                            {Number.isFinite(item.mtfWinM ?? NaN) && (
                              <span className="rank-score-badge">M勝 {formatPct(item.mtfWinM)}</span>
                            )}
                            {Number.isFinite(item.mtfCoverage ?? NaN) && (
                              <span className="rank-score-badge">MTF充足 {formatPct(item.mtfCoverage)}</span>
                            )}
                            {Number.isFinite(item.mtfLiquidity20d ?? NaN) && (
                              <span className="rank-score-badge">MTF流動 {(item.mtfLiquidity20d ?? 0).toFixed(0)}</span>
                            )}
                            <span className="rank-score-badge">RankUp {formatRankScore(item.mlRankUp)}</span>
                            <span className="rank-score-badge">RankDown {formatRankScore(item.mlRankDown)}</span>
                            <span className="rank-score-badge">
                              {dir === "up"
                                ? `転換買い ${formatTurnProb(item.mlPTurnUp, item.mlPTurnDown)}`
                                : `転換売り ${formatTurnProb(item.mlPTurnUp, item.mlPTurnDown)}`}
                            </span>
                            {Number.isFinite(item.prob5d ?? NaN) && (
                              <span className="rank-score-badge">5D確率 {formatPct(item.prob5d)}</span>
                            )}
                            {Number.isFinite(item.prob10d ?? NaN) && (
                              <span className="rank-score-badge">10D確率 {formatPct(item.prob10d)}</span>
                            )}
                            {Number.isFinite(item.prob20d ?? NaN) && (
                              <span className="rank-score-badge">20D確率 {formatPct(item.prob20d)}</span>
                            )}
                            {Number.isFinite(item.maStreak60Up ?? NaN) && (
                              <span className="rank-score-badge">60上 {Math.round(item.maStreak60Up ?? 0)}</span>
                            )}
                            {Number.isFinite(item.maStreak100Up ?? NaN) && (
                              <span className="rank-score-badge">100上 {Math.round(item.maStreak100Up ?? 0)}</span>
                            )}
                            {item.maStreakAligned === true && (
                              <span className="rank-score-badge">MA本数 適合</span>
                            )}
                            {item.weakEarlyPattern === true && (
                              <span className="rank-score-badge">初期弱含み 警戒</span>
                            )}
                            {item.patternA1MaturedBreakout === true && (
                              <span className="rank-score-badge">A1 成熟Box抜け</span>
                            )}
                            {item.patternA2BoxTrend === true && (
                              <span className="rank-score-badge">A2 Box上半トレンド</span>
                            )}
                            {item.patternA3CapitulationRebound === true && (
                              <span className="rank-score-badge">A3 反発余地</span>
                            )}
                            {item.patternS1WeakBreakdown === true && (
                              <span className="rank-score-badge">S1 下抜け弱形 警戒</span>
                            )}
                            {item.patternS2WeakBox === true && (
                              <span className="rank-score-badge">S2 Box下限弱形 警戒</span>
                            )}
                            {item.patternS3LateBreakout === true && (
                              <span className="rank-score-badge">S3 伸び切り 警戒</span>
                            )}
                            {item.patternD1ShortBreakdown === true && (
                              <span className="rank-score-badge">D1 弱形下抜け 売り</span>
                            )}
                            {item.patternD2ShortMixedFar === true && (
                              <span className="rank-score-badge">D2 混在→崩れ 売り</span>
                            )}
                            {item.patternD3ShortNaBelow === true && (
                              <span className="rank-score-badge">D3 初期弱含み 売り</span>
                            )}
                            {item.patternD4ShortDoubleTop === true && (
                              <span className="rank-score-badge">D4 ダブルトップ 売り</span>
                            )}
                            {item.patternD5ShortHeadShoulders === true && (
                              <span className="rank-score-badge">D5 三尊天井 売り</span>
                            )}
                            {item.patternDTrapStackDownFar === true && (
                              <span className="rank-score-badge">D罠 売られ過ぎ 警戒</span>
                            )}
                            {item.patternDTrapOverheatMomentum === true && (
                              <span className="rank-score-badge">D罠 過熱順行 警戒</span>
                            )}
                            {item.patternDTrapTopFakeout === true && (
                              <span className="rank-score-badge">D罠 天井だまし 警戒</span>
                            )}
                            <span className="rank-score-badge">
                              確率カーブ {item.probCurveAligned === false ? "NG" : item.probCurveAligned === true ? "OK" : "--"}
                            </span>
                            <span className="rank-score-badge">
                              {dir === "up"
                                ? `3本買い ${formatPct(displayTripletProb)}`
                                : `3本売り ${formatPct(displayTripletProb)}`}
                            </span>
                            <span className="rank-score-badge">
                              {dir === "up"
                                ? `月抜け ${formatPct(displayMonthlyBreakoutProb)}`
                                : `月下抜け ${formatPct(displayMonthlyBreakoutProb)}`}
                            </span>
                            <span className="rank-score-badge">
                              月レンジ {formatPct(item.monthlyRangeProb)}
                            </span>
                            {isMonthlyList && Number.isFinite(item.target20Gate ?? NaN) && (
                              <span className="rank-score-badge">
                                20%ゲート {formatPct(item.target20Gate)}
                              </span>
                            )}
                            {isMonthlyList && Number.isFinite(item.breakoutReadiness ?? NaN) && (
                              <span className="rank-score-badge">
                                抜け準備 {formatPct(item.breakoutReadiness)}
                              </span>
                            )}
                            {isMonthlyList && Number.isFinite(item.accumulationScore ?? NaN) && (
                              <span className="rank-score-badge">
                                貯め度 {formatPct(item.accumulationScore)}
                              </span>
                            )}
                            {isMonthlyList && (
                              <span className="rank-score-badge">
                                EDI状態 {formatEdinetStatus(item.edinetStatus)}
                              </span>
                            )}
                            {isMonthlyList && item.edinetStatus && item.edinetStatus !== "ok" && (
                              <span className="rank-score-badge">
                                {formatEdinetNotAppliedReason(item.edinetStatus)}
                              </span>
                            )}
                            {isMonthlyList && Number.isFinite(item.edinetFreshnessDays ?? NaN) && (
                              <span className="rank-score-badge">
                                EDI鮮度 {Math.max(0, Math.round(item.edinetFreshnessDays ?? 0))}日
                              </span>
                            )}
                            {isMonthlyList && Number.isFinite(item.edinetQualityScore ?? NaN) && (
                              <span className="rank-score-badge">
                                EDI品質 {formatPct(item.edinetQualityScore)}
                              </span>
                            )}
                            {isMonthlyList && Number.isFinite(item.edinetScoreBonus ?? NaN) && (
                              <span className="rank-score-badge">
                                EDI補正 {formatPctSigned(item.edinetScoreBonus)}
                              </span>
                            )}
                            {isMonthlyList && Number.isFinite(item.edinetRoe ?? NaN) && (
                              <span className="rank-score-badge">ROE {formatPct(item.edinetRoe)}</span>
                            )}
                            {isMonthlyList && Number.isFinite(item.edinetEquityRatio ?? NaN) && (
                              <span className="rank-score-badge">自己資本比率 {formatPct(item.edinetEquityRatio)}</span>
                            )}
                            {isMonthlyList && Number.isFinite(item.edinetDebtRatio ?? NaN) && (
                              <span className="rank-score-badge">D/E {formatRankScore(item.edinetDebtRatio)}</span>
                            )}
                            {isMonthlyList && Number.isFinite(item.edinetOperatingCfMargin ?? NaN) && (
                              <span className="rank-score-badge">営業CF率 {formatPct(item.edinetOperatingCfMargin)}</span>
                            )}
                            {isMonthlyList && Number.isFinite(item.edinetRevenueGrowthYoy ?? NaN) && (
                              <span className="rank-score-badge">売上成長率 {formatPct(item.edinetRevenueGrowthYoy)}</span>
                            )}
                          </>
                        )}
                        <span className="rank-score-badge">
                          総合 {formatPct(item.hybridScore)}
                        </span>
                        {showExtendedMetrics && (
                          <span className="rank-score-badge">日付 {formatAsOf(item.asOf)}</span>
                        )}
                        <button
                          type="button"
                          className={`favorite-toggle ${item.is_favorite ? "active" : ""}`}
                          aria-pressed={Boolean(item.is_favorite)}
                          aria-label={item.is_favorite ? "お気に入り解除" : "お気に入り追加"}
                          onClick={(event) => {
                            event.stopPropagation();
                            handleToggleFavorite(item.code, Boolean(item.is_favorite));
                          }}
                        >
                          {item.is_favorite ? <IconHeartFilled size={16} /> : <IconHeart size={16} />}
                        </button>
                      </>
                    }
                  />
                );
              })}

            </div>
          </>
        )}
      </div>
      <div
        className={`consult-sheet ${consultVisible ? "is-visible" : "is-hidden"} ${consultExpanded ? "is-expanded" : "is-mini"
          }`}
      >
        <button
          type="button"
          className="consult-handle"
          onClick={() => {
            if (!consultVisible) return;
            setConsultExpanded((prev) => !prev);
          }}
          aria-label={consultExpanded ? "相談バーを折りたたむ" : "相談バーを展開する"}
        />
        {!consultExpanded && (
          <div className="consult-mini">
            <div className="consult-mini-left">
              <div className="consult-mini-count">選択 {selectedCodes.length}件</div>
              <div className="consult-chips">
                {selectedChips.visible.map((code) => (
                  <span key={code} className="consult-chip">
                    {code}
                  </span>
                ))}
                {selectedChips.extra > 0 && (
                  <span className="consult-chip">+{selectedChips.extra}</span>
                )}
              </div>
            </div>
            <div className="consult-mini-actions">
              <button
                type="button"
                className="consult-primary"
                onClick={buildConsultation}
                disabled={!selectedCodes.length || consultBusy}
              >
                {consultBusy ? "作成中..." : "相談作成"}
              </button>
              <button
                type="button"
                onClick={handleCreateScreenshots}
                disabled={!selectedCodes.length || screenshotBusy}
              >
                {screenshotBusy ? "作成中..." : "スクショ作成"}
              </button>
              <button type="button" onClick={handleCopyConsult} disabled={!consultText}>
                コピー
              </button>
              <button
                type="button"
                onClick={() => window.pywebview?.api?.open_screenshot_dir?.()}
                disabled={!window.pywebview?.api?.open_screenshot_dir}
              >
                フォルダ
              </button>
              <button type="button" onClick={() => setConsultVisible(false)}>
                閉じる
              </button>
            </div>
          </div>
        )}
        {consultExpanded && (
          <div className="consult-expanded">
            <div className="consult-expanded-header">
              <div className="consult-tabs">
                <button
                  type="button"
                  className={consultTab === "selection" ? "active" : ""}
                  onClick={() => setConsultTab("selection")}
                >
                  選定相談
                </button>
                <button
                  type="button"
                  className={consultTab === "position" ? "active" : ""}
                  onClick={() => setConsultTab("position")}
                >
                  建玉相談
                </button>
              </div>
              <div className="consult-expanded-actions">
                <button
                  type="button"
                  className="consult-primary"
                  onClick={buildConsultation}
                  disabled={!selectedCodes.length || consultBusy}
                >
                  {consultBusy ? "作成中..." : "相談作成"}
                </button>
                <button
                  type="button"
                  onClick={handleCreateScreenshots}
                  disabled={!selectedCodes.length || screenshotBusy}
                >
                  {screenshotBusy ? "作成中..." : "スクショ作成"}
                </button>
                <button type="button" onClick={handleCopyConsult} disabled={!consultText}>
                  コピー
                </button>
                <button
                  type="button"
                  onClick={() => window.pywebview?.api?.open_screenshot_dir?.()}
                  disabled={!window.pywebview?.api?.open_screenshot_dir}
                >
                  フォルダ
                </button>
                <button type="button" onClick={() => setConsultVisible(false)}>
                  閉じる
                </button>
              </div>
            </div>
            <div className="consult-expanded-body">
              <div className="consult-expanded-meta-row">
                <div className="consult-expanded-meta">
                  選択 {selectedCodes.length}件
                  {consultMeta.omitted
                    ? ` / 表示外 ${consultMeta.omitted}件`
                    : " / 最大10件まで表示"}
                </div>
                <div className="consult-sort">
                  <span>並び順</span>
                  <div className="segmented segmented-compact">
                    {(["score", "code"] as ConsultationSort[]).map((key) => (
                      <button
                        key={key}
                        className={consultSort === key ? "active" : ""}
                        onClick={() => setConsultSort(key)}
                      >
                        {key === "score" ? "スコア順" : "コード順"}
                      </button>
                    ))}
                  </div>
                </div>
              </div>
              {consultTab === "selection" ? (
                <textarea className="consult-drawer-body" value={consultText} readOnly />
              ) : (
                <div className="consult-placeholder">建玉相談は準備中です。</div>
              )}
            </div>
          </div>
        )}
      </div>
      <Toast
        message={toastMessage}
        onClose={() => {
          setToastMessage(null);
          setToastAction(null);
        }}
        action={toastAction}
      />
    </div>
  );
}





