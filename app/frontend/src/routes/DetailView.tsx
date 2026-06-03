// @ts-nocheck
import { lazy, Suspense, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import type { FormEvent, MouseEvent as ReactMouseEvent, TouchEvent as ReactTouchEvent } from "react";
import { useCallback } from "react";
import { startTransition } from "react";
import { useLocation, useNavigate, useParams } from "react-router-dom";
import {
  IconAdjustments,
  IconArrowLeft,
  IconArrowRight,
  IconCamera,
  IconHeart,
  IconHeartFilled,
  IconInfoCircle,
} from "@tabler/icons-react";
import { api } from "../api";
import { useBackendReadyState } from "../backendReady";
import DetailChart, {
  DetailChartHandle,
  type DrawTool,
  type SelectedDrawingInfo
} from "../components/DetailChart";
import Toast from "../components/Toast";
import IconButton from "../components/IconButton";
import DetailHeaderChrome from "../components/DetailHeaderChrome";
import DetailModeTabs from "../components/DetailModeTabs";
import DetailTimeframeSwitcher from "../components/DetailTimeframeSwitcher";
import DetailDrawToolbar from "../components/DetailDrawToolbar";
import TradexShadowReadout from "../components/TradexShadowReadout";
import DataFreshnessBadges from "../components/DataFreshnessBadges";
import ProductStateNotice, { type ProductStateNoticeKind } from "../components/ProductStateNotice";
import ScreenPanel from "../components/ScreenPanel";
import {
  buildAiExplainBarsPayload,
  buildAiExplainImages,
} from "../features/aiExplain/aiExplainImages";
import { Box, MaSetting, useStore } from "../store";
import { computeSignalMetrics } from "../utils/signals";
import type { TradeEvent, CurrentPosition, DailyPosition } from "../utils/positions";
import { buildCurrentPositions, buildDailyPositions, buildPositionLedger } from "../utils/positions";
import { captureAndCopyScreenshot, getScreenType } from "../utils/windowScreenshot";
import { formatEventBadgeDate, parseEventDateMs } from "../utils/events";
import DailyMemoPanel from "../components/DailyMemoPanel";
import { buildConsultCopyText, copyToClipboard as copyConsultToClipboard } from "../utils/consultCopy";
import { useChartSync } from "../hooks/useChartSync";
import { useDetailInfo } from "../hooks/useDetailInfo";
import { shouldShowOperatorConsole } from "../utils/operatorConsole";
import { useExactDecisionRange, type ExactDecisionTone } from "./detail/hooks/useExactDecisionRange";
import { useAsOfItemFetch } from "./detail/hooks/useAsOfItemFetch";
import { useDetailReplayRun } from "./detail/useDetailReplayRun";
import type { MeeMeeDataFreshnessContract } from "../dataFreshnessContract";
import {
  hasCompleteDetailChartPrefetch,
  loadDetailChartPrefetch,
  prefetchDetailChartFrames,
  readDetailChartPrefetchSync,
  type ChartPrefetchEntry,
  type ChartPrefetchFrames,
} from "./detail/detailChartPrefetch";
import {
  buildDetailChartLifecycle,
} from "./detail/detailChartLifecycle";
import { openDetailWithPrefetch } from "./detail/openDetailWithPrefetch";
import {
  readDetailListBackPath,
  readDetailListCodes,
} from "./detail/detailNavigationContext";
import {
  buildDetailChartPanelInput,
  buildDetailMaLines,
  buildDetailParsedFrame as buildCandlesWithStats,
  buildDetailVolume as buildVolume,
  toDetailChartMaLines,
} from "./detail/detailChartFrameAdapter";
import DetailDebugBanner from "./detail/components/DetailDebugBanner";
import DetailReplayPanel from "./detail/DetailReplayPanel";
import DetailPositionLedgerSheet from "./detail/components/DetailPositionLedgerSheet";
import { useDetailDrawings } from "./detail/hooks/useDetailDrawings";
import { buildPositionRiskOverlayDrawings } from "./detail/positionRiskOverlay";
import AnnotationToolbar from "./detail/AnnotationToolbar";
import ChartReadingPanel, {
  type ReadingCommentType,
  type ReadingTargetType,
  type ReadingTimeframe,
} from "./detail/ChartReadingPanel";
import ChartNotePanel, {
  type ChartNoteLinkedObject,
  type ChartNoteParagraph,
  type ChartNoteTimeframe,
} from "./detail/ChartNotePanel";
import {
  annotationToDrawBox,
  annotationToCallout,
  annotationToEventMarker,
  annotationToHorizontalLine,
  emptyAnnotationPayload,
  parseTagsInput,
  type AnnotationFilter,
  type AnnotationTool,
  type ChartAnnotation,
  type ChartAnnotationType,
} from "./detail/annotations";

const loadSimilarSearchPanel = () => import("../components/SimilarSearchPanel");
const loadDetailJudgementPanel = () =>
  import("./detail/DetailJudgementPanel").then((mod) => ({ default: mod.DetailJudgementPanel }));
const loadDetailFinancialPanel = () =>
  import("./detail/DetailFinancialPanel").then((mod) => ({ default: mod.DetailFinancialPanel }));
const loadDetailTdnetCard = () =>
  import("./detail/DetailTdnetCard").then((mod) => ({ default: mod.DetailTdnetCard }));
const loadTradexAnalysisMount = () => import("./detail/TradexAnalysisMount");
const loadDetailIndicatorOverlay = () =>
  import("./detail/components/DetailIndicatorOverlay");

const LazySimilarSearchPanel = lazy(loadSimilarSearchPanel);
const LazyAiExplainDock = lazy(() => import("../features/aiExplain/AiExplainDock"));
const LazyDetailJudgementPanel = lazy(loadDetailJudgementPanel);
const LazyDetailFinancialPanel = lazy(loadDetailFinancialPanel);
const LazyDetailTdnetCard = lazy(loadDetailTdnetCard);
const LazyTradexAnalysisMount = lazy(loadTradexAnalysisMount);
const LazyDetailIndicatorOverlay = lazy(loadDetailIndicatorOverlay);

import type {
  Timeframe,
  FocusPanel,
  Candle,
  FetchState,
  JobStatusPayload,
  ApiWarnings,
  BarsMeta,
  CompareListPayload,
  AnalysisHorizonKey,
  RankRiskMode,
  SellAnalysisFallback,
  PhaseFallback,
  EdinetFinancialPanel,
  TaisyakuSnapshot,
  TdnetDisclosureItem,
  TdnetDisclosureMeta,
  AnalysisFallback,
} from "./detail/detailTypes";
import {
  ANALYSIS_BACKFILL_ACTIVE_STATUSES,
  EMPTY_EXACT_DECISION_TONE_BY_DATE,
  EXACT_DECISION_TONE_CACHE_BY_SCOPE,
  isCanceledRequestError,
  DEFAULT_LIMITS,
  LIMIT_STEP,
  MAX_DAILY_BATCH_BARS_LIMIT,
  MAX_MONTHLY_BATCH_BARS_LIMIT,
  RANGE_PRESETS,
  ANALYSIS_DECISION_WINDOW_BARS,
  buildMonthBoundaries,
  buildYearBoundaries,
  buildPeriodTerminalDateMap,
  MIN_WEEKLY_RATIO,
  MIN_MONTHLY_RATIO,
  MAX_EVENT_OFFSET_SEC,
  formatNumber,
  formatSignedNumber,
  formatPercentLabel,
  formatFinancialAmountLabel,
  formatSignedPercentLabel,
  buildEdinetFinancialDisplay,
  buildTaisyakuDisplay,
  buildTdnetReactionSummary,
  buildTdnetHighlights,
  formatTdnetDisclosureStatusLabel,
  formatResearchPriorMetaLine,
  formatEdinetStatus,
  isNonEmptyString,
  joinMetaSegments,
  normalizeTickerName,
  toFiniteNumber,
  toBoolean,
  resolveSellShortScore,
  normalizeRiskMode,
  resolveRiskModeFromSession,
  normalizeEntryPolicy,
  normalizeResearchPrior,
  normalizeEdinetSummary,
  normalizeSwingPlan,
  normalizeSwingDiagnostics,
  normalizeAnalysisDecision,
  normalizeHorizonAnalysis,
  normalizeAdditiveSignals,
  normalizeBuyStagePrecision,
  formatLedgerDate,
  normalizeTime,
  clamp,
  incrementBarLimit,
  computeEnvironmentTone,
  normalizeEdinetFinancialPanel,
  normalizeTaisyakuSnapshot,
  normalizeTdnetDisclosureItem,
  normalizeTdnetDisclosureMeta,
  resolveAutoEdinetOfficialBackfillRequest,
  resolveAutoEdinetOfficialBackfillSubmitOutcome,
  shouldAutoRefreshTaisyaku,
  shouldAutoRefreshTdnet,
  buildRange,
  buildRangeEndingAt,
  buildRangeFromEndTime,
  hasSignificantRangeChange,
  formatDateLabel,
  resolveAnalysisBaseAsOfTime,
  resolveAutoAnalysisBackfillRequest,
  resolveLatestAnalysisAvailableAsOfTime,
  resolveLatestResolvedMetaDate,
  toDateKey,
  countInRange,
  filterCandlesByAsOf,
  filterVolumeByAsOf,
  findNearestCandleIndex,
  findNearestCandleTime,
} from "./detail/detailHelpers";
import { formatDateTimeLabel } from "../utils/dateLabels";

const DETAIL_DAILY_ROW_RATIO = 0.72;
const DETAIL_DEFAULT_WEEKLY_RATIO = 0.64;
const COMPARE_DETAIL_PREFETCH_TIMEFRAMES: Timeframe[] = ["daily", "monthly"];
const DETAIL_ALL_TIMEFRAMES: Timeframe[] = ["daily", "weekly", "monthly"];
const buildPriorityDetailTimeframeGroups = (focusPanel: Timeframe | null): Timeframe[][] => {
  const primary = focusPanel ?? "daily";
  return [[primary], DETAIL_ALL_TIMEFRAMES.filter((timeframe) => timeframe !== primary)];
};
const COMPARE_INITIAL_DAILY_LIMIT = 420;
const DETAIL_CHROME_DAILY = { timeframe: "daily" as const };
const DETAIL_CHROME_WEEKLY = { timeframe: "weekly" as const };
const DETAIL_CHROME_MONTHLY = { timeframe: "monthly" as const };
const ANNOTATION_API_RETRY_DELAYS_MS = [350, 900, 1600];

const sleep = (ms: number) => new Promise((resolve) => window.setTimeout(resolve, ms));

const isRetryableAnnotationApiError = (error: unknown) => {
  const response = (error as { response?: { status?: number } })?.response;
  return response?.status === 503;
};

const withAnnotationApiRetry = async <T,>(operation: () => Promise<T>): Promise<T> => {
  let lastError: unknown = null;
  for (let attempt = 0; attempt <= ANNOTATION_API_RETRY_DELAYS_MS.length; attempt += 1) {
    try {
      return await operation();
    } catch (error) {
      lastError = error;
      if (!isRetryableAnnotationApiError(error) || attempt >= ANNOTATION_API_RETRY_DELAYS_MS.length) {
        throw error;
      }
      await sleep(ANNOTATION_API_RETRY_DELAYS_MS[attempt]);
    }
  }
  throw lastError;
};

const toAnnotationDate = (time: number | null | undefined) => {
  if (time == null) return null;
  const raw = String(toDateKey(time));
  return raw.length === 8 ? `${raw.slice(0, 4)}-${raw.slice(4, 6)}-${raw.slice(6, 8)}` : raw;
};

const addUtcMonths = (time: number, months: number) => {
  const date = new Date(time * 1000);
  date.setUTCMonth(date.getUTCMonth() + months);
  return Math.floor(date.getTime() / 1000);
};

const buildCompareAroundAsOfRange = (candles: { time: number }[], asOfTime: number | null) => {
  if (!candles.length || !asOfTime) return null;
  const first = candles[0].time;
  const last = candles[candles.length - 1].time;
  const from = Math.max(first, addUtcMonths(asOfTime, -COMPARE_LOOKBACK_MONTHS));
  const to = Math.min(last, addUtcMonths(asOfTime, COMPARE_LOOKAHEAD_MONTHS));
  return {
    from,
    to: Math.max(from, to),
  };
};

type DetailFrameOverwriteObservability = {
  cacheSource: "memory" | "indexeddb" | null;
  dataVersion: string | null;
  chartSourceProvider: string | null;
  displayBasisClassification: "confirmed" | "provisional" | "mixed" | null;
  judgmentBasisClassification: "confirmed" | "provisional" | "dual" | null;
  overwriteStatus: "authoritative_confirmed" | "provisional_only" | "provisional_replaced_by_confirmed" | null;
  confirmedChartSourceProvider: string | null;
  provisionalChartSourceProvider: string | null;
  confirmedJudgmentBasis: string | null;
  provisionalJudgmentBasis: string | null;
  confirmedJudgmentAvailable: boolean | null;
  provisionalJudgmentAvailable: boolean | null;
  confirmedLastAvailableDate: number | null;
  provisionalLastAvailableDate: number | null;
};

type DetailOverwriteObservability = {
  timeframe: Timeframe;
  mainAsOf: string | null;
  daily: DetailFrameOverwriteObservability | null;
  weekly: DetailFrameOverwriteObservability | null;
  monthly: DetailFrameOverwriteObservability | null;
};

type DailyChartShape = {
  confirmed?: boolean | null;
  shape_label?: string | null;
  shape_family?: string | null;
  bias?: string | null;
  actionability?: string | null;
  description?: string | null;
  confidence?: number | null;
  window?: number | null;
  reasons?: string[] | null;
  metrics?: Record<string, number | string | null> | null;
  multi_window?: {
    event_shape?: DailyChartShape | null;
    context_shape?: DailyChartShape | null;
    trend_shape?: DailyChartShape | null;
    conflict_flags?: string[] | null;
  } | null;
};

const summarizeDetailFrameOverwriteObservability = (
  frame: ChartPrefetchEntry | null | undefined
): DetailFrameOverwriteObservability | null => {
  if (!frame) return null;
  const provenance = frame.provenance ?? null;
  return {
    cacheSource: frame.cacheSource ?? null,
    dataVersion: frame.dataVersion ?? null,
    chartSourceProvider: provenance?.chart_source_provider ?? null,
    displayBasisClassification: provenance?.display_basis_classification ?? null,
    judgmentBasisClassification: provenance?.judgment_basis_classification ?? null,
    overwriteStatus: provenance?.overwrite_status ?? null,
    confirmedChartSourceProvider: provenance?.confirmed_chart_source_provider ?? null,
    provisionalChartSourceProvider: provenance?.provisional_chart_source_provider ?? null,
    confirmedJudgmentBasis: provenance?.confirmed_judgment_basis ?? null,
    provisionalJudgmentBasis: provenance?.provisional_judgment_basis ?? null,
    confirmedJudgmentAvailable: provenance?.confirmed_judgment_available ?? null,
    provisionalJudgmentAvailable: provenance?.provisional_judgment_available ?? null,
    confirmedLastAvailableDate: provenance?.confirmed_last_available_date ?? null,
    provisionalLastAvailableDate: provenance?.provisional_last_available_date ?? null,
  };
};

const summarizeDetailOverwriteObservability = (
  frames: Partial<ChartPrefetchFrames> | null | undefined,
  timeframe: Timeframe,
  mainAsOf: string | null
): DetailOverwriteObservability | null => {
  if (!frames) return null;
  return {
    timeframe,
    mainAsOf,
    daily: summarizeDetailFrameOverwriteObservability(frames.daily),
    weekly: summarizeDetailFrameOverwriteObservability(frames.weekly),
    monthly: summarizeDetailFrameOverwriteObservability(frames.monthly),
  };
};

const summarizeDetailTickerForAiExplain = (
  ticker: Record<string, unknown> | null | undefined,
  fallback: { code: string; name: string }
) => {
  const source = ticker ?? {};
  const rankingScore = Number.isFinite(Number(source.displayScore))
    ? Number(source.displayScore)
    : Number.isFinite(Number(source.score))
      ? Number(source.score)
      : null;
  const rankingScoreSource =
    (typeof source.displayScoreSource === "string" && source.displayScoreSource) ||
    (typeof source.display_score_source === "string" && source.display_score_source) ||
    (rankingScore != null ? "none" : null);
  return {
    code: fallback.code,
    name: fallback.name,
    score: rankingScore,
    rankingScore,
    rankingScoreSource,
    stage: source.stage ?? source.buyState ?? source.statusLabel ?? null,
    buyState: source.buyState ?? null,
    buyStateScore: source.buyStateScore ?? null,
    buyEligible: source.buyEligible ?? null,
    buyPatternName: source.buyPatternName ?? null,
    buyStateReason: source.buyStateReason ?? null,
    entryPriorityScore: source.entryPriorityScore ?? null,
    entryPriorityLabel: source.entryPriorityLabel ?? null,
    swingScore: source.swingScore ?? null,
    swingQualified: source.swingQualified ?? null,
    swingReasons: source.swingReasons ?? null,
    chg1D: source.chg1D ?? null,
    chg1W: source.chg1W ?? null,
    chg1M: source.chg1M ?? null,
    chg1Q: source.chg1Q ?? null,
    chg1Y: source.chg1Y ?? null,
    boxState: source.boxState ?? null,
    eventEarningsDate: source.eventEarningsDate ?? null,
    eventRightsDate: source.eventRightsDate ?? null,
    reason: source.reason ?? null,
    reasons: source.reasons ?? null,
    phaseN: source.phaseN ?? null,
    phaseReasons: source.phaseReasons ?? null,
    shortPriorityLabel: source.shortPriorityLabel ?? null,
    shortPriorityReasons: source.shortPriorityReasons ?? null
  };
};

type TradesResponsePayload = {
  events?: TradeEvent[];
  warnings?: ApiWarnings;
  errors?: string[];
  currentPosition?: { longLots: number; shortLots: number };
  currentPositions?: CurrentPosition[];
  retryable?: boolean;
  message?: string;
};

type RouteReadyPhase = "chart" | "analysis";
const tradesCache = new Map<
  string,
  {
    events: TradeEvent[];
    warnings: ApiWarnings;
    errors: string[];
    currentPositions: CurrentPosition[] | null;
    fetchedAt: number;
  }
>();
const COMPARE_LOOKBACK_MONTHS = 10;
const COMPARE_LOOKAHEAD_MONTHS = 2;
const COMPARE_FORWARD_DAILY_BARS = 60;
const COMPARE_FORWARD_MONTHLY_BARS = 2;
const RANGE_SETTLE_MS = 2_000;
const DETAIL_CACHE_REFRESH_DELAY_MS = 4_000;

const getRetryDelayMs = (error: unknown) => {
  const retryAfterHeader = (error as { response?: { headers?: Record<string, unknown> } })?.response?.headers?.[
    "retry-after"
  ];
  const retryAfter =
    typeof retryAfterHeader === "string"
      ? Number.parseInt(retryAfterHeader, 10)
      : typeof retryAfterHeader === "number"
        ? retryAfterHeader
        : null;
  if (retryAfter != null && Number.isFinite(retryAfter) && retryAfter > 0) {
    return retryAfter * 1000;
  }
  return 1000;
};

const describeDetailBarsFetchError = (error: unknown) => {
  const response = (error as {
    response?: {
      status?: number;
      data?: { detail?: unknown; message?: unknown; error?: unknown };
    };
    code?: string;
    message?: string;
  })?.response;
  const detail = response?.data?.detail;
  const detailPayload = detail && typeof detail === "object" ? (detail as { error?: unknown; message?: unknown }) : null;
  const errorCode = String(detailPayload?.error ?? response?.data?.error ?? "").trim();
  const detailMessage = String(detailPayload?.message ?? response?.data?.message ?? "").trim();
  const rawMessage = String((error as { message?: string })?.message ?? "").trim();
  const normalized = `${errorCode} ${detailMessage} ${rawMessage}`.toLowerCase();
  if (
    errorCode === "chart_db_busy" ||
    response?.status === 503 ||
    normalized.includes("database is temporarily busy") ||
    normalized.includes("db_busy")
  ) {
    return "チャートDBが一時的に混み合っています。少し待ってから再読込してください。";
  }
  if ((error as { code?: string })?.code === "ECONNABORTED" || normalized.includes("timeout")) {
    return "チャートデータの取得に時間がかかっています。少し待ってから再読込してください。";
  }
  return rawMessage || "Bars fetch failed";
};

const SECONDARY_FETCH_STABLE_DELAY_MS = 450;
const QUALIFICATION_TRACE_DELAY_MS = 800;
const DETAIL_MARKERS_DELAY_MS = 4000;
const SECONDARY_JOB_READY_DELAY_MS = 2500;
const FINANCIAL_BACKGROUND_JOB_DELAY_MS = 4000;
const ANALYSIS_NEIGHBOR_PREFETCH_DELAY_MS = 1200;
const ANALYSIS_JOB_POLL_DELAY_MS = 1200;
const TRADES_FETCH_DELAY_MS = 1200;

const isRetryableTradesError = (error: unknown) => {
  const status = (error as { response?: { status?: number } })?.response?.status;
  const payload = (error as { response?: { data?: { retryable?: boolean } } })?.response?.data;
  return status === 503 && payload?.retryable === true;
};

export default function DetailView() {
  const { code } = useParams();
  const location = useLocation();
  const navigate = useNavigate();
  const { ready: backendReady } = useBackendReadyState();
  const overwriteLiveValidationMode = useMemo(() => {
    const params = new URLSearchParams(location.search);
    return shouldShowOperatorConsole() && params.get("overwriteLiveValidation") === "1";
  }, [location.search]);
  const dailyChartRef = useRef<DetailChartHandle | null>(null);
  const weeklyChartRef = useRef<DetailChartHandle | null>(null);
  const monthlyChartRef = useRef<DetailChartHandle | null>(null);
  const compareDailyChartRef = useRef<DetailChartHandle | null>(null);
  const compareMonthlyChartRef = useRef<DetailChartHandle | null>(null);
  const bottomRowRef = useRef<HTMLDivElement | null>(null);
  const financialPanelRef = useRef<HTMLDivElement | null>(null);
  const draggingRef = useRef(false);
  const manualDailyRangeRef = useRef<{ from: number; to: number } | null>(null);
  const manualWeeklyRangeRef = useRef<{ from: number; to: number } | null>(null);
  const manualMonthlyRangeRef = useRef<{ from: number; to: number } | null>(null);
  const manualCompareDailyRangeRef = useRef<{ from: number; to: number } | null>(null);
  const manualCompareMonthlyRangeRef = useRef<{ from: number; to: number } | null>(null);
  const analysisBaseAsOfRef = useRef<number | null>(null);
  // Guard: suppress programmatic visible-range events from resetting rangeMonths
  const rangeSettleRef = useRef(0);

  const tickers = useStore((state) => state.tickers);
  const ensureListLoaded = useStore((state) => state.ensureListLoaded);
  const loadingList = useStore((state) => state.loadingList);
  const favoritesLoaded = useStore((state) => state.favoritesLoaded);
  const loadFavorites = useStore((state) => state.loadFavorites);
  const setFavoriteLocal = useStore((state) => state.setFavoriteLocal);
  const showBoxes = useStore((state) => state.settings.showBoxes);
  const setShowBoxes = useStore((state) => state.setShowBoxes);
  const maSettings = useStore((state) => state.maSettings);
  const compareMaSettings = useStore((state) => state.compareMaSettings);
  const updateMaSetting = useStore((state) => state.updateMaSetting);
  const updateCompareMaSetting = useStore((state) => state.updateCompareMaSetting);
  const resetMaSettings = useStore((state) => state.resetMaSettings);
  const resetCompareMaSettings = useStore((state) => state.resetCompareMaSettings);

  const [dailyLimit, setDailyLimit] = useState(DEFAULT_LIMITS.daily);
  const [monthlyLimit, setMonthlyLimit] = useState(DEFAULT_LIMITS.monthly);
  const [dailyData, setDailyData] = useState<number[][]>([]);
  const [weeklyData, setWeeklyData] = useState<number[][]>([]);
  const [monthlyData, setMonthlyData] = useState<number[][]>([]);
  const [boxes, setBoxes] = useState<Box[]>([]);
  const [compareBoxes, setCompareBoxes] = useState<Box[]>([]);
  const [headerMode, setHeaderMode] = useState<"chart" | "positions" | "analysis" | "financial">("chart");
  const [tradexAnalysisDetailsOpen, setTradexAnalysisDetailsOpen] = useState(false);
  const prioritizeTradesFetch = headerMode === "positions";
  const [displayOpen, setDisplayOpen] = useState(false);
  const [showGapBands, setShowGapBands] = useState(true);
  const [showVolumeEnabled, setShowVolumeEnabled] = useState(true);
  const showDecisionMarkers = true;
  const [showTdnetMarkers, setShowTdnetMarkers] = useState(true);
  const [routeReadyPhase, setRouteReadyPhase] = useState<RouteReadyPhase>("chart");
  const [showTradeMarkers, setShowTradeMarkers] = useState(true);
  const [showRankingMarkers, setShowRankingMarkers] = useState(true);
  const [activeDrawTool, setActiveDrawTool] = useState<DrawTool | null>(null);
  const [drawingSelectionMode, setDrawingSelectionMode] = useState(false);
  const [continuousDraw, setContinuousDraw] = useState(true);
  const [selectedDrawing, setSelectedDrawing] = useState<SelectedDrawingInfo | null>(null);
  const COLOR_PALETTE = ["#ef4444", "#22c55e", "#0ea5e9", "#f59e0b", "#64748b"];
  const [activeDrawColorIndex, setActiveDrawColorIndex] = useState(4);
  const activeDrawColor = COLOR_PALETTE[activeDrawColorIndex] ?? "#64748b";
  const [activeLineOpacity, setActiveLineOpacity] = useState(0.8);
  const [activeLineWidth, setActiveLineWidth] = useState(2);
  const selectDrawTool = (tool: DrawTool | null) => {
    setActiveDrawTool(tool);
    setDrawingSelectionMode(tool === null);
  };
  const handleDrawCommit = () => {
    if (annotationMode || continuousDraw) return;
    setActiveDrawTool(null);
    setDrawingSelectionMode(true);
  };
  const [trades, setTrades] = useState<TradeEvent[]>([]);
  const [compareTrades, setCompareTrades] = useState<TradeEvent[]>([]);
  const [tradeWarnings, setTradeWarnings] = useState<ApiWarnings>({ items: [] });
  const [tradeErrors, setTradeErrors] = useState<string[]>([]);
  const [currentPositionsFromApi, setCurrentPositionsFromApi] = useState<CurrentPosition[] | null>(null);
  const [dailyErrors, setDailyErrors] = useState<string[]>([]);
  const [weeklyErrors, setWeeklyErrors] = useState<string[]>([]);
  const [monthlyErrors, setMonthlyErrors] = useState<string[]>([]);
  const [dailyBarsMeta, setDailyBarsMeta] = useState<BarsMeta | null>(null);
  const [monthlyBarsMeta, setMonthlyBarsMeta] = useState<BarsMeta | null>(null);
  const [mainChartOverwriteObservability, setMainChartOverwriteObservability] =
    useState<DetailOverwriteObservability | null>(null);
  const [detailDataFreshnessContract, setDetailDataFreshnessContract] =
    useState<MeeMeeDataFreshnessContract | null>(null);
  const [dailyFetch, setDailyFetch] = useState<FetchState>({
    status: "idle",
    responseCount: 0,
    errorMessage: null
  });
  const [weeklyFetch, setWeeklyFetch] = useState<FetchState>({
    status: "idle",
    responseCount: 0,
    errorMessage: null
  });
  const [monthlyFetch, setMonthlyFetch] = useState<FetchState>({
    status: "idle",
    responseCount: 0,
    errorMessage: null
  });
  const [loadingDaily, setLoadingDaily] = useState(false);
  const [loadingMonthly, setLoadingMonthly] = useState(false);
  const [mainChartPendingSwap, setMainChartPendingSwap] = useState(false);
  const [hasMoreDaily, setHasMoreDaily] = useState(true);
  const [hasMoreMonthly, setHasMoreMonthly] = useState(true);
  const [showIndicators, setShowIndicators] = useState(false);
  const [maEditMode, setMaEditMode] = useState<"main" | "compare">("main");
  const [weeklyRatio, setWeeklyRatio] = useState(DETAIL_DEFAULT_WEEKLY_RATIO);
  const [rangeMonths, setRangeMonths] = useState<number | null>(12);
  const [showTradesOverlay] = useState(true);
  const [showPnLPanel] = useState(true);
  const [syncRanges, setSyncRanges] = useState(true);
  const [focusPanel, setFocusPanel] = useState<FocusPanel>(null);
  const [toastMessage, setToastMessage] = useState<string | null>(null);
  const [toastAction, setToastAction] = useState<{ label: string; onClick: () => void } | null>(null);
  const [screenshotBusy, setScreenshotBusy] = useState(false);
  const [deleteBusy, setDeleteBusy] = useState(false);
  const [showPositionLedger, setShowPositionLedger] = useState(false);
  const [financialPanel, setFinancialPanel] = useState<EdinetFinancialPanel | null>(null);
  const [financialLoading, setFinancialLoading] = useState(false);
  const [financialFetchedOnce, setFinancialFetchedOnce] = useState(false);
  const [financialRefreshToken, setFinancialRefreshToken] = useState(0);
  const [taisyakuSnapshot, setTaisyakuSnapshot] = useState<TaisyakuSnapshot | null>(null);
  const [taisyakuLoading, setTaisyakuLoading] = useState(false);
  const [taisyakuFetchedOnce, setTaisyakuFetchedOnce] = useState(false);
  const [tdnetDisclosures, setTdnetDisclosures] = useState<TdnetDisclosureItem[]>([]);
  const [tdnetMeta, setTdnetMeta] = useState<TdnetDisclosureMeta | null>(null);
  const [tdnetLoading, setTdnetLoading] = useState(false);
  const [tdnetFetchedOnce, setTdnetFetchedOnce] = useState(false);
  const [selectedTdnetDisclosures, setSelectedTdnetDisclosures] = useState<TdnetDisclosureItem[]>([]);
  const [selectedTdnetDisclosureIndex, setSelectedTdnetDisclosureIndex] = useState(0);
  const [taisyakuRefreshToken, setTaisyakuRefreshToken] = useState(0);
  const [tdnetRefreshToken, setTdnetRefreshToken] = useState(0);
  const [secondaryFetchReady, setSecondaryFetchReady] = useState(false);
  const [secondaryFetchStableReady, setSecondaryFetchStableReady] = useState(false);
  const [secondaryJobReady, setSecondaryJobReady] = useState(false);
  const [financialBackgroundJobReady, setFinancialBackgroundJobReady] = useState(false);
  const [analysisNeighborPrefetchReady, setAnalysisNeighborPrefetchReady] = useState(false);
  const [positionLedgerExpanded, setPositionLedgerExpanded] = useState(false);
  const [tickerCodeInput, setTickerCodeInput] = useState(code ?? "");
  const [ledgerViewMode, setLedgerViewMode] = useState<"iizuka" | "stock">(() => {
    try {
      const stored = window.localStorage.getItem("positionLedgerMode");
      return stored === "stock" ? "stock" : "iizuka";
    } catch {
      return "iizuka";
    }
  });

  // Cursor mode state
  const [cursorMode, setCursorMode] = useState(false);
  const [selectedBarIndex, setSelectedBarIndex] = useState<number | null>(null);
  const [selectedDate, setSelectedDate] = useState<string | null>(null);
  const [selectedBarData, setSelectedBarData] = useState<Candle | null>(null);
  const replayRunId = useMemo(() => {
    const params = new URLSearchParams(location.search);
    const raw = params.get("replayRunId");
    if (!raw) return null;
    const trimmed = raw.trim();
    return trimmed || null;
  }, [location.search]);
  const showReplayPanel = replayRunId != null;
  const replayRun = useDetailReplayRun({
    runId: replayRunId,
    symbol: code ?? null,
    selectedDate,
  });
  const [analysisCursorTime, setAnalysisCursorTime] = useState<number | null>(null);
  const [analysisBackfillJob, setAnalysisBackfillJob] = useState<JobStatusPayload | null>(null);
  const [analysisFetchRefreshToken, setAnalysisFetchRefreshToken] = useState(0);
  const [analysisRecalcSubmitting, setAnalysisRecalcSubmitting] = useState<"current" | "auto" | "batch" | null>(null);
  const [legacyAnalysisDisabled, setLegacyAnalysisDisabled] = useState(false);
  const [legacyAnalysisDisabledReason, setLegacyAnalysisDisabledReason] = useState<string | null>(null);
  const [exactDecisionToneCacheByScope, setExactDecisionToneCacheByScope] = useState<
    Map<string, Map<number, ExactDecisionTone>>
  >(() => new Map(EXACT_DECISION_TONE_CACHE_BY_SCOPE));
  const analysisBackfillActiveRef = useRef(false);
  const taisyakuAutoImportRequestedRef = useRef(new Set<string>());
  const tdnetAutoImportRequestedRef = useRef(new Set<string>());
  const analysisAutoBackfillRequestKeyRef = useRef<string | null>(null);
  const edinetAutoBackfillRequestedRef = useRef(new Set<string>());
  const edinetAutoBackfillRefreshedRef = useRef(new Set<string>());
  const edinetAutoBackfillJobIdsRef = useRef(new Map<string, string>());
  const edinetAutoBackfillRefreshOnlyRef = useRef(new Map<string, number>());
  const prevShowAnalysisPanelRef = useRef(false);

  const syncRangesRef = useRef(syncRanges);
  const [showSimilar, setShowSimilar] = useState(false);
  const [similarSearchMounted, setSimilarSearchMounted] = useState(false);
  const [aiExplainDockMounted, setAiExplainDockMounted] = useState(false);
  useEffect(() => {
    if (showSimilar) {
      setSimilarSearchMounted(true);
    }
  }, [showSimilar]);
  useEffect(() => {
    if (aiExplainDockMounted) return;
    if (loadingDaily || loadingMonthly || mainChartPendingSwap) return;
    const timer = window.setTimeout(() => {
      setAiExplainDockMounted(true);
    }, 0);
    return () => window.clearTimeout(timer);
  }, [aiExplainDockMounted, loadingDaily, loadingMonthly, mainChartPendingSwap]);
  const resetMainChartState = useCallback((seed?: Partial<ChartPrefetchFrames>) => {
    const nextDailyLimit = DEFAULT_LIMITS.daily;
    const nextMonthlyLimit = DEFAULT_LIMITS.monthly;
    const dailySeed = seed?.daily ?? null;
    const weeklySeed = seed?.weekly ?? null;
    const monthlySeed = seed?.monthly ?? null;
    const dailyRows = Array.isArray(dailySeed?.rows) ? dailySeed.rows : [];
    const weeklyRows = Array.isArray(weeklySeed?.rows) ? weeklySeed.rows : [];
    const monthlyRows = Array.isArray(monthlySeed?.rows) ? monthlySeed.rows : [];
    const monthlyBoxes = Array.isArray(monthlySeed?.boxes) ? monthlySeed.boxes : [];
    setDailyLimit(nextDailyLimit);
    setMonthlyLimit(nextMonthlyLimit);
    setDailyData(dailyRows);
    setWeeklyData(weeklyRows);
    setMonthlyData(monthlyRows);
    setBoxes(monthlyBoxes);
    setDailyErrors([]);
    setWeeklyErrors([]);
    setMonthlyErrors([]);
    setDailyBarsMeta(null);
    setMonthlyBarsMeta(null);
    setDailyFetch({
      status: dailySeed ? "success" : "idle",
      responseCount: dailyRows.length,
      errorMessage: null,
    });
    setWeeklyFetch({
      status: weeklySeed ? "success" : "idle",
      responseCount: weeklyRows.length,
      errorMessage: null,
    });
    setMonthlyFetch({
      status: monthlySeed ? "success" : "idle",
      responseCount: monthlyRows.length,
      errorMessage: null,
    });
    setLoadingDaily(false);
    setLoadingMonthly(false);
    setHasMoreDaily(dailyRows.length >= nextDailyLimit);
    setHasMoreMonthly(monthlyRows.length >= nextMonthlyLimit);
    setMainChartOverwriteObservability(
      summarizeDetailOverwriteObservability(seed ?? null, "daily", null)
    );
  }, []);
  const resetCompareChartState = useCallback((seed?: Partial<ChartPrefetchFrames>) => {
    const dailySeed = seed?.daily ?? null;
    const monthlySeed = seed?.monthly ?? null;
    const dailyRows = Array.isArray(dailySeed?.rows) ? dailySeed.rows : [];
    const monthlyRows = Array.isArray(monthlySeed?.rows) ? monthlySeed.rows : [];
    const monthlyBoxes = Array.isArray(monthlySeed?.boxes) ? monthlySeed.boxes : [];
    setCompareDailyLimit(COMPARE_INITIAL_DAILY_LIMIT);
    setCompareMonthlyData(monthlyRows);
    setCompareMonthlyErrors([]);
    setCompareLoading(false);
    setCompareDailyData(dailyRows);
    setCompareDailyErrors([]);
    setCompareDailyLoading(false);
    setCompareBoxes(monthlyBoxes);
    setCompareTrades([]);
  }, []);
  const compareCode = useMemo(() => {
    const params = new URLSearchParams(location.search);
    const raw = params.get("compare");
    if (!raw) return null;
    const trimmed = raw.trim();
    if (!trimmed || trimmed === code) return null;
    return trimmed;
  }, [location.search, code]);
  const analysisAvailable = !compareCode && !showReplayPanel;
  const analysisFetchEnabled = analysisAvailable && headerMode === "analysis";
  const analysisNetworkReady = analysisFetchEnabled && routeReadyPhase === "analysis";
  const compareAsOf = useMemo(() => {
    const params = new URLSearchParams(location.search);
    const raw = params.get("compareAsOf");
    return raw ? raw.trim() : null;
  }, [location.search]);
  const compareIndex = useMemo(() => {
    const params = new URLSearchParams(location.search);
    const raw = params.get("compareIndex");
    if (!raw) return null;
    const parsed = Number.parseInt(raw, 10);
    return Number.isFinite(parsed) && parsed >= 0 ? parsed : null;
  }, [location.search]);
  const mainAsOf = useMemo(() => {
    const params = new URLSearchParams(location.search);
    const raw = params.get("mainAsOf");
    return raw ? raw.trim() : null;
  }, [location.search]);
  const compareAsOfTime = useMemo(() => {
    if (!compareAsOf) return null;
    return normalizeTime(compareAsOf);
  }, [compareAsOf]);
  const mainAsOfTime = useMemo(() => {
    if (!mainAsOf) return null;
    return normalizeTime(mainAsOf);
  }, [mainAsOf]);
  const isFavorite = useStore((state) => (code ? state.favorites.includes(code) : false));
  const [compareMonthlyData, setCompareMonthlyData] = useState<number[][]>([]);
  const [compareMonthlyErrors, setCompareMonthlyErrors] = useState<string[]>([]);
  const [compareLoading, setCompareLoading] = useState(false);
  const [compareDailyData, setCompareDailyData] = useState<number[][]>([]);
  const [compareDailyErrors, setCompareDailyErrors] = useState<string[]>([]);
  const [compareDailyLoading, setCompareDailyLoading] = useState(false);
  const [compareChartPendingSwap, setCompareChartPendingSwap] = useState(false);
  const [compareDailyLimit, setCompareDailyLimit] = useState(COMPARE_INITIAL_DAILY_LIMIT);
  const [analysisHorizon] = useState<AnalysisHorizonKey>(20);
  const [analysisRiskMode, setAnalysisRiskMode] = useState<RankRiskMode>(() => resolveRiskModeFromSession());
  const [analysisAsOfTime, setAnalysisAsOfTime] = useState<number | null>(null);
  const [annotationMode, setAnnotationMode] = useState(false);
  const [chartNoteExpanded, setChartNoteExpanded] = useState(false);
  const [annotationTool, setAnnotationTool] = useState<AnnotationTool>("select");
  const [annotationFilter, setAnnotationFilter] = useState<AnnotationFilter>("all");
  const [annotations, setAnnotations] = useState<ChartAnnotation[]>([]);
  const [selectedAnnotationId, setSelectedAnnotationId] = useState<string | null>(null);
  const [annotationsLoading, setAnnotationsLoading] = useState(false);
  const [readingTimeframe, setReadingTimeframe] = useState<ReadingTimeframe>("daily");
  const [readingTargetType, setReadingTargetType] = useState<ReadingTargetType>("bar");
  const [readingCommentType, setReadingCommentType] = useState<ReadingCommentType>("review");
  const [readingNoteText, setReadingNoteText] = useState("");
  const [readingTagsText, setReadingTagsText] = useState("");
  const [readingSaving, setReadingSaving] = useState(false);
  const [maRoleReview, setMaRoleReview] = useState<any>(null);
  const [chartNoteTitle, setChartNoteTitle] = useState("");
  const [chartNoteTimeframe, setChartNoteTimeframe] = useState<ChartNoteTimeframe>("mixed");
  const [chartNoteParagraphs, setChartNoteParagraphs] = useState<ChartNoteParagraph[]>([]);
  const [chartNoteSaving, setChartNoteSaving] = useState(false);
  const [pendingCalloutAnchor, setPendingCalloutAnchor] = useState<{
    time: number;
    date: string | null;
    price: number;
    anchorType: "bar" | "indicator" | "region" | "line";
    anchorTarget: string;
    anchorObjectId?: string | null;
    resolution?: "payload_fallback" | "annotation_id";
    linkedObject?: Record<string, any> | null;
  } | null>(null);
  const displayRef = useRef<HTMLDivElement | null>(null);
  const {
    dailyDrawingKey,
    weeklyDrawingKey,
    monthlyDrawingKey,
    compareDailyDrawingKey,
    compareMonthlyDrawingKey,
    dailyDrawings,
    weeklyDrawings,
    monthlyDrawings,
    compareDailyDrawings,
    compareMonthlyDrawings,
    addTimeZone,
    updateTimeZone,
    addPriceBand,
    updatePriceBand,
    addDrawBox,
    updateDrawBox,
    addHorizontalLine,
    updateHorizontalLine,
    deleteTimeZone,
    deletePriceBand,
    deleteDrawBox,
    deleteHorizontalLine,
    resetAllDrawings,
  } = useDetailDrawings({
    code,
    compareCode,
    onResetSelection: () => setSelectedDrawing(null),
  });
  useEffect(() => {
    if (!displayOpen) return;
    const handleClick = (event: MouseEvent) => {
      const target = event.target as HTMLElement;
      if (displayRef.current && displayRef.current.contains(target)) return;
      setDisplayOpen(false);
    };
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [displayOpen]);

  useEffect(() => {
    if (headerMode === "positions") {
      setShowPositionLedger(true);
      return;
    }
    setShowPositionLedger(false);
    setPositionLedgerExpanded(false);
  }, [headerMode]);

  useEffect(() => {
    const syncRiskMode = () => {
      setAnalysisRiskMode(resolveRiskModeFromSession());
    };
    syncRiskMode();
    window.addEventListener("focus", syncRiskMode);
    return () => window.removeEventListener("focus", syncRiskMode);
  }, []);

  useLayoutEffect(() => {
    const seed =
      compareCode == null
        ? undefined
        : readDetailChartPrefetchSync({
            code: compareCode,
            dailyLimit: COMPARE_INITIAL_DAILY_LIMIT,
            weeklyLimit: DEFAULT_LIMITS.weekly,
            monthlyLimit: DEFAULT_LIMITS.monthly,
            asof: compareAsOf,
            forwardBars: compareAsOf
              ? { daily: COMPARE_FORWARD_DAILY_BARS, monthly: COMPARE_FORWARD_MONTHLY_BARS }
              : undefined,
          }, COMPARE_DETAIL_PREFETCH_TIMEFRAMES);
    setCompareChartPendingSwap(!hasCompleteDetailChartPrefetch(seed, COMPARE_DETAIL_PREFETCH_TIMEFRAMES));
    resetCompareChartState(seed);
  }, [compareAsOf, compareCode, resetCompareChartState]);

  useEffect(() => {
    if (!compareCode) return;
    manualCompareDailyRangeRef.current = null;
    manualCompareMonthlyRangeRef.current = null;
  }, [compareCode, compareAsOf]);

  useLayoutEffect(() => {
    setAnalysisAsOfTime(null);
    analysisBaseAsOfRef.current = null;
    setRouteReadyPhase("chart");
    setSecondaryFetchReady(false);
    setSecondaryFetchStableReady(false);
    setSecondaryJobReady(false);
    setDetailDataFreshnessContract(null);
    const seed =
      code == null
        ? undefined
        : readDetailChartPrefetchSync({
            code,
            dailyLimit: DEFAULT_LIMITS.daily,
            weeklyLimit: DEFAULT_LIMITS.weekly,
            monthlyLimit: DEFAULT_LIMITS.monthly,
            asof: mainAsOf,
          });
    setMainChartPendingSwap(!hasCompleteDetailChartPrefetch(seed));
    resetMainChartState(seed);
    // Reset cursor selection – will be re-initialized once dailyCandles load
    setSelectedBarIndex(null);
    setSelectedBarData(null);
    setAnalysisCursorTime(null);
    setTrades([]);
    setTradeWarnings({ items: [] });
    setTradeErrors([]);
    setCurrentPositionsFromApi(null);
    setTdnetDisclosures([]);
    setTdnetMeta(null);
    setTdnetLoading(false);
    setTdnetFetchedOnce(false);
    // Keep selectedDate so we can restore cursor position in new candle data
  }, [code, mainAsOf, resetMainChartState]);

  useEffect(() => {
    if (cursorMode) return;
    setAnalysisCursorTime(null);
  }, [cursorMode]);

  useEffect(() => {
    setRangeMonths(12);
    manualDailyRangeRef.current = null;
    manualWeeklyRangeRef.current = null;
    manualMonthlyRangeRef.current = null;
    manualCompareDailyRangeRef.current = null;
    manualCompareMonthlyRangeRef.current = null;
    // Suppress programmatic range events after code change
    rangeSettleRef.current = Date.now() + RANGE_SETTLE_MS;
  }, [code]);

  const tickerByCode = useMemo(() => {
    return new Map(tickers.map((item) => [item.code, item]));
  }, [tickers]);
  const tickerName = useMemo(() => {
    if (!code) return "";
    return normalizeTickerName(tickerByCode.get(code)?.name);
  }, [tickerByCode, code]);
  const activeTicker = useMemo(() => (code ? tickerByCode.get(code) ?? null : null), [tickerByCode, code]);
  const [qualificationTrace, setQualificationTrace] = useState<any>(null);
  const [persistedSignalEvents, setPersistedSignalEvents] = useState<any[]>([]);
  const [persistedRankingAppearances, setPersistedRankingAppearances] = useState<any[]>([]);
  const [chartRankingAppearances, setChartRankingAppearances] = useState<any[]>([]);
  const [persistedMarkersLoading, setPersistedMarkersLoading] = useState(false);
  const [secondaryChartsReady, setSecondaryChartsReady] = useState(false);
  const earningsLabel = useMemo(
    () => formatEventBadgeDate(activeTicker?.eventEarningsDate),
    [activeTicker?.eventEarningsDate]
  );
  const rightsLabel = useMemo(
    () => formatEventBadgeDate(activeTicker?.eventRightsDate),
    [activeTicker?.eventRightsDate]
  );
  const compareTickerName = useMemo(() => {
    if (!compareCode) return "";
    return normalizeTickerName(tickerByCode.get(compareCode)?.name);
  }, [tickerByCode, compareCode]);
  const sharedDailyParse = useMemo(() => buildCandlesWithStats(dailyData), [dailyData]);
  const latestSharedDailyAsOfTime = useMemo(() => {
    return sharedDailyParse.candles.reduce<number | null>((maxValue, candle) => {
      if (!candle || typeof candle.time !== "number") return maxValue;
      if (maxValue == null || candle.time > maxValue) return candle.time;
      return maxValue;
    }, null);
  }, [sharedDailyParse.candles]);
  const chartAsOfTime = useMemo(() => {
    if (latestSharedDailyAsOfTime == null) return mainAsOfTime;
    if (mainAsOfTime == null) return latestSharedDailyAsOfTime;
    return Math.min(mainAsOfTime, latestSharedDailyAsOfTime);
  }, [latestSharedDailyAsOfTime, mainAsOfTime]);
  const analysisPrefetchCandles = useMemo(
    () => filterCandlesByAsOf(sharedDailyParse.candles, chartAsOfTime),
    [chartAsOfTime, sharedDailyParse.candles]
  );
  const analysisPrefetchAsofs = useMemo(() => {
    if (!cursorMode || selectedBarIndex == null) return [];
    if (!analysisPrefetchCandles.length) return [];
    const offsets = [-2, -1, 1, 2];
    return offsets
      .map((offset) => analysisPrefetchCandles[selectedBarIndex + offset]?.time ?? null)
      .filter((value): value is number => value != null);
  }, [cursorMode, selectedBarIndex, analysisPrefetchCandles]);
  const analysisDeferredPrefetchAsofs = useMemo(
    () => (analysisNeighborPrefetchReady ? analysisPrefetchAsofs : []),
    [analysisNeighborPrefetchReady, analysisPrefetchAsofs]
  );

  useEffect(() => {
    setSecondaryChartsReady(false);
    if (!code || mainChartPendingSwap || analysisPrefetchCandles.length === 0) {
      return;
    }
    const timerId = window.setTimeout(() => {
      setSecondaryChartsReady(true);
    }, 220);
    return () => {
      window.clearTimeout(timerId);
    };
  }, [analysisPrefetchCandles.length, code, mainChartPendingSwap]);

  useEffect(() => {
    setAnalysisNeighborPrefetchReady(false);
    if (!analysisNetworkReady || !analysisFetchEnabled) return;
    const timerId = window.setTimeout(() => {
      setAnalysisNeighborPrefetchReady(true);
    }, ANALYSIS_NEIGHBOR_PREFETCH_DELAY_MS);
    return () => {
      window.clearTimeout(timerId);
    };
  }, [analysisAsOfTime, analysisFetchEnabled, analysisNetworkReady, code]);

  useEffect(() => {
    if (!backendReady || !code || !secondaryFetchStableReady || mainChartPendingSwap || !analysisFetchEnabled) {
      setQualificationTrace(null);
      return;
    }
    let cancelled = false;
    const timerId = window.setTimeout(() => {
      void (async () => {
        try {
          const res = await api.get("/rankings/trace/code-qualified", {
            params: {
              code,
              tf: "D",
              which: "latest",
              risk_mode: analysisRiskMode,
              lookback_days: 120,
              recent_hits: 5,
            },
            timeout: 60000,
          });
          if (!cancelled) setQualificationTrace(res.data ?? null);
        } catch {
          if (!cancelled) setQualificationTrace(null);
        }
      })();
    }, QUALIFICATION_TRACE_DELAY_MS);
    return () => {
      cancelled = true;
      window.clearTimeout(timerId);
    };
  }, [analysisFetchEnabled, backendReady, code, analysisRiskMode, mainChartPendingSwap, secondaryFetchStableReady]);

  useEffect(() => {
    setAnalysisBackfillJob(null);
    setAnalysisFetchRefreshToken(0);
    analysisBackfillActiveRef.current = false;
    analysisAutoBackfillRequestKeyRef.current = null;
    edinetAutoBackfillRequestedRef.current.clear();
    edinetAutoBackfillRefreshedRef.current.clear();
    edinetAutoBackfillJobIdsRef.current.clear();
    edinetAutoBackfillRefreshOnlyRef.current.clear();
    setFinancialFetchedOnce(false);
    setFinancialRefreshToken(0);
  }, [code]);

  const {
    item: phaseFallback,
    loading: phaseFallbackLoading,
  } = useAsOfItemFetch<PhaseFallback>({
    backendReady,
    code,
    asof: analysisAsOfTime,
    prefetchAsofs: analysisDeferredPrefetchAsofs,
    readyToFetch: analysisNetworkReady,
    endpoint: "/ticker/phase",
    timeoutMs: 10000,
    enabled:
      analysisFetchEnabled &&
      !(
        activeTicker?.bodyScore != null ||
        activeTicker?.earlyScore != null ||
        activeTicker?.lateScore != null ||
        typeof activeTicker?.phaseN === "number"
      ) || !(activeTicker?.phaseReasons?.length),
    parseItem: (item) => {
      if (!item || typeof item !== "object") return null;
      const source = item as Record<string, unknown>;
      const reasonsRaw = source.reasonsTop3;
      const reasons = Array.isArray(reasonsRaw)
        ? (reasonsRaw as string[])
        : typeof reasonsRaw === "string"
          ? reasonsRaw
            .split(",")
            .map((part) => part.trim())
            .filter(Boolean)
          : [];
      return {
        dt: typeof source.dt === "number" ? source.dt : null,
        earlyScore: Number.isFinite(source.earlyScore) ? Number(source.earlyScore) : null,
        lateScore: Number.isFinite(source.lateScore) ? Number(source.lateScore) : null,
        bodyScore: Number.isFinite(source.bodyScore) ? Number(source.bodyScore) : null,
        n: typeof source.n === "number" ? source.n : null,
        reasons,
      };
    },
  });

  const {
    item: analysisFallback,
    loading: analysisLoading,
  } = useAsOfItemFetch<AnalysisFallback>({
    backendReady,
    code,
    asof: analysisAsOfTime,
    prefetchAsofs: analysisDeferredPrefetchAsofs,
    enabled: analysisFetchEnabled,
    readyToFetch: analysisNetworkReady,
    endpoint: "/ticker/analysis",
    timeoutMs: 30000,
    maxRetries: 4,
    retryDelayMs: 1200,
    retryOnNull: true,
    negativeCacheTtlMs: 8000,
    requestKeyExtra: `${analysisRiskMode}|refresh:${analysisFetchRefreshToken}`,
    buildParams: (symbol, asof) => ({ code: symbol, asof, risk_mode: analysisRiskMode }),
    parseItem: (item) => {
      if (!item || typeof item !== "object") return null;
      const source = item as Record<string, unknown>;
      return {
        dt: source.dt ?? null,
        pUp: toFiniteNumber(source.pUp),
        pDown: toFiniteNumber(source.pDown),
        pTurnUp: toFiniteNumber(source.pTurnUp),
        pTurnDown: toFiniteNumber(source.pTurnDown),
        pTurnDownHorizon: toFiniteNumber(source.pTurnDownHorizon),
        retPred20: toFiniteNumber(source.retPred20),
        ev20: toFiniteNumber(source.ev20),
        ev20Net: toFiniteNumber(source.ev20Net),
        horizonAnalysis: normalizeHorizonAnalysis(source.horizonAnalysis),
        additiveSignals: normalizeAdditiveSignals(source.additiveSignals),
        entryPolicy: normalizeEntryPolicy(source.entryPolicy),
        riskMode: source.riskMode == null ? null : normalizeRiskMode(source.riskMode),
        buyStagePrecision: normalizeBuyStagePrecision(source.buyStagePrecision),
        researchPrior: normalizeResearchPrior(source.researchPrior),
        edinetSummary: normalizeEdinetSummary(source.edinetSummary),
        modelVersion: typeof source.modelVersion === "string" ? source.modelVersion : null,
        decision: normalizeAnalysisDecision(source.decision),
        swingPlan: normalizeSwingPlan(source.swingPlan),
        swingDiagnostics: normalizeSwingDiagnostics(source.swingDiagnostics),
      };
    },
  });

  const {
    item: dailyChartShape,
    loading: dailyChartShapeLoading,
  } = useAsOfItemFetch<DailyChartShape>({
    backendReady,
    code,
    asof: analysisAsOfTime,
    prefetchAsofs: analysisDeferredPrefetchAsofs,
    enabled: analysisFetchEnabled,
    readyToFetch: analysisNetworkReady,
    endpoint: "/ticker/daily/shape",
    timeoutMs: 10000,
    requestKeyExtra: "windows:10,20,60",
    buildParams: (symbol, asof) => ({ code: symbol, asof, window: 10, windows: "10,20,60" }),
    parseItem: (item) => {
      if (!item || typeof item !== "object") return null;
      const source = item as Record<string, unknown>;
      const shapeSource =
        source.shape && typeof source.shape === "object"
          ? (source.shape as Record<string, unknown>)
          : source;
      const multiWindow =
        source.multi_window && typeof source.multi_window === "object"
          ? (source.multi_window as Record<string, unknown>)
          : null;
      return {
        confirmed: typeof shapeSource.confirmed === "boolean" ? shapeSource.confirmed : null,
        shape_label: typeof shapeSource.shape_label === "string" ? shapeSource.shape_label : null,
        shape_family: typeof shapeSource.shape_family === "string" ? shapeSource.shape_family : null,
        bias: typeof shapeSource.bias === "string" ? shapeSource.bias : null,
        actionability: typeof shapeSource.actionability === "string" ? shapeSource.actionability : null,
        description: typeof shapeSource.description === "string" ? shapeSource.description : null,
        confidence: toFiniteNumber(shapeSource.confidence),
        window: toFiniteNumber(shapeSource.window),
        reasons: Array.isArray(shapeSource.reasons)
          ? shapeSource.reasons.filter((value): value is string => typeof value === "string")
          : [],
        metrics:
          shapeSource.metrics && typeof shapeSource.metrics === "object"
            ? (shapeSource.metrics as Record<string, number | string | null>)
            : {},
        multi_window: multiWindow as DailyChartShape["multi_window"],
      };
    },
  });

  const {
    item: sellAnalysisFallback,
    loading: sellAnalysisLoading,
  } = useAsOfItemFetch<SellAnalysisFallback>({
    backendReady,
    code,
    asof: analysisAsOfTime,
    prefetchAsofs: analysisDeferredPrefetchAsofs,
    enabled: analysisFetchEnabled,
    readyToFetch: analysisNetworkReady,
    endpoint: "/ticker/analysis/sell",
    timeoutMs: 30000,
    maxRetries: 4,
    retryDelayMs: 1200,
    retryOnNull: true,
    negativeCacheTtlMs: 8000,
    requestKeyExtra: `refresh:${analysisFetchRefreshToken}`,
    parseItem: (item) => {
      if (!item || typeof item !== "object") return null;
      const source = item as Record<string, unknown>;
      return {
        dt: source.dt ?? null,
        close: toFiniteNumber(source.close),
        dayChangePct: toFiniteNumber(source.dayChangePct),
        pDown: toFiniteNumber(source.pDown),
        pTurnDown: toFiniteNumber(source.pTurnDown),
        ev20Net: toFiniteNumber(source.ev20Net),
        rankDown20: toFiniteNumber(source.rankDown20),
        predDt: source.predDt ?? null,
        pUp5: toFiniteNumber(source.pUp5),
        pUp10: toFiniteNumber(source.pUp10),
        pUp20: toFiniteNumber(source.pUp20),
        shortScore: toFiniteNumber(source.shortScore),
        aScore: toFiniteNumber(source.aScore),
        bScore: toFiniteNumber(source.bScore),
        ma20: toFiniteNumber(source.ma20),
        ma60: toFiniteNumber(source.ma60),
        ma20Slope: toFiniteNumber(source.ma20Slope),
        ma60Slope: toFiniteNumber(source.ma60Slope),
        distMa20Signed: toFiniteNumber(source.distMa20Signed),
        distMa60Signed: toFiniteNumber(source.distMa60Signed),
        trendDown: source.trendDown == null ? null : toBoolean(source.trendDown),
        trendDownStrict: source.trendDownStrict == null ? null : toBoolean(source.trendDownStrict),
        fwdClose5: toFiniteNumber(source.fwdClose5),
        fwdClose10: toFiniteNumber(source.fwdClose10),
        fwdClose20: toFiniteNumber(source.fwdClose20),
        shortRet5: toFiniteNumber(source.shortRet5),
        shortRet10: toFiniteNumber(source.shortRet10),
        shortRet20: toFiniteNumber(source.shortRet20),
        shortWin5: source.shortWin5 == null ? null : toBoolean(source.shortWin5),
        shortWin10: source.shortWin10 == null ? null : toBoolean(source.shortWin10),
        shortWin20: source.shortWin20 == null ? null : toBoolean(source.shortWin20),
      };
    },
  });

  useEffect(() => {
    if (!backendReady || !code || headerMode !== "financial") return;
    let cancelled = false;
    setFinancialLoading(true);
    setFinancialFetchedOnce(false);
    void api
      .get("/ticker/edinet/financials", { params: { code } })
      .then((response) => {
        if (cancelled) return;
        const item = response.data && typeof response.data === "object"
          ? (response.data as { item?: unknown }).item
          : null;
        setFinancialPanel(normalizeEdinetFinancialPanel(item));
      })
      .catch(() => {
        if (cancelled) return;
        setFinancialPanel(null);
      })
      .finally(() => {
        if (!cancelled) setFinancialLoading(false);
        if (!cancelled) setFinancialFetchedOnce(true);
      });
    return () => {
      cancelled = true;
    };
  }, [backendReady, code, headerMode, financialRefreshToken]);

  useEffect(() => {
    if (headerMode !== "financial" || compareCode) return;
    financialPanelRef.current?.scrollTo({ top: 0, behavior: "auto" });
  }, [code, compareCode, headerMode]);

  useEffect(() => {
    if (!backendReady || !code || headerMode !== "financial") return;
    let cancelled = false;
    setTaisyakuLoading(true);
    setTaisyakuFetchedOnce(false);
    void api
      .get("/ticker/taisyaku/snapshot", { params: { code, history_limit: 10 } })
      .then((response) => {
        if (cancelled) return;
        const item = response.data && typeof response.data === "object"
          ? (response.data as { item?: unknown }).item
          : null;
        setTaisyakuSnapshot(normalizeTaisyakuSnapshot(item));
      })
      .catch(() => {
        if (!cancelled) setTaisyakuSnapshot(null);
      })
      .finally(() => {
        if (!cancelled) {
          setTaisyakuLoading(false);
          setTaisyakuFetchedOnce(true);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [backendReady, code, headerMode, taisyakuRefreshToken]);

  useEffect(() => {
    if (!backendReady || !code || !secondaryJobReady || !financialBackgroundJobReady || headerMode !== "financial") return;
    if (!taisyakuFetchedOnce || taisyakuLoading) return;
    if (!shouldAutoRefreshTaisyaku(taisyakuSnapshot)) return;
    const requestKey = `${code}:${taisyakuSnapshot ? "stale" : "empty"}`;
    if (taisyakuAutoImportRequestedRef.current.has(requestKey)) return;
    taisyakuAutoImportRequestedRef.current.add(requestKey);
    let cancelled = false;
    void api
      .post("/jobs/taisyaku/import")
      .catch(() => null)
      .finally(() => {
        if (cancelled) return;
        window.setTimeout(() => {
          if (!cancelled) setTaisyakuRefreshToken((prev) => prev + 1);
        }, 3500);
      });
    return () => {
      cancelled = true;
    };
  }, [backendReady, code, financialBackgroundJobReady, headerMode, secondaryJobReady, taisyakuFetchedOnce, taisyakuLoading, taisyakuSnapshot]);

  useEffect(() => {
    if (!backendReady || !code || !secondaryFetchStableReady || headerMode !== "financial") return;
    let cancelled = false;
    const controller = new AbortController();
    setTdnetLoading(true);
    setTdnetFetchedOnce(false);
    void api
      .get("/ticker/tdnet/disclosures", { params: { code, limit: 30 }, signal: controller.signal })
      .then((response) => {
        if (cancelled) return;
        const items = response.data && typeof response.data === "object"
          ? (response.data as { items?: unknown }).items
          : [];
        const meta = response.data && typeof response.data === "object"
          ? (response.data as { meta?: unknown }).meta
          : null;
        const normalized = Array.isArray(items)
          ? items.map(normalizeTdnetDisclosureItem).filter((item): item is TdnetDisclosureItem => item !== null)
          : [];
        setTdnetDisclosures(normalized);
        setTdnetMeta(normalizeTdnetDisclosureMeta(meta));
      })
      .catch((error) => {
        if (cancelled || isCanceledRequestError(error)) return;
        setTdnetDisclosures([]);
        setTdnetMeta({
          status: "error",
          statusDetail: "request_failed",
          sourceConfigured: null,
          missingTables: [],
          totalCount: null,
          matchedCount: null,
          latestPublishedAt: null,
          latestFetchedAt: null,
        });
      })
      .finally(() => {
        if (!cancelled) {
          setTdnetLoading(false);
          setTdnetFetchedOnce(true);
        }
      });
    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [backendReady, code, headerMode, secondaryFetchStableReady, tdnetRefreshToken]);

  useEffect(() => {
    if (!backendReady || !code || !secondaryJobReady || !financialBackgroundJobReady || headerMode !== "financial") return;
    if (!tdnetFetchedOnce || tdnetLoading) return;
    if (!shouldAutoRefreshTdnet(tdnetDisclosures, Date.now(), tdnetMeta)) return;
    const requestKey = `${code}:${tdnetDisclosures.length > 0 ? "stale" : "empty"}`;
    if (tdnetAutoImportRequestedRef.current.has(requestKey)) return;
    tdnetAutoImportRequestedRef.current.add(requestKey);
    let cancelled = false;
    void api
      .post("/jobs/tdnet/import", null, { params: { code, limit: 30 } })
      .catch(() => null)
      .finally(() => {
        if (cancelled) return;
        window.setTimeout(() => {
          if (!cancelled) setTdnetRefreshToken((prev) => prev + 1);
        }, 3500);
      });
    return () => {
      cancelled = true;
    };
  }, [backendReady, code, financialBackgroundJobReady, headerMode, secondaryJobReady, tdnetDisclosures, tdnetFetchedOnce, tdnetLoading, tdnetMeta]);

  // 依存配列で参照する派生値は先に宣言して、TDZ を避ける。
  const edinetOfficialBackfillRequest = useMemo(
    () => resolveAutoEdinetOfficialBackfillRequest({ code, financialPanel }),
    [code, financialPanel]
  );

  useEffect(() => {
    if (!backendReady || !code || !financialBackgroundJobReady || headerMode !== "financial") return;
    if (!financialFetchedOnce || financialLoading) return;
    if (!edinetOfficialBackfillRequest) return;
    if (financialPanel?.bootstrapState?.active === true) return;
    const requestKey = edinetOfficialBackfillRequest.requestKey;
    if (edinetAutoBackfillRequestedRef.current.has(requestKey)) return;
    edinetAutoBackfillRequestedRef.current.add(requestKey);
    let cancelled = false;
    void (async () => {
      try {
        const response = await api.post("/jobs/edinet/official-backfill", null, {
          params: { code },
          timeout: 10000,
        });
        if (cancelled) return;
        const outcome = resolveAutoEdinetOfficialBackfillSubmitOutcome({ responseData: response.data });
        if (outcome.action === "poll") {
          edinetAutoBackfillJobIdsRef.current.set(requestKey, outcome.jobId);
        }
      } catch (error) {
        if (cancelled) return;
        const outcome = resolveAutoEdinetOfficialBackfillSubmitOutcome({ error });
        if (outcome.action === "refresh") {
          edinetAutoBackfillRefreshOnlyRef.current.set(requestKey, outcome.delayMs);
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [
    backendReady,
    code,
    edinetOfficialBackfillRequest,
    financialFetchedOnce,
    financialLoading,
    financialPanel?.bootstrapState?.active,
    financialBackgroundJobReady,
    headerMode,
  ]);

  useEffect(() => {
    if (!backendReady || !code || !financialBackgroundJobReady || headerMode !== "financial") return;
    if (!financialFetchedOnce || financialLoading) return;
    if (!edinetOfficialBackfillRequest) return;
    const requestKey = edinetOfficialBackfillRequest.requestKey;
    if (edinetAutoBackfillRefreshedRef.current.has(requestKey)) return;
    const refreshOnlyDelayMs = edinetAutoBackfillRefreshOnlyRef.current.get(requestKey) ?? null;
    const shouldWatch =
      financialPanel?.bootstrapState?.active === true ||
      edinetAutoBackfillJobIdsRef.current.has(requestKey);
    if (!shouldWatch && refreshOnlyDelayMs == null) return;

    let cancelled = false;
    let timerId: number | null = null;
    const startedAt = Date.now();
    const settleAndRefresh = (delayMs = 3000) => {
      if (cancelled || edinetAutoBackfillRefreshedRef.current.has(requestKey)) return;
      edinetAutoBackfillRefreshedRef.current.add(requestKey);
      edinetAutoBackfillRefreshOnlyRef.current.delete(requestKey);
      timerId = window.setTimeout(() => {
        if (!cancelled) {
          setFinancialRefreshToken((prev) => prev + 1);
        }
      }, delayMs);
    };

    if (refreshOnlyDelayMs != null) {
      settleAndRefresh(refreshOnlyDelayMs);
      return () => {
        cancelled = true;
        if (timerId != null) {
          window.clearTimeout(timerId);
        }
      };
    }

    const pollCurrentJob = async () => {
      const jobId = edinetAutoBackfillJobIdsRef.current.get(requestKey);
      if (!jobId) {
        if (Date.now() - startedAt >= 15_000) {
          settleAndRefresh();
          return;
        }
        timerId = window.setTimeout(() => {
          void pollCurrentJob();
        }, 500);
        return;
      }
      try {
        const res = await api.get(`/jobs/${jobId}`, { timeout: 4000 });
        if (cancelled) return;
        const payload = (res.data ?? null) as JobStatusPayload | null;
        const currentStatus = payload?.status ?? null;
        if (!ANALYSIS_BACKFILL_ACTIVE_STATUSES.has(currentStatus ?? "")) {
          settleAndRefresh();
          return;
        }
      } catch {
        if (cancelled) return;
      }
      if (Date.now() - startedAt >= 15_000) {
        settleAndRefresh();
        return;
      }
      timerId = window.setTimeout(() => {
        void pollCurrentJob();
      }, 1500);
    };

    void pollCurrentJob();
    return () => {
      cancelled = true;
      if (timerId != null) {
        window.clearTimeout(timerId);
      }
    };
  }, [
    backendReady,
    code,
    edinetOfficialBackfillRequest,
    financialFetchedOnce,
    financialLoading,
    financialPanel?.bootstrapState?.active,
    financialBackgroundJobReady,
    headerMode,
  ]);

  useEffect(() => {
    setSelectedTdnetDisclosures([]);
    setSelectedTdnetDisclosureIndex(0);
  }, [code]);

  const hasPhaseScores =
    activeTicker?.bodyScore != null ||
    activeTicker?.earlyScore != null ||
    activeTicker?.lateScore != null ||
    typeof activeTicker?.phaseN === "number";
  const phaseScores = hasPhaseScores ? activeTicker : phaseFallback;
  const phaseReasons = activeTicker?.phaseReasons?.length
    ? activeTicker.phaseReasons
    : phaseFallback?.reasons ?? [];
  const phaseDtValue = hasPhaseScores ? activeTicker?.phaseDt ?? null : phaseFallback?.dt ?? null;
  const phaseNValue = hasPhaseScores ? activeTicker?.phaseN : phaseFallback?.n;
  const hasPhaseData =
    phaseScores?.bodyScore != null ||
    phaseScores?.earlyScore != null ||
    phaseScores?.lateScore != null ||
    typeof phaseNValue === "number" ||
    phaseDtValue != null ||
    phaseReasons.length > 0;
  const hasPhasePanelData = hasPhaseData || phaseFallbackLoading;
  const latestDailyAsOfTime = useMemo(() => {
    return dailyData.reduce<number | null>((maxValue, row) => {
      if (!Array.isArray(row) || row.length === 0) return maxValue;
      const normalized = normalizeTime(row[0]);
      if (normalized == null) return maxValue;
      if (maxValue == null || normalized > maxValue) return normalized;
      return maxValue;
    }, null);
  }, [dailyData]);
  const latestResolvedMetaDate = useMemo(
    () => resolveLatestResolvedMetaDate(dailyBarsMeta, monthlyBarsMeta),
    [dailyBarsMeta, monthlyBarsMeta]
  );
  useEffect(() => {
    if (mainAsOfTime != null) {
      analysisBaseAsOfRef.current = mainAsOfTime;
      return;
    }
    const nextBaseAsOfTime = resolveLatestAnalysisAvailableAsOfTime({
      latestResolvedMetaDate,
      latestDailyAsOfTime,
    });
    if (nextBaseAsOfTime == null) return;
    if (analysisBaseAsOfRef.current == null || analysisBaseAsOfRef.current < nextBaseAsOfTime) {
      analysisBaseAsOfRef.current = nextBaseAsOfTime;
    }
  }, [mainAsOfTime, latestDailyAsOfTime, latestResolvedMetaDate]);
  const resolvedCursorAsOfTime = useMemo(() => {
    if (!cursorMode) return null;
    if (selectedBarData?.time != null) return selectedBarData.time;
    return analysisCursorTime;
  }, [cursorMode, selectedBarData?.time, analysisCursorTime]);
  const detailAsOfTime = useMemo(() => {
    return resolveAnalysisBaseAsOfTime({
      mainAsOfTime,
      resolvedCursorAsOfTime,
      analysisBaseAsOfTime: analysisBaseAsOfRef.current,
      latestResolvedMetaDate,
      latestDailyAsOfTime,
    });
  }, [resolvedCursorAsOfTime, mainAsOfTime, latestResolvedMetaDate, latestDailyAsOfTime]);
  const analysisCursorDateLabel = useMemo(() => {
    if (!cursorMode) return "";
    const label = formatDateLabel(resolvedCursorAsOfTime);
    return label ? label.replace(/\//g, "-") : "";
  }, [cursorMode, resolvedCursorAsOfTime]);
  useEffect(() => {
    if (!analysisFetchEnabled || detailAsOfTime == null) {
      setAnalysisAsOfTime(null);
      return;
    }
    // Debounce analysis fetch: cursor mode uses shorter delay, normal mode
    // uses 300ms to absorb rapid changes from data loading / range init.
    const delay = cursorMode ? 80 : 300;
    const timerId = window.setTimeout(() => {
      setAnalysisAsOfTime(detailAsOfTime);
    }, delay);
    return () => {
      window.clearTimeout(timerId);
    };
  }, [analysisFetchEnabled, detailAsOfTime, cursorMode]);
  const analysisHorizonData =
    analysisFallback?.horizonAnalysis?.items?.[String(analysisHorizon) as `${AnalysisHorizonKey}`] ?? null;
  const analysisPUp =
    analysisHorizonData?.pUp ?? (analysisHorizon === 20 ? (analysisFallback?.pUp ?? null) : null);
  const analysisPDown =
    analysisHorizonData?.pDown ??
    (analysisPUp != null ? 1 - analysisPUp : analysisHorizon === 20 ? (analysisFallback?.pDown ?? null) : null);
  const analysisEvNet =
    analysisHorizonData?.evNet ?? (analysisHorizon === 20 ? (analysisFallback?.ev20Net ?? null) : null);
  const analysisPTurnUp =
    analysisHorizonData?.pTurnUp ?? (analysisHorizon === 20 ? (analysisFallback?.pTurnUp ?? null) : null);
  const analysisPTurnDown =
    analysisHorizonData?.pTurnDown ?? (analysisHorizon === 20 ? (analysisFallback?.pTurnDown ?? null) : null);
  const analysisAdditive = analysisFallback?.additiveSignals ?? null;
  const hasAnalysisData =
    analysisPUp != null ||
    analysisPDown != null ||
    analysisEvNet != null ||
    analysisPTurnUp != null ||
    analysisPTurnDown != null;
  const hasSellAnalysisData =
    sellAnalysisFallback?.pDown != null ||
    sellAnalysisFallback?.pTurnDown != null ||
    sellAnalysisFallback?.ev20Net != null ||
    sellAnalysisFallback?.shortScore != null ||
    sellAnalysisFallback?.aScore != null ||
    sellAnalysisFallback?.bScore != null ||
    sellAnalysisFallback?.trendDown != null ||
    sellAnalysisFallback?.trendDownStrict != null ||
    sellAnalysisFallback?.shortRet5 != null ||
    sellAnalysisFallback?.shortRet10 != null ||
    sellAnalysisFallback?.shortRet20 != null;
  const analysisFallbackDtTime = normalizeTime(analysisFallback?.dt ?? null);
  const sellAnalysisFallbackDtTime = normalizeTime(sellAnalysisFallback?.dt ?? null);
  const analysisDataStale =
    analysisFetchEnabled &&
    analysisAsOfTime != null &&
    !analysisLoading &&
    analysisFallbackDtTime != null &&
    analysisFallbackDtTime < analysisAsOfTime;
  const sellAnalysisDataStale =
    analysisFetchEnabled &&
    analysisAsOfTime != null &&
    !sellAnalysisLoading &&
    sellAnalysisFallbackDtTime != null &&
    sellAnalysisFallbackDtTime < analysisAsOfTime;
  const analysisCacheIncomplete =
    analysisFetchEnabled &&
    analysisAsOfTime != null &&
    !analysisLoading &&
    !sellAnalysisLoading &&
    (!hasAnalysisData || !hasSellAnalysisData || analysisDataStale || sellAnalysisDataStale);
  const analysisBackfillActive =
    analysisBackfillJob?.type === "analysis_backfill" &&
    ANALYSIS_BACKFILL_ACTIVE_STATUSES.has(analysisBackfillJob.status ?? "");
  const analysisRecalcDisabled = legacyAnalysisDisabled;
  const analysisRecalcDisabledReason =
    legacyAnalysisDisabledReason ?? "Phase 1 では売買判定更新を利用します。";
  const analysisPreparationVisible = analysisBackfillActive || analysisRecalcSubmitting === "auto";
  const analysisMissingDataVisible = analysisCacheIncomplete && !analysisPreparationVisible;
  const analysisBackfillProgressLabel = analysisBackfillActive
    ? typeof analysisBackfillJob?.progress === "number"
      ? `解析準備中 ${Math.round(Math.max(0, analysisBackfillJob.progress))}%`
      : "解析準備中"
    : null;
  const analysisBackfillMessage =
    analysisBackfillActive ? (analysisBackfillJob?.message?.trim() || "解析データを再計算しています。") : null;
  const analysisDecisionFromBackend = analysisFallback?.decision ?? null;
  const patternSummary = useMemo(() => {
    if (analysisDecisionFromBackend) {
      const scenarios = analysisDecisionFromBackend.scenarios.length
        ? analysisDecisionFromBackend.scenarios.map((scenario) => ({
          key: scenario.key,
          label: scenario.label,
          tone: scenario.tone,
          score: clamp(scenario.score, 0, 1),
          reasons: [] as string[],
        }))
        : [
          {
            key: "up" as const,
            label: "上昇継続（押し目再開）",
            tone: "up" as const,
            score: clamp(analysisDecisionFromBackend.buyProb ?? 0, 0, 1),
            reasons: [] as string[],
          },
          {
            key: "down" as const,
            label: "下落継続（戻り売り優位）",
            tone: "down" as const,
            score: clamp(analysisDecisionFromBackend.sellProb ?? 0, 0, 1),
            reasons: [] as string[],
          },
          {
            key: "range" as const,
            label: "往復レンジ（上下振れ）",
            tone: "neutral" as const,
            score: clamp(analysisDecisionFromBackend.neutralProb ?? 0, 0, 1),
            reasons: [] as string[],
          },
        ];
      scenarios.sort((a, b) => b.score - a.score);
      const tone = analysisDecisionFromBackend.tone;
      return {
        environmentLabel:
          analysisDecisionFromBackend.environmentLabel ??
          (tone === "up"
            ? "上昇優位"
            : tone === "down"
              ? "下落優位"
              : "方向感拮抗"),
        environmentTone: tone,
        markerTone: tone === "up" || tone === "down" ? tone : null,
        markerIsSetup: false,
        scenarios
      };
    }
    return computeEnvironmentTone({
      analysisPUp,
      analysisPDown,
      analysisPTurnUp,
      analysisPTurnDown,
      analysisEvNet,
      playbookUpScoreBonus: toFiniteNumber(analysisFallback?.entryPolicy?.up?.playbookScoreBonus),
      playbookDownScoreBonus: toFiniteNumber(analysisFallback?.entryPolicy?.down?.playbookScoreBonus),
      additiveSignals: analysisAdditive,
      sellAnalysis: sellAnalysisFallback
    });
  }, [
    analysisDecisionFromBackend,
    analysisPUp,
    analysisPDown,
    analysisEvNet,
    analysisPTurnUp,
    analysisPTurnDown,
    analysisFallback?.entryPolicy?.up?.playbookScoreBonus,
    analysisFallback?.entryPolicy?.down?.playbookScoreBonus,
    analysisAdditive,
    sellAnalysisFallback
  ]);
  const analysisDecision = useMemo(() => {
    if (analysisDecisionFromBackend) {
      const tone = analysisDecisionFromBackend.tone;
      return {
        tone,
        sideLabel:
          analysisDecisionFromBackend.sideLabel ??
          (tone === "up" ? "買い" : tone === "down" ? "売り" : "中立"),
        patternLabel: analysisDecisionFromBackend.patternLabel ?? "--",
        confidence: toFiniteNumber(analysisDecisionFromBackend.confidence),
        buyProb: toFiniteNumber(analysisDecisionFromBackend.buyProb),
        sellProb: toFiniteNumber(analysisDecisionFromBackend.sellProb),
        neutralProb: toFiniteNumber(analysisDecisionFromBackend.neutralProb)
      };
    }
    const scenarioMap = new Map(patternSummary.scenarios.map((scenario) => [scenario.key, scenario]));
    const buyScenario = scenarioMap.get("up") ?? null;
    const sellScenario = scenarioMap.get("down") ?? null;
    const neutralScenario = scenarioMap.get("range") ?? null;
    const tone = patternSummary.environmentTone;
    const sideLabel = tone === "up" ? "買い" : tone === "down" ? "売り" : "中立";
    const selectedScenario =
      tone === "up"
        ? buyScenario
        : tone === "down"
          ? sellScenario
          : neutralScenario ?? patternSummary.scenarios[0] ?? null;
    return {
      tone,
      sideLabel,
      patternLabel: selectedScenario?.label ?? "--",
      confidence: selectedScenario?.score ?? null,
      buyProb: buyScenario?.score ?? null,
      sellProb: sellScenario?.score ?? null,
      neutralProb: neutralScenario?.score ?? null
    };
  }, [analysisDecisionFromBackend, patternSummary.environmentTone, patternSummary.scenarios]);
  const analysisEntryPolicy = analysisFallback?.entryPolicy ?? null;
  const analysisStagePrecision = analysisFallback?.buyStagePrecision ?? null;
  const analysisGuidance = useMemo(() => {
    const buyProb = clamp(analysisDecision.buyProb ?? 0, 0, 1);
    const sellProb = clamp(analysisDecision.sellProb ?? 0, 0, 1);
    const neutralProb = clamp(analysisDecision.neutralProb ?? 0, 0, 1);
    const entryPolicy = analysisEntryPolicy;
    const upPolicy = entryPolicy?.up ?? null;
    const downPolicy = entryPolicy?.down ?? null;
    const stagePrecision = analysisStagePrecision;
    const strategyBacktest = stagePrecision?.strategy ?? null;
    const strategySamples = strategyBacktest?.samples ?? 0;
    const coreSamples = stagePrecision?.core?.samples ?? 0;
    const confidence = clamp(analysisDecision.confidence ?? 0, 0, 1);
    const tone = analysisDecision.tone;
    const spread = Math.abs(buyProb - sellProb);
    const turnUp = clamp(analysisPTurnUp ?? 0.5, 0, 1);
    const turnDown = clamp(analysisPTurnDown ?? 0.5, 0, 1);
    const evBias = analysisEvNet == null ? 0 : clamp(analysisEvNet / 0.06, -1, 1);
    const confidenceRank = confidence >= 0.66 ? "高" : confidence >= 0.56 ? "中" : "低";

    let action = "中立";
    let watchpoint = "優勢側の仕込み確率が上がるまで監視。";

    const tonePenalty = tone === "down" ? 0.08 : tone === "neutral" ? 0.03 : 0;
    const baseLongPrecision = clamp(
      0.5 * buyProb +
      0.2 * (1 - sellProb) +
      0.15 * turnUp +
      0.1 * confidence +
      0.05 * (0.5 + evBias * 0.5) -
      tonePenalty,
      0,
      1
    );
    const corePrecision = clamp(
      baseLongPrecision + 0.55 * spread + 0.12 * (turnUp - turnDown) - 0.1 * neutralProb,
      0,
      1
    );
    const corePrecisionResolved = stagePrecision?.core?.precision ?? corePrecision;
    const strategyPrecisionResolved = strategyBacktest?.precision ?? corePrecisionResolved;
    const strategyPrecisionLabel = (() => {
      if (strategySamples > 0) {
        return `${formatPercentLabel(strategyPrecisionResolved)} (n${strategySamples})`;
      }
      if (coreSamples > 0) {
        return `${formatPercentLabel(corePrecisionResolved)} (本玉 n${coreSamples})`;
      }
      return `${formatPercentLabel(strategyPrecisionResolved)} (推定)`;
    })();
    const coreReady = buyProb >= 0.58 && spread >= 0.1 && turnUp >= turnDown;
    const currentStageLabel = coreReady
      ? "本玉成立"
      : tone === "up"
        ? "買い監視"
        : tone === "down"
          ? "売り監視"
          : "中立監視";
    const buyTimingTitle = "仕込み状態";
    if (tone === "up") {
      action = "買い寄り";
      watchpoint = "買い仕込みを優先監視。";
    } else if (tone === "down") {
      action = "売り寄り";
      watchpoint = "売り仕込みを優先監視。";
    } else {
      action = "中立";
      watchpoint = "買い/売り仕込みの優勢側を監視。";
    }
    if (coreReady) {
      action = "買い本玉成立";
    }
    const upPlaybookBias = clamp((toFiniteNumber(upPolicy?.playbookScoreBonus) ?? 0) / 0.04, -0.35, 0.35);
    const downPlaybookBias = clamp((toFiniteNumber(downPolicy?.playbookScoreBonus) ?? 0) / 0.04, -0.35, 0.35);
    const trendDown = sellAnalysisFallback?.trendDown === true;
    const trendDownStrict = sellAnalysisFallback?.trendDownStrict === true;
    const resolvedShortScore = resolveSellShortScore(sellAnalysisFallback);
    const shortScoreNorm = clamp(((resolvedShortScore ?? 70) - 70) / 90, 0, 1);
    const distMa20Signed = toFiniteNumber(sellAnalysisFallback?.distMa20Signed);
    const ma20Slope = toFiniteNumber(sellAnalysisFallback?.ma20Slope);
    const ma60Slope = toFiniteNumber(sellAnalysisFallback?.ma60Slope);
    const bearishStructure = Boolean(
      trendDownStrict ||
      (
        distMa20Signed != null &&
        ma20Slope != null &&
        ma60Slope != null &&
        distMa20Signed <= -0.003 &&
        ma20Slope <= 0 &&
        ma60Slope <= 0
      )
    );
    const bullishStructure = Boolean(
      !trendDown &&
      distMa20Signed != null &&
      ma20Slope != null &&
      ma60Slope != null &&
      distMa20Signed > 0 &&
      ma20Slope >= 0 &&
      ma60Slope >= 0
    );
    const downLead = clamp(sellProb - buyProb, -1, 1);
    const turnLeadDown = clamp(turnDown - turnUp, -1, 1);
    const bearishPressure = clamp(
      (trendDownStrict ? 0.12 : trendDown ? 0.06 : 0) +
      Math.max(0, downLead) * 0.14 +
      Math.max(0, turnLeadDown) * 0.08,
      0,
      0.32
    );
    const bullishOffset = clamp(
      Math.max(0, buyProb - sellProb) * 0.08 +
      Math.max(0, turnUp - turnDown) * 0.04,
      0,
      0.18
    );
    const buySetupProbRaw = clamp(
      0.5 * buyProb +
      0.2 * turnUp +
      0.12 * (1 - sellProb) +
      0.1 * (0.5 + evBias * 0.5) +
      0.08 * (0.5 + upPlaybookBias * 0.5),
      0,
      1
    );
    const sellSetupProbRaw = clamp(
      0.5 * sellProb +
      0.2 * turnDown +
      0.12 * (1 - buyProb) +
      0.1 * (0.5 - evBias * 0.5) +
      0.08 * (0.5 + downPlaybookBias * 0.5),
      0,
      1
    );
    const buySetupProb = clamp(
      buySetupProbRaw - bearishPressure + 0.04 * bullishOffset,
      0,
      1
    );
    const sellSetupProb = clamp(
      sellSetupProbRaw + 0.55 * bearishPressure,
      0,
      1
    );
    const sellSetupQuality = clamp(
      0.42 * sellProb +
      0.2 * turnDown +
      0.16 * shortScoreNorm +
      0.12 * (bearishStructure ? 1 : 0) +
      0.1 * clamp((-(analysisEvNet ?? 0) + 0.005) / 0.04, 0, 1) -
      0.16 * (bullishStructure ? 1 : 0),
      0,
      1
    );
    const buyReadyProbGate = trendDownStrict ? 0.66 : trendDown ? 0.62 : 0.58;
    const buyReadyLeadGate = trendDownStrict ? 0.05 : trendDown ? 0.03 : -0.02;
    const buyReadyTurnGate = trendDownStrict ? 0.04 : trendDown ? 0.02 : -0.03;
    const buySetupReady = Boolean(
      buySetupProb >= buyReadyProbGate &&
      buyProb >= sellProb + buyReadyLeadGate &&
      turnUp >= turnDown + buyReadyTurnGate &&
      (!trendDownStrict || (analysisEvNet ?? 0) > 0)
    );
    const buyWatchProbGate = trendDown ? 0.56 : 0.5;
    const buyWatchLeadGate = trendDownStrict ? 0.05 : -0.02;
    const buyWatchTurnGate = trendDownStrict ? 0.04 : -0.08;
    const buySetupWatch = Boolean(
      !buySetupReady &&
      buySetupProb >= buyWatchProbGate &&
      buyProb >= sellProb + buyWatchLeadGate &&
      turnUp >= turnDown + buyWatchTurnGate
    );
    const sellReadyProbGate = trendDownStrict ? 0.56 : trendDown ? 0.59 : 0.63;
    const sellReadyLeadGate = trendDown ? -0.04 : 0.02;
    const sellReadyTurnGate = trendDown ? -0.04 : 0.01;
    const sellReadyQualityGate = trendDownStrict ? 0.50 : trendDown ? 0.56 : 0.62;
    const sellReadyShortScoreGate = trendDownStrict ? 58 : trendDown ? 64 : 72;
    const sellReadyEvGate = trendDownStrict ? 0.008 : trendDown ? 0.002 : -0.002;
    const sellSetupReady = Boolean(
      sellSetupProb >= sellReadyProbGate &&
      sellProb >= buyProb + sellReadyLeadGate &&
      turnDown >= turnUp + sellReadyTurnGate &&
      sellSetupQuality >= sellReadyQualityGate &&
      (resolvedShortScore ?? 70) >= sellReadyShortScoreGate &&
      (analysisEvNet == null || analysisEvNet <= sellReadyEvGate) &&
      !bullishStructure
    );
    const sellSetupWatch = Boolean(
      !sellSetupReady &&
      sellSetupProb >= (trendDown ? 0.5 : 0.54) &&
      sellSetupQuality >= (trendDown ? 0.46 : 0.52)
    );
    const buySetupState = buySetupReady ? "実行" : buySetupWatch ? "監視" : "待機";
    const sellSetupState = sellSetupReady ? "実行" : sellSetupWatch ? "監視" : "待機";
    if (tone === "neutral") {
      action = buySetupProb >= sellSetupProb ? "中立（買い仕込み監視）" : "中立（売り仕込み監視）";
      watchpoint = `買い ${buySetupState} / 売り ${sellSetupState} を監視。`;
    }
    const buySetupLabel = `${buySetupState} ${formatPercentLabel(buySetupProb)}`;
    const sellSetupLabel = `${sellSetupState} ${formatPercentLabel(sellSetupProb)}`;
    const setupTimingLines = [
      `買い仕込み: ${buySetupLabel}`,
      `売り仕込み: ${sellSetupLabel}`
    ];

    const buyTimingPlan = [
      `現在判定: ${currentStageLabel} / 主精度 ${strategyPrecisionLabel}`,
      ...setupTimingLines
    ];

    const shortScoreLabel = resolvedShortScore == null ? "--" : resolvedShortScore.toFixed(1);
    const reasonLines = [
      `方向確率 上昇 ${formatPercentLabel(buyProb)} / 下落 ${formatPercentLabel(sellProb)} / 中立 ${formatPercentLabel(neutralProb)}`,
      `仕込み 買い ${buySetupLabel} / 売り ${sellSetupLabel}`,
      `売り品質 ${formatPercentLabel(sellSetupQuality)} / shortScore ${shortScoreLabel}`,
      `下降圧力 ${formatPercentLabel(bearishPressure)}`,
      analysisEvNet == null ? null : `期待値 ${formatSignedPercentLabel(analysisEvNet)}`
    ].filter(isNonEmptyString);

    return {
      confidenceRank,
      action,
      watchpoint,
      buyTimingTitle,
      buyTimingPlan,
      buyWidth: Math.round(buyProb * 100),
      sellWidth: Math.round(sellProb * 100),
      neutralWidth: Math.round(neutralProb * 100),
      buySetupProb,
      sellSetupProb,
      buySetupWidth: Math.round(buySetupProb * 100),
      sellSetupWidth: Math.round(sellSetupProb * 100),
      buySetupState,
      sellSetupState,
      reasonLines
    };
  }, [
    analysisDecision.buyProb,
    analysisDecision.sellProb,
    analysisDecision.neutralProb,
    analysisDecision.confidence,
    analysisDecision.tone,
    analysisEntryPolicy,
    analysisStagePrecision,
    analysisPTurnUp,
    analysisPTurnDown,
    analysisEvNet,
    sellAnalysisFallback
  ]);
  const canShowPhase = hasPhasePanelData;
  const showBuyAnalysis = hasAnalysisData || analysisLoading;
  const showSellAnalysis = hasSellAnalysisData || sellAnalysisLoading;
  const canShowAnalysis = showBuyAnalysis || showSellAnalysis;
  const analysisLoadingText = analysisLoading ? "読込中..." : null;
  const sellAnalysisLoadingText = sellAnalysisLoading ? "読込中..." : null;
  const selectedDateKey = useMemo(() => {
    if (!selectedDate) return null;
    const normalized = normalizeTime(selectedDate);
    if (normalized == null) return null;
    return toDateKey(normalized);
  }, [selectedDate]);
  const { items: selectedDayExactDecisionRange } = useExactDecisionRange({
    backendReady,
    code,
    startDt: selectedDateKey,
    endDt: selectedDateKey,
    riskMode: analysisRiskMode,
    enabled: analysisFetchEnabled && showDecisionMarkers && selectedDateKey != null,
    readyToFetch: analysisNetworkReady,
    cacheKeyExtra: analysisFetchRefreshToken,
  });
  const selectedExactDecisionItem = useMemo(() => {
    return selectedDayExactDecisionRange[0] ?? null;
  }, [selectedDayExactDecisionRange]);
  const selectedExactAnalysisDecision = useMemo(() => {
    if (!selectedExactDecisionItem) return null;
    return normalizeAnalysisDecision(selectedExactDecisionItem.decision);
  }, [selectedExactDecisionItem]);
  const analysisDtLabel = useMemo(() => {
    if (!analysisFallback) return "";
    const normalized = normalizeTime(analysisFallback.dt);
    return formatDateLabel(normalized);
  }, [analysisFallback]);
  const sellAnalysisDtLabel = useMemo(() => {
    if (!sellAnalysisFallback) return "";
    const normalized = normalizeTime(sellAnalysisFallback.dt);
    return formatDateLabel(normalized);
  }, [sellAnalysisFallback]);
  const sellPredDtLabel = useMemo(() => {
    if (!sellAnalysisFallback) return "";
    const normalized = normalizeTime(sellAnalysisFallback.predDt);
    return formatDateLabel(normalized);
  }, [sellAnalysisFallback]);
  const selectedAnalysisDtLabel = useMemo(() => {
    if (!selectedExactDecisionItem) return null;
    const normalized = normalizeTime(selectedExactDecisionItem.dtKey);
    return normalized == null ? null : formatDateLabel(normalized);
  }, [selectedExactDecisionItem]);
  const analysisDecisionForDisplay = useMemo(() => {
    if (!selectedExactAnalysisDecision) return analysisDecision;
    return {
      ...analysisDecision,
      tone: selectedExactAnalysisDecision.tone,
      sideLabel:
        selectedExactAnalysisDecision.sideLabel ??
        (selectedExactAnalysisDecision.tone === "up"
          ? "買い"
          : selectedExactAnalysisDecision.tone === "down"
            ? "売り"
            : "中立"),
      patternLabel: selectedExactAnalysisDecision.patternLabel ?? analysisDecision.patternLabel,
      confidence: toFiniteNumber(selectedExactAnalysisDecision.confidence),
      buyProb: toFiniteNumber(selectedExactAnalysisDecision.buyProb),
      sellProb: toFiniteNumber(selectedExactAnalysisDecision.sellProb),
      neutralProb: toFiniteNumber(selectedExactAnalysisDecision.neutralProb),
    };
  }, [analysisDecision, selectedExactAnalysisDecision]);
  const analysisGuidanceForDisplay = useMemo(() => {
    if (!selectedExactAnalysisDecision) return analysisGuidance;
    const buyProb = clamp(selectedExactAnalysisDecision.buyProb ?? 0, 0, 1);
    const sellProb = clamp(selectedExactAnalysisDecision.sellProb ?? 0, 0, 1);
    const neutralProb = clamp(selectedExactAnalysisDecision.neutralProb ?? 0, 0, 1);
    const confidence = clamp(selectedExactAnalysisDecision.confidence ?? 0, 0, 1);
    const confidenceRank = confidence >= 0.66 ? "高" : confidence >= 0.56 ? "中" : "低";
    const action =
      selectedExactAnalysisDecision.tone === "up"
        ? "買い優先"
        : selectedExactAnalysisDecision.tone === "down"
          ? "売り優先"
          : "中立";
    const watchpoint = selectedAnalysisDtLabel
      ? `選択日 ${selectedAnalysisDtLabel} の exact decision を表示`
      : "選択日の exact decision を表示";
    return {
      ...analysisGuidance,
      confidenceRank,
      action,
      watchpoint,
      buyWidth: Math.round(buyProb * 100),
      sellWidth: Math.round(sellProb * 100),
      neutralWidth: Math.round(neutralProb * 100),
      buySetupProb: buyProb,
      sellSetupProb: sellProb,
      buySetupWidth: Math.round(buyProb * 100),
      sellSetupWidth: Math.round(sellProb * 100),
      buySetupState:
        selectedExactAnalysisDecision.tone === "up"
          ? "確定"
          : selectedExactAnalysisDecision.tone === "down"
            ? "警戒"
            : "中立",
      sellSetupState:
        selectedExactAnalysisDecision.tone === "down"
          ? "確定"
          : selectedExactAnalysisDecision.tone === "up"
            ? "警戒"
            : "中立",
    };
  }, [analysisGuidance, selectedExactAnalysisDecision, selectedAnalysisDtLabel]);
  const analysisResearchPrior = analysisFallback?.researchPrior ?? null;
  const researchPriorRunId = analysisResearchPrior?.runId ?? null;
  const researchPriorUpMeta = formatResearchPriorMetaLine("研究連携 上", analysisResearchPrior?.up ?? null);
  const researchPriorDownMeta = formatResearchPriorMetaLine("研究連携 下", analysisResearchPrior?.down ?? null);
  const analysisEdinetSummary = analysisFallback?.edinetSummary ?? null;
  const edinetStatusMeta = analysisEdinetSummary
    ? `EDI状態 ${formatEdinetStatus(analysisEdinetSummary.status)}${analysisEdinetSummary.mapped == null
      ? ""
      : analysisEdinetSummary.mapped
        ? " / マップ済み"
        : " / 未マップ"
    }`
    : null;
  const edinetQualityMeta = analysisEdinetSummary
    ? joinMetaSegments([
      Number.isFinite(analysisEdinetSummary.freshnessDays ?? NaN)
        ? `鮮度 ${Math.max(0, Math.round(analysisEdinetSummary.freshnessDays ?? 0))}日`
        : null,
      Number.isFinite(analysisEdinetSummary.metricCount ?? NaN)
        ? `指標 ${Math.max(0, Math.round(analysisEdinetSummary.metricCount ?? 0))}件`
        : null,
      Number.isFinite(analysisEdinetSummary.qualityScore ?? NaN)
        ? `品質 ${formatPercentLabel(analysisEdinetSummary.qualityScore)}`
        : null,
      Number.isFinite(analysisEdinetSummary.dataScore ?? NaN)
        ? `データ ${formatPercentLabel(analysisEdinetSummary.dataScore)}`
        : null,
    ])
    : null;
  const edinetMetricsMeta = analysisEdinetSummary
    ? joinMetaSegments([
      Number.isFinite(analysisEdinetSummary.roe ?? NaN)
        ? `ROE ${formatPercentLabel(analysisEdinetSummary.roe)}`
        : null,
      Number.isFinite(analysisEdinetSummary.equityRatio ?? NaN)
        ? `自己資本比率 ${formatPercentLabel(analysisEdinetSummary.equityRatio)}`
        : null,
      Number.isFinite(analysisEdinetSummary.debtRatio ?? NaN)
        ? `D/E ${formatNumber(analysisEdinetSummary.debtRatio, 2)}`
        : null,
      Number.isFinite(analysisEdinetSummary.operatingCfMargin ?? NaN)
        ? `営業CF率 ${formatPercentLabel(analysisEdinetSummary.operatingCfMargin)}`
        : null,
      Number.isFinite(analysisEdinetSummary.revenueGrowthYoy ?? NaN)
        ? `売上成長率 ${formatPercentLabel(analysisEdinetSummary.revenueGrowthYoy)}`
        : null,
    ])
    : null;
  const edinetBonusMeta =
    analysisEdinetSummary && Number.isFinite(analysisEdinetSummary.scoreBonus ?? NaN)
      ? `EDI補正 ${formatSignedPercentLabel(analysisEdinetSummary.scoreBonus)}${analysisEdinetSummary.featureFlagApplied == null
        ? ""
        : analysisEdinetSummary.featureFlagApplied
          ? " (適用ON)"
          : " (適用OFF)"
      }`
      : null;
  const financialSeries = financialPanel?.series ?? [];
  const latestFinancialPoint = financialSeries.length > 0 ? financialSeries[financialSeries.length - 1] : null;
  const latestPrice = activeTicker?.close ?? null;
  const financialFetchedLabel = financialPanel?.fetchedAt
    ? formatDateTimeLabel(financialPanel.fetchedAt)
    : null;
  const financialDisplay = useMemo(
    () =>
      buildEdinetFinancialDisplay({
        latestFinancialPoint,
        latestPrice,
        edinetSummary: analysisEdinetSummary,
      }),
    [analysisEdinetSummary, latestFinancialPoint, latestPrice]
  );
  const taisyakuDisplay = useMemo(() => buildTaisyakuDisplay(taisyakuSnapshot), [taisyakuSnapshot]);
  const taisyakuStatusLabel = useMemo(() => {
    if (!taisyakuSnapshot?.fetchedAt) {
      return taisyakuLoading ? "貸借データ補完取得を確認中です。" : null;
    }
    return `貸借最終取得 ${formatDateTimeLabel(taisyakuSnapshot.fetchedAt)}`;
  }, [taisyakuLoading, taisyakuSnapshot]);
  const tdnetHighlights = useMemo(() => buildTdnetHighlights(tdnetDisclosures, 3), [tdnetDisclosures]);
  const tdnetStatusLabel = useMemo(() => {
    const fetchedValues = tdnetDisclosures
      .map((item) => (item.fetchedAt ? Date.parse(item.fetchedAt) : Number.NaN))
      .filter((value) => Number.isFinite(value));
    if (fetchedValues.length === 0) {
      return tdnetLoading ? "TDNET補完取得を確認中です。" : null;
    }
    const latestFetched = Math.max(...fetchedValues);
    return `TDNET最終取得 ${formatDateTimeLabel(latestFetched)}`;
  }, [tdnetDisclosures, tdnetLoading]);
  const tdnetResolvedStatusLabel = useMemo(
    () => formatTdnetDisclosureStatusLabel({ meta: tdnetMeta, items: tdnetDisclosures, loading: tdnetLoading }),
    [tdnetDisclosures, tdnetLoading, tdnetMeta]
  );
  const showFinancialPanel = headerMode === "financial" && !compareCode && !showReplayPanel;
  const swingPlan = analysisFallback?.swingPlan ?? null;
  const swingDiagnostics = analysisFallback?.swingDiagnostics ?? null;
  const swingSetupExpectancy = swingDiagnostics?.setupExpectancy ?? null;
  const swingSideLabel =
    swingPlan?.side === "long"
      ? "買い"
      : swingPlan?.side === "short"
        ? "売り"
        : "--";
  const swingReasonsLabel = joinMetaSegments(
    Array.isArray(swingPlan?.reasons) ? (swingPlan.reasons as Array<string | null | undefined>) : []
  );
  const hasSwingData = Boolean(swingPlan || swingDiagnostics);
  const showAnalysisPanel = analysisFetchEnabled;
  const rankingDisplayScore = toFiniteNumber(activeTicker?.displayScore ?? activeTicker?.score ?? null);
  const rankingDisplayScoreSource =
    activeTicker?.displayScoreSource ??
    (rankingDisplayScore != null ? "none" : null);
  const rankingDisplayScoreSourceLabel =
    rankingDisplayScoreSource === "ranking_entry"
      ? "entryScore（売買候補）"
      : rankingDisplayScoreSource === "ranking_hybrid"
        ? "hybridScore（補完）"
        : rankingDisplayScoreSource === "none"
          ? "score source 未設定"
          : "--";
  const rankingJudgementSurfaceLabel =
    rankingDisplayScore != null || activeTicker?.entryPriorityScore != null || activeTicker?.hybridScore != null
      ? "残存: 表示スコアと厳選通過で買い/売り優先度を出す"
      : "根拠不足: この銘柄のランキング判定なし";
  const rightRailKind = showReplayPanel
    ? "replay"
    : annotationMode || (cursorMode && !compareCode && headerMode === "chart" && selectedDate && selectedBarData)
      ? "annotation"
    : showAnalysisPanel
    ? "analysis"
    : showFinancialPanel
      ? "financial"
      : null;
  const showRightPanel = rightRailKind !== null;
  const headerDrawToolControls = (
    <>
      <AnnotationToolbar
        enabled={annotationMode}
        activeTool={annotationTool}
        filter={annotationFilter}
        onToggleEnabled={() => setAnnotationMode((current) => !current)}
        onSelectTool={setAnnotationTool}
        onFilterChange={setAnnotationFilter}
      />
      {!annotationMode && (
        <DetailDrawToolbar
          activeTool={activeDrawTool}
          activeDrawColor={activeDrawColor}
          activeLineOpacity={activeLineOpacity}
          activeLineWidth={activeLineWidth}
          continuousDraw={continuousDraw}
          onSelectTool={selectDrawTool}
          onResetAll={resetAllDrawings}
          onToggleContinuous={() => setContinuousDraw((current) => !current)}
          onCycleColor={() => setActiveDrawColorIndex((prev) => (prev + 1) % COLOR_PALETTE.length)}
          onLineOpacityChange={setActiveLineOpacity}
          onLineWidthChange={setActiveLineWidth}
        />
      )}
    </>
  );
  useEffect(() => {
    if (!backendReady || !showAnalysisPanel) {
      return;
    }

    let disposed = false;
    let timerId: number | null = null;
    let startTimerId: number | null = null;
    const pollCurrentJob = async () => {
      try {
        const res = await api.get("/jobs/current", { timeout: 4000 });
        if (disposed) return;
        const payload = (res.data ?? null) as JobStatusPayload | null;
        const nextJob = payload?.type === "analysis_backfill" ? payload : null;
        const nextActive =
          nextJob != null && ANALYSIS_BACKFILL_ACTIVE_STATUSES.has(nextJob.status ?? "");
        const wasActive = analysisBackfillActiveRef.current;
        setAnalysisBackfillJob(nextJob);
        analysisBackfillActiveRef.current = nextActive;
        if (wasActive && !nextActive) {
          setAnalysisFetchRefreshToken((prev) => prev + 1);
        }
      } catch {
        if (disposed) {
          return;
        }
      } finally {
        if (!disposed && analysisBackfillActiveRef.current) {
          timerId = window.setTimeout(pollCurrentJob, 1500);
        }
      }
    };

    startTimerId = window.setTimeout(() => {
      void pollCurrentJob();
    }, ANALYSIS_JOB_POLL_DELAY_MS);
    return () => {
      disposed = true;
      if (startTimerId != null) {
        window.clearTimeout(startTimerId);
      }
      if (timerId != null) {
        window.clearTimeout(timerId);
      }
    };
  }, [
    backendReady,
    showAnalysisPanel,
    analysisBackfillActive,
  ]);

  useEffect(() => {
    if (!backendReady) return;
    if (!tickers.length && !loadingList) {
      void ensureListLoaded();
    }
  }, [backendReady, tickers.length, loadingList, ensureListLoaded]);

  useEffect(() => {
    if (!backendReady) return;
    if (!favoritesLoaded) {
      loadFavorites();
    }
  }, [backendReady, favoritesLoaded, loadFavorites]);

  useEffect(() => {
    if (!code) {
      setSecondaryFetchReady(false);
      return;
    }
    const mainReady =
      (dailyFetch.status === "success" || dailyFetch.status === "error") &&
      (weeklyFetch.status === "success" || weeklyFetch.status === "error") &&
      (monthlyFetch.status === "success" || monthlyFetch.status === "error");
    if (!mainReady) {
      setSecondaryFetchReady(false);
      return;
    }
    const timerId = window.setTimeout(() => {
      setSecondaryFetchReady(true);
    }, 0);
    return () => {
      window.clearTimeout(timerId);
    };
  }, [code, dailyFetch.status, weeklyFetch.status, monthlyFetch.status]);

  useEffect(() => {
    setSecondaryFetchStableReady(false);
    setSecondaryJobReady(false);
    setFinancialBackgroundJobReady(false);
    if (!backendReady || !code || !secondaryFetchReady) return;
    const stableTimerId = window.setTimeout(() => {
      setSecondaryFetchStableReady(true);
    }, SECONDARY_FETCH_STABLE_DELAY_MS);
    const jobTimerId = window.setTimeout(() => {
      setSecondaryJobReady(true);
    }, SECONDARY_JOB_READY_DELAY_MS);
    return () => {
      window.clearTimeout(stableTimerId);
      window.clearTimeout(jobTimerId);
    };
  }, [backendReady, code, compareCode, secondaryFetchReady]);

  useEffect(() => {
    setFinancialBackgroundJobReady(false);
    if (headerMode !== "financial") return;
    if (!secondaryJobReady) return;
    const timerId = window.setTimeout(() => {
      setFinancialBackgroundJobReady(true);
    }, FINANCIAL_BACKGROUND_JOB_DELAY_MS);
    return () => {
      window.clearTimeout(timerId);
    };
  }, [code, headerMode, secondaryJobReady]);

  useEffect(() => {
    if (!backendReady) return;
    if (!code) return;
    const requestParams = {
      code,
      dailyLimit,
      weeklyLimit: DEFAULT_LIMITS.weekly,
      monthlyLimit,
      asof: mainAsOf,
    };
    const applyAvailableFrames = (frames: Partial<ChartPrefetchFrames>) => {
      let applied = false;
      if (Array.isArray(frames.daily?.rows)) {
        const dailyRows = frames.daily.rows;
        setLoadingDaily(false);
        setDailyData(dailyRows);
        setHasMoreDaily(dailyRows.length >= dailyLimit);
        setDailyErrors([]);
        setDailyFetch({
          status: "success",
          responseCount: dailyRows.length,
          errorMessage: null,
        });
        applied = true;
      }
      if (Array.isArray(frames.weekly?.rows)) {
        const weeklyRows = frames.weekly.rows;
        setLoadingDaily(false);
        setWeeklyData(weeklyRows);
        setWeeklyErrors([]);
        setWeeklyFetch({
          status: "success",
          responseCount: weeklyRows.length,
          errorMessage: null,
        });
        applied = true;
      }
      if (Array.isArray(frames.monthly?.rows)) {
        const monthlyRows = frames.monthly.rows;
        setLoadingMonthly(false);
        setMonthlyData(monthlyRows);
        setBoxes(Array.isArray(frames.monthly?.boxes) ? frames.monthly.boxes : []);
        setHasMoreMonthly(monthlyRows.length >= monthlyLimit);
        setMonthlyErrors([]);
        setMonthlyFetch({
          status: "success",
          responseCount: monthlyRows.length,
          errorMessage: null,
        });
        applied = true;
      }
      if (applied) {
        setMainChartOverwriteObservability(
          summarizeDetailOverwriteObservability(frames, "daily", null)
        );
        if (frames.dataFreshnessContract) {
          setDetailDataFreshnessContract(frames.dataFreshnessContract);
        }
      }
      return applied;
    };
    const applyFrames = (frames: ChartPrefetchFrames) => {
      if (!hasCompleteDetailChartPrefetch(frames)) {
        applyAvailableFrames(frames);
        return false;
      }
      const dailyRows = Array.isArray(frames.daily?.rows) ? frames.daily.rows : [];
      const weeklyRows = Array.isArray(frames.weekly?.rows) ? frames.weekly.rows : [];
      const monthlyRows = Array.isArray(frames.monthly?.rows) ? frames.monthly.rows : [];
      const monthlyBoxes = Array.isArray(frames.monthly?.boxes) ? frames.monthly.boxes : [];
      setLoadingDaily(false);
      setLoadingMonthly(false);
      setMainChartPendingSwap(false);
      setDailyErrors([]);
      setWeeklyErrors([]);
      setMonthlyErrors([]);
      setDailyBarsMeta(null);
      setMonthlyBarsMeta(null);
      setDetailDataFreshnessContract(null);
      setDailyData(dailyRows);
      setWeeklyData(weeklyRows);
      setMonthlyData(monthlyRows);
      setBoxes(monthlyBoxes);
      setHasMoreDaily(dailyRows.length >= dailyLimit);
      setHasMoreMonthly(monthlyRows.length >= monthlyLimit);
      setDailyFetch({
        status: "success",
        responseCount: dailyRows.length,
        errorMessage: null,
      });
      setWeeklyFetch({
        status: "success",
        responseCount: weeklyRows.length,
        errorMessage: null,
      });
      setMonthlyFetch({
        status: "success",
        responseCount: monthlyRows.length,
        errorMessage: null,
      });
      setMainChartOverwriteObservability(
        summarizeDetailOverwriteObservability(frames, "daily", null)
      );
      setDetailDataFreshnessContract(frames.dataFreshnessContract ?? null);
      rangeSettleRef.current = Date.now() + RANGE_SETTLE_MS;
      return true;
    };
    const prefetched = readDetailChartPrefetchSync(requestParams);
    const hasSeed = applyFrames(prefetched);
    let active = true;
    let refreshTimerId: number | null = null;
    const controller = new AbortController();
    if (!hasSeed) {
      setLoadingDaily(true);
      setLoadingMonthly(true);
      setDailyErrors([]);
      setWeeklyErrors([]);
      setMonthlyErrors([]);
      setDailyBarsMeta(null);
      setMonthlyBarsMeta(null);
      setDailyFetch((prev) => ({ ...prev, status: "loading", errorMessage: null }));
      setWeeklyFetch((prev) => ({ ...prev, status: "loading", errorMessage: null }));
      setMonthlyFetch((prev) => ({ ...prev, status: "loading", errorMessage: null }));
    }
    const runNetworkRefreshForTimeframes = async (timeframes: Timeframe[]) => {
      if (!timeframes.length) return false;
      const frames = await prefetchDetailChartFrames(requestParams, {
        signal: controller.signal,
        forceNetwork: true,
        timeframes,
      });
      if (!active) return false;
      const applied = applyAvailableFrames(frames);
      setMainChartPendingSwap(false);
      if (frames.dataFreshnessContract) {
        setDetailDataFreshnessContract(frames.dataFreshnessContract);
      }
      return applied;
    };
    const runNetworkRefresh = async () => {
      try {
        let applied = false;
        for (const timeframes of buildPriorityDetailTimeframeGroups(focusPanel)) {
          applied = (await runNetworkRefreshForTimeframes(timeframes)) || applied;
          if (!active) return;
        }
        const frames = readDetailChartPrefetchSync(requestParams);
        const appliedComplete = applyFrames(frames);
        applied = appliedComplete || applied;
        if (!applied) {
          const dailyRows = Array.isArray(frames.daily?.rows) ? frames.daily.rows : [];
          const weeklyRows = Array.isArray(frames.weekly?.rows) ? frames.weekly.rows : [];
          const monthlyRows = Array.isArray(frames.monthly?.rows) ? frames.monthly.rows : [];
          const dailyMessage = frames.daily ? null : "daily_response_invalid";
          const weeklyMessage = frames.weekly ? null : "weekly_response_invalid";
          const monthlyMessage = frames.monthly ? null : "monthly_response_invalid";
          setMainChartPendingSwap(false);
          setDailyData(dailyRows);
          setWeeklyData(weeklyRows);
          setMonthlyData(monthlyRows);
          setBoxes(Array.isArray(frames.monthly?.boxes) ? frames.monthly.boxes : []);
          setHasMoreDaily(dailyRows.length >= dailyLimit);
          setHasMoreMonthly(monthlyRows.length >= monthlyLimit);
          setDailyErrors(dailyMessage ? [dailyMessage] : []);
          setWeeklyErrors(weeklyMessage ? [weeklyMessage] : []);
          setMonthlyErrors(monthlyMessage ? [monthlyMessage] : []);
          setDailyFetch({
            status: dailyMessage ? "error" : "success",
            responseCount: dailyRows.length,
            errorMessage: dailyMessage,
          });
          setWeeklyFetch({
            status: weeklyMessage ? "error" : "success",
            responseCount: weeklyRows.length,
            errorMessage: weeklyMessage,
          });
          setMonthlyFetch({
            status: monthlyMessage ? "error" : "success",
            responseCount: monthlyRows.length,
            errorMessage: monthlyMessage,
          });
          setDetailDataFreshnessContract(frames.dataFreshnessContract ?? null);
        }
      } catch (error) {
        if (!active || isCanceledRequestError(error)) return;
        const message = describeDetailBarsFetchError(error);
        setMainChartPendingSwap(false);
        setDailyErrors([message]);
        setWeeklyErrors([message]);
        setMonthlyErrors([message]);
        setDailyFetch((prev) => ({
          status: "error",
          responseCount: prev.responseCount,
          errorMessage: message
        }));
        setWeeklyFetch((prev) => ({
          status: "error",
          responseCount: prev.responseCount,
          errorMessage: message
        }));
        setMonthlyFetch((prev) => ({
          status: "error",
          responseCount: prev.responseCount,
          errorMessage: message
        }));
      } finally {
        if (!active) return;
        setLoadingDaily(false);
        setLoadingMonthly(false);
        rangeSettleRef.current = Date.now() + RANGE_SETTLE_MS;
      }
    };
    void (async () => {
      let cachedApplied = hasSeed;
      try {
        const cached = await loadDetailChartPrefetch(requestParams);
        if (active) {
          cachedApplied = applyFrames(cached) || cachedApplied;
        }
      } catch {
        // 永続キャッシュの読み込み失敗は network refresh で補う。
      }
      if (!active) return;
      if (cachedApplied) {
        refreshTimerId = window.setTimeout(() => {
          refreshTimerId = null;
          void runNetworkRefresh();
        }, DETAIL_CACHE_REFRESH_DELAY_MS);
        return;
      }
      await runNetworkRefresh();
    })();
    return () => {
      active = false;
      controller.abort();
      if (refreshTimerId != null) {
        window.clearTimeout(refreshTimerId);
      }
    };
  }, [backendReady, code, dailyLimit, focusPanel, mainAsOf, monthlyLimit]);

  useEffect(() => {
    if (!analysisFetchEnabled) {
      setRouteReadyPhase("chart");
      return;
    }
    if (routeReadyPhase === "analysis") return;
    if (dailyFetch.status !== "success" && dailyFetch.status !== "error") return;
    startTransition(() => {
      setRouteReadyPhase("analysis");
    });
  }, [analysisFetchEnabled, dailyFetch.status, routeReadyPhase]);

  useEffect(() => {
    if (!backendReady) return;
    if (!compareCode) return;
    const requestParams = {
      code: compareCode,
      dailyLimit: compareDailyLimit,
      weeklyLimit: DEFAULT_LIMITS.weekly,
      monthlyLimit,
      asof: compareAsOf,
      forwardBars: compareAsOf
        ? { daily: COMPARE_FORWARD_DAILY_BARS, monthly: COMPARE_FORWARD_MONTHLY_BARS }
        : undefined,
    };
    const applyCompareFrames = (frames: ChartPrefetchFrames) => {
      if (!hasCompleteDetailChartPrefetch(frames, COMPARE_DETAIL_PREFETCH_TIMEFRAMES)) return false;
      setCompareDailyLoading(false);
      setCompareLoading(false);
      setCompareChartPendingSwap(false);
      setCompareDailyErrors([]);
      setCompareMonthlyErrors([]);
      setCompareDailyData(frames.daily.rows);
      setCompareMonthlyData(frames.monthly.rows);
      setCompareBoxes(frames.monthly.boxes);
      rangeSettleRef.current = Date.now() + RANGE_SETTLE_MS;
      return true;
    };
    const prefetched = readDetailChartPrefetchSync(requestParams);
    const hasSeed = applyCompareFrames(prefetched);
    let active = true;
    let refreshTimerId: number | null = null;
    const controller = new AbortController();
    if (!hasSeed) {
      setCompareDailyLoading(true);
      setCompareLoading(true);
      setCompareDailyErrors([]);
      setCompareMonthlyErrors([]);
    }
    const runCompareNetworkRefresh = async () => {
      try {
        const frames = await prefetchDetailChartFrames(requestParams, {
          signal: controller.signal,
          timeframes: COMPARE_DETAIL_PREFETCH_TIMEFRAMES,
        });
        if (!active) return;
        const applied = applyCompareFrames(frames);
        if (!applied) {
          setCompareDailyErrors(frames.daily ? [] : ["daily_response_invalid"]);
          setCompareMonthlyErrors(frames.monthly ? [] : ["monthly_response_invalid"]);
        }
      } catch (error) {
        if (!active || isCanceledRequestError(error)) return;
        const message = error?.message || "Bars fetch failed";
        setCompareChartPendingSwap(false);
        setCompareDailyErrors([message]);
        setCompareMonthlyErrors([message]);
      } finally {
        if (!active) return;
        setCompareDailyLoading(false);
        setCompareLoading(false);
        rangeSettleRef.current = Date.now() + RANGE_SETTLE_MS;
      }
    };
    void (async () => {
      let cachedApplied = hasSeed;
      try {
        const cached = await loadDetailChartPrefetch(requestParams, COMPARE_DETAIL_PREFETCH_TIMEFRAMES);
        if (active) {
          cachedApplied = applyCompareFrames(cached) || cachedApplied;
        }
      } catch {
        // 永続キャッシュの読み込み失敗は network refresh で補う。
      }
      if (!active) return;
      if (cachedApplied) {
        refreshTimerId = window.setTimeout(() => {
          refreshTimerId = null;
          void runCompareNetworkRefresh();
        }, DETAIL_CACHE_REFRESH_DELAY_MS);
        return;
      }
      await runCompareNetworkRefresh();
    })();
    return () => {
      active = false;
      controller.abort();
      if (refreshTimerId != null) {
        window.clearTimeout(refreshTimerId);
      }
    };
  }, [backendReady, compareAsOf, compareCode, compareDailyLimit, monthlyLimit]);

  useEffect(() => {
    if (!compareCode) return;
    setFocusPanel(null);
  }, [compareCode]);
  useEffect(() => {
    if (compareCode) return;
    setMaEditMode("main");
  }, [compareCode]);

  useEffect(() => {
    if (!backendReady || !code || !secondaryFetchStableReady) return;
    const cached = tradesCache.get(code);
    setTradeErrors([]);
    if (cached) {
      setTrades(cached.events);
      setTradeWarnings(cached.warnings);
      setCurrentPositionsFromApi(cached.currentPositions);
    } else {
      setTradeWarnings({ items: [] });
      setCurrentPositionsFromApi(null);
      setTrades([]);
    }
    let cancelled = false;
    let startTimerId: number | null = null;
    let retryTimerId: number | null = null;
    let controller: AbortController | null = null;
    const fetchTrades = (attempt: number) => {
      controller?.abort();
      controller = new AbortController();
      void api
        .get(`/trades/${code}`, { signal: controller.signal })
        .then((res) => {
          if (cancelled) return;
          const payload = res.data as TradesResponsePayload;
          if (!payload || !Array.isArray(payload.events)) {
            throw new Error("Trades response is invalid");
          }
          const nextWarnings = normalizeWarnings(payload.warnings);
          const nextErrors = Array.isArray(payload.errors) ? payload.errors : [];
          const nextCurrentPositions = Array.isArray(payload.currentPositions) ? payload.currentPositions : null;
          tradesCache.set(code, {
            events: payload.events ?? [],
            warnings: nextWarnings,
            errors: nextErrors,
            currentPositions: nextCurrentPositions,
            fetchedAt: Date.now(),
          });
          setTrades(payload.events ?? []);
          setCurrentPositionsFromApi(nextCurrentPositions);
          setTradeWarnings(nextWarnings);
          setTradeErrors(nextErrors);
        })
        .catch((error) => {
          if (cancelled) return;
          if (isCanceledRequestError(error)) return;
          if (isRetryableTradesError(error)) {
            if (prioritizeTradesFetch && attempt < 2) {
              retryTimerId = window.setTimeout(() => {
                retryTimerId = null;
                fetchTrades(attempt + 1);
              }, getRetryDelayMs(error));
            }
            const retryableMessage = cached
              ? "建玉データを再接続中です。前回取得済みの内容を表示しています。"
              : "建玉データを再接続中です。";
            setTradeWarnings({ items: [], info: [retryableMessage] });
            setTradeErrors([]);
            if (!cached) {
              setTrades([]);
              setCurrentPositionsFromApi(null);
            }
            return;
          }
          const message = error?.message || "Trades fetch failed";
          setTradeErrors([message]);
          if (!cached) {
            setTrades([]);
            setTradeWarnings({ items: [] });
            setCurrentPositionsFromApi(null);
          }
        });
    };
    startTimerId = window.setTimeout(
      () => {
        if (!cancelled) fetchTrades(0);
      },
      prioritizeTradesFetch ? SECONDARY_FETCH_STABLE_DELAY_MS : TRADES_FETCH_DELAY_MS,
    );
    return () => {
      cancelled = true;
      if (startTimerId != null) {
        window.clearTimeout(startTimerId);
      }
      controller?.abort();
      if (retryTimerId != null) {
        window.clearTimeout(retryTimerId);
      }
    };
  }, [backendReady, code, prioritizeTradesFetch, secondaryFetchStableReady]);


  useEffect(() => {
    if (!backendReady || !compareCode || !secondaryFetchStableReady) return;
    const cached = tradesCache.get(compareCode);
    if (cached) {
      setCompareTrades(cached.events);
    } else {
      setCompareTrades([]);
    }
    let cancelled = false;
    let startTimerId: number | null = null;
    let retryTimerId: number | null = null;
    let controller: AbortController | null = null;
    const fetchCompareTrades = (attempt: number) => {
      controller?.abort();
      controller = new AbortController();
      void api
        .get(`/trades/${compareCode}`, { signal: controller.signal })
        .then((res) => {
          if (cancelled) return;
          const payload = res.data as TradesResponsePayload;
          if (!payload || !Array.isArray(payload.events)) {
            throw new Error("Trades response is invalid");
          }
          const nextWarnings = normalizeWarnings(payload.warnings);
          const nextErrors = Array.isArray(payload.errors) ? payload.errors : [];
          const nextCurrentPositions = Array.isArray(payload.currentPositions) ? payload.currentPositions : null;
          tradesCache.set(compareCode, {
            events: payload.events ?? [],
            warnings: nextWarnings,
            errors: nextErrors,
            currentPositions: nextCurrentPositions,
            fetchedAt: Date.now(),
          });
          setCompareTrades(payload.events ?? []);
        })
        .catch((error) => {
          if (cancelled) return;
          if (isCanceledRequestError(error)) return;
          if (isRetryableTradesError(error)) {
            if (prioritizeTradesFetch && attempt < 2) {
              retryTimerId = window.setTimeout(() => {
                retryTimerId = null;
                fetchCompareTrades(attempt + 1);
              }, getRetryDelayMs(error));
            }
            if (!cached) {
              setCompareTrades([]);
            }
            return;
          }
          if (!cached) {
            setCompareTrades([]);
          }
        });
    };
    startTimerId = window.setTimeout(
      () => {
        if (!cancelled) fetchCompareTrades(0);
      },
      prioritizeTradesFetch ? SECONDARY_FETCH_STABLE_DELAY_MS : TRADES_FETCH_DELAY_MS,
    );
    return () => {
      cancelled = true;
      if (startTimerId != null) {
        window.clearTimeout(startTimerId);
      }
      controller?.abort();
      if (retryTimerId != null) {
        window.clearTimeout(retryTimerId);
      }
    };
  }, [backendReady, compareCode, prioritizeTradesFetch, secondaryFetchStableReady]);

  const dailyParse = sharedDailyParse;
  const weeklyParse = useMemo(() => buildCandlesWithStats(weeklyData), [weeklyData]);
  const monthlyParse = useMemo(() => buildCandlesWithStats(monthlyData), [monthlyData]);
  const compareDailyParse = useMemo(() => buildCandlesWithStats(compareDailyData), [compareDailyData]);
  const compareMonthlyParse = useMemo(
    () => buildCandlesWithStats(compareMonthlyData),
    [compareMonthlyData]
  );
  const dailyCandles = useMemo(
    () => filterCandlesByAsOf(dailyParse.candles, chartAsOfTime),
    [chartAsOfTime, dailyParse.candles]
  );
  const maRoleChartMarkers = useMemo(() => {
    const markers = Array.isArray(maRoleReview?.chart_markers) ? maRoleReview.chart_markers : [];
    return markers
      .map((marker: any) => {
        const time = normalizeTime(marker.date);
        if (!Number.isFinite(time)) return null;
        const markerTime = findNearestCandleTime(dailyCandles, time);
        if (markerTime == null || Math.abs(markerTime - time) > MAX_EVENT_OFFSET_SEC) return null;
        return {
          time: markerTime,
          kind: "ranking-up" as const,
          label: marker.label || "MA",
        };
      })
      .filter(Boolean);
  }, [dailyCandles, maRoleReview]);
  const dailyMaLines = useMemo(() => {
    return buildDetailMaLines(dailyCandles, maSettings.daily);
  }, [dailyCandles, maSettings.daily]);
  const annotationAsOfDate = useMemo(() => {
    if (mainAsOfTime != null) return toAnnotationDate(mainAsOfTime);
    if (detailAsOfTime != null) return toAnnotationDate(detailAsOfTime);
    if (selectedDate) return selectedDate;
    const lastDaily = dailyCandles[dailyCandles.length - 1]?.time;
    return toAnnotationDate(lastDaily);
  }, [dailyCandles, detailAsOfTime, mainAsOfTime, selectedDate]);
  const selectedAnnotation = useMemo(
    () => annotations.find((annotation) => annotation.id === selectedAnnotationId) ?? null,
    [annotations, selectedAnnotationId]
  );
  const selectedBarForChartReading = useMemo(() => {
    if (!selectedBarData) return null;
    return {
      date: toAnnotationDate(selectedBarData.time),
      time: selectedBarData.time,
      open: selectedBarData.open,
      high: selectedBarData.high,
      low: selectedBarData.low,
      close: selectedBarData.close,
      volume: null,
    };
  }, [selectedBarData]);
  const chartReadingNoteDate = useMemo(() => {
    if (readingTargetType === "bar" && selectedBarForChartReading?.date) {
      return selectedBarForChartReading.date;
    }
    return annotationAsOfDate;
  }, [annotationAsOfDate, readingTargetType, selectedBarForChartReading?.date]);
  const handleSelectDrawing = useCallback((drawing: SelectedDrawingInfo | null) => {
    setSelectedDrawing(drawing);
    if (drawing?.annotationId) {
      setSelectedAnnotationId(drawing.annotationId);
    }
  }, []);
  const visibleAnnotations = useMemo(() => {
    if (annotationFilter === "hidden") return [];
    return annotations.filter(
      (annotation) =>
        annotationFilter === "all" ||
        annotation.object_type === annotationFilter ||
        (annotationFilter === "context" &&
          (annotation.object_type === "chart_context" || annotation.payload?.context_type === "environment"))
    );
  }, [annotationFilter, annotations]);
  const annotationDrawBoxes = useMemo(
    () => visibleAnnotations.map(annotationToDrawBox).filter(Boolean),
    [visibleAnnotations]
  );
  const annotationHorizontalLines = useMemo(
    () => visibleAnnotations.map(annotationToHorizontalLine).filter(Boolean),
    [visibleAnnotations]
  );
  const annotationEventMarkers = useMemo(
    () => visibleAnnotations.map(annotationToEventMarker).filter(Boolean),
    [visibleAnnotations]
  );
  const annotationCallouts = useMemo(
    () =>
      visibleAnnotations
        .map((annotation) => annotationToCallout(annotation, annotation.id === selectedAnnotationId))
        .filter(Boolean),
    [selectedAnnotationId, visibleAnnotations]
  );
  const resolveIndicatorCalloutAnchor = useCallback(
    (candleTime: number, clickedPrice: number | null | undefined) => {
      if (!Number.isFinite(clickedPrice)) return null;
      const supported = new Map([
        [7, "ma7"],
        [20, "ma20"],
        [60, "ma60"],
        [100, "ma100"],
      ]);
      let best: { target: string; price: number; distance: number } | null = null;
      dailyMaLines.forEach((line) => {
        const target = supported.get(Number(line.period));
        if (!target || line.visible === false) return;
        const point = (line.data ?? []).find((item) => item.time === candleTime);
        const price = Number(point?.value);
        if (!Number.isFinite(price)) return;
        const distance = Math.abs(price - clickedPrice!);
        if (!best || distance < best.distance) {
          best = { target, price, distance };
        }
      });
      if (!best) return null;
      const tolerance = Math.max(120, Math.abs(best.price) * 0.012);
      if (best.distance <= tolerance) return best;
      const ma20 = dailyMaLines
        .find((line) => Number(line.period) === 20 && line.visible !== false)
        ?.data?.find((item) => item.time === candleTime);
      const ma20Price = Number(ma20?.value);
      return Number.isFinite(ma20Price) ? { target: "ma20", price: ma20Price, distance: Math.abs(ma20Price - clickedPrice!) } : null;
    },
    [dailyMaLines]
  );
  const resolveSelectedShapeCalloutAnchor = useCallback(() => {
    if (selectedAnnotation?.object_type === "region") {
      const payload = selectedAnnotation.payload ?? {};
      const start = Number(payload.start_time);
      const end = Number(payload.end_time);
      const low = Number(payload.price_low);
      const high = Number(payload.price_high);
      if ([start, end, low, high].every(Number.isFinite)) {
        return {
          time: Math.round((start + end) / 2),
          date: toAnnotationDate(Math.round((start + end) / 2)),
          price: (low + high) / 2,
          anchorType: "region" as const,
          anchorTarget: "region",
          anchorObjectId: selectedAnnotation.id,
          resolution: "annotation_id" as const,
          linkedObject: {
            object_type: "region",
            annotation_id: selectedAnnotation.id,
            resolution: "annotation_id",
          },
        };
      }
    }
    if (selectedAnnotation?.object_type === "line") {
      const payload = selectedAnnotation.payload ?? {};
      const price = Number(payload.price);
      if (Number.isFinite(price)) {
        return {
          time: Number.isFinite(Number(payload.start_time)) ? Number(payload.start_time) : Number(String(annotationAsOfDate ?? "").replaceAll("-", "")),
          date: annotationAsOfDate,
          price,
          anchorType: "line" as const,
          anchorTarget: payload.line_type || "line",
          anchorObjectId: selectedAnnotation.id,
          resolution: "annotation_id" as const,
          linkedObject: {
            object_type: "line",
            annotation_id: selectedAnnotation.id,
            resolution: "annotation_id",
          },
        };
      }
    }
    if (readingTargetType === "region" && selectedDrawing?.kind === "drawBox") {
      const startTime = Math.min(selectedDrawing.startTime, selectedDrawing.endTime);
      const endTime = Math.max(selectedDrawing.startTime, selectedDrawing.endTime);
      const priceLow = Math.min(selectedDrawing.topPrice, selectedDrawing.bottomPrice);
      const priceHigh = Math.max(selectedDrawing.topPrice, selectedDrawing.bottomPrice);
      const centerTime = Math.round((startTime + endTime) / 2);
      return {
        time: centerTime,
        date: toAnnotationDate(centerTime),
        price: (priceLow + priceHigh) / 2,
        anchorType: "region" as const,
        anchorTarget: "region",
        resolution: "payload_fallback" as const,
        linkedObject: {
          object_type: "region",
          resolution: "payload_fallback",
          date_start: toAnnotationDate(startTime),
          date_end: toAnnotationDate(endTime),
          price_low: priceLow,
          price_high: priceHigh,
        },
      };
    }
    if (readingTargetType === "line" && selectedDrawing?.kind === "horizontalLine") {
      return {
        time: Number(String(annotationAsOfDate ?? "").replaceAll("-", "")),
        date: annotationAsOfDate,
        price: selectedDrawing.price,
        anchorType: "line" as const,
        anchorTarget: "line",
        resolution: "payload_fallback" as const,
        linkedObject: {
          object_type: "line",
          resolution: "payload_fallback",
          price: selectedDrawing.price,
        },
      };
    }
    return null;
  }, [annotationAsOfDate, readingTargetType, selectedAnnotation, selectedDrawing]);
  const dailyAnnotationDrawTool: DrawTool | null = annotationMode
    ? annotationTool === "region"
      ? "drawBox"
      : annotationTool === "line"
        ? "horizontalLine"
        : null
    : activeDrawTool;

  const fetchAnnotations = useCallback(async () => {
    if (!code || !annotationAsOfDate) return;
    setAnnotationsLoading(true);
    try {
      const response = await withAnnotationApiRetry(() =>
        api.get("/chart-annotations", {
          params: { code, as_of_date: annotationAsOfDate },
        })
      );
      const nextAnnotations = Array.isArray(response.data?.items) ? response.data.items : [];
      setAnnotations(nextAnnotations);
      setSelectedAnnotationId((current) =>
        current && nextAnnotations.some((annotation: ChartAnnotation) => annotation.id === current)
          ? current
          : nextAnnotations[0]?.id ?? null
      );
    } finally {
      setAnnotationsLoading(false);
    }
  }, [annotationAsOfDate, code]);

  useEffect(() => {
    if (!annotationMode) return;
    void fetchAnnotations();
  }, [annotationMode, fetchAnnotations]);

  const fetchChartReadingNote = useCallback(async () => {
    if (!code || !chartReadingNoteDate) return;
    const response = await withAnnotationApiRetry(() =>
      api.get("/chart-reading/bundle", {
        params: { code, as_of_date: chartReadingNoteDate },
      })
    );
    const notes = Array.isArray(response.data?.notes) ? response.data.notes : [];
    setMaRoleReview(response.data?.ma_role_review ?? null);
    const note =
      notes.find((item) => item.as_of_date === chartReadingNoteDate && item.timeframe === readingTimeframe) ??
      notes.find((item) => item.as_of_date === chartReadingNoteDate) ??
      null;
    setReadingNoteText(note?.note_text ?? "");
    setReadingTagsText(Array.isArray(note?.tags) ? note.tags.join(", ") : "");
  }, [chartReadingNoteDate, code, readingTimeframe]);

  useEffect(() => {
    if (!annotationMode) return;
    void fetchChartReadingNote();
  }, [annotationMode, fetchChartReadingNote]);

  useEffect(() => {
    if (!code || !annotationAsOfDate) return;
    let cancelled = false;
    api.get("/chart-reading/bundle", {
      params: { code, as_of_date: annotationAsOfDate },
    }).then((response) => {
      if (!cancelled) setMaRoleReview(response.data?.ma_role_review ?? null);
    }).catch(() => {
      if (!cancelled) {
        setMaRoleReview({
          schema_version: "ma_role_readonly_review_v1",
          available: false,
          reason: "bundle_unavailable",
          matches: [],
          read_only: true,
          ranking_effect: false,
          automatic_trade_action: false,
        });
      }
    });
    return () => {
      cancelled = true;
    };
  }, [annotationAsOfDate, code]);

  const fetchChartNote = useCallback(async () => {
    if (!code || !annotationAsOfDate) return;
    const response = await withAnnotationApiRetry(() =>
      api.get("/chart-reading/bundle", {
        params: { code, as_of_date: annotationAsOfDate },
      })
    );
    const notes = Array.isArray(response.data?.notes) ? response.data.notes : [];
    const note =
      notes.find((item) => item.as_of_date === annotationAsOfDate && item.timeframe === "mixed") ??
      notes.find((item) => item.as_of_date === annotationAsOfDate && Array.isArray(item.paragraphs) && item.paragraphs.length) ??
      null;
    setChartNoteTitle(note?.title ?? "");
    setChartNoteTimeframe((note?.timeframe as ChartNoteTimeframe) || "mixed");
    setChartNoteParagraphs(
      Array.isArray(note?.paragraphs) && note.paragraphs.length
        ? note.paragraphs
        : [
            {
              paragraph_id: "p1",
              order: 1,
              text: "",
              comment_type: "daily_bar_reading",
              linked_objects: [],
              reason_tags: [],
              no_lookahead: true,
            },
          ]
    );
  }, [annotationAsOfDate, code]);

  useEffect(() => {
    if (!annotationMode || !chartNoteExpanded) return;
    void fetchChartNote();
  }, [annotationMode, chartNoteExpanded, fetchChartNote]);

  const persistAnnotation = useCallback(async (annotation: ChartAnnotation) => {
    const response = await withAnnotationApiRetry(() =>
      api.put(`/chart-annotations/${annotation.id}`, {
        code: annotation.code,
        as_of_date: annotation.as_of_date,
        object_type: annotation.object_type,
        timeframe: annotation.timeframe,
        payload: annotation.payload ?? {},
        tags: annotation.tags ?? [],
        no_lookahead: annotation.no_lookahead !== false,
      })
    );
    const saved = response.data?.annotation ?? annotation;
    setAnnotations((current) => current.map((item) => (item.id === saved.id ? saved : item)));
    setSelectedAnnotationId(saved.id);
  }, []);

  const createAndPersistAnnotation = useCallback(
    async (
      objectType: ChartAnnotationType,
      payload: Record<string, any>,
      tags: string[] = [],
      timeframe: ReadingTimeframe | "daily" | "weekly" | "monthly" | "environment" = "daily"
    ) => {
      if (!code || !annotationAsOfDate) return null;
      const response = await withAnnotationApiRetry(() =>
        api.post("/chart-annotations", {
          code,
          as_of_date: annotationAsOfDate,
          timeframe,
          object_type: objectType,
          payload,
          tags,
          no_lookahead: payload.no_lookahead !== false,
        })
      );
      const saved = response.data?.annotation ?? null;
      if (saved) {
        setAnnotations((current) => [...current.filter((item) => item.id !== saved.id), saved]);
        setSelectedAnnotationId(saved.id);
      }
      return saved;
    },
    [annotationAsOfDate, code]
  );

  const buildAnnotationPayload = useCallback(
    (objectType: ChartAnnotationType, patch: Record<string, any>) => ({
      ...emptyAnnotationPayload(objectType),
      timeframe: "daily",
      code,
      as_of_date: annotationAsOfDate,
      ...patch,
      no_lookahead: patch.no_lookahead ?? true,
    }),
    [annotationAsOfDate, code]
  );

  const handleAnnotationChange = useCallback(
    (annotation: ChartAnnotation) => {
      setAnnotations((current) => current.map((item) => (item.id === annotation.id ? annotation : item)));
      void persistAnnotation(annotation);
    },
    [persistAnnotation]
  );

  const handleAnnotationDelete = useCallback(async (annotation: ChartAnnotation) => {
    await withAnnotationApiRetry(() => api.delete(`/chart-annotations/${annotation.id}`));
    setAnnotations((current) => current.filter((item) => item.id !== annotation.id));
    setSelectedAnnotationId((current) => (current === annotation.id ? null : current));
  }, []);

  const handleSaveChartReadingNote = useCallback(async (options?: { allowDelete?: boolean }) => {
    if (!code || !chartReadingNoteDate) return;
    const nextNoteText = readingNoteText.trim();
    const nextTags = parseTagsInput(readingTagsText);
    if (!options?.allowDelete && !nextNoteText && nextTags.length === 0) return;
    setReadingSaving(true);
    try {
      const linkedObjects: Array<Record<string, unknown>> = [
        {
          object_type: readingTargetType,
          comment_type: readingCommentType,
          resolution: "note_target",
        },
      ];
      if (readingTargetType === "bar" && selectedBarForChartReading) {
        const linkedBar: Record<string, unknown> = {
          object_type: "bar",
          timeframe: "daily",
          bar_date: selectedBarForChartReading.date,
          bar_time: selectedBarForChartReading.time,
          open: selectedBarForChartReading.open,
          high: selectedBarForChartReading.high,
          low: selectedBarForChartReading.low,
          close: selectedBarForChartReading.close,
          volume: selectedBarForChartReading.volume,
          comment_type: readingCommentType,
          resolution: "selected_bar",
        };
        if (
          selectedAnnotation?.object_type === "bar" &&
          Number(selectedAnnotation.payload?.bar_time) === selectedBarForChartReading.time
        ) {
          linkedBar.annotation_id = selectedAnnotation.id;
          linkedBar.resolution = "annotation_id";
        }
        linkedObjects.push(linkedBar);
      }
      await withAnnotationApiRetry(() =>
        api.put("/chart-notes", {
          code,
          as_of_date: chartReadingNoteDate,
          timeframe: readingTimeframe,
          note_text: nextNoteText,
          tags: nextTags,
          linked_objects: linkedObjects,
          no_lookahead: true,
        })
      );
    } finally {
      setReadingSaving(false);
    }
  }, [
    chartReadingNoteDate,
    code,
    readingCommentType,
    readingNoteText,
    readingTagsText,
    readingTargetType,
    readingTimeframe,
    selectedAnnotation,
    selectedBarForChartReading,
  ]);

  useEffect(() => {
    if (!annotationMode || !chartReadingNoteDate) return;
    if (!readingNoteText.trim() && parseTagsInput(readingTagsText).length === 0) return;
    const timer = window.setTimeout(() => {
      void handleSaveChartReadingNote();
    }, 700);
    return () => window.clearTimeout(timer);
  }, [annotationMode, chartReadingNoteDate, handleSaveChartReadingNote, readingNoteText, readingTagsText]);

  const handleClearChartReadingNote = useCallback(() => {
    setReadingNoteText("");
    setReadingTagsText("");
    void handleSaveChartReadingNote({ allowDelete: true });
  }, [handleSaveChartReadingNote]);

  const handleAddChartNoteParagraph = useCallback(() => {
    setChartNoteParagraphs((current) => [
      ...current,
      {
        paragraph_id: `p${current.length + 1}`,
        order: current.length + 1,
        text: "",
        comment_type: "review",
        linked_objects: [],
        reason_tags: [],
        no_lookahead: true,
      },
    ]);
  }, []);

  const handleChartNoteParagraphChange = useCallback((paragraph: ChartNoteParagraph) => {
    setChartNoteParagraphs((current) =>
      current.map((item) => (item.paragraph_id === paragraph.paragraph_id ? paragraph : item))
    );
  }, []);

  const appendLinkToParagraph = useCallback((paragraphId: string, link: ChartNoteLinkedObject) => {
    setChartNoteParagraphs((current) =>
      current.map((paragraph) =>
        paragraph.paragraph_id === paragraphId
          ? {
              ...paragraph,
              linked_objects: [
                ...paragraph.linked_objects.filter(
                  (item) =>
                    !(
                      item.annotation_id === link.annotation_id &&
                      item.anchor_target === link.anchor_target &&
                      item.object_type === link.object_type
                    )
                ),
                link,
              ],
            }
          : paragraph
      )
    );
  }, []);

  const handleLinkSelectedAnnotationToParagraph = useCallback(
    (paragraphId: string) => {
      if (!selectedAnnotation) return;
      const payload = selectedAnnotation.payload ?? {};
      appendLinkToParagraph(paragraphId, {
        object_type: selectedAnnotation.object_type,
        annotation_id: selectedAnnotation.id,
        anchor_type: payload.anchor_type,
        anchor_target: payload.anchor_target,
        resolution: "annotation_id",
      });
    },
    [appendLinkToParagraph, selectedAnnotation]
  );

  const handleLinkMa20ToParagraph = useCallback(
    (paragraphId: string) => {
      appendLinkToParagraph(paragraphId, {
        object_type: "indicator",
        anchor_type: "indicator",
        anchor_target: "ma20",
        resolution: "indicator_anchor",
      });
    },
    [appendLinkToParagraph]
  );

  const handleSaveChartNote = useCallback(async () => {
    if (!code || !annotationAsOfDate) return;
    setChartNoteSaving(true);
    try {
      const normalizedParagraphs = chartNoteParagraphs.map((paragraph, index) => ({
        ...paragraph,
        order: index + 1,
        no_lookahead: paragraph.no_lookahead !== false,
      }));
      await withAnnotationApiRetry(() =>
        api.put("/chart-notes", {
          code,
          as_of_date: annotationAsOfDate,
          timeframe: chartNoteTimeframe,
          title: chartNoteTitle,
          note_text: normalizedParagraphs.map((paragraph) => paragraph.text).filter(Boolean).join("\n"),
          paragraphs: normalizedParagraphs,
          tags: Array.from(new Set(normalizedParagraphs.flatMap((paragraph) => paragraph.reason_tags ?? []))),
          no_lookahead: true,
        })
      );
      await fetchChartNote();
    } finally {
      setChartNoteSaving(false);
    }
  }, [annotationAsOfDate, chartNoteParagraphs, chartNoteTimeframe, chartNoteTitle, code, fetchChartNote]);

  const handleAnnotateSelectedDrawing = useCallback(() => {
    if (!selectedDrawing) return;
    const tags = parseTagsInput(readingTagsText);
    if (selectedDrawing.kind === "drawBox") {
      const startTime = Math.min(selectedDrawing.startTime, selectedDrawing.endTime);
      const endTime = Math.max(selectedDrawing.startTime, selectedDrawing.endTime);
      const priceLow = Math.min(selectedDrawing.topPrice, selectedDrawing.bottomPrice);
      const priceHigh = Math.max(selectedDrawing.topPrice, selectedDrawing.bottomPrice);
      void createAndPersistAnnotation(
        "region",
        buildAnnotationPayload("region", {
          timeframe: readingTimeframe,
          start_time: startTime,
          end_time: endTime,
          date_start: toAnnotationDate(startTime),
          date_end: toAnnotationDate(endTime),
          price_low: priceLow,
          price_high: priceHigh,
          free_text: readingNoteText,
          tags,
          linked_object: {
            object_type: "region",
            resolution: "payload_fallback",
            timeframe: readingTimeframe,
            payload: {
              start_time: startTime,
              end_time: endTime,
              price_low: priceLow,
              price_high: priceHigh,
            },
          },
        }),
        tags,
        readingTimeframe
      );
      return;
    }
    if (selectedDrawing.kind === "horizontalLine") {
      void createAndPersistAnnotation(
        "line",
        buildAnnotationPayload("line", {
          timeframe: readingTimeframe,
          price: selectedDrawing.price,
          date_start: annotationAsOfDate,
          free_text: readingNoteText,
          tags,
          linked_object: {
            object_type: "line",
            resolution: "payload_fallback",
            timeframe: readingTimeframe,
            payload: { price: selectedDrawing.price },
          },
        }),
        tags,
        readingTimeframe
      );
    }
  }, [
    annotationAsOfDate,
    buildAnnotationPayload,
    createAndPersistAnnotation,
    readingNoteText,
    readingTagsText,
    readingTimeframe,
    selectedDrawing,
  ]);

  const handleDailyAddDrawBox = useCallback(
    (box) => {
      if (annotationMode && annotationTool === "region") {
        const startTime = Math.min(box.startTime, box.endTime);
        const endTime = Math.max(box.startTime, box.endTime);
        const priceLow = Math.min(box.topPrice, box.bottomPrice);
        const priceHigh = Math.max(box.topPrice, box.bottomPrice);
        void createAndPersistAnnotation(
          "region",
          buildAnnotationPayload("region", {
            start_time: startTime,
            end_time: endTime,
            date_start: toAnnotationDate(startTime),
            date_end: toAnnotationDate(endTime),
            price_low: priceLow,
            price_high: priceHigh,
          })
        );
        return;
      }
      addDrawBox(dailyDrawingKey)(box);
    },
    [addDrawBox, annotationMode, annotationTool, buildAnnotationPayload, createAndPersistAnnotation, dailyDrawingKey]
  );

  const handleDailyAddHorizontalLine = useCallback(
    (line) => {
      if (annotationMode && annotationTool === "line") {
        void createAndPersistAnnotation(
          "line",
          buildAnnotationPayload("line", {
            price: line.price,
            date_start: annotationAsOfDate,
          })
        );
        return;
      }
      addHorizontalLine(dailyDrawingKey)(line);
    },
    [
      addHorizontalLine,
      annotationAsOfDate,
      annotationMode,
      annotationTool,
      buildAnnotationPayload,
      createAndPersistAnnotation,
      dailyDrawingKey,
    ]
  );
  const detailMarkerRange = useMemo(() => {
    if (!dailyCandles.length) return null;
    return {
      from: toDateKey(dailyCandles[0].time),
      to: toDateKey(dailyCandles[dailyCandles.length - 1].time),
    };
  }, [dailyCandles]);
  useEffect(() => {
    if (!backendReady || !code || !detailMarkerRange || !secondaryFetchStableReady || mainChartPendingSwap || !analysisFetchEnabled) {
      setPersistedSignalEvents([]);
      setPersistedRankingAppearances([]);
      setPersistedMarkersLoading(false);
      return;
    }
    let cancelled = false;
    const abortController = new AbortController();
    setPersistedMarkersLoading(true);
    const timerId = window.setTimeout(() => {
      void (async () => {
        try {
          const response = await api.get("/signal-tracking/markers", {
            params: {
              code,
              from: detailMarkerRange.from,
              to: detailMarkerRange.to,
              logic_version: "latest",
              ranking_logic_version: "latest",
            },
            timeout: 60000,
            signal: abortController.signal,
          });
          if (cancelled) return;
          setPersistedSignalEvents(Array.isArray(response.data?.signal_events) ? response.data.signal_events : []);
          setPersistedRankingAppearances(
            Array.isArray(response.data?.ranking_appearances) ? response.data.ranking_appearances : []
          );
        } catch (error) {
          if (!cancelled) {
            console.error("[detail] persisted markers load failed", error);
            setPersistedSignalEvents([]);
            setPersistedRankingAppearances([]);
          }
        } finally {
          if (!cancelled) {
            setPersistedMarkersLoading(false);
          }
        }
      })();
    }, DETAIL_MARKERS_DELAY_MS);
    return () => {
      cancelled = true;
      abortController.abort();
      window.clearTimeout(timerId);
      setPersistedMarkersLoading(false);
    };
  }, [analysisFetchEnabled, backendReady, code, detailMarkerRange, mainChartPendingSwap, secondaryFetchStableReady]);
  useEffect(() => {
    if (!backendReady || !code || !detailMarkerRange || !showRankingMarkers) {
      setChartRankingAppearances([]);
      return;
    }
    let cancelled = false;
    const abortController = new AbortController();
    const timerId = window.setTimeout(() => {
      void (async () => {
        try {
          const responses = await Promise.all(
            ["active", "completed", "archive"].map((status) =>
              api.get("/ranking-history/appearances", {
                params: {
                  code,
                  from: detailMarkerRange.from,
                  to: detailMarkerRange.to,
                  dir: "up",
                  status,
                  ranking_logic_version: "latest",
                  limit: 500,
                  sort: "recent",
                },
                timeout: 30000,
                signal: abortController.signal,
              })
            )
          );
          if (cancelled) return;
          const rankingItems = responses.flatMap((response) =>
            Array.isArray(response.data?.items) ? response.data.items : []
          );
          try {
            const currentResponse = await api.get("/rankings", {
              params: {
                tf: "D",
                which: "latest",
                dir: "up",
                mode: "trade",
                risk_mode: "balanced",
                limit: 200,
              },
              timeout: 30000,
              signal: abortController.signal,
            });
            const currentItems = Array.isArray(currentResponse.data?.items) ? currentResponse.data.items : [];
            const currentIndex = currentItems.findIndex((item) => String(item?.code ?? "").trim() === code);
            const snapshotAsOf =
              typeof currentResponse.data?.snapshot_as_of === "string"
                ? currentResponse.data.snapshot_as_of
                : null;
            if (currentIndex >= 0 && snapshotAsOf) {
              rankingItems.push({
                appearance_id: `current-ranking:up:${code}:${snapshotAsOf}`,
                code,
                date_iso: snapshotAsOf,
                dir: "up",
                rank: currentIndex + 1,
                status: "current",
              });
            }
          } catch (error) {
            if (!cancelled) {
              console.error("[detail] current ranking marker load failed", error);
            }
          }
          if (cancelled) return;
          setChartRankingAppearances(rankingItems);
        } catch (error) {
          if (!cancelled) {
            console.error("[detail] chart ranking appearances load failed", error);
            setChartRankingAppearances([]);
          }
        }
      })();
    }, DETAIL_MARKERS_DELAY_MS);
    return () => {
      cancelled = true;
      abortController.abort();
      window.clearTimeout(timerId);
    };
  }, [backendReady, code, detailMarkerRange, showRankingMarkers]);
  const monthlyCandles = useMemo(
    () => filterCandlesByAsOf(monthlyParse.candles, chartAsOfTime),
    [chartAsOfTime, monthlyParse.candles]
  );
  const weeklyCandles = useMemo(
    () => filterCandlesByAsOf(weeklyParse.candles, chartAsOfTime),
    [chartAsOfTime, weeklyParse.candles]
  );
  const compareDailyCandles = useMemo(
    () => compareDailyParse.candles,
    [compareDailyParse.candles]
  );
  const compareMonthlyCandles = useMemo(
    () => compareMonthlyParse.candles,
    [compareMonthlyParse.candles]
  );
  const weeklyChromeTerminalDates = useMemo(
    () => buildPeriodTerminalDateMap(dailyCandles, "weekly"),
    [dailyCandles]
  );
  const monthlyChromeTerminalDates = useMemo(
    () => buildPeriodTerminalDateMap(dailyCandles, "monthly"),
    [dailyCandles]
  );
  const compareMonthlyChromeTerminalDates = useMemo(
    () => buildPeriodTerminalDateMap(compareDailyCandles, "monthly"),
    [compareDailyCandles]
  );
  const dailyVolume = useMemo(
    () => filterVolumeByAsOf(buildVolume(dailyData), chartAsOfTime),
    [chartAsOfTime, dailyData]
  );
  const weeklyVolume = useMemo(
    () => filterVolumeByAsOf(buildVolume(weeklyData), chartAsOfTime),
    [chartAsOfTime, weeklyData]
  );
  const monthlyVolume = useMemo(
    () => filterVolumeByAsOf(buildVolume(monthlyData), chartAsOfTime),
    [chartAsOfTime, monthlyData]
  );
  const compareDailyVolume = useMemo(
    () => buildVolume(compareDailyData),
    [compareDailyData]
  );
  const analysisSummaryLoading =
    analysisLoadingText != null ||
    sellAnalysisLoadingText != null;

  const dailyEventMarkers = useMemo<{ time: number; kind: "earnings" | "decision-buy" | "decision-sell" | "decision-neutral" | "tdnet-positive" | "tdnet-negative" | "tdnet-neutral"; label?: string }[]>(() => {
    const markers: { time: number; kind: "earnings" | "decision-buy" | "decision-sell" | "decision-neutral" | "tdnet-positive" | "tdnet-negative" | "tdnet-neutral"; label?: string }[] = [];
    const eventMs = parseEventDateMs(activeTicker?.eventEarningsDate);
    if (eventMs != null && dailyCandles.length > 0) {
      const eventTime = Math.floor(eventMs / 1000);
      const nearestTime = findNearestCandleTime(dailyCandles, eventTime);
      if (nearestTime != null && Math.abs(nearestTime - eventTime) <= MAX_EVENT_OFFSET_SEC) {
        markers.push({ time: nearestTime, kind: "earnings", label: "E" });
      }
    }
    if (showTdnetMarkers && dailyCandles.length > 0) {
      tdnetDisclosures.forEach((item) => {
        if (!item.publishedAt) return;
        const publishedMs = Date.parse(item.publishedAt);
        if (!Number.isFinite(publishedMs)) return;
        const eventTime = Math.floor(publishedMs / 1000);
        const nearestTime = findNearestCandleTime(dailyCandles, eventTime);
        if (nearestTime == null) return;
        if (Math.abs(nearestTime - eventTime) > 5 * 24 * 60 * 60) return;
        const kind =
          item.sentiment === "positive"
            ? "tdnet-positive"
            : item.sentiment === "negative"
              ? "tdnet-negative"
              : "tdnet-neutral";
        const label =
          item.eventType === "forecast_revision"
            ? "予"
            : item.eventType === "dividend_revision"
              ? "配"
              : item.eventType === "share_buyback"
                ? "買"
                : item.eventType === "share_split"
                  ? "分"
                  : "T";
        markers.push({ time: nearestTime, kind, label });
      });
    }
    return markers;
  }, [activeTicker?.eventEarningsDate, dailyCandles, showTdnetMarkers, tdnetDisclosures]);
  const tdnetDisclosureByCandleTime = useMemo(() => {
    const mapped = new Map<number, TdnetDisclosureItem[]>();
    if (!dailyCandles.length) return mapped;
    tdnetDisclosures.forEach((item) => {
      if (!item.publishedAt) return;
      const publishedMs = Date.parse(item.publishedAt);
      if (!Number.isFinite(publishedMs)) return;
      const eventTime = Math.floor(publishedMs / 1000);
      const nearestTime = findNearestCandleTime(dailyCandles, eventTime);
      if (nearestTime == null) return;
      if (Math.abs(nearestTime - eventTime) > 5 * 24 * 60 * 60) return;
      const bucket = mapped.get(nearestTime) ?? [];
      bucket.push(item);
      mapped.set(nearestTime, bucket);
    });
    return mapped;
  }, [dailyCandles, tdnetDisclosures]);
  const activeTdnetDisclosure =
    selectedTdnetDisclosures.length > 0
      ? selectedTdnetDisclosures[
          Math.max(0, Math.min(selectedTdnetDisclosureIndex, selectedTdnetDisclosures.length - 1))
        ] ?? null
      : null;
  const activeTdnetReaction = useMemo(
    () => buildTdnetReactionSummary(dailyCandles, dailyVolume, activeTdnetDisclosure),
    [activeTdnetDisclosure, dailyCandles, dailyVolume]
  );

  const dailyMonthBoundaries = useMemo(() => buildMonthBoundaries(dailyCandles), [dailyCandles]);
  const weeklyMonthBoundaries = useMemo(() => buildMonthBoundaries(weeklyCandles), [weeklyCandles]);
  const monthlyYearBoundaries = useMemo(() => buildYearBoundaries(monthlyCandles), [monthlyCandles]);
  const dailySignalBars = useMemo(
    () => dailyCandles.map((candle) => [candle.time, candle.open, candle.high, candle.low, candle.close]),
    [dailyCandles]
  );
  const dailySignalMetrics = useMemo(
    () => computeSignalMetrics(dailySignalBars),
    [dailySignalBars]
  );
  const dailySignals = dailySignalMetrics.signals;
  const positionData = useMemo(
    () => buildDailyPositions(dailyCandles, trades),
    [dailyCandles, trades]
  );
  const replayPositionData = useMemo(
    () => (showReplayPanel ? buildDailyPositions(dailyCandles, replayRun.tradeEvents) : null),
    [dailyCandles, replayRun.tradeEvents, showReplayPanel]
  );
  const activePositionData = replayPositionData ?? positionData;
  const dailyPositions = activePositionData.dailyPositions;
  const tradeMarkers = activePositionData.tradeMarkers;
  const currentPositions = useMemo(
    () =>
      showReplayPanel
        ? null
        : currentPositionsFromApi !== null
          ? currentPositionsFromApi
          : buildCurrentPositions(trades),
    [currentPositionsFromApi, showReplayPanel, trades]
  );
  const latestTradeTime = useMemo(() => {
    const sourceTrades = showReplayPanel ? replayRun.tradeEvents : trades;
    if (sourceTrades.length === 0) return null;
    const times = sourceTrades
      .map((trade) => Date.parse(`${trade.date}T00:00:00Z`))
      .filter((value) => Number.isFinite(value))
      .map((value) => Math.floor(value / 1000));
    if (!times.length) return null;
    return Math.max(...times);
  }, [showReplayPanel, replayRun.tradeEvents, trades]);
  const comparePositionData = useMemo(
    () => buildDailyPositions(compareDailyCandles, compareTrades),
    [compareDailyCandles, compareTrades]
  );
  const compareDailyPositions = comparePositionData.dailyPositions;
  const compareTradeMarkers = comparePositionData.tradeMarkers;
  const positionLedger = useMemo(() => buildPositionLedger(trades), [trades]);
  const dailyPositionMap = useMemo(() => {
    const map = new Map<string, Map<string, DailyPosition>>();
    dailyPositions.forEach((pos) => {
      const groupKey = pos.brokerGroupKey ?? `${pos.brokerKey ?? "unknown"}|${pos.account ?? ""}`;
      const dateMap = map.get(groupKey) ?? new Map<string, DailyPosition>();
      dateMap.set(pos.date, pos);
      map.set(groupKey, dateMap);
    });
    return map;
  }, [dailyPositions]);
  const ledgerGroups = useMemo(() => {
    const brokerOrder = (key: string) => {
      if (key === "rakuten") return 0;
      if (key === "sbi") return 1;
      if (key === "unknown") return 2;
      return 3;
    };
    const map = new Map<
      string,
      { brokerKey: string; brokerLabel: string; account: string; rows: typeof positionLedger }
    >();
    positionLedger.forEach((row) => {
      const brokerKey = row.brokerKey ?? "unknown";
      const brokerLabel = row.brokerLabel ?? "N/A";
      const account = row.account ?? "";
      const groupKey = `${brokerKey}|${account}`;
      const existing = map.get(groupKey);
      if (existing) {
        existing.rows.push(row);
      } else {
        map.set(groupKey, { brokerKey, brokerLabel, account, rows: [row] });
      }
    });
    return Array.from(map.values()).sort((a, b) => {
      const order = brokerOrder(a.brokerKey) - brokerOrder(b.brokerKey);
      if (order !== 0) return order;
      return `${a.brokerLabel}${a.account}`.localeCompare(`${b.brokerLabel}${b.account}`);
    });
  }, [positionLedger]);
  const ledgerEligible = ledgerGroups.some((group) =>
    group.rows.some((row) => row.realizedPnL !== null || row.price !== null)
  );
  const ledgerIizukaGroups = useMemo(() => {
    return ledgerGroups
      .map((group) => {
        const groupKey = `${group.brokerKey}|${group.account}`;
        const dateMap = new Map<string, typeof group.rows>();
        group.rows.forEach((row) => {
          const dateKey = formatLedgerDate(row.date);
          const list = dateMap.get(dateKey) ?? [];
          list.push(row);
          dateMap.set(dateKey, list);
        });
        const dates = Array.from(dateMap.keys()).sort((a, b) => a.localeCompare(b));
        let prevLong = 0;
        let prevShort = 0;
        let prevRealized = 0;
        const rows = dates.map((date) => {
          const pos = dailyPositionMap.get(groupKey)?.get(date);
          const longLots = pos?.longLots ?? prevLong;
          const shortLots = pos?.shortLots ?? prevShort;
          const realized = pos?.realizedPnL ?? prevRealized;
          const deltaLong = longLots - prevLong;
          const deltaShort = shortLots - prevShort;
          const realizedDelta = realized - prevRealized;
          prevLong = longLots;
          prevShort = shortLots;
          prevRealized = realized;
          const kindSet = new Set<string>();
          (dateMap.get(date) ?? []).forEach((row) => {
            const raw = row.kindLabel?.trim();
            if (!raw) return;
            const lower = raw.toLowerCase();
            if (lower.includes("open") || raw.includes("新規")) {
              kindSet.add("新規");
              return;
            }
            if (lower.includes("close") || raw.includes("決済")) {
              kindSet.add("決済");
              return;
            }
            if (lower.includes("delivery") || raw.includes("現渡")) {
              kindSet.add("現渡");
              return;
            }
            if (lower.includes("take_delivery") || raw.includes("現引")) {
              kindSet.add("現引");
              return;
            }
            if (lower.includes("inbound") || raw.includes("入庫")) {
              kindSet.add("入庫");
              return;
            }
            if (lower.includes("outbound") || raw.includes("出庫")) {
              kindSet.add("出庫");
              return;
            }
            kindSet.add(raw);
          });
          const kindLabel = kindSet.size === 0 ? "--" : Array.from(kindSet).slice(0, 2).join(" / ");
          return {
            date,
            kindLabel,
            deltaLong,
            deltaShort,
            longLots,
            shortLots,
            avgLongPrice: pos?.avgLongPrice ?? null,
            avgShortPrice: pos?.avgShortPrice ?? null,
            realizedDelta
          };
        });
        return { ...group, rows };
      })
      .filter((group) => group.rows.length > 0);
  }, [ledgerGroups, dailyPositionMap]);
  const ledgerStockGroups = useMemo(() => {
    return ledgerGroups
      .map((group) => {
        const groupKey = `${group.brokerKey}|${group.account}`;
        const dateMap = new Map<string, typeof group.rows>();
        group.rows.forEach((row) => {
          const dateKey = formatLedgerDate(row.date);
          const list = dateMap.get(dateKey) ?? [];
          list.push(row);
          dateMap.set(dateKey, list);
        });
        const dates = Array.from(dateMap.keys()).sort((a, b) => a.localeCompare(b));
        let prevLong = 0;
        let prevShort = 0;
        let prevRealized = 0;
        const rows = dates.map((date) => {
          const pos = dailyPositionMap.get(groupKey)?.get(date);
          const longLots = pos?.longLots ?? prevLong;
          const shortLots = pos?.shortLots ?? prevShort;
          const realized = pos?.realizedPnL ?? prevRealized;
          const deltaLong = longLots - prevLong;
          const deltaShort = shortLots - prevShort;
          const realizedDelta = realized - prevRealized;
          prevLong = longLots;
          prevShort = shortLots;
          prevRealized = realized;
          const kindSet = new Set<string>();
          (dateMap.get(date) ?? []).forEach((row) => {
            const raw = row.kindLabel?.trim();
            if (!raw) return;
            const lower = raw.toLowerCase();
            if (lower.includes("open") || raw.includes("新規")) {
              kindSet.add("新規");
              return;
            }
            if (lower.includes("close") || raw.includes("決済")) {
              kindSet.add("決済");
              return;
            }
            if (lower.includes("delivery") || raw.includes("現渡")) {
              kindSet.add("現渡");
              return;
            }
            if (lower.includes("take_delivery") || raw.includes("現引")) {
              kindSet.add("現引");
              return;
            }
            if (lower.includes("inbound") || raw.includes("入庫")) {
              kindSet.add("入庫");
              return;
            }
            if (lower.includes("outbound") || raw.includes("出庫")) {
              kindSet.add("出庫");
              return;
            }
            kindSet.add(raw);
          });
          const kindLabel = kindSet.size === 0 ? "--" : Array.from(kindSet).slice(0, 2).join(" / ");
          const qtyShares = (dateMap.get(date) ?? []).reduce((sum, row) => sum + row.qtyShares, 0);
          return {
            date,
            kindLabel,
            qtyShares,
            deltaSellShares: deltaShort * 100,
            deltaBuyShares: deltaLong * 100,
            closeSellShares: shortLots * 100,
            closeBuyShares: longLots * 100,
            buyAvgPrice: pos?.avgLongPrice ?? null,
            sellAvgPrice: pos?.avgShortPrice ?? null,
            realizedDelta
          };
        });
        return { ...group, rows };
      })
      .filter((group) => group.rows.length > 0);
  }, [ledgerGroups, dailyPositionMap]);
  const handleLedgerViewModeChange = (mode: "iizuka" | "stock") => {
    setLedgerViewMode(mode);
    try {
      window.localStorage.setItem("positionLedgerMode", mode);
    } catch {
      // ignore storage errors
    }
  };
  const handleClosePositionLedger = () => {
    setHeaderMode("chart");
    setPositionLedgerExpanded(false);
  };
  const dailyRangeCount = useMemo(
    () => countInRange(dailyCandles, rangeMonths),
    [dailyCandles, rangeMonths]
  );
  const weeklyRangeCount = useMemo(
    () => countInRange(weeklyCandles, rangeMonths),
    [weeklyCandles, rangeMonths]
  );
  const monthlyRangeCount = useMemo(
    () => countInRange(monthlyCandles, rangeMonths),
    [monthlyCandles, rangeMonths]
  );

  useEffect(() => {
    syncRangesRef.current = syncRanges;
  }, [syncRanges]);

  const dailyInvalidCount =
    dailyParse.stats.invalidRow + dailyParse.stats.invalidTime + dailyParse.stats.invalidValue;
  const weeklyInvalidCount =
    weeklyParse.stats.invalidRow + weeklyParse.stats.invalidTime + weeklyParse.stats.invalidValue;
  const monthlyInvalidCount =
    monthlyParse.stats.invalidRow + monthlyParse.stats.invalidTime + monthlyParse.stats.invalidValue;
  const dailyHasEmpty = dailyFetch.status === "success" && dailyFetch.responseCount === 0;
  const weeklyHasEmpty = weeklyFetch.status === "success" && weeklyFetch.responseCount === 0;
  const monthlyHasEmpty = monthlyFetch.status === "success" && monthlyFetch.responseCount === 0;
  const dailyHasParsedZero = dailyParse.stats.parsed === 0 && dailyParse.stats.total > 0;
  const weeklyHasParsedZero = weeklyParse.stats.parsed === 0 && weeklyParse.stats.total > 0;
  const monthlyHasParsedZero = monthlyParse.stats.parsed === 0 && monthlyParse.stats.total > 0;

  const chartLifecycle = useMemo(
    () =>
      buildDetailChartLifecycle({
        daily: buildDetailChartPanelInput({
          fetch: dailyFetch,
          errors: dailyErrors,
          candleCount: dailyCandles.length,
          parseStats: dailyParse.stats,
          loading: loadingDaily,
          pendingSwap: mainChartPendingSwap,
        }),
        weekly: buildDetailChartPanelInput({
          fetch: weeklyFetch,
          errors: weeklyErrors,
          candleCount: weeklyCandles.length,
          parseStats: weeklyParse.stats,
          loading: loadingDaily,
          pendingSwap: mainChartPendingSwap,
        }),
        monthly: buildDetailChartPanelInput({
          fetch: monthlyFetch,
          errors: monthlyErrors,
          candleCount: monthlyCandles.length,
          parseStats: monthlyParse.stats,
          loading: loadingMonthly,
          pendingSwap: mainChartPendingSwap,
        }),
        contract: detailDataFreshnessContract,
      }),
    [
      dailyFetch,
      dailyErrors,
      dailyCandles.length,
      dailyParse.stats,
      loadingDaily,
      mainChartPendingSwap,
      weeklyFetch,
      weeklyErrors,
      weeklyCandles.length,
      weeklyParse.stats,
      monthlyFetch,
      monthlyErrors,
      monthlyCandles.length,
      monthlyParse.stats,
      loadingMonthly,
      detailDataFreshnessContract,
    ]
  );

  const dailyError =
    chartLifecycle.daily.status === "error" ||
    chartLifecycle.daily.status === "empty" ||
    chartLifecycle.daily.status === "missing"
      ? chartLifecycle.daily.message
      : null;

  const monthlyError =
    chartLifecycle.monthly.status === "error" ||
    chartLifecycle.monthly.status === "empty" ||
    chartLifecycle.monthly.status === "missing"
      ? chartLifecycle.monthly.message
      : null;

  const weeklyError =
    chartLifecycle.weekly.status === "error" ||
    chartLifecycle.weekly.status === "empty" ||
    chartLifecycle.weekly.status === "missing"
      ? chartLifecycle.weekly.message
      : dailyCandles.length === 0
        ? dailyError ?? "表示できるデータがありません"
        : null;
  const tradeWarningItems = useMemo(() => tradeWarnings.items ?? [], [tradeWarnings.items]);
  const marketDataStatusMeta =
    mainAsOf
      ? null
      : (dailyBarsMeta?.panDelayed ? dailyBarsMeta : null) ??
        (monthlyBarsMeta?.panDelayed ? monthlyBarsMeta : null) ??
        dailyBarsMeta ??
        monthlyBarsMeta;
  const marketDataStatusMessage = marketDataStatusMeta?.message ?? null;
  const marketDataStatusDelayed = Boolean(marketDataStatusMeta?.panDelayed);
  const tradeInfoItems = useMemo(() => tradeWarnings.info ?? [], [tradeWarnings.info]);
  const unrecognizedCount = tradeWarnings.unrecognized_labels?.count ?? 0;
  const errors = useMemo(
    () => [...dailyErrors, ...weeklyErrors, ...monthlyErrors, ...tradeErrors],
    [dailyErrors, weeklyErrors, monthlyErrors, tradeErrors]
  );
  const otherWarningsCount = tradeWarningItems.length;
  const infoCount = tradeInfoItems.length;
  const warningCount = errors.length + unrecognizedCount + otherWarningsCount;
  const hasIssues = warningCount > 0 || infoCount > 0;
  const bannerTone = warningCount > 0 ? "warning" : "info";
  const bannerTitle = warningCount > 0 ? "データ確認が必要です" : "データのお知らせ";

  const [debugOpen, setDebugOpen] = useState(false);
  const [showInfoDetails, setShowInfoDetails] = useState(false);
  const [copyFallbackText, setCopyFallbackText] = useState<string | null>(null);

  const debugSummary = useMemo(() => {
    const parts: string[] = [];
    if (errors.length) parts.push(`確認事項 ${errors.length}`);
    if (unrecognizedCount) parts.push(`未確認ラベル ${unrecognizedCount}`);
    if (otherWarningsCount) parts.push(`注意 ${otherWarningsCount}`);
    if (infoCount) parts.push(`補足 ${infoCount}`);
    if (dailyHasEmpty) parts.push("日足データなし");
    if (dailyHasParsedZero) parts.push("日足を読み取れません");
    if (dailyInvalidCount > 0) parts.push(`日足の不整合 ${dailyInvalidCount}`);
    if (weeklyHasEmpty) parts.push("週足データなし");
    if (weeklyHasParsedZero) parts.push("週足を読み取れません");
    if (weeklyInvalidCount > 0) parts.push(`週足の不整合 ${weeklyInvalidCount}`);
    if (monthlyHasEmpty) parts.push("月足データなし");
    if (monthlyHasParsedZero) parts.push("月足を読み取れません");
    if (monthlyInvalidCount > 0) parts.push(`月足の不整合 ${monthlyInvalidCount}`);
    return parts;
  }, [
    errors,
    unrecognizedCount,
    otherWarningsCount,
    infoCount,
    dailyHasEmpty,
    dailyHasParsedZero,
    dailyInvalidCount,
    weeklyHasEmpty,
    weeklyHasParsedZero,
    weeklyInvalidCount,
    monthlyHasEmpty,
    monthlyHasParsedZero,
    monthlyInvalidCount
  ]);

  const tradeInfoLines = useMemo(() => {
    return tradeInfoItems.map((item) => {
      if (item.startsWith("duplicate_skipped:")) {
        const parts = item.split(":");
        const code = parts[1] || "-";
        const count = parts[2] || "0";
        return `取引履歴: 重複 ${count} 件を除外 (${code})`;
      }
      return item;
    });
  }, [tradeInfoItems]);

  const debugLines = useMemo(() => {
    const lines: string[] = [];
    lines.push(
      `日足: 取得 ${dailyFetch.responseCount}件 | 読取 ${dailyParse.stats.parsed}件 | 表示 ${dailyRangeCount}件 | 不整合 ${dailyInvalidCount}件 | 状態 ${dailyError ?? "-"}`
    );
    lines.push(
      `週足: 取得 ${weeklyFetch.responseCount}件 | 読取 ${weeklyParse.stats.parsed}件 | 表示 ${weeklyRangeCount}件 | 不整合 ${weeklyInvalidCount}件 | 状態 ${weeklyError ?? "-"}`
    );
    lines.push(
      `月足: 取得 ${monthlyFetch.responseCount}件 | 読取 ${monthlyParse.stats.parsed}件 | 表示 ${monthlyRangeCount}件 | 不整合 ${monthlyInvalidCount}件 | 状態 ${monthlyError ?? "-"}`
    );
    if (tradeWarningItems.length > 0) {
      lines.push(`取引履歴の注意: ${tradeWarningItems.slice(0, 5).join(", ")}`);
    }
    if (showInfoDetails && tradeInfoLines.length > 0) {
      lines.push(`取引履歴の補足: ${tradeInfoLines.slice(0, 5).join(", ")}`);
    }
    if (tradeWarnings.unrecognized_labels) {
      lines.push(
        `未確認ラベル ${tradeWarnings.unrecognized_labels.count} 件: ${tradeWarnings.unrecognized_labels.samples.join(", ")}`
      );
    }
    if (tradeErrors.length > 0) {
      lines.push(`取引履歴の確認事項: ${tradeErrors.slice(0, 3).join(", ")}`);
    }
    return lines;
  }, [
    dailyFetch.status,
    dailyFetch.responseCount,
    dailyParse.stats.parsed,
    dailyInvalidCount,
    dailyRangeCount,
    dailyError,
    weeklyFetch.status,
    weeklyFetch.responseCount,
    weeklyParse.stats.parsed,
    weeklyInvalidCount,
    weeklyRangeCount,
    weeklyError,
    monthlyFetch.status,
    monthlyFetch.responseCount,
    monthlyParse.stats.parsed,
    monthlyInvalidCount,
    monthlyRangeCount,
    monthlyError,
    tradeWarningItems,
    tradeInfoLines,
    showInfoDetails,
    tradeWarnings.unrecognized_labels,
    tradeErrors
  ]);

  const showShortToast = (message: string) => {
    setToastAction(null);
    setToastMessage(message);
    window.setTimeout(() => {
      setToastMessage((prev) => (prev == message ? null : prev));
    }, 800);
  };

  useEffect(() => {
    if (!backendReady || headerMode === "chart") return;
    let cancelled = false;
    api
      .get("/jobs/ml/status", { timeout: 10000 })
      .then((res) => {
        if (cancelled) return;
        const payload = (res.data ?? {}) as { disabled?: unknown; message?: unknown };
        const disabled = payload?.disabled === true;
        setLegacyAnalysisDisabled(disabled);
        setLegacyAnalysisDisabledReason(
          disabled && typeof payload?.message === "string" && payload.message.trim().length > 0
            ? payload.message
            : null
        );
      })
      .catch(() => {
        if (cancelled) return;
      });
    return () => {
      cancelled = true;
    };
  }, [backendReady, headerMode]);

  const submitUniverseAnalysisPublish = async () => {
    if (!backendReady || analysisRecalcSubmitting != null) {
      return;
    }
    const asOf =
      analysisAsOfTime != null
        ? toDateKey(analysisAsOfTime)
        : dailyCandles.length > 0
          ? toDateKey(dailyCandles[dailyCandles.length - 1].time)
          : null;
    setAnalysisRecalcSubmitting("batch");
    setToastAction(null);
    try {
      const res = await api.post("/jobs/analysis/publish-latest", null, {
        params: asOf != null ? { as_of: asOf } : undefined,
        timeout: 10000,
      });
      const payload = (res.data ?? {}) as {
        ok?: boolean;
        started?: boolean;
        skipped?: boolean;
        reason?: string;
        message?: string;
        job_id?: string;
        jobId?: string;
        error?: string;
      };
      if (payload.ok !== true) {
        throw new Error(typeof payload.error === "string" ? payload.error : "売買判定更新ジョブの開始に失敗しました。");
      }
      if (payload.started === false || payload.skipped === true) {
        setToastMessage(
          typeof payload.message === "string" && payload.message.trim().length > 0
            ? payload.message
            : `as_of=${asOf ?? "latest"} は既に publish 済みです。`
        );
        return;
      }
      setToastMessage(
        `売買判定更新を開始しました。${asOf != null ? `(as_of=${asOf})` : ""}`
      );
    } catch (error: unknown) {
      const response = (error as {
        response?: { status?: number; data?: { error?: unknown; message?: unknown; detail?: unknown } };
        message?: string;
      }).response;
      const detail =
        response?.data?.message ??
        response?.data?.error ??
        response?.data?.detail ??
        (error as { message?: string }).message ??
        "詳細不明";
      if (response?.status === 409) {
        setToastMessage("売買判定更新はすでに実行中です。");
      } else {
        setToastMessage(`売買判定更新の開始に失敗しました。(${String(detail)})`);
      }
    } finally {
      setAnalysisRecalcSubmitting((current) => (current === "batch" ? null : current));
    }
  };

  const submitAnalysisRecalc = async () => {
    if (!backendReady) {
      setToastAction(null);
      setToastMessage("backend 未接続のため売買判定を更新できません。");
      return;
    }
    if (analysisRecalcDisabled) {
      await submitUniverseAnalysisPublish();
      return;
    }
    if (analysisBackfillActive || analysisRecalcSubmitting != null) {
      setToastAction(null);
      setToastMessage("売買判定更新はすでに実行中です。");
      return;
    }

    const targetRange = visibleAnalysisRecalcRange;
    let requestLabel = "";
    let params: Record<string, string | number | boolean> | null = null;
    if (!targetRange) {
      setToastAction(null);
      setToastMessage("再計算範囲を特定できませんでした。");
      return;
    }
    requestLabel = `${targetRange.startLabel} - ${targetRange.endLabel}`;
    params = {
      start_dt: targetRange.startDt,
      end_dt: targetRange.endDt,
      include_sell: true,
      include_phase: false,
      force_recompute: true,
    };

    if (params == null) {
      setToastAction(null);
      setToastMessage("再計算リクエストを作成できませんでした。");
      return;
    }

    setAnalysisRecalcSubmitting("current");
    setToastAction(null);
    try {
      const res = await api.post("/jobs/analysis/backfill-missing", null, {
        params,
        timeout: 10000,
      });
      const payload = (res.data ?? {}) as JobStatusPayload & { ok?: boolean; job_id?: string; jobId?: string };
      if (payload.ok !== true) {
        throw new Error("売買判定更新ジョブの開始に失敗しました。");
      }
      setAnalysisBackfillJob({
        id: typeof payload.job_id === "string" ? payload.job_id : payload.jobId,
        type: "analysis_backfill",
        status: "queued",
        progress: 0,
        message: "解析データを再計算しています。",
      });
      analysisBackfillActiveRef.current = true;
      setToastMessage(`売買判定更新を開始しました。(${requestLabel})`);
    } catch (error: unknown) {
      const response = (error as {
        response?: { status?: number; data?: { error?: unknown; message?: unknown; detail?: unknown } };
        message?: string;
      }).response;
      if (response?.status === 409) {
        setToastMessage("売買判定更新はすでに実行中です。");
      } else if (
        response?.status === 410 &&
        (response?.data?.error === "legacy_analysis_disabled" ||
          typeof response?.data?.message === "string")
      ) {
        const detail =
          response?.data?.message ??
          "Phase 1 では外部 publish 済みの売買判定更新を利用してください。";
        setLegacyAnalysisDisabled(true);
        setLegacyAnalysisDisabledReason(String(detail));
        setToastMessage(String(detail));
      } else {
        const detail =
          response?.data?.message ??
          response?.data?.error ??
          response?.data?.detail ??
          (error as { message?: string }).message ??
          "詳細不明";
        setToastMessage(`売買判定更新の開始に失敗しました。(${String(detail)})`);
      }
    } finally {
      setAnalysisRecalcSubmitting((current) => (current === "current" ? null : current));
    }
  };

  const mainDailyTargetRange = useMemo(
    () => (rangeMonths ? buildRangeFromEndTime(rangeMonths, chartAsOfTime) : null),
    [chartAsOfTime, rangeMonths]
  );
  const mainMonthlyTargetRange = useMemo(
    () => (rangeMonths ? buildRangeFromEndTime(rangeMonths, chartAsOfTime) : null),
    [chartAsOfTime, rangeMonths]
  );
  const dailyVisibleRange = useMemo(() => {
    if (!rangeMonths) return null;
    if (mainDailyTargetRange) {
      return mainDailyTargetRange;
    }
    return buildRange(dailyCandles, rangeMonths);
  }, [dailyCandles, rangeMonths, mainDailyTargetRange]);
  const weeklyVisibleRange = useMemo(
    () => (rangeMonths ? buildRange(weeklyCandles, rangeMonths) : null),
    [weeklyCandles, rangeMonths]
  );
  const monthlyVisibleRange = useMemo(() => {
    if (!rangeMonths) return null;
    if (chartAsOfTime) {
      return buildRangeEndingAt(monthlyCandles, rangeMonths, chartAsOfTime);
    }
    return buildRange(monthlyCandles, rangeMonths);
  }, [chartAsOfTime, monthlyCandles, rangeMonths]);
  const resolvedDailyVisibleRange = rangeMonths ? dailyVisibleRange : manualDailyRangeRef.current;
  const resolvedWeeklyVisibleRange = rangeMonths ? weeklyVisibleRange : manualWeeklyRangeRef.current;
  const resolvedMonthlyVisibleRange = rangeMonths ? monthlyVisibleRange : manualMonthlyRangeRef.current;

  // Cursor mode functions
  const autoPanToBar = useCallback((time: number) => {
    if (overwriteLiveValidationMode) return;
    if (!dailyChartRef.current) return;

    if (!resolvedDailyVisibleRange) return;

    const { from, to } = resolvedDailyVisibleRange;
    if (time < from || time > to) {
      const rangeSize = to - from;
      let newFrom = time - rangeSize / 2;
      let newTo = time + rangeSize / 2;
      const minTime = dailyCandles[0]?.time ?? null;
      const maxTime = dailyCandles[dailyCandles.length - 1]?.time ?? null;
      if (minTime != null && maxTime != null) {
        if (newFrom < minTime) {
          const overflow = minTime - newFrom;
          newFrom += overflow;
          newTo += overflow;
        }
        if (newTo > maxTime) {
          const overflow = newTo - maxTime;
          newFrom -= overflow;
          newTo -= overflow;
        }
        if (newFrom < minTime) {
          newFrom = minTime;
        }
        if (newTo > maxTime) {
          newTo = maxTime;
        }
      }
      dailyChartRef.current.setVisibleRange({ from: newFrom, to: newTo });
    }
  }, [resolvedDailyVisibleRange, dailyCandles, overwriteLiveValidationMode]);

  const updateSelectedBar = useCallback((index: number) => {
    if (index < 0 || index >= dailyCandles.length) return;

    const bar = dailyCandles[index];
    setSelectedBarIndex(index);
    setSelectedBarData(bar);
    setAnalysisCursorTime(bar.time);

    const date = new Date(bar.time * 1000);
    const dateStr = date.toISOString().split("T")[0];
    setSelectedDate(dateStr);

    autoPanToBar(bar.time);
  }, [dailyCandles, autoPanToBar]);

  const moveToPrevDay = useCallback(() => {
    if (selectedBarIndex === null || selectedBarIndex <= 0) return;
    updateSelectedBar(selectedBarIndex - 1);
  }, [selectedBarIndex, updateSelectedBar]);

  const moveToNextDay = useCallback(() => {
    if (selectedBarIndex === null || selectedBarIndex >= dailyCandles.length - 1) return;
    updateSelectedBar(selectedBarIndex + 1);
  }, [selectedBarIndex, dailyCandles.length, updateSelectedBar]);

  const toggleCursorMode = useCallback(() => {
    setCursorMode((prev) => !prev);
    if (!cursorMode && dailyCandles.length > 0) {
      updateSelectedBar(dailyCandles.length - 1);
    }
  }, [cursorMode, dailyCandles.length, updateSelectedBar]);

  // Re-initialize cursor when dailyCandles change (e.g. after stock navigation)
  useEffect(() => {
    if (!cursorMode || dailyCandles.length === 0) return;
    // Already valid selection in current candles?
    if (
      selectedBarIndex != null &&
      selectedBarIndex < dailyCandles.length &&
      selectedBarData != null
    ) {
      const bar = dailyCandles[selectedBarIndex];
      if (bar && bar.time === selectedBarData.time) return; // still valid
    }
    // Try to find the same date in new candles
    if (selectedDate) {
      const targetTime = normalizeTime(selectedDate);
      if (targetTime != null) {
        const idx = findNearestCandleIndex(dailyCandles, targetTime);
        if (idx != null && dailyCandles[idx]?.time === targetTime) {
          updateSelectedBar(idx);
          return;
        }
      }
    }
    // Fallback: select last bar
    updateSelectedBar(dailyCandles.length - 1);
  }, [cursorMode, dailyCandles, selectedBarIndex, selectedBarData, selectedDate, updateSelectedBar]);

  const handleDailyChartClick = (time: number | null, point?: { x: number; y: number; price: number | null } | null) => {
    if (time === null) return;
    const nearestIndex = findNearestCandleIndex(dailyCandles, time);
    if (nearestIndex != null) {
      const candleTime = dailyCandles[nearestIndex]?.time ?? null;
      if (candleTime != null) {
        const tdnetItems = tdnetDisclosureByCandleTime.get(candleTime) ?? [];
        setSelectedTdnetDisclosures(tdnetItems);
        setSelectedTdnetDisclosureIndex(0);
      } else {
        setSelectedTdnetDisclosures([]);
        setSelectedTdnetDisclosureIndex(0);
      }
    } else {
      setSelectedTdnetDisclosures([]);
      setSelectedTdnetDisclosureIndex(0);
    }
    if (nearestIndex != null) {
      updateSelectedBar(nearestIndex);
      if (annotationMode && annotationTool === "bar") {
        const candle = dailyCandles[nearestIndex];
        if (candle) {
          void createAndPersistAnnotation(
            "bar",
            buildAnnotationPayload("bar", {
              bar_time: candle.time,
              bar_date: toAnnotationDate(candle.time),
            })
          );
        }
      }
      if (annotationMode && annotationTool === "callout") {
        const candle = dailyCandles[nearestIndex];
        if (candle) {
          const tags = parseTagsInput(readingTagsText);
          if (!pendingCalloutAnchor) {
            const shapeAnchor =
              readingTargetType === "region" || readingTargetType === "line" ? resolveSelectedShapeCalloutAnchor() : null;
            const indicatorAnchor =
              readingTargetType === "indicator"
                ? resolveIndicatorCalloutAnchor(candle.time, point?.price ?? candle.close)
                : null;
            if (readingTargetType === "indicator" && !indicatorAnchor) {
              return;
            }
            if ((readingTargetType === "region" || readingTargetType === "line") && !shapeAnchor) {
              return;
            }
            setPendingCalloutAnchor({
              time: shapeAnchor?.time ?? candle.time,
              date: shapeAnchor?.date ?? toAnnotationDate(candle.time),
              price:
                shapeAnchor?.price ??
                indicatorAnchor?.price ??
                (Number.isFinite(point?.price) ? point!.price! : candle.close),
              anchorType: shapeAnchor?.anchorType ?? (indicatorAnchor ? "indicator" : "bar"),
              anchorTarget: shapeAnchor?.anchorTarget ?? indicatorAnchor?.target ?? "candle",
              anchorObjectId: shapeAnchor?.anchorObjectId ?? null,
              resolution: shapeAnchor?.resolution,
              linkedObject: shapeAnchor?.linkedObject ?? null,
            });
            return;
          }
          void createAndPersistAnnotation(
            "callout",
            buildAnnotationPayload("callout", {
              timeframe: readingTimeframe,
              anchor_type: pendingCalloutAnchor.anchorType,
              anchor_target: pendingCalloutAnchor.anchorTarget,
              anchor_time: pendingCalloutAnchor.time,
              anchor_date: pendingCalloutAnchor.date,
              anchor_price: pendingCalloutAnchor.price,
              anchor_object_id: pendingCalloutAnchor.anchorObjectId ?? undefined,
              resolution: pendingCalloutAnchor.resolution,
              linked_object: pendingCalloutAnchor.linkedObject ?? undefined,
              label_position: {
                date: toAnnotationDate(candle.time),
                price: Number.isFinite(point?.price) ? point!.price : candle.close,
              },
              leader_line: true,
              free_text: readingNoteText,
              tags,
              comment_type: readingCommentType,
            }),
            tags,
            readingTimeframe
          );
          setPendingCalloutAnchor(null);
        }
      }
    }
  };

  const handleCopyForConsult = async () => {
    if (!selectedDate || !selectedBarData || !code) return;

    // Get current memo
    let memo = "";
    try {
      const response = await api.get("/memo", {
        params: { symbol: code, date: selectedDate, timeframe: "D" },
      });
      memo = response.data.memo || "";
    } catch (error) {
      console.error("Failed to fetch memo:", error);
    }

    // Get position for selected date
    const selectedTime = selectedBarData.time;
    const positionsAtTime = dailyPositions.filter(p => p.time === selectedTime);
    let totalLong = 0;
    let totalShort = 0;
    positionsAtTime.forEach(p => {
      totalLong += p.longLots;
      totalShort += p.shortLots;
    });

    // Get MA values and trends for selected date
    const maData: any = {};
    const ma7Line = dailyMaLineByPeriod.get(7);
    const ma20Line = dailyMaLineByPeriod.get(20);
    const ma60Line = dailyMaLineByPeriod.get(60);

    const getMaTrend = (maLine: typeof ma7Line, barIndex: number | null) => {
      if (!maLine || barIndex == null || barIndex < 1) return "--";
      const currentValue = dailyMaValueMapByPeriod.get(maLine.period)?.get(selectedBarData.time);
      const prevBar = dailyCandles[barIndex - 1];
      const prevValue = prevBar ? (dailyMaValueMapByPeriod.get(maLine.period)?.get(prevBar.time) ?? null) : null;
      if (currentValue == null || prevValue == null) return "--";
      if (selectedBarData.close > currentValue && prevBar.close > prevValue) return "UP";
      if (selectedBarData.close < currentValue && prevBar.close < prevValue) return "DOWN";
      return "FLAT";
    };

    const barIndex = findNearestCandleIndex(dailyCandles, selectedTime);

    if (ma7Line?.visible) {
      const value = dailyMaValueMapByPeriod.get(7)?.get(selectedTime);
      if (value != null) {
        maData.ma7 = { value, trend: getMaTrend(ma7Line, barIndex) };
      }
    }
    if (ma20Line?.visible) {
      const value = dailyMaValueMapByPeriod.get(20)?.get(selectedTime);
      if (value != null) {
        maData.ma20 = { value, trend: getMaTrend(ma20Line, barIndex) };
      }
    }
    if (ma60Line?.visible) {
      const value = dailyMaValueMapByPeriod.get(60)?.get(selectedTime);
      if (value != null) {
        maData.ma60 = { value, trend: getMaTrend(ma60Line, barIndex) };
      }
    }

    // Get signals for selected date
    const signalLabels: string[] = [];
    if (dailySignals && Array.isArray(dailySignals)) {
      dailySignals.forEach(signal => {
        if (signal && typeof signal === 'object' && 'label' in signal) {
          signalLabels.push(signal.label);
        }
      });
    }

    const consultData = {
      symbol: code,
      name: tickerName || code,
      date: selectedDate,
      ohlc: {
        open: selectedBarData.open,
        high: selectedBarData.high,
        low: selectedBarData.low,
        close: selectedBarData.close,
      },
      volume: dailyVolumeByTime.get(selectedBarData.time),
      position: totalLong > 0 || totalShort > 0 ? { sell: totalShort, buy: totalLong } : undefined,
      ma: Object.keys(maData).length > 0 ? maData : undefined,
      signals: signalLabels.length > 0 ? signalLabels : undefined,
      memo,
    };

    const text = buildConsultCopyText(consultData);
    const success = await copyConsultToClipboard(text);

    if (success) {
      setToastMessage("相談用データをコピーしました");
      setTimeout(() => setToastMessage(null), 2000);
    } else {
      setToastMessage("コピーに失敗しました");
      setTimeout(() => setToastMessage(null), 2000);
    }
  };


  const handleCopyDebug = async () => {
    const timestamp = new Date().toISOString();
    const textToCopy = [`Timestamp: ${timestamp}`, ...debugLines].join("\n");
    try {
      if (navigator.clipboard && navigator.clipboard.writeText) {
        await navigator.clipboard.writeText(textToCopy);
        showShortToast("Copied");
        setCopyFallbackText(null);
        return;
      }
    } catch {
      // fallback below
    }

    try {
      const textarea = document.createElement("textarea");
      textarea.value = textToCopy;
      textarea.setAttribute("readonly", "true");
      textarea.style.position = "fixed";
      textarea.style.opacity = "0";
      textarea.style.pointerEvents = "none";
      document.body.appendChild(textarea);
      textarea.select();
      const ok = document.execCommand("copy");
      document.body.removeChild(textarea);
      if (ok) {
        showShortToast("Copied");
        setCopyFallbackText(null);
        return;
      }
    } catch {
      // ignore
    }

    showShortToast("Copy failed");
    setCopyFallbackText(textToCopy);
  };



  const dailyChartMaLines = useMemo(() => toDetailChartMaLines(dailyMaLines), [dailyMaLines]);
  const dailyPositionRiskOverlayDrawings = useMemo(
    () =>
      buildPositionRiskOverlayDrawings({
        candles: dailyCandles,
        maLines: dailyMaLines,
        currentPositions,
      }),
    [currentPositions, dailyCandles, dailyMaLines]
  );
  const dailyPriceBandsWithPositionRisk = useMemo(
    () => [...dailyDrawings.priceBands, ...dailyPositionRiskOverlayDrawings.priceBands],
    [dailyDrawings.priceBands, dailyPositionRiskOverlayDrawings.priceBands]
  );
  const dailyDrawBoxesWithAnnotations = useMemo(
    () => [...dailyDrawings.drawBoxes, ...annotationDrawBoxes],
    [annotationDrawBoxes, dailyDrawings.drawBoxes]
  );
  const dailyHorizontalLinesWithPositionRisk = useMemo(
    () => [
      ...dailyDrawings.horizontalLines,
      ...annotationHorizontalLines,
      ...dailyPositionRiskOverlayDrawings.horizontalLines,
    ],
    [
      annotationHorizontalLines,
      dailyDrawings.horizontalLines,
      dailyPositionRiskOverlayDrawings.horizontalLines,
    ]
  );
  const dailyMaLineByPeriod = useMemo(
    () => new Map(dailyMaLines.map((line) => [line.period, line])),
    [dailyMaLines]
  );
  const dailyMaValueMapByPeriod = useMemo(
    () => new Map(dailyMaLines.map((line) => [line.period, new Map(line.data.map((point) => [point.time, point.value]))])),
    [dailyMaLines]
  );
  const dailyVolumeByTime = useMemo(
    () => new Map(dailyVolume.map((item) => [item.time, item.value])),
    [dailyVolume]
  );
  const compareDailyMaLines = useMemo(() => {
    return buildDetailMaLines(compareDailyCandles, compareMaSettings.daily);
  }, [compareDailyCandles, compareMaSettings.daily]);
  const compareDailyChartMaLines = useMemo(
    () => toDetailChartMaLines(compareDailyMaLines),
    [compareDailyMaLines]
  );
  const memoPanelData = useDetailInfo(
    selectedBarData,
    selectedBarIndex ?? -1,
    dailyCandles,
    dailyPositions,
    dailyMaLines
  );

  const weeklyMaLines = useMemo(() => {
    return buildDetailMaLines(weeklyCandles, maSettings.weekly);
  }, [weeklyCandles, maSettings.weekly]);
  const weeklyChartMaLines = useMemo(() => toDetailChartMaLines(weeklyMaLines), [weeklyMaLines]);

  const monthlyMaLines = useMemo(() => {
    return buildDetailMaLines(monthlyCandles, maSettings.monthly);
  }, [monthlyCandles, maSettings.monthly]);
  const monthlyChartMaLines = useMemo(() => toDetailChartMaLines(monthlyMaLines), [monthlyMaLines]);
  const compareMonthlyMaLines = useMemo(() => {
    return buildDetailMaLines(compareMonthlyCandles, compareMaSettings.monthly);
  }, [compareMonthlyCandles, compareMaSettings.monthly]);
  const compareMonthlyChartMaLines = useMemo(
    () => toDetailChartMaLines(compareMonthlyMaLines),
    [compareMonthlyMaLines]
  );

  const visibleAnalysisRecalcRange = useMemo(() => {
    if (!dailyCandles.length) return null;
    const anchorTime =
      analysisAsOfTime ??
      detailAsOfTime ??
      latestDailyAsOfTime ??
      dailyCandles[dailyCandles.length - 1]?.time ??
      null;
    const anchorIndex =
      anchorTime == null
        ? dailyCandles.length - 1
        : (findNearestCandleIndex(dailyCandles, anchorTime) ?? (dailyCandles.length - 1));
    const halfWindow = Math.floor(ANALYSIS_DECISION_WINDOW_BARS / 2);
    let startIndex = Math.max(0, anchorIndex - halfWindow);
    const endIndex = Math.min(dailyCandles.length - 1, startIndex + ANALYSIS_DECISION_WINDOW_BARS - 1);
    startIndex = Math.max(0, endIndex - (ANALYSIS_DECISION_WINDOW_BARS - 1));
    const startTime = dailyCandles[startIndex]?.time ?? null;
    const endTime = dailyCandles[endIndex]?.time ?? null;
    if (startTime == null || endTime == null) return null;
    const orderedStartTime = Math.min(startTime, endTime);
    const orderedEndTime = Math.max(startTime, endTime);
    return {
      startDt: toDateKey(orderedStartTime),
      endDt: toDateKey(orderedEndTime),
      startLabel: formatDateLabel(orderedStartTime),
      endLabel: formatDateLabel(orderedEndTime),
      bars: Math.max(1, endIndex - startIndex + 1),
    };
  }, [analysisAsOfTime, dailyCandles, detailAsOfTime, latestDailyAsOfTime]);
  const { items: exactDecisionRange } = useExactDecisionRange({
    backendReady,
    code,
    startDt: visibleAnalysisRecalcRange?.startDt ?? null,
    endDt: visibleAnalysisRecalcRange?.endDt ?? null,
    riskMode: analysisRiskMode,
    enabled: analysisFetchEnabled && showDecisionMarkers && visibleAnalysisRecalcRange != null,
    readyToFetch: analysisNetworkReady,
    cacheKeyExtra: analysisFetchRefreshToken,
  });
  const exactDecisionToneScopeKey = code ? `${code}|${analysisRiskMode}` : "";
  useEffect(() => {
    if (!exactDecisionRange.length || !exactDecisionToneScopeKey) return;
    setExactDecisionToneCacheByScope((current) => {
      const scopedCurrent = current.get(exactDecisionToneScopeKey) ?? EMPTY_EXACT_DECISION_TONE_BY_DATE;
      let nextScoped: Map<number, ExactDecisionTone> | null = null;
      exactDecisionRange.forEach((item) => {
        if (scopedCurrent.get(item.dtKey) === item.tone) return;
        if (nextScoped == null) {
          nextScoped = new Map(scopedCurrent);
        }
        nextScoped.set(item.dtKey, item.tone);
      });
      if (nextScoped == null) {
        return current;
      }
      const next = new Map(current);
      next.set(exactDecisionToneScopeKey, nextScoped);
      EXACT_DECISION_TONE_CACHE_BY_SCOPE.set(exactDecisionToneScopeKey, nextScoped);
      return next;
    });
  }, [exactDecisionRange, exactDecisionToneScopeKey]);
  const exactDecisionToneByDate = useMemo(() => {
    if (!exactDecisionToneScopeKey) {
      return EMPTY_EXACT_DECISION_TONE_BY_DATE;
    }
    const cached = exactDecisionToneCacheByScope.get(exactDecisionToneScopeKey);
    if (cached != null) {
      return cached;
    }
    if (!exactDecisionRange.length) {
      return EMPTY_EXACT_DECISION_TONE_BY_DATE;
    }
    const fallback = new Map<number, ExactDecisionTone>();
    exactDecisionRange.forEach((item) => {
      fallback.set(item.dtKey, item.tone);
    });
    return fallback;
  }, [exactDecisionRange, exactDecisionToneCacheByScope, exactDecisionToneScopeKey]);
  const persistedSignalEventsByDate = useMemo(() => {
    const grouped = new Map<string, any[]>();
    persistedSignalEvents.forEach((item) => {
      const key = typeof item?.signalDate === "string" ? item.signalDate : null;
      if (!key) return;
      const bucket = grouped.get(key) ?? [];
      bucket.push(item);
      grouped.set(key, bucket);
    });
    return grouped;
  }, [persistedSignalEvents]);
  const persistedRankingAppearancesByDate = useMemo(() => {
    const grouped = new Map<string, any[]>();
    persistedRankingAppearances.forEach((item) => {
      const key = typeof item?.date_iso === "string" ? item.date_iso : null;
      if (!key) return;
      const bucket = grouped.get(key) ?? [];
      bucket.push(item);
      grouped.set(key, bucket);
    });
    return grouped;
  }, [persistedRankingAppearances]);
  const selectedPersistedSignalEvents = useMemo(() => {
    if (!selectedDate) return [];
    return persistedSignalEventsByDate.get(selectedDate) ?? [];
  }, [persistedSignalEventsByDate, selectedDate]);
  const selectedRankingAppearances = useMemo(() => {
    if (!selectedDate) return [];
    return persistedRankingAppearancesByDate.get(selectedDate) ?? [];
  }, [persistedRankingAppearancesByDate, selectedDate]);
  const selectedCodeRankingAppearances = useMemo(() => {
    if (!code) return [];
    return selectedRankingAppearances.filter((item) => {
      if (!item || typeof item !== "object") return false;
      return String((item as Record<string, unknown>).code ?? "").trim() === code;
    });
  }, [code, selectedRankingAppearances]);
  const selectedRankingAppearancesForSummary = useMemo(() => {
    if (!selectedCodeRankingAppearances.length) return [];
    if (!selectedDate) {
      const latestDateCandidates = selectedCodeRankingAppearances
        .map((item) => (typeof (item as Record<string, unknown>).date_iso === "string" ? String((item as Record<string, unknown>).date_iso) : null))
        .filter((value): value is string => Boolean(value))
        .sort();
      const latestDate = latestDateCandidates.length > 0 ? latestDateCandidates[latestDateCandidates.length - 1] : null;
      if (!latestDate) return selectedCodeRankingAppearances;
      return selectedCodeRankingAppearances.filter((item) => {
        const dateIso = String((item as Record<string, unknown>).date_iso ?? "");
        return dateIso === latestDate;
      });
    }
    return selectedCodeRankingAppearances;
  }, [selectedCodeRankingAppearances, selectedDate]);
  const selectedRankingJudgement = useMemo(() => {
    const empty = {
      analysisDtLabel: selectedDate ?? null,
      analysisSummaryLoading: persistedMarkersLoading,
      analysisMissingDataVisible: false,
      analysisDecision: {
        tone: "neutral",
        sideLabel: null,
        patternLabel: null,
        confidence: null,
        buyProb: null,
        sellProb: null,
        neutralProb: null,
      },
      analysisGuidance: {
        confidenceRank: "low",
        action: "neutral_watch",
        watchpoint: "no ranking appearance data",
        buyWidth: 0,
        sellWidth: 0,
        neutralWidth: 100,
        buySetupProb: null,
        sellSetupProb: null,
        buySetupWidth: 0,
        sellSetupWidth: 0,
        buySetupState: "wait",
        sellSetupState: "wait",
      },
      analysisEntryPolicy: {
        riskMode: analysisRiskMode,
        up: null,
        down: null,
      },
      patternSummary: {
        environmentLabel: "ranking",
        environmentTone: "neutral",
        markerTone: "neutral",
        markerIsSetup: false,
        scenarios: [],
      },
      analysisResearchPrior: null,
    };

    if (!selectedRankingAppearancesForSummary.length) {
      return empty;
    }

    const itemByDir = (dir: "up" | "down") =>
      (selectedRankingAppearancesForSummary.find((item) => {
        if (!item || typeof item !== "object") return false;
        return String((item as Record<string, unknown>).dir ?? "").trim() === dir;
      }) ?? null) as Record<string, unknown> | null;

    const upItem = itemByDir("up");
    const downItem = itemByDir("down");
    const toScore = (item: Record<string, unknown> | null) => {
      const raw = toFiniteNumber(item?.display_score);
      return raw == null ? null : clamp(raw / 100, 0, 1);
    };
    const upScore = toScore(upItem);
    const downScore = toScore(downItem);
    const bestItem =
      (upScore ?? 0) >= (downScore ?? 0)
        ? upItem ?? downItem
        : downItem ?? upItem;
    const bestScore = Math.max(upScore ?? 0, downScore ?? 0);
    const scoreGap = Math.abs((upScore ?? 0) - (downScore ?? 0));
    const bestQualified = Boolean(bestItem?.entry_qualified_at_appearance);
    const bestTone =
      bestItem == null || bestScore < 0.26 || (scoreGap < 0.05 && !bestQualified)
        ? "neutral"
        : (upScore ?? 0) >= (downScore ?? 0)
          ? "up"
          : "down";
    const confidence = clamp(0.35 + bestScore * 0.75 + scoreGap * 1.2 + (bestQualified ? 0.08 : 0), 0, 1);
    const confidenceRank = confidence >= 0.7 ? "high" : confidence >= 0.55 ? "mid" : "low";
    const formatAppearanceSummary = (item: Record<string, unknown> | null) => {
      if (!item) return null;
      const parts: string[] = [];
      const rankValue = toFiniteNumber(item.rank);
      const scoreValue = toFiniteNumber(item.display_score);
      const setupType = typeof item.setup_type_at_appearance === "string" ? item.setup_type_at_appearance.trim() : "";
      const signalState = typeof item.signal_state_at_appearance === "string" ? item.signal_state_at_appearance.trim() : "";
      if (rankValue != null) parts.push(`rank ${Math.round(rankValue)}`);
      if (scoreValue != null) parts.push(`score ${formatPercentLabel(scoreValue / 100)}`);
      if (setupType) parts.push(setupType);
      if (signalState) parts.push(signalState);
      if (item.entry_qualified_at_appearance === true) parts.push("entryQualified");
      return parts.length > 0 ? parts.join(" / ") : null;
    };
    const bestSummary = formatAppearanceSummary(bestItem);
    const buildPolicySide = (item: Record<string, unknown> | null, score: number | null) => {
      if (!item) return null;
      const breakStatus = typeof item.break_status === "string" ? item.break_status.trim() : "";
      const breakReason = typeof item.break_reason === "string" ? item.break_reason.trim() : "";
      const holdReasonParts = [
        score != null ? `priority ${formatPercentLabel(score)}` : null,
        formatAppearanceSummary(item),
      ].filter(isNonEmptyString);
      return {
        setupType: typeof item.setup_type_at_appearance === "string" ? item.setup_type_at_appearance.trim() || null : null,
        recommendedHoldDays: null,
        recommendedHoldMinDays: null,
        recommendedHoldMaxDays: null,
        recommendedHoldReason: holdReasonParts.length > 0 ? holdReasonParts.join(" / ") : null,
        invalidationTrigger: breakStatus || null,
        invalidationConservativeAction: breakStatus ? "exit" : "hold",
        invalidationAggressiveAction: breakStatus ? "exit" : "hold",
        invalidationRecommendedAction: breakStatus ? "exit" : "hold",
        invalidationDotenRecommended: false,
        invalidationOppositeHoldDays: null,
        invalidationExpectedDeltaMean: toFiniteNumber(item.current_directional_return ?? item.return_30d ?? null),
        invalidationPolicyNote: breakReason || null,
        playbookScoreBonus: score,
      };
    };
    const buyWidth = Math.round((upScore ?? 0) * 100);
    const sellWidth = Math.round((downScore ?? 0) * 100);
    const neutralProb = clamp(bestTone === "neutral" ? Math.max(0.55, 1 - bestScore) : 1 - bestScore, 0, 1);
    const neutralWidth = Math.round(neutralProb * 100);
    const buySetupState = upItem ? (upItem.entry_qualified_at_appearance === true ? "monitor" : "wait") : "--";
    const sellSetupState = downItem ? (downItem.entry_qualified_at_appearance === true ? "monitor" : "wait") : "--";
    const action =
      bestTone === "up"
        ? bestQualified
          ? "buy_watch"
          : "buy_setup"
        : bestTone === "down"
          ? bestQualified
            ? "sell_watch"
            : "sell_setup"
          : "neutral_watch";
    const watchpoint = [
      selectedDate ? `date ${selectedDate}` : null,
      bestSummary,
      bestItem?.break_status ? `break ${String(bestItem.break_status)}` : null,
      bestItem?.break_reason ? String(bestItem.break_reason) : null,
    ].filter(isNonEmptyString).join(" / ") || "no ranking appearance data";
    const analysisDecision = {
      tone: bestTone,
      sideLabel: null,
      patternLabel: bestSummary,
      confidence,
      buyProb: upScore,
      sellProb: downScore,
      neutralProb,
    };
    const analysisGuidance = {
      confidenceRank,
      action,
      watchpoint,
      buyWidth,
      sellWidth,
      neutralWidth,
      buySetupProb: upScore,
      sellSetupProb: downScore,
      buySetupWidth: buyWidth,
      sellSetupWidth: sellWidth,
      buySetupState,
      sellSetupState,
    };
    const analysisEntryPolicy = {
      riskMode: analysisRiskMode,
      up: buildPolicySide(upItem, upScore),
      down: buildPolicySide(downItem, downScore),
    };
    const analysisResearchPrior = null;
    const patternSummary = {
      environmentLabel: selectedDate ? `ranking ${selectedDate}` : "latest ranking",
      environmentTone: bestTone,
      markerTone: bestTone,
      markerIsSetup: bestQualified,
      scenarios: [
        {
          key: "up",
          label: "buy",
          tone: "up",
          score: upScore ?? 0,
          reasons: [formatAppearanceSummary(upItem), upItem?.break_status ? `break ${String(upItem.break_status)}` : null].filter(
            isNonEmptyString
          ),
        },
        {
          key: "range",
          label: "neutral",
          tone: "neutral",
          score: neutralProb,
          reasons: [
            selectedDate ? `date ${selectedDate}` : "latest",
            bestItem?.entry_qualified_at_appearance === true ? "entryQualified" : "wait",
          ],
        },
        {
          key: "down",
          label: "sell",
          tone: "down",
          score: downScore ?? 0,
          reasons: [formatAppearanceSummary(downItem), downItem?.break_status ? `break ${String(downItem.break_status)}` : null].filter(
            isNonEmptyString
          ),
        },
      ],
    };

    return {
      analysisDtLabel: selectedDate ?? (typeof bestItem?.date_iso === "string" ? bestItem.date_iso : null),
      analysisSummaryLoading: false,
      analysisMissingDataVisible: false,
      analysisDecision,
      analysisGuidance,
      analysisEntryPolicy,
      patternSummary,
      analysisResearchPrior,
    };
  }, [analysisRiskMode, formatPercentLabel, selectedCodeRankingAppearances, selectedDate, selectedRankingAppearancesForSummary]);
  const holdDailyChartUntilDecisionReady = false;
  const shouldRenderCompareMonthlyChart = compareMonthlyCandles.length > 0;
  const autoAnalysisBackfillRequest = useMemo(
    () =>
      resolveAutoAnalysisBackfillRequest({
        code,
        analysisAsOfTime,
        analysisMissingDataVisible,
      }),
    [analysisAsOfTime, analysisMissingDataVisible, code]
  );
  const analysisPanelJustOpened = showAnalysisPanel && !prevShowAnalysisPanelRef.current;
  useEffect(() => {
    prevShowAnalysisPanelRef.current = showAnalysisPanel;
  }, [showAnalysisPanel]);
  useEffect(() => {
    if (!backendReady || !analysisNetworkReady || !showAnalysisPanel || !analysisPanelJustOpened || !code) {
      analysisAutoBackfillRequestKeyRef.current = null;
      return;
    }
    if (analysisRecalcDisabled) {
      analysisAutoBackfillRequestKeyRef.current = null;
      return;
    }
    if (analysisBackfillActive || analysisRecalcSubmitting != null) {
      return;
    }

    let requestKey: string | null = null;
    let params: Record<string, string | number | boolean> | null = null;
    let queuedMessage = "未計算の解析データを準備しています。";

    if (autoAnalysisBackfillRequest) {
      requestKey = autoAnalysisBackfillRequest.requestKey;
      params = autoAnalysisBackfillRequest.params;
      queuedMessage = autoAnalysisBackfillRequest.queuedMessage;
    } else {
      analysisAutoBackfillRequestKeyRef.current = null;
      return;
    }

    if (!requestKey || !params || analysisAutoBackfillRequestKeyRef.current === requestKey) {
      return;
    }

    analysisAutoBackfillRequestKeyRef.current = requestKey;
    let cancelled = false;
    setAnalysisRecalcSubmitting("auto");

    api
      .post("/jobs/analysis/backfill-missing", null, {
        params,
        timeout: 10000,
      })
      .then((res) => {
        if (cancelled) return;
        const payload = (res.data ?? {}) as JobStatusPayload & { ok?: boolean; job_id?: string; jobId?: string };
        if (payload.ok !== true) {
          throw new Error("auto backfill submit failed");
        }
        setAnalysisBackfillJob({
          id: typeof payload.job_id === "string" ? payload.job_id : payload.jobId,
          type: "analysis_backfill",
          status: "queued",
          progress: 0,
          message: queuedMessage,
        });
        analysisBackfillActiveRef.current = true;
      })
      .catch((error: unknown) => {
        if (cancelled) return;
        setToastAction(null);
        const response = (error as {
          response?: { status?: number; data?: { error?: unknown; message?: unknown } };
        }).response;
        if (
          response?.status === 410 &&
          (response?.data?.error === "legacy_analysis_disabled" ||
            typeof response?.data?.message === "string")
        ) {
          const detail =
            response?.data?.message ??
            "Phase 1 では外部 publish 済みの売買判定更新を利用してください。";
          setLegacyAnalysisDisabled(true);
          setLegacyAnalysisDisabledReason(String(detail));
          return;
        }
        setToastMessage("未計算の解析データを自動準備できませんでした。必要なら再計算を実行してください。");
      })
      .finally(() => {
        if (cancelled) return;
        setAnalysisRecalcSubmitting((current) => (current === "auto" ? null : current));
      });

    return () => {
      cancelled = true;
    };
  }, [
    backendReady,
    analysisNetworkReady,
    showAnalysisPanel,
    analysisPanelJustOpened,
    code,
    analysisRecalcDisabled,
    analysisBackfillActive,
    analysisRecalcSubmitting,
    autoAnalysisBackfillRequest,
  ]);
  const mergedDailyEventMarkers = useMemo(() => {
    const merged = [...dailyEventMarkers, ...annotationEventMarkers, ...maRoleChartMarkers];
    if (annotationMode && readingTargetType === "bar" && selectedBarForChartReading) {
      merged.push({
        time: selectedBarForChartReading.time,
        label: "注釈対象",
        kind: "tdnet-neutral",
      });
    }
    if (showRankingMarkers && dailyCandles.length > 0 && code) {
      chartRankingAppearances.forEach((item) => {
        if (!item || typeof item !== "object") return;
        const appearanceCode = String((item as Record<string, unknown>).code ?? "").trim();
        if (appearanceCode && appearanceCode !== code) return;
        const dateIso = typeof (item as Record<string, unknown>).date_iso === "string"
          ? String((item as Record<string, unknown>).date_iso)
          : null;
        if (!dateIso) return;
        const appearanceTime = normalizeTime(dateIso);
        if (!Number.isFinite(appearanceTime)) return;
        const markerTime = findNearestCandleTime(dailyCandles, appearanceTime);
        if (markerTime == null || Math.abs(markerTime - appearanceTime) > MAX_EVENT_OFFSET_SEC) return;
        const direction = String((item as Record<string, unknown>).dir ?? "").trim();
        const status = String((item as Record<string, unknown>).status ?? "").trim();
        const signalState = String((item as Record<string, unknown>).signal_state_at_appearance ?? "")
          .trim()
          .toLowerCase();
        const entryQualified = (item as Record<string, unknown>).entry_qualified_at_appearance === true;
        const isCurrentRanking = status === "current";
        const isBuySignal = direction === "up" && (isCurrentRanking || entryQualified || signalState === "buy");
        const isSellSignal = direction === "down" && (isCurrentRanking || entryQualified || signalState === "sell");
        if (!isBuySignal && !isSellSignal) return;
        merged.push({
          time: markerTime,
          kind: isSellSignal ? "ranking-down" : "ranking-up",
          label: isSellSignal ? "売り" : "買い",
        });
      });
    }
    if (showDecisionMarkers) {
      if (persistedSignalEvents.length > 0) {
        persistedSignalEvents.forEach((item) => {
          const signalDate = typeof item?.signalDate === "string" ? item.signalDate : null;
          if (!signalDate) return;
          const time = normalizeTime(signalDate);
          if (!Number.isFinite(time)) return;
          merged.push({
            time,
            kind: item?.side === "sell" ? "decision-sell" : "decision-buy",
            label: typeof item?.setup_type === "string" ? item.setup_type : undefined,
          });
        });
      } else {
        dailyCandles.forEach((candle) => {
          const tone = exactDecisionToneByDate.get(toDateKey(candle.time));
          if (tone === "up") {
            merged.push({ time: candle.time, kind: "decision-buy" });
          } else if (tone === "down") {
            merged.push({ time: candle.time, kind: "decision-sell" });
          }
        });
      }
    }
    const deduped = new Map<string, (typeof merged)[number]>();
    merged.forEach((marker) => {
      const key =
        marker.kind === "earnings"
          ? `earnings:${marker.time}`
          : marker.kind?.startsWith("ranking-")
            ? `${marker.kind}:${marker.time}:${marker.label ?? ""}`
          : marker.kind?.startsWith("tdnet-")
            ? `${marker.kind}:${marker.time}:${marker.label ?? ""}`
            : `decision:${marker.time}`;
      deduped.set(key, marker);
    });
    return [...deduped.values()].sort((a, b) => a.time - b.time);
  }, [
    code,
    chartRankingAppearances,
    annotationEventMarkers,
    maRoleChartMarkers,
    annotationMode,
    dailyEventMarkers,
    dailyCandles,
    exactDecisionToneByDate,
    persistedSignalEvents,
    readingTargetType,
    selectedBarForChartReading,
    showDecisionMarkers,
    showRankingMarkers,
  ]);
  const compareMonthlyInitialRange = useMemo(() => {
    const months = rangeMonths;
    if (!months) return null;
    if (compareAsOfTime) return buildCompareAroundAsOfRange(compareMonthlyCandles, compareAsOfTime);
    return buildRange(compareMonthlyCandles, months);
  }, [rangeMonths, compareMonthlyCandles, compareAsOfTime]);
  const compareMonthlyBaseRange = useMemo(() => {
    if (!rangeMonths) return null;
    if (mainMonthlyTargetRange) return mainMonthlyTargetRange;
    return buildRange(monthlyCandles, rangeMonths);
  }, [rangeMonths, mainMonthlyTargetRange, monthlyCandles]);
  const compareDailyInitialRange = useMemo(() => {
    if (!compareDailyCandles.length) return null;
    const months = rangeMonths;
    if (!months) return null;
    if (compareAsOfTime) return buildCompareAroundAsOfRange(compareDailyCandles, compareAsOfTime);
    return buildRange(compareDailyCandles, months);
  }, [compareDailyCandles, rangeMonths, compareAsOfTime]);
  const compareMonthlyVisibleRange = useMemo(
    () => manualCompareMonthlyRangeRef.current ?? compareMonthlyInitialRange,
    [compareMonthlyInitialRange]
  );
  const compareDailyVisibleRange = useMemo(
    () => manualCompareDailyRangeRef.current ?? compareDailyInitialRange,
    [compareDailyInitialRange]
  );
  const compareRequiredFrom = useMemo(
    () => compareMonthlyVisibleRange?.from ?? compareDailyVisibleRange?.from ?? null,
    [compareMonthlyVisibleRange, compareDailyVisibleRange]
  );
  const dailyRangeLabel = useMemo(() => {
    if (!rangeMonths) return "全期間";
    if (rangeMonths === 3) return "3M";
    if (rangeMonths === 6) return "6M";
    if (rangeMonths === 12) return "1Y";
    if (rangeMonths === 24) return "2Y";
    return `${rangeMonths}M`;
  }, [rangeMonths]);
  const leftDailyRangeLabel = useMemo(() => {
    if (mainDailyTargetRange) {
      return `対象期間: ${formatDateLabel(mainDailyTargetRange.from)} - ${formatDateLabel(mainDailyTargetRange.to)}`;
    }
    return `表示期間: ${dailyRangeLabel}`;
  }, [mainDailyTargetRange, dailyRangeLabel]);
  const rightDailyRangeLabel = useMemo(() => {
    if (compareDailyVisibleRange) {
      const base = `表示期間: ${formatDateLabel(compareDailyVisibleRange.from)} - ${formatDateLabel(compareDailyVisibleRange.to)}`;
      if (
        compareDailyInitialRange &&
        (compareDailyInitialRange.from !== compareDailyVisibleRange.from ||
          compareDailyInitialRange.to !== compareDailyVisibleRange.to)
      ) {
        const compareBase = `比較期間: ${formatDateLabel(compareDailyInitialRange.from)} - ${formatDateLabel(compareDailyInitialRange.to)}`;
        if (compareAsOfTime) {
          return `${base} / ${compareBase} / 類似日: ${formatDateLabel(compareAsOfTime)}`;
        }
        return `${base} / ${compareBase}`;
      }
      if (compareAsOfTime) {
        return `${base} / 類似日: ${formatDateLabel(compareAsOfTime)}`;
      }
      return base;
    }
    if (compareAsOfTime) {
      return `一致日: ${formatDateLabel(compareAsOfTime)}`;
    }
    return "一致期間: --";
  }, [compareDailyVisibleRange, compareDailyInitialRange, compareAsOfTime]);
  const leftMonthlyRangeLabel = useMemo(() => {
    if (mainMonthlyTargetRange) {
      return `対象期間: ${formatDateLabel(mainMonthlyTargetRange.from)} - ${formatDateLabel(mainMonthlyTargetRange.to)}`;
    }
    return `表示期間: ${dailyRangeLabel}`;
  }, [mainMonthlyTargetRange, dailyRangeLabel]);
  const rightMonthlyRangeLabel = useMemo(() => {
    if (compareMonthlyVisibleRange) {
      const base = `表示期間: ${formatDateLabel(compareMonthlyVisibleRange.from)} - ${formatDateLabel(compareMonthlyVisibleRange.to)}`;
      if (
        compareMonthlyInitialRange &&
        (compareMonthlyInitialRange.from !== compareMonthlyVisibleRange.from ||
          compareMonthlyInitialRange.to !== compareMonthlyVisibleRange.to)
      ) {
        const compareBase = `比較期間: ${formatDateLabel(compareMonthlyInitialRange.from)} - ${formatDateLabel(compareMonthlyInitialRange.to)}`;
        if (compareAsOfTime) {
          return `${base} / ${compareBase} / 類似日: ${formatDateLabel(compareAsOfTime)}`;
        }
        return `${base} / ${compareBase}`;
      }
      if (compareAsOfTime) {
        return `${base} / 類似日: ${formatDateLabel(compareAsOfTime)}`;
      }
      return base;
    }
    return `表示期間: ${dailyRangeLabel}`;
  }, [compareMonthlyVisibleRange, compareMonthlyInitialRange, compareAsOfTime, dailyRangeLabel]);
  const aiExplainImageSources = useMemo(
    () => {
      if (!aiExplainDockMounted) return [];
      return [
        {
          code: code ?? "main",
          payload: buildAiExplainBarsPayload(dailyCandles),
          boxes,
          maSettings: maSettings.daily,
          rangeBars: rangeMonths ? Math.max(30, rangeMonths * 20) : null,
          timeframeLabel: "daily",
          maxBars: rangeMonths ? Math.max(30, rangeMonths * 20) : null,
          showBoxes,
        },
        {
          code: code ?? "main",
          payload: buildAiExplainBarsPayload(monthlyCandles),
          boxes,
          maSettings: maSettings.monthly,
          rangeBars: rangeMonths ? Math.max(12, rangeMonths) : null,
          timeframeLabel: "monthly",
          maxBars: rangeMonths ? Math.max(12, rangeMonths) : null,
          showBoxes,
        },
        compareCode
          ? {
              code: compareCode,
              payload: buildAiExplainBarsPayload(compareDailyCandles),
              boxes: compareBoxes,
              maSettings: compareMaSettings.daily,
              rangeBars: compareDailyVisibleRange ? Math.max(30, rangeMonths ? rangeMonths * 20 : 60) : null,
              timeframeLabel: "compare-daily",
              maxBars: compareDailyVisibleRange ? Math.max(30, rangeMonths ? rangeMonths * 20 : 60) : null,
              showBoxes,
            }
          : null,
      ].filter(Boolean) as Parameters<typeof buildAiExplainImages>[0];
    },
    [
      aiExplainDockMounted,
      boxes,
      code,
      compareBoxes,
      compareCode,
      compareDailyCandles,
      compareDailyVisibleRange,
      compareMaSettings.daily,
      dailyCandles,
      maSettings.daily,
      maSettings.monthly,
      monthlyCandles,
      rangeMonths,
      showBoxes,
    ]
  );
  const aiExplainImages = useMemo(
    () => {
      if (!aiExplainDockMounted) return [];
      return buildAiExplainImages(aiExplainImageSources, 3);
    },
    [aiExplainDockMounted, aiExplainImageSources]
  );
  const aiExplainSnapshot = useMemo(() => {
    if (!aiExplainDockMounted) {
      return {
        mode: compareCode ? "compare" : "explain",
        screenType: compareCode ? "compare" : "detail",
        asOfDate: compareCode ? compareAsOf ?? null : detailAsOfTime ?? null,
        userQuestion: "",
        selectedSymbols: code ? [code] : [],
        compareSymbols: compareCode ? [compareCode] : [],
        visibleTimeframe: {
          daily: null,
          weekly: null,
          monthly: null,
          compareDaily: null,
          compareMonthly: null,
        },
        main: null,
        compare: null,
        compareDifference: null,
      };
    }
    const rangeToSnapshot = (range: { from: number; to: number } | null | undefined) =>
      range ? { from: range.from, to: range.to } : null;
    const mainSummary = summarizeDetailTickerForAiExplain(activeTicker, {
      code: code ?? "",
      name: tickerName || code || "",
    });
    const compareSummary = compareCode
      ? summarizeDetailTickerForAiExplain(tickerByCode.get(compareCode) ?? null, {
          code: compareCode,
          name: compareTickerName || compareCode,
        })
      : null;
    return {
      mode: compareCode ? "compare" : "explain",
      screenType: compareCode ? "compare" : "detail",
      asOfDate: compareCode ? compareAsOf ?? null : detailAsOfTime ?? null,
      userQuestion: "",
      selectedSymbols: code ? [code] : [],
      compareSymbols: compareCode ? [compareCode] : [],
      visibleTimeframe: {
        daily: rangeToSnapshot(dailyVisibleRange),
        weekly: rangeToSnapshot(resolvedWeeklyVisibleRange),
        monthly: rangeToSnapshot(resolvedMonthlyVisibleRange),
        compareDaily: rangeToSnapshot(compareDailyVisibleRange),
        compareMonthly: rangeToSnapshot(compareMonthlyVisibleRange),
      },
      main: mainSummary,
      compare: compareSummary,
      compareDifference: compareSummary
        ? {
            code: compareCode,
            targetLabel: `${code ?? ""} vs ${compareCode}`,
          }
        : null,
    };
  }, [
    activeTicker,
    aiExplainDockMounted,
    code,
    compareAsOf,
    compareCode,
    compareDailyVisibleRange,
    compareMonthlyVisibleRange,
    compareTickerName,
    dailyVisibleRange,
    detailAsOfTime,
    resolvedMonthlyVisibleRange,
    resolvedWeeklyVisibleRange,
    tickerByCode,
    tickerName,
  ]);
  const compareDailyNeedsMore = useMemo(() => {
    if (!compareDailyVisibleRange || !compareDailyCandles.length) return false;
    const earliest = compareDailyCandles[0]?.time;
    if (!earliest) return false;
    const hasMore = compareDailyData.length >= compareDailyLimit;
    return compareDailyVisibleRange.from < earliest && hasMore;
  }, [compareDailyVisibleRange, compareDailyCandles, compareDailyData.length, compareDailyLimit]);
  const shouldRenderCompareDailyChart =
    !compareDailyNeedsMore &&
    compareDailyCandles.length > 0;
  const mainMonthlyNeedsMore = useMemo(() => {
    if (!compareCode || !compareRequiredFrom || !monthlyCandles.length) return false;
    const earliest = monthlyCandles[0]?.time;
    if (!earliest) return false;
    const hasMore = monthlyData.length >= monthlyLimit;
    return compareRequiredFrom < earliest && hasMore;
  }, [compareCode, compareRequiredFrom, monthlyCandles, monthlyData.length, monthlyLimit]);
  const compareMonthlyNeedsMore = useMemo(() => {
    if (!compareCode || !compareRequiredFrom || !compareMonthlyCandles.length) return false;
    const earliest = compareMonthlyCandles[0]?.time;
    if (!earliest) return false;
    const hasMore = compareMonthlyData.length >= monthlyLimit;
    return compareRequiredFrom < earliest && hasMore;
  }, [
    compareCode,
    compareRequiredFrom,
    compareMonthlyCandles,
    compareMonthlyData.length,
    monthlyLimit
  ]);

  useEffect(() => {
    if (!compareCode) return;
    if (compareDailyLoading) return;
    if (!compareDailyNeedsMore) return;
    setCompareDailyLimit((prev) => incrementBarLimit(prev, LIMIT_STEP.daily, MAX_DAILY_BATCH_BARS_LIMIT));
  }, [compareCode, compareDailyLoading, compareDailyNeedsMore]);
  useEffect(() => {
    if (!compareCode) return;
    if (loadingMonthly || compareLoading) return;
    if (!mainMonthlyNeedsMore && !compareMonthlyNeedsMore) return;
    setMonthlyLimit((prev) => incrementBarLimit(prev, LIMIT_STEP.monthly, MAX_MONTHLY_BATCH_BARS_LIMIT));
  }, [
    compareCode,
    loadingMonthly,
    compareLoading,
    mainMonthlyNeedsMore,
    compareMonthlyNeedsMore
  ]);

  useEffect(() => {
    const handleMove = (event: MouseEvent | TouchEvent) => {
      if (!draggingRef.current || !bottomRowRef.current) return;
      let clientX = 0;
      if ("touches" in event) {
        if (!event.touches.length) return;
        event.preventDefault();
        clientX = event.touches[0].clientX;
      } else {
        clientX = event.clientX;
      }
      const rect = bottomRowRef.current.getBoundingClientRect();
      const position = clamp((clientX - rect.left) / rect.width, 0.05, 0.95);
      const nextWeekly = clamp(position, MIN_WEEKLY_RATIO, 1 - MIN_MONTHLY_RATIO);
      setWeeklyRatio(nextWeekly);
    };

    const handleUp = () => {
      draggingRef.current = false;
    };

    window.addEventListener("mousemove", handleMove);
    window.addEventListener("mouseup", handleUp);
    window.addEventListener("touchmove", handleMove, { passive: false });
    window.addEventListener("touchend", handleUp);

    return () => {
      window.removeEventListener("mousemove", handleMove);
      window.removeEventListener("mouseup", handleUp);
      window.removeEventListener("touchmove", handleMove);
      window.removeEventListener("touchend", handleUp);
    };
  }, []);

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        if (showPositionLedger) {
          setHeaderMode("chart");
          setPositionLedgerExpanded(false);
          return;
        }
        setFocusPanel(null);
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => {
      window.removeEventListener("keydown", handleKeyDown);
    };
  }, [showPositionLedger]);

  // Selected daily candle keyboard handler
  useEffect(() => {
    if (!cursorMode && !selectedBarData) return;

    const handleCursorKeyDown = (e: KeyboardEvent) => {
      // Do not steal caret movement or select navigation while editing fields.
      const target = e.target as HTMLElement;
      if (
        target.tagName === "TEXTAREA" ||
        target.tagName === "INPUT" ||
        target.tagName === "SELECT" ||
        target.isContentEditable
      ) {
        return;
      }

      switch (e.key) {
        case "ArrowLeft":
          e.preventDefault();
          moveToPrevDay();
          break;
        case "ArrowRight":
          e.preventDefault();
          moveToNextDay();
          break;
        case "c":
        case "C":
          if (cursorMode && !e.ctrlKey && !e.metaKey) {
            e.preventDefault();
            toggleCursorMode();
          }
          break;
        case "Escape":
          if (cursorMode) {
            e.preventDefault();
            setCursorMode(false);
          }
          break;
      }
    };

    window.addEventListener("keydown", handleCursorKeyDown);
    return () => window.removeEventListener("keydown", handleCursorKeyDown);
  }, [cursorMode, moveToNextDay, moveToPrevDay, selectedBarData, toggleCursorMode]);


  const compareHasMoreDaily = compareDailyData.length >= compareDailyLimit;
  const compareHasMoreMonthly = compareMonthlyData.length >= monthlyLimit; // monthlyLimit is shared
  const canLoadMoreDaily = hasMoreDaily && dailyLimit < MAX_DAILY_BATCH_BARS_LIMIT;
  const canLoadMoreMonthly = hasMoreMonthly && monthlyLimit < MAX_MONTHLY_BATCH_BARS_LIMIT;
  const canLoadMoreCompareDaily = compareHasMoreDaily && compareDailyLimit < MAX_DAILY_BATCH_BARS_LIMIT;
  const canLoadMoreCompareMonthly = compareHasMoreMonthly && monthlyLimit < MAX_MONTHLY_BATCH_BARS_LIMIT;

  const mainSync = useChartSync(dailyChartRef, monthlyChartRef, weeklyChartRef, {
    enabled: (syncRanges ?? true) && !overwriteLiveValidationMode,
    cursorEnabled: true,
    onLoadMoreDaily: () => setDailyLimit((prev) => incrementBarLimit(prev, LIMIT_STEP.daily, MAX_DAILY_BATCH_BARS_LIMIT)),
    onLoadMoreMonthly: () =>
      setMonthlyLimit((prev) => incrementBarLimit(prev, LIMIT_STEP.monthly, MAX_MONTHLY_BATCH_BARS_LIMIT)),
    hasMoreDaily: canLoadMoreDaily,
    loadingDaily,
    hasMoreMonthly: canLoadMoreMonthly,
    loadingMonthly,
    dailyCandles,
    monthlyCandles
  });

  const compareSync = useChartSync(compareDailyChartRef, compareMonthlyChartRef, undefined, {
    enabled: (syncRanges ?? true) && !overwriteLiveValidationMode,
    cursorEnabled: true,
    onLoadMoreDaily: () =>
      setCompareDailyLimit((prev) => incrementBarLimit(prev, LIMIT_STEP.daily, MAX_DAILY_BATCH_BARS_LIMIT)),
    // compare monthly load more is implicitly handled by shared monthlyLimit, but comparing data length:
    onLoadMoreMonthly: () =>
      setMonthlyLimit((prev) => incrementBarLimit(prev, LIMIT_STEP.monthly, MAX_MONTHLY_BATCH_BARS_LIMIT)),
    hasMoreDaily: canLoadMoreCompareDaily,
    loadingDaily: compareDailyLoading,
    hasMoreMonthly: canLoadMoreCompareMonthly,
    loadingMonthly: compareLoading, // compareLoading is for monthly
    dailyCandles: compareDailyCandles,
    monthlyCandles: compareMonthlyCandles
  });

  // Removed scheduleHoverTime

  const showVolumeDaily = dailyVolume.length > 0 && showVolumeEnabled;
  const gapBandsOverride = showGapBands ? undefined : [];

  const handleDailyVisibleRangeChange = (range: { from: number; to: number } | null) => {
    if (rangeMonths && range) {
      // Suppress programmatic range events (chart init, data load, setVisibleRange)
      // for a short settling window after data/range changes.
      if (Date.now() < rangeSettleRef.current) {
        mainSync.handleDailyVisibleRangeChange(range);
        return;
      }
      if (compareCode) {
        mainSync.handleDailyVisibleRangeChange(range);
        return;
      }
      const shouldSwitchToManual = hasSignificantRangeChange(dailyVisibleRange, range);
      if (!shouldSwitchToManual) {
        return;
      }
      manualDailyRangeRef.current = range;
      manualWeeklyRangeRef.current = range;
      manualMonthlyRangeRef.current = range;
      setRangeMonths(null);
    }
    mainSync.handleDailyVisibleRangeChange(range);
    if (!rangeMonths && range) {
      manualDailyRangeRef.current = range;
    }
  };

  const handleWeeklyVisibleRangeChange = (range: { from: number; to: number } | null) => {
    if (rangeMonths) return;
    mainSync.handleWeeklyVisibleRangeChange(range);
    if (range) {
      manualWeeklyRangeRef.current = range;
    }
  };

  const handleMonthlyVisibleRangeChange = (range: { from: number; to: number } | null) => {
    if (rangeMonths) return;
    mainSync.handleMonthlyVisibleRangeChange(range);
    if (range) {
      manualMonthlyRangeRef.current = range;
    }
  };

  const handleCompareMonthlyVisibleRangeChange = (range: { from: number; to: number } | null) => {
    if (rangeMonths && range) {
      if (Date.now() >= rangeSettleRef.current) {
        const shouldTrackManualRange = hasSignificantRangeChange(compareMonthlyInitialRange, range);
        manualCompareMonthlyRangeRef.current = shouldTrackManualRange ? range : null;
      }
      compareSync.handleMonthlyVisibleRangeChange(range);
      return;
    }
    compareSync.handleMonthlyVisibleRangeChange(range);
    if (!rangeMonths) {
      manualCompareMonthlyRangeRef.current = range;
    }
  };

  const handleCompareDailyVisibleRangeChange = (range: { from: number; to: number } | null) => {
    if (rangeMonths && range && Date.now() >= rangeSettleRef.current) {
      const shouldTrackManualRange = hasSignificantRangeChange(compareDailyInitialRange, range);
      manualCompareDailyRangeRef.current = shouldTrackManualRange ? range : null;
    }
    compareSync.handleDailyVisibleRangeChange(range);
    if (!rangeMonths && range) {
      manualCompareDailyRangeRef.current = range;
    }
  };

  const loadMoreDailyAndMonthly = () => {
    if (canLoadMoreDaily) {
      setDailyLimit((prev) => incrementBarLimit(prev, LIMIT_STEP.daily, MAX_DAILY_BATCH_BARS_LIMIT));
    }
    if (canLoadMoreMonthly) {
      setMonthlyLimit((prev) => incrementBarLimit(prev, LIMIT_STEP.monthly, MAX_MONTHLY_BATCH_BARS_LIMIT));
    }
  };
  const loadMoreDisabled = loadingDaily || loadingMonthly || (!canLoadMoreDaily && !canLoadMoreMonthly);
  const loadMoreLabel =
    loadingDaily || loadingMonthly
      ? "読み込み中..."
      : canLoadMoreDaily || canLoadMoreMonthly
        ? "日足・月足を追加読み込み"
        : "すべて読込済み";

  const toggleRange = (months: number) => {
    setRangeMonths(months);
    manualCompareDailyRangeRef.current = null;
    manualCompareMonthlyRangeRef.current = null;
    // Suppress programmatic visible-range events after preset change
    rangeSettleRef.current = Date.now() + RANGE_SETTLE_MS;
  };

  // Visible range sync is handled by hook; wrapper keeps manual range for load-more.

  const normalizeWarnings = (value: unknown): ApiWarnings => {
    if (Array.isArray(value)) return { items: value.filter((item) => typeof item === "string") };
    if (!value || typeof value !== "object") return { items: [] };
    const payload = value as ApiWarnings;
    const items = Array.isArray(payload.items) ? payload.items : [];
    const info = Array.isArray(payload.info) ? payload.info : [];
    const unrecognized = payload.unrecognized_labels;
    if (!unrecognized || typeof unrecognized.count !== "number") {
      return info.length ? { items, info } : { items };
    }
    const samples = Array.isArray(unrecognized.samples) ? unrecognized.samples : [];
    return { items, info, unrecognized_labels: { count: unrecognized.count, samples } };
  };

  const activeMaSettings = maEditMode === "compare" ? compareMaSettings : maSettings;

  const updateSetting = (timeframe: Timeframe, index: number, patch: Partial<MaSetting>) => {
    if (maEditMode === "compare") {
      updateCompareMaSetting(timeframe, index, patch);
      return;
    }
    updateMaSetting(timeframe, index, patch);
  };

  const resetSettings = (timeframe: Timeframe) => {
    if (maEditMode === "compare") {
      resetCompareMaSettings(timeframe);
      return;
    }
    resetMaSettings(timeframe);
  };

  const startDrag = () => (event: ReactMouseEvent | ReactTouchEvent) => {
    event.preventDefault();
    draggingRef.current = true;
  };

  const toggleFocus = (panel: Timeframe) => {
    setFocusPanel((prev) => (prev === panel ? null : panel));
  };

  const handleToggleFavorite = async () => {
    if (!code) {
      setToastMessage("お気に入り更新に失敗しました（code未指定）");
      return;
    }
    const next = !isFavorite;
    setFavoriteLocal(code, next);
    try {
      if (next) {
        await api.post(`/favorites/${encodeURIComponent(code)}`);
      } else {
        await api.delete(`/favorites/${encodeURIComponent(code)}`);
      }
    } catch (error: any) {
      setFavoriteLocal(code, !next);
      const status = error?.response?.status;
      const detail =
        error?.response?.data?.error ??
        error?.response?.data?.detail ??
        error?.response?.data ??
        error?.message;
      if (status) {
        setToastMessage(`お気に入り更新に失敗しました（HTTP ${status}）`);
      } else if (detail) {
        setToastMessage(`お気に入り更新に失敗しました（${String(detail)}）`);
      } else {
        setToastMessage("お気に入り更新に失敗しました");
      }
    }
  };

  const handleDeleteTicker = async () => {
    if (!code || deleteBusy) return;
    const confirmed =
      typeof window === "undefined"
        ? false
        : window.confirm(
          `${code} を削除しますか？関連する code.txt / data/txt / DB / お気に入り / 練習セッションも削除されます。`
        );
    if (!confirmed) return;
    setDeleteBusy(true);
    setToastAction(null);
    try {
      const res = await api.post("/watchlist/remove", {
        code,
        deleteArtifacts: true,
        deleteDb: true,
        deleteRelated: true
      });
      const payload = res.data as {
        ok?: boolean;
        error?: string;
        removed?: boolean;
        dbDeletedTotal?: number;
        favoritesDeleted?: number;
        practiceDeleted?: number;
      };
      if (!payload?.ok) {
        setToastMessage(
          payload?.error ? `削除に失敗しました: ${payload.error}` : "削除に失敗しました"
        );
        return;
      }
      const dbDeleted = payload.dbDeletedTotal ?? 0;
      const favoritesDeleted = payload.favoritesDeleted ?? 0;
      const practiceDeleted = payload.practiceDeleted ?? 0;
      if (!payload.removed && dbDeleted == 0) {
        setToastMessage("削除対象が見つかりませんでした");
      } else {
        setToastMessage(
          `削除しました (DB:${dbDeleted} お気に入り:${favoritesDeleted} 練習:${practiceDeleted})`
        );
      }
      await loadList();
      if (nextCode) {
        navigate(`/${detailRouteTarget}/${nextCode}`, { state: { from: listBackPath } });
      } else {
        navigate(listBackPath);
      }
    } catch {
      setToastMessage("削除に失敗しました");
    } finally {
      setDeleteBusy(false);
    }
  };

  /* Handlers replaced by hooks */
  const syncAnalysisCursorTime = (time: number | null) => {
    if (!cursorMode) return;
    if (time == null) {
      if (analysisCursorTime != null) {
        setAnalysisCursorTime(null);
      }
      return;
    }
    const nearestTime = findNearestCandleTime(dailyCandles, time);
    if (nearestTime == null || nearestTime === analysisCursorTime) return;
    setAnalysisCursorTime(nearestTime);
  };

  const handleDailyCrosshair = (
    time: number | null,
    point?: { x: number; y: number } | null
  ) => {
    mainSync.handleDailyCrosshair(time, point ?? null);
    syncAnalysisCursorTime(time);
  };
  const handleWeeklyCrosshair = (
    time: number | null,
    point?: { x: number; y: number } | null
  ) => {
    mainSync.handleWeeklyCrosshair(time, point ?? null);
    syncAnalysisCursorTime(time);
  };
  const handleMonthlyCrosshair = (
    time: number | null,
    point?: { x: number; y: number } | null
  ) => {
    mainSync.handleMonthlyCrosshair(time, point ?? null);
    syncAnalysisCursorTime(time);
  };

  const handleCompareMonthlyCrosshair = (
    time: number | null,
    source: "left" | "right",
    point?: { x: number; y: number } | null
  ) => {
    if (source === "left") {
      // Main chart (Left)
      handleMonthlyCrosshair(time, point ?? null);
    } else {
      // Compare chart (Right)
      compareSync.handleMonthlyCrosshair(time, point ?? null);
    }
  };

  const handleCompareDailyCrosshair = (
    time: number | null,
    source: "left" | "right",
    point?: { x: number; y: number } | null
  ) => {
    if (source === "left") {
      handleDailyCrosshair(time, point ?? null);
    } else {
      compareSync.handleDailyCrosshair(time, point ?? null);
    }
  };

  const dailyEmptyMessage =
    dailyCandles.length === 0
      ? chartLifecycle.daily.message ?? (chartLifecycle.daily.status === "idle" ? "表示できるデータがありません" : null)
      : null;
  const weeklyEmptyMessage =
    weeklyCandles.length === 0
      ? chartLifecycle.weekly.message ?? (chartLifecycle.weekly.status === "idle" ? null : weeklyError)
      : null;
  const monthlyEmptyMessage =
    monthlyCandles.length === 0
      ? chartLifecycle.monthly.message ?? (chartLifecycle.monthly.status === "idle" ? "表示できるデータがありません" : null)
      : null;
  const chartNoticeKind = (status: string): ProductStateNoticeKind => {
    if (status === "loading") return "loading";
    if (status === "error") return "error";
    if (status === "missing") return "missing";
    if (status === "empty") return "empty";
    return "empty";
  };

  const monthlyRatio = 1 - weeklyRatio;
  const focusTitle =
    focusPanel === "daily" ? "日足（拡大）" : focusPanel === "weekly" ? "週足（拡大）" : "月足（拡大）";
  const listBackPath = useMemo(() => {
    const state = location.state as { from?: string } | null;
    return readDetailListBackPath(state);
  }, [location.state]);
  const detailRouteTarget = location.pathname.startsWith("/detail-v2/") ? "detail-v2" : "detail";
  const listCodes = useMemo(() => {
    return readDetailListCodes();
  }, []);
  useEffect(() => {
    setTickerCodeInput(code ?? "");
  }, [code]);
  const handleTickerCodeSubmit = useCallback(
    (event: FormEvent<HTMLFormElement>) => {
      event.preventDefault();
      const nextCode = tickerCodeInput.trim();
      if (!nextCode || nextCode === code || !/^[0-9A-Za-z.-]+$/.test(nextCode)) return;
      void openDetailWithPrefetch({
        navigate,
        code: nextCode,
        listCodes,
        backPath: listBackPath,
        asof: mainAsOf,
        backendReady,
        prefetchWaitMs: 120,
        targetRoute: detailRouteTarget,
      });
    },
    [backendReady, code, detailRouteTarget, listBackPath, listCodes, mainAsOf, navigate, tickerCodeInput]
  );
  const compareList = useMemo(() => {
    if (typeof window === "undefined") return null;
    try {
      const stored = window.sessionStorage.getItem("similarCompareList");
      if (!stored) return null;
      const parsed = JSON.parse(stored) as CompareListPayload;
      if (!parsed || typeof parsed !== "object") return null;
      if (typeof parsed.queryTicker !== "string" || !Array.isArray(parsed.items)) return null;
      const items = parsed.items
        .map((item) => ({
          ticker: typeof item?.ticker === "string" ? item.ticker : "",
          asof: typeof item?.asof === "string" ? item.asof : null
        }))
        .filter((item) => item.ticker);
      return {
        queryTicker: parsed.queryTicker,
        mainAsOf: typeof parsed.mainAsOf === "string" ? parsed.mainAsOf : null,
        items
      };
    } catch {
      return null;
    }
  }, []);
  const compareListItems = useMemo(() => compareList?.items ?? [], [compareList]);
  const compareListEligible = useMemo(() => {
    if (!compareList) return false;
    if (compareList.queryTicker !== code) return false;
    const storedMainAsOf = compareList.mainAsOf ?? null;
    const currentMainAsOf = mainAsOf ?? null;
    return storedMainAsOf === currentMainAsOf;
  }, [compareList, code, mainAsOf]);
  const currentCompareListIndex = useMemo(() => {
    if (!compareListEligible || !compareCode) return null;
    const index =
      compareIndex != null &&
      compareListItems[compareIndex]?.ticker === compareCode &&
      (compareListItems[compareIndex]?.asof ?? null) === (compareAsOf ?? null)
        ? compareIndex
        : compareListItems.findIndex(
            (item) => item.ticker === compareCode && (item.asof ?? null) === (compareAsOf ?? null)
          );
    return index >= 0 ? index : null;
  }, [compareListEligible, compareListItems, compareCode, compareAsOf, compareIndex]);
  const nextCompareItem = useMemo(() => {
    if (currentCompareListIndex == null) return null;
    return compareListItems[currentCompareListIndex + 1] ?? null;
  }, [compareListItems, currentCompareListIndex]);
  const prevCode = useMemo(() => {
    if (!code) return null;
    const index = listCodes.indexOf(code);
    if (index <= 0) return null;
    return listCodes[index - 1] ?? null;
  }, [listCodes, code]);

  const nextCode = useMemo(() => {
    if (!code) return null;
    const index = listCodes.indexOf(code);
    if (index < 0) return null;
    return listCodes[index + 1] ?? null;
  }, [listCodes, code]);
  const detailPrefetchTargets = useMemo(() => {
    if (!code) {
      return {
        immediate: [] as string[],
        delayed: [] as string[],
      };
    }
    const index = listCodes.indexOf(code);
    if (index < 0) {
      return {
        immediate: [] as string[],
        delayed: [] as string[],
      };
    }
    const immediate: string[] = [];
    const delayed: string[] = [];
    for (let offset = 1; offset <= 8; offset += 1) {
      const left = listCodes[index - offset] ?? null;
      const right = listCodes[index + offset] ?? null;
      const bucket = offset <= 2 ? immediate : delayed;
      if (left) bucket.push(left);
      if (right) bucket.push(right);
    }
    return { immediate, delayed };
  }, [listCodes, code]);
  useEffect(() => {
    if (!backendReady || compareCode) return;
    if (dailyFetch.status !== "success") return;
    if (!detailPrefetchTargets.immediate.length && !detailPrefetchTargets.delayed.length) return;
    let cancelled = false;
    const sleep = (delayMs: number) =>
      new Promise<void>((resolve) => {
        window.setTimeout(resolve, delayMs);
      });
    const runPrefetchQueue = async (candidates: string[], initialDelayMs: number, stepDelayMs: number) => {
      for (let index = 0; index < candidates.length; index += 1) {
        const candidate = candidates[index];
        if (!candidate) continue;
        const delayMs = index === 0 ? initialDelayMs : stepDelayMs;
        await sleep(delayMs);
        if (cancelled) return;
        try {
          await prefetchDetailChartFrames({
            code: candidate,
            dailyLimit: DEFAULT_LIMITS.daily,
            weeklyLimit: DEFAULT_LIMITS.weekly,
            monthlyLimit: DEFAULT_LIMITS.monthly,
            asof: mainAsOf,
          });
        } catch {
          // 背景先読みの失敗は現在表示中の詳細チャートに影響させない。
        }
      }
    };
    void (async () => {
      await runPrefetchQueue(detailPrefetchTargets.immediate, 120, 160);
      if (cancelled) return;
      await runPrefetchQueue(detailPrefetchTargets.delayed, 420, 240);
    })();
    return () => {
      cancelled = true;
    };
  }, [backendReady, compareCode, dailyFetch.status, detailPrefetchTargets, mainAsOf]);
  const headerScreenshotButton = (
    <IconButton
      label="スクショ"
      icon={<IconCamera size={18} />}
      variant="iconLabel"
      disabled={screenshotBusy}
      tooltip="スクショ"
      className="screenshot-button"
      onClick={async () => {
        if (screenshotBusy) return;
        setScreenshotBusy(true);
        setToastAction(null);
        try {
          const screenType = getScreenType(location.pathname);
          const result = await captureAndCopyScreenshot({ screenType, code });
          if (!result.success) {
            setToastMessage(result.error ?? "スクショに失敗しました");
            return;
          }

          if (!result.copied) {
            setToastMessage("スクショをクリップボードにコピーできませんでした。");
            setToastAction(null);
            return;
          }

          setToastMessage("スクショをコピーしました。");
          setToastAction(null);
        } finally {
          setScreenshotBusy(false);
        }
      }}
    />
  );
  const headerRangeControls = (
    <DetailTimeframeSwitcher presets={RANGE_PRESETS} rangeMonths={rangeMonths} onChange={toggleRange} />
  );

  const headerModeControls = (
    <DetailModeTabs
      activeMode={headerMode}
      similarActive={showSimilar}
      showAnalysis={analysisAvailable}
      onChart={() => setHeaderMode("chart")}
      onAnalysis={() => {
        setHeaderMode("analysis");
        if (!cursorMode) {
          setCursorMode(true);
          if (dailyCandles.length > 0) {
            updateSelectedBar(dailyCandles.length - 1);
          }
        }
      }}
      onSimilar={() => setShowSimilar(true)}
      onFinancial={() => setHeaderMode("financial")}
      onPractice={() => {
        if (code) navigate(`/practice/${code}`);
      }}
      onPositions={() => setHeaderMode("positions")}
    />
  );

  const headerDisplayMenu = (
    <div className="popover-anchor" ref={displayRef}>
      <IconButton
        icon={<IconAdjustments size={18} />}
        label="表示"
        variant="iconLabel"
        tooltip="表示設定"
        ariaLabel="表示設定メニューを開く"
        selected={displayOpen}
        className="display-button"
        onClick={() => setDisplayOpen((prev) => !prev)}
      />
      {displayOpen && (
        <div className="popover-panel">
          <div className="popover-section">
            <div className="popover-title">表示</div>
            <button
              type="button"
              className={`popover-item ${showBoxes ? "active" : ""}`}
              onClick={() => setShowBoxes(!showBoxes)}
            >
              <span className="popover-item-label">Boxes</span>
              {showBoxes && <span className="popover-check">ON</span>}
            </button>
            <button
              type="button"
              className={`popover-item ${showGapBands ? "active" : ""}`}
              onClick={() => setShowGapBands((prev) => !prev)}
            >
              <span className="popover-item-label">窓</span>
              {showGapBands && <span className="popover-check">ON</span>}
            </button>
            <button
              type="button"
              className={`popover-item ${showVolumeEnabled ? "active" : ""}`}
              onClick={() => setShowVolumeEnabled((prev) => !prev)}
            >
              <span className="popover-item-label">出来高</span>
              {showVolumeEnabled && <span className="popover-check">ON</span>}
            </button>
            <button
              type="button"
              className={`popover-item ${showTdnetMarkers ? "active" : ""}`}
              onClick={() => setShowTdnetMarkers((prev) => !prev)}
            >
              <span className="popover-item-label">TDNETマーカー</span>
              {showTdnetMarkers && <span className="popover-check">ON</span>}
            </button>
            <button
              type="button"
              className={`popover-item ${showTradeMarkers ? "active" : ""}`}
              onClick={() => setShowTradeMarkers((prev) => !prev)}
            >
              <span className="popover-item-label">売買マーカー</span>
              {showTradeMarkers && <span className="popover-check">ON</span>}
            </button>
            <button
              type="button"
              className={`popover-item ${showRankingMarkers ? "active" : ""}`}
              onClick={() => setShowRankingMarkers((prev) => !prev)}
            >
              <span className="popover-item-label">シグナルマーカー</span>
              {showRankingMarkers && <span className="popover-check">ON</span>}
            </button>
            <button
              type="button"
              className={`popover-item ${syncRanges ? "active" : ""}`}
              onClick={() => setSyncRanges((prev) => !prev)}
            >
              <span className="popover-item-label">連動</span>
              {syncRanges && <span className="popover-check">ON</span>}
            </button>
            <button
              type="button"
              className={`popover-item ${cursorMode ? "active" : ""}`}
              onClick={toggleCursorMode}
            >
              <span className="popover-item-label">日付選択</span>
              {cursorMode && <span className="popover-check">ON</span>}
            </button>
          </div>
          <div className="popover-section">
            <button type="button" className="popover-item" onClick={() => setShowIndicators(true)}>
              <span className="popover-item-label">MA/Indicators</span>
            </button>
          </div>
          <div className="popover-section">
            <button
              type="button"
              className="popover-item"
              disabled={deleteBusy || !code}
              onClick={() => {
                setDisplayOpen(false);
                handleDeleteTicker();
              }}
            >
              <span className="popover-item-label">銘柄を削除</span>
            </button>
          </div>
        </div>
      )}
    </div>
  );

  return (
    <div className={`detail-shell ${focusPanel ? "detail-shell-focus" : ""}`}>
      <DetailHeaderChrome
        summaryBack={
          <button
            className="back nav-button nav-primary"
            onClick={() => navigate(listBackPath)}
          >
            <span className="nav-icon">
              <IconArrowLeft size={16} />
            </span>
            <span className="nav-label">一覧に戻る</span>
          </button>
        }
        summaryMain={
          <div className="detail-summary-title">
            <form className="detail-summary-code-form" onSubmit={handleTickerCodeSubmit}>
              <input
                className="detail-summary-code"
                aria-label="銘柄コードを入力"
                inputMode="text"
                value={tickerCodeInput}
                onChange={(event) => setTickerCodeInput(event.target.value)}
                onBlur={() => setTickerCodeInput(code ?? "")}
              />
            </form>
            {tickerName && <div className="detail-summary-name">{tickerName}</div>}
          </div>
        }
        summaryStatus={
          (rightsLabel || earningsLabel) ? (
            <div className="detail-event-badges detail-event-badges-inline">
              {rightsLabel && <span className="event-badge event-rights">権利 {rightsLabel}</span>}
              {earningsLabel && <span className="event-badge event-earnings">決算 {earningsLabel}</span>}
            </div>
          ) : null
        }
        summaryCenter={
          <div className="detail-status-stack">
            <details className="detail-status-disclosure">
              <summary className="detail-status-disclosure-button" aria-label="表示状態">
                <IconInfoCircle size={16} />
              </summary>
              <div className="detail-status-disclosure-panel">
                <TradexShadowReadout variant="detail" />
                <DataFreshnessBadges contract={detailDataFreshnessContract} scope="detail" compact />
              </div>
            </details>
            {headerRangeControls}
          </div>
        }
        summaryActions={
          <>
            <button
              type="button"
              className={isFavorite ? "favorite-toggle active" : "favorite-toggle"}
              aria-pressed={isFavorite}
              aria-label={isFavorite ? "お気に入り解除" : "お気に入り追加"}
              onClick={handleToggleFavorite}
            >
              {isFavorite ? <IconHeartFilled size={18} /> : <IconHeart size={18} />}
            </button>
            <button
              className="back nav-button"
              onClick={() => {
                if (!prevCode) return;
                void openDetailWithPrefetch({
                  navigate,
                  code: prevCode,
                  listCodes,
                  backPath: listBackPath,
                  asof: mainAsOf,
                  backendReady,
                  prefetchWaitMs: 120,
                  targetRoute: detailRouteTarget,
                });
              }}
              disabled={!prevCode}
            >
              <span className="nav-icon">
                <IconArrowLeft size={16} />
              </span>
              <span className="nav-label">前の銘柄</span>
            </button>
            <button
              className="back nav-button"
              onClick={() => {
                if (!nextCode) return;
                void openDetailWithPrefetch({
                  navigate,
                  code: nextCode,
                  listCodes,
                  backPath: listBackPath,
                  asof: mainAsOf,
                  backendReady,
                  prefetchWaitMs: 120,
                  targetRoute: detailRouteTarget,
                });
              }}
              disabled={!nextCode}
            >
              <span className="nav-icon">
                <IconArrowRight size={16} />
              </span>
              <span className="nav-label">次の銘柄</span>
            </button>
          </>
        }
        modeControls={headerModeControls}
        topbarActions={
          <>
            {headerDisplayMenu}
            {headerScreenshotButton}
            {headerDrawToolControls}
          </>
        }
      />
      {marketDataStatusMessage && (
        <div className={`detail-market-data-status ${marketDataStatusDelayed ? "is-delayed" : ""}`}>
          {marketDataStatusMessage}
        </div>
      )}
      <div className={`detail-content ${showRightPanel ? "with-right-rail" : ""}`}>
        <div className={`detail-split ${focusPanel ? "detail-split-focus" : ""}`}>
          {compareCode && (
            <div className="detail-compare">
              <div className="detail-compare-header">
                <div>
                  <div className="detail-compare-title">
                    比較 {code} / {compareCode}
                  </div>
                  {compareAsOf && (
                    <div className="detail-compare-subtitle">類似日付: {compareAsOf}</div>
                  )}
                </div>
                <div className="detail-compare-actions">
                  <button
                    type="button"
                    className="detail-compare-close"
                    disabled={!nextCompareItem}
                    onClick={() => {
                      if (!nextCompareItem) return;
                      const params = new URLSearchParams();
                      params.set("compare", nextCompareItem.ticker);
                      if (mainAsOf) {
                        params.set("mainAsOf", mainAsOf);
                      }
                      if (nextCompareItem.asof) {
                        params.set("compareAsOf", nextCompareItem.asof);
                      }
                      const nextIndex = currentCompareListIndex == null ? -1 : currentCompareListIndex + 1;
                      if (nextIndex >= 0) {
                        params.set("compareIndex", String(nextIndex));
                      }
                      navigate(`/detail/${code}?${params.toString()}`);
                    }}
                  >
                    次の比較
                  </button>
                  <button
                    type="button"
                    className="detail-compare-close"
                    onClick={() => navigate(`/detail/${code}`)}
                  >
                    比較解除
                  </button>
                </div>
              </div>
              <div className="detail-compare-grid">
                <div className="detail-compare-cell">
                  <div className="detail-compare-cell-header">
                    <div className="detail-compare-cell-title">{code} {tickerName}</div>
                    <div className="detail-compare-cell-meta">月足 ({leftMonthlyRangeLabel})</div>
                  </div>
                  <div className="detail-chart detail-compare-chart">
                    <DetailChart
                      ref={monthlyChartRef}
                      candles={monthlyCandles}
                      volume={monthlyVolume}
                      maLines={monthlyChartMaLines}
                      showVolume={false}
                      boxes={boxes}
                      showBoxes={showBoxes}
                      gapBands={gapBandsOverride}
                      drawingEnabled={activeDrawTool != null}
                      timeZones={monthlyDrawings.timeZones}
                      priceBands={monthlyDrawings.priceBands}
                      drawBoxes={monthlyDrawings.drawBoxes}
                      horizontalLines={monthlyDrawings.horizontalLines}
                      showPriceBands
                      meeMeeDetailChrome={DETAIL_CHROME_MONTHLY}
                      meeMeeDetailChromeTerminalDates={monthlyChromeTerminalDates}
                      selectionEnabled={!annotationMode && drawingSelectionMode}
                      activeTool={activeDrawTool}
                      onDrawCommit={handleDrawCommit}
                      activeDrawColor={activeDrawColor}
                      activeLineOpacity={activeLineOpacity}
                      activeLineWidth={activeLineWidth}
                      onSelectShape={handleSelectDrawing}
                      onAddTimeZone={addTimeZone(monthlyDrawingKey)}
                      onUpdateTimeZone={updateTimeZone(monthlyDrawingKey)}
                      onDeleteTimeZone={deleteTimeZone(monthlyDrawingKey)}
                      onAddPriceBand={addPriceBand(monthlyDrawingKey)}
                      onUpdatePriceBand={updatePriceBand(monthlyDrawingKey)}
                      onDeletePriceBand={deletePriceBand(monthlyDrawingKey)}
                      onAddDrawBox={addDrawBox(monthlyDrawingKey)}
                      onUpdateDrawBox={updateDrawBox(monthlyDrawingKey)}
                      onDeleteDrawBox={deleteDrawBox(monthlyDrawingKey)}
                      onAddHorizontalLine={addHorizontalLine(monthlyDrawingKey)}
                      onUpdateHorizontalLine={updateHorizontalLine(monthlyDrawingKey)}
                      onDeleteHorizontalLine={deleteHorizontalLine(monthlyDrawingKey)}
                      partialTimes={monthlyYearBoundaries}
                      visibleRange={monthlyCandles.length ? compareMonthlyBaseRange : null}
                      onCrosshairMove={(time, point) =>
                        handleCompareMonthlyCrosshair(time, "left", point)
                      }
                      onVisibleRangeChange={handleMonthlyVisibleRangeChange}
                    />
                    {monthlyEmptyMessage && (
                      <ProductStateNotice
                        kind={chartNoticeKind(chartLifecycle.monthly.status)}
                        prefix="月足"
                        className="detail-chart-empty"
                      >
                        {monthlyEmptyMessage}
                      </ProductStateNotice>
                    )}
                  </div>
                </div>
                <div className="detail-compare-cell">
                  <div className="detail-compare-cell-header">
                    <div className="detail-compare-cell-title">{compareCode} {compareTickerName}</div>
                    <div className="detail-compare-cell-meta">月足 ({rightMonthlyRangeLabel})</div>
                  </div>
                  <div className="detail-chart detail-compare-chart">
                    {shouldRenderCompareMonthlyChart && (
                      <DetailChart
                        ref={compareMonthlyChartRef}
                        candles={compareMonthlyCandles}
                        volume={[]}
                        maLines={compareMonthlyChartMaLines}
                        showVolume={false}
                        boxes={compareBoxes}
                        showBoxes={showBoxes}
                        gapBands={gapBandsOverride}
                        drawingEnabled={dailyAnnotationDrawTool != null}
                        timeZones={compareMonthlyDrawings.timeZones}
                        priceBands={compareMonthlyDrawings.priceBands}
                        drawBoxes={compareMonthlyDrawings.drawBoxes}
                        horizontalLines={compareMonthlyDrawings.horizontalLines}
                        showPriceBands
                        meeMeeDetailChrome={DETAIL_CHROME_MONTHLY}
                        meeMeeDetailChromeTerminalDates={compareMonthlyChromeTerminalDates}
                        selectionEnabled={!annotationMode && drawingSelectionMode}
                        activeTool={activeDrawTool}
                        onDrawCommit={handleDrawCommit}
                        activeDrawColor={activeDrawColor}
                        activeLineOpacity={activeLineOpacity}
                        activeLineWidth={activeLineWidth}
                        onSelectShape={handleSelectDrawing}
                        onAddTimeZone={addTimeZone(compareMonthlyDrawingKey)}
                        onUpdateTimeZone={updateTimeZone(compareMonthlyDrawingKey)}
                        onDeleteTimeZone={deleteTimeZone(compareMonthlyDrawingKey)}
                        onAddPriceBand={addPriceBand(compareMonthlyDrawingKey)}
                        onUpdatePriceBand={updatePriceBand(compareMonthlyDrawingKey)}
                        onDeletePriceBand={deletePriceBand(compareMonthlyDrawingKey)}
                        onAddDrawBox={addDrawBox(compareMonthlyDrawingKey)}
                        onUpdateDrawBox={updateDrawBox(compareMonthlyDrawingKey)}
                        onDeleteDrawBox={deleteDrawBox(compareMonthlyDrawingKey)}
                        onAddHorizontalLine={addHorizontalLine(compareMonthlyDrawingKey)}
                        onUpdateHorizontalLine={updateHorizontalLine(compareMonthlyDrawingKey)}
                        onDeleteHorizontalLine={deleteHorizontalLine(compareMonthlyDrawingKey)}
                        visibleRange={compareMonthlyVisibleRange}
                        onCrosshairMove={(time, point) =>
                          handleCompareMonthlyCrosshair(time, "right", point)
                        }
                        onVisibleRangeChange={handleCompareMonthlyVisibleRangeChange}
                      />
                    )}
                    {(compareLoading || compareChartPendingSwap) && compareMonthlyCandles.length === 0 && (
                      <ProductStateNotice kind="loading" className="detail-chart-empty">読み込み中...</ProductStateNotice>
                    )}
                    {!compareLoading && !compareChartPendingSwap && compareMonthlyErrors.length > 0 && (
                      <ProductStateNotice kind="error" prefix="月足" className="detail-chart-empty">
                        {compareMonthlyErrors[0]}
                      </ProductStateNotice>
                    )}
                    {!compareLoading && !compareChartPendingSwap && compareMonthlyErrors.length === 0 && compareMonthlyCandles.length === 0 && (
                      <ProductStateNotice kind="empty" prefix="月足" className="detail-chart-empty">
                        データがありません
                      </ProductStateNotice>
                    )}
                  </div>
                </div>
                <div className="detail-compare-cell">
                  <div className="detail-compare-cell-header">
                    <div className="detail-compare-cell-title">{code} {tickerName}</div>
                    <div className="detail-compare-cell-meta">日足 ({leftDailyRangeLabel})</div>
                  </div>
                  <div className="detail-chart detail-compare-chart">
                    {!holdDailyChartUntilDecisionReady && (
                      <DetailChart
                        ref={dailyChartRef}
                        candles={dailyCandles}
                        volume={dailyVolume}
                        maLines={dailyChartMaLines}
                        showVolume={showVolumeDaily}
                        eventMarkers={mergedDailyEventMarkers}
                        boxes={boxes}
                        showBoxes={showBoxes}
                        gapBands={gapBandsOverride}
                        drawingEnabled={activeDrawTool != null}
                        timeZones={dailyDrawings.timeZones}
                        priceBands={dailyPriceBandsWithPositionRisk}
                        drawBoxes={dailyDrawBoxesWithAnnotations}
                        horizontalLines={dailyHorizontalLinesWithPositionRisk}
                        callouts={annotationCallouts}
                        showPriceBands
                        meeMeeDetailChrome={DETAIL_CHROME_DAILY}
                        selectionEnabled={!annotationMode && drawingSelectionMode}
                        activeTool={dailyAnnotationDrawTool}
                        onDrawCommit={handleDrawCommit}
                        activeDrawColor={activeDrawColor}
                        activeLineOpacity={activeLineOpacity}
                        activeLineWidth={activeLineWidth}
                        onSelectShape={handleSelectDrawing}
                        onSelectCallout={setSelectedAnnotationId}
                        onAddTimeZone={addTimeZone(dailyDrawingKey)}
                        onUpdateTimeZone={updateTimeZone(dailyDrawingKey)}
                        onDeleteTimeZone={deleteTimeZone(dailyDrawingKey)}
                        onAddPriceBand={addPriceBand(dailyDrawingKey)}
                        onUpdatePriceBand={updatePriceBand(dailyDrawingKey)}
                        onDeletePriceBand={deletePriceBand(dailyDrawingKey)}
                        onAddDrawBox={handleDailyAddDrawBox}
                        onUpdateDrawBox={updateDrawBox(dailyDrawingKey)}
                        onDeleteDrawBox={deleteDrawBox(dailyDrawingKey)}
                        onAddHorizontalLine={handleDailyAddHorizontalLine}
                        onUpdateHorizontalLine={updateHorizontalLine(dailyDrawingKey)}
                        onDeleteHorizontalLine={deleteHorizontalLine(dailyDrawingKey)}
                        partialTimes={dailyMonthBoundaries}
                        visibleRange={dailyCandles.length ? resolvedDailyVisibleRange : null}
                        positionOverlay={{
                          dailyPositions,
                          tradeMarkers,
                          showOverlay: showTradesOverlay,
                          showMarkers: showTradeMarkers,
                          showPnL: showPnLPanel,
                          hoverTime: resolvedCursorAsOfTime ?? mainSync.hoverTime,
                          currentPositions,
                          latestTradeTime
                        }}
                        cursorTime={resolvedCursorAsOfTime}
                        selectedTime={selectedBarData?.time ?? null}
                        onCrosshairMove={(time, point) =>
                          handleCompareDailyCrosshair(time, "left", point)
                        }
                        onVisibleRangeChange={handleDailyVisibleRangeChange}
                        onChartClick={handleDailyChartClick}
                      />
                    )}
                    {holdDailyChartUntilDecisionReady && (
                      <ProductStateNotice kind="loading" className="detail-chart-empty">判定マークを読み込み中...</ProductStateNotice>
                    )}
                    {dailyEmptyMessage && (
                      <ProductStateNotice
                        kind={chartNoticeKind(chartLifecycle.daily.status)}
                        prefix="日足"
                        className="detail-chart-empty"
                      >
                        {dailyEmptyMessage}
                      </ProductStateNotice>
                    )}
                  </div>
                </div>
                <div className="detail-compare-cell">
                  <div className="detail-compare-cell-header">
                    <div className="detail-compare-cell-title">{compareCode} {compareTickerName}</div>
                    <div className="detail-compare-cell-meta">日足 ({rightDailyRangeLabel})</div>
                  </div>
                  <div className="detail-chart detail-compare-chart">
                    {shouldRenderCompareDailyChart && (
                      <DetailChart
                        ref={compareDailyChartRef}
                        candles={compareDailyCandles}
                        volume={compareDailyVolume}
                        maLines={compareDailyChartMaLines}
                        showVolume={showVolumeEnabled && compareDailyVolume.length > 0}
                        boxes={compareBoxes}
                        showBoxes={showBoxes}
                        gapBands={gapBandsOverride}
                        drawingEnabled={activeDrawTool != null}
                        timeZones={compareDailyDrawings.timeZones}
                        priceBands={compareDailyDrawings.priceBands}
                        drawBoxes={compareDailyDrawings.drawBoxes}
                        horizontalLines={compareDailyDrawings.horizontalLines}
                        showPriceBands
                        meeMeeDetailChrome={DETAIL_CHROME_DAILY}
                        selectionEnabled={!annotationMode && drawingSelectionMode}
                        activeTool={activeDrawTool}
                        onDrawCommit={handleDrawCommit}
                        activeDrawColor={activeDrawColor}
                        activeLineOpacity={activeLineOpacity}
                        activeLineWidth={activeLineWidth}
                        onSelectShape={handleSelectDrawing}
                        onAddTimeZone={addTimeZone(compareDailyDrawingKey)}
                        onUpdateTimeZone={updateTimeZone(compareDailyDrawingKey)}
                        onDeleteTimeZone={deleteTimeZone(compareDailyDrawingKey)}
                        onAddPriceBand={addPriceBand(compareDailyDrawingKey)}
                        onUpdatePriceBand={updatePriceBand(compareDailyDrawingKey)}
                        onDeletePriceBand={deletePriceBand(compareDailyDrawingKey)}
                        onAddDrawBox={addDrawBox(compareDailyDrawingKey)}
                        onUpdateDrawBox={updateDrawBox(compareDailyDrawingKey)}
                        onDeleteDrawBox={deleteDrawBox(compareDailyDrawingKey)}
                        onAddHorizontalLine={addHorizontalLine(compareDailyDrawingKey)}
                        onUpdateHorizontalLine={updateHorizontalLine(compareDailyDrawingKey)}
                        onDeleteHorizontalLine={deleteHorizontalLine(compareDailyDrawingKey)}
                        visibleRange={compareDailyVisibleRange}
                        positionOverlay={{
                          dailyPositions: compareDailyPositions,
                          tradeMarkers: compareTradeMarkers,
                          showOverlay: showTradesOverlay,
                          showMarkers: showTradeMarkers,
                          showPnL: showPnLPanel,
                          hoverTime: compareSync.hoverTime
                        }}
                        onCrosshairMove={(time, point) =>
                          handleCompareDailyCrosshair(time, "right", point)
                        }
                        onVisibleRangeChange={handleCompareDailyVisibleRangeChange}
                      />
                    )}
                    {(compareDailyLoading || compareChartPendingSwap || compareDailyNeedsMore) && compareDailyCandles.length === 0 && (
                      <ProductStateNotice kind="loading" className="detail-chart-empty">一致期間のデータを読み込み中...</ProductStateNotice>
                    )}
                    {!compareDailyLoading && !compareChartPendingSwap && compareDailyErrors.length > 0 && (
                      <ProductStateNotice kind="error" prefix="日足" className="detail-chart-empty">
                        {compareDailyErrors[0]}
                      </ProductStateNotice>
                    )}
                    {!compareDailyLoading && !compareChartPendingSwap && compareDailyErrors.length === 0 && compareDailyCandles.length === 0 && (
                      <ProductStateNotice kind="empty" prefix="日足" className="detail-chart-empty">
                        データがありません
                      </ProductStateNotice>
                    )}
                  </div>
                </div>
              </div>
            </div>
          )}
          {compareCode ? null : focusPanel ? (
            <div className="detail-row detail-row-focus">
              <div
                className="detail-chart detail-chart-focused"
                onDoubleClick={() => toggleFocus(focusPanel)}
              >
                {focusPanel === "daily" && (
                  !holdDailyChartUntilDecisionReady && (
                    <DetailChart
                      ref={dailyChartRef}
                      candles={dailyCandles}
                      volume={dailyVolume}
                      maLines={dailyChartMaLines}
                      showVolume={showVolumeDaily}
                      eventMarkers={mergedDailyEventMarkers}
                      boxes={boxes}
                      showBoxes={showBoxes}
                      gapBands={gapBandsOverride}
                      drawingEnabled={dailyAnnotationDrawTool != null}
                      timeZones={dailyDrawings.timeZones}
                      priceBands={dailyPriceBandsWithPositionRisk}
                      drawBoxes={dailyDrawBoxesWithAnnotations}
                      horizontalLines={dailyHorizontalLinesWithPositionRisk}
                      callouts={annotationCallouts}
                      showPriceBands
                      meeMeeDetailChrome={DETAIL_CHROME_DAILY}
                      selectionEnabled={!annotationMode && drawingSelectionMode}
                      activeTool={dailyAnnotationDrawTool}
                      onDrawCommit={handleDrawCommit}
                      activeDrawColor={activeDrawColor}
                      activeLineOpacity={activeLineOpacity}
                      activeLineWidth={activeLineWidth}
                      onSelectShape={handleSelectDrawing}
                      onSelectCallout={setSelectedAnnotationId}
                      onAddTimeZone={addTimeZone(dailyDrawingKey)}
                      onUpdateTimeZone={updateTimeZone(dailyDrawingKey)}
                      onDeleteTimeZone={deleteTimeZone(dailyDrawingKey)}
                      onAddPriceBand={addPriceBand(dailyDrawingKey)}
                      onUpdatePriceBand={updatePriceBand(dailyDrawingKey)}
                      onDeletePriceBand={deletePriceBand(dailyDrawingKey)}
                      onAddDrawBox={handleDailyAddDrawBox}
                      onUpdateDrawBox={updateDrawBox(dailyDrawingKey)}
                      onDeleteDrawBox={deleteDrawBox(dailyDrawingKey)}
                      onAddHorizontalLine={handleDailyAddHorizontalLine}
                      onUpdateHorizontalLine={updateHorizontalLine(dailyDrawingKey)}
                      onDeleteHorizontalLine={deleteHorizontalLine(dailyDrawingKey)}
                      partialTimes={dailyMonthBoundaries}
                      visibleRange={resolvedDailyVisibleRange}
                      positionOverlay={{
                        dailyPositions,
                        tradeMarkers,
                        showOverlay: showTradesOverlay,
                        showMarkers: showTradeMarkers,
                        showPnL: showPnLPanel,
                        hoverTime: resolvedCursorAsOfTime ?? mainSync.hoverTime,
                        currentPositions,
                        latestTradeTime
                      }}
                      cursorTime={resolvedCursorAsOfTime}
                      selectedTime={selectedBarData?.time ?? null}
                      onCrosshairMove={handleDailyCrosshair}
                      onVisibleRangeChange={handleDailyVisibleRangeChange}
                      onChartClick={handleDailyChartClick}
                    />
                  )
                )}
                {focusPanel === "weekly" && (
                  <DetailChart
                    ref={weeklyChartRef}
                    candles={weeklyCandles}
                    volume={weeklyVolume}
                    maLines={weeklyChartMaLines}
                    showVolume={false}
                    boxes={boxes}
                    showBoxes={showBoxes}
                    gapBands={gapBandsOverride}
                    drawingEnabled={activeDrawTool != null}
                    timeZones={weeklyDrawings.timeZones}
                    priceBands={weeklyDrawings.priceBands}
                    drawBoxes={weeklyDrawings.drawBoxes}
                    horizontalLines={weeklyDrawings.horizontalLines}
                    showPriceBands
                    meeMeeDetailChrome={DETAIL_CHROME_WEEKLY}
                    meeMeeDetailChromeTerminalDates={weeklyChromeTerminalDates}
                    selectionEnabled={!annotationMode && drawingSelectionMode}
                    activeTool={activeDrawTool}
                    onDrawCommit={handleDrawCommit}
                    activeDrawColor={activeDrawColor}
                    activeLineOpacity={activeLineOpacity}
                    activeLineWidth={activeLineWidth}
                    onSelectShape={handleSelectDrawing}
                    onAddTimeZone={addTimeZone(weeklyDrawingKey)}
                    onUpdateTimeZone={updateTimeZone(weeklyDrawingKey)}
                    onDeleteTimeZone={deleteTimeZone(weeklyDrawingKey)}
                    onAddPriceBand={addPriceBand(weeklyDrawingKey)}
                    onUpdatePriceBand={updatePriceBand(weeklyDrawingKey)}
                    onDeletePriceBand={deletePriceBand(weeklyDrawingKey)}
                    onAddDrawBox={addDrawBox(weeklyDrawingKey)}
                    onUpdateDrawBox={updateDrawBox(weeklyDrawingKey)}
                    onDeleteDrawBox={deleteDrawBox(weeklyDrawingKey)}
                    onAddHorizontalLine={addHorizontalLine(weeklyDrawingKey)}
                    onUpdateHorizontalLine={updateHorizontalLine(weeklyDrawingKey)}
                    onDeleteHorizontalLine={deleteHorizontalLine(weeklyDrawingKey)}
                    partialTimes={weeklyMonthBoundaries}
                    visibleRange={resolvedWeeklyVisibleRange}
                    positionOverlay={{
                      dailyPositions,
                      tradeMarkers,
                      showOverlay: showTradesOverlay,
                      showMarkers: false,
                      showPnL: showPnLPanel,
                      hoverTime: resolvedCursorAsOfTime ?? mainSync.hoverTime,
                      currentPositions,
                      latestTradeTime
                    }}
                    cursorTime={resolvedCursorAsOfTime}
                    selectedTime={selectedBarData?.time ?? null}
                    onCrosshairMove={handleWeeklyCrosshair}
                    onVisibleRangeChange={handleWeeklyVisibleRangeChange}
                  />
                )}
                {focusPanel === "monthly" && (
                  <DetailChart
                    ref={monthlyChartRef}
                    candles={monthlyCandles}
                    volume={monthlyVolume}
                    maLines={monthlyChartMaLines}
                    showVolume={false}
                    boxes={boxes}
                    showBoxes={showBoxes}
                    gapBands={gapBandsOverride}
                    drawingEnabled={activeDrawTool != null}
                    timeZones={monthlyDrawings.timeZones}
                    priceBands={monthlyDrawings.priceBands}
                    drawBoxes={monthlyDrawings.drawBoxes}
                    horizontalLines={monthlyDrawings.horizontalLines}
                    showPriceBands
                    meeMeeDetailChrome={DETAIL_CHROME_MONTHLY}
                    meeMeeDetailChromeTerminalDates={monthlyChromeTerminalDates}
                    selectionEnabled={!annotationMode && drawingSelectionMode}
                    activeTool={activeDrawTool}
                    onDrawCommit={handleDrawCommit}
                    activeDrawColor={activeDrawColor}
                    activeLineOpacity={activeLineOpacity}
                    activeLineWidth={activeLineWidth}
                    onSelectShape={handleSelectDrawing}
                    onAddTimeZone={addTimeZone(monthlyDrawingKey)}
                    onUpdateTimeZone={updateTimeZone(monthlyDrawingKey)}
                    onDeleteTimeZone={deleteTimeZone(monthlyDrawingKey)}
                    onAddPriceBand={addPriceBand(monthlyDrawingKey)}
                    onUpdatePriceBand={updatePriceBand(monthlyDrawingKey)}
                    onDeletePriceBand={deletePriceBand(monthlyDrawingKey)}
                    onAddDrawBox={addDrawBox(monthlyDrawingKey)}
                    onUpdateDrawBox={updateDrawBox(monthlyDrawingKey)}
                    onDeleteDrawBox={deleteDrawBox(monthlyDrawingKey)}
                    onAddHorizontalLine={addHorizontalLine(monthlyDrawingKey)}
                    onUpdateHorizontalLine={updateHorizontalLine(monthlyDrawingKey)}
                    onDeleteHorizontalLine={deleteHorizontalLine(monthlyDrawingKey)}
                    partialTimes={monthlyYearBoundaries}
                    visibleRange={resolvedMonthlyVisibleRange}
                    positionOverlay={{
                      dailyPositions,
                      tradeMarkers,
                      showOverlay: showTradesOverlay,
                      showMarkers: false,
                      showPnL: showPnLPanel,
                      hoverTime: resolvedCursorAsOfTime ?? mainSync.hoverTime,
                      currentPositions,
                      latestTradeTime
                    }}
                    cursorTime={resolvedCursorAsOfTime}
                    selectedTime={selectedBarData?.time ?? null}
                    onCrosshairMove={handleMonthlyCrosshair}
                    onVisibleRangeChange={handleMonthlyVisibleRangeChange}
                  />
                )}
                {focusPanel === "daily" && dailyEmptyMessage && (
                  <ProductStateNotice
                    kind={chartNoticeKind(chartLifecycle.daily.status)}
                    prefix="日足"
                    className="detail-chart-empty"
                  >
                    {dailyEmptyMessage}
                  </ProductStateNotice>
                )}
                {focusPanel === "daily" && holdDailyChartUntilDecisionReady && (
                  <ProductStateNotice kind="loading" className="detail-chart-empty">判定マークを読み込み中...</ProductStateNotice>
                )}
                {focusPanel === "weekly" && weeklyEmptyMessage && (
                  <ProductStateNotice
                    kind={chartNoticeKind(chartLifecycle.weekly.status)}
                    prefix="週足"
                    className="detail-chart-empty"
                  >
                    {weeklyEmptyMessage}
                  </ProductStateNotice>
                )}
                {focusPanel === "monthly" && monthlyEmptyMessage && (
                  <ProductStateNotice
                    kind={chartNoticeKind(chartLifecycle.monthly.status)}
                    prefix="月足"
                    className="detail-chart-empty"
                  >
                    {monthlyEmptyMessage}
                  </ProductStateNotice>
                )}
                <button
                  type="button"
                  className="detail-focus-back"
                  onClick={() => setFocusPanel(null)}
                >
                  3画面に戻る
                </button>
              </div>
            </div>
          ) : (
            <>
              <div className="detail-row detail-row-top" style={{ flex: `${DETAIL_DAILY_ROW_RATIO} 1 0%` }}>
                <div
                  className="detail-chart detail-chart-focusable"
                  onDoubleClick={() => toggleFocus("daily")}
                >
                  {!holdDailyChartUntilDecisionReady && (
                    <DetailChart
                      ref={dailyChartRef}
                      candles={dailyCandles}
                      volume={dailyVolume}
                      maLines={dailyChartMaLines}
                      showVolume={showVolumeDaily}
                      eventMarkers={mergedDailyEventMarkers}
                      boxes={boxes}
                      showBoxes={showBoxes}
                      gapBands={gapBandsOverride}
                      drawingEnabled={dailyAnnotationDrawTool != null}
                      timeZones={dailyDrawings.timeZones}
                      priceBands={dailyPriceBandsWithPositionRisk}
                      drawBoxes={dailyDrawBoxesWithAnnotations}
                      horizontalLines={dailyHorizontalLinesWithPositionRisk}
                      callouts={annotationCallouts}
                      showPriceBands
                      meeMeeDetailChrome={DETAIL_CHROME_DAILY}
                      selectionEnabled={!annotationMode && drawingSelectionMode}
                      activeTool={dailyAnnotationDrawTool}
                      onDrawCommit={handleDrawCommit}
                      activeDrawColor={activeDrawColor}
                      activeLineOpacity={activeLineOpacity}
                      activeLineWidth={activeLineWidth}
                      onSelectShape={handleSelectDrawing}
                      onSelectCallout={setSelectedAnnotationId}
                      onAddTimeZone={addTimeZone(dailyDrawingKey)}
                      onUpdateTimeZone={updateTimeZone(dailyDrawingKey)}
                      onDeleteTimeZone={deleteTimeZone(dailyDrawingKey)}
                      onAddPriceBand={addPriceBand(dailyDrawingKey)}
                      onUpdatePriceBand={updatePriceBand(dailyDrawingKey)}
                      onDeletePriceBand={deletePriceBand(dailyDrawingKey)}
                      onAddDrawBox={handleDailyAddDrawBox}
                      onUpdateDrawBox={updateDrawBox(dailyDrawingKey)}
                      onDeleteDrawBox={deleteDrawBox(dailyDrawingKey)}
                      onAddHorizontalLine={handleDailyAddHorizontalLine}
                      onUpdateHorizontalLine={updateHorizontalLine(dailyDrawingKey)}
                      onDeleteHorizontalLine={deleteHorizontalLine(dailyDrawingKey)}
                      partialTimes={dailyMonthBoundaries}
                      visibleRange={resolvedDailyVisibleRange}
                      positionOverlay={{
                        dailyPositions,
                        tradeMarkers,
                        showOverlay: showTradesOverlay,
                        showMarkers: showTradeMarkers,
                        showPnL: showPnLPanel,
                        hoverTime: resolvedCursorAsOfTime ?? mainSync.hoverTime,
                        currentPositions,
                        latestTradeTime
                      }}
                        cursorTime={resolvedCursorAsOfTime}
                        selectedTime={selectedBarData?.time ?? null}
                        onCrosshairMove={handleDailyCrosshair}
                      onVisibleRangeChange={handleDailyVisibleRangeChange}
                      onChartClick={handleDailyChartClick}
                    />
                  )}
                  {holdDailyChartUntilDecisionReady && (
                    <ProductStateNotice kind="loading" className="detail-chart-empty">判定マークを読み込み中...</ProductStateNotice>
                  )}
                  {dailyEmptyMessage && (
                    <ProductStateNotice
                      kind={chartNoticeKind(chartLifecycle.daily.status)}
                      prefix="日足"
                      className="detail-chart-empty"
                    >
                      {dailyEmptyMessage}
                    </ProductStateNotice>
                  )}
                </div>
              </div>
              <div
                className="detail-row detail-row-bottom"
                style={{ flex: `${1 - DETAIL_DAILY_ROW_RATIO} 1 0%` }}
                ref={bottomRowRef}
              >
                <div className="detail-pane" style={{ flex: `${weeklyRatio} 1 0%` }}>
                  <div
                    className="detail-chart detail-chart-focusable"
                    onDoubleClick={() => toggleFocus("weekly")}
                  >
                    {secondaryChartsReady ? (
                      <DetailChart
                        ref={weeklyChartRef}
                        candles={weeklyCandles}
                        volume={weeklyVolume}
                        maLines={weeklyChartMaLines}
                        showVolume={false}
                        boxes={boxes}
                        showBoxes={showBoxes}
                        gapBands={gapBandsOverride}
                        drawingEnabled={activeDrawTool != null}
                        timeZones={weeklyDrawings.timeZones}
                        priceBands={weeklyDrawings.priceBands}
                        drawBoxes={weeklyDrawings.drawBoxes}
                        horizontalLines={weeklyDrawings.horizontalLines}
                        showPriceBands
                        meeMeeDetailChrome={DETAIL_CHROME_WEEKLY}
                        meeMeeDetailChromeTerminalDates={weeklyChromeTerminalDates}
                        selectionEnabled={!annotationMode && drawingSelectionMode}
                        activeTool={activeDrawTool}
                        onDrawCommit={handleDrawCommit}
                        activeDrawColor={activeDrawColor}
                        activeLineOpacity={activeLineOpacity}
                        activeLineWidth={activeLineWidth}
                        onSelectShape={handleSelectDrawing}
                        onAddTimeZone={addTimeZone(weeklyDrawingKey)}
                        onUpdateTimeZone={updateTimeZone(weeklyDrawingKey)}
                        onDeleteTimeZone={deleteTimeZone(weeklyDrawingKey)}
                        onAddPriceBand={addPriceBand(weeklyDrawingKey)}
                        onUpdatePriceBand={updatePriceBand(weeklyDrawingKey)}
                        onDeletePriceBand={deletePriceBand(weeklyDrawingKey)}
                        onAddDrawBox={addDrawBox(weeklyDrawingKey)}
                        onUpdateDrawBox={updateDrawBox(weeklyDrawingKey)}
                        onDeleteDrawBox={deleteDrawBox(weeklyDrawingKey)}
                        onAddHorizontalLine={addHorizontalLine(weeklyDrawingKey)}
                        onUpdateHorizontalLine={updateHorizontalLine(weeklyDrawingKey)}
                        onDeleteHorizontalLine={deleteHorizontalLine(weeklyDrawingKey)}
                        partialTimes={weeklyMonthBoundaries}
                        visibleRange={resolvedWeeklyVisibleRange}
                        cursorTime={resolvedCursorAsOfTime}
                        selectedTime={selectedBarData?.time ?? null}
                        onCrosshairMove={handleWeeklyCrosshair}
                        onVisibleRangeChange={handleWeeklyVisibleRangeChange}
                      />
                    ) : (
                      <ProductStateNotice kind="loading" prefix="週足" className="detail-chart-empty">
                        準備中...
                      </ProductStateNotice>
                    )}
                    {weeklyEmptyMessage && (
                      <ProductStateNotice
                        kind={chartNoticeKind(chartLifecycle.weekly.status)}
                        prefix="週足"
                        className="detail-chart-empty"
                      >
                        {weeklyEmptyMessage}
                      </ProductStateNotice>
                    )}
                  </div>
                </div>
                <div
                  className="detail-divider detail-divider-vertical"
                  onMouseDown={startDrag()}
                  onTouchStart={startDrag()}
                />
                <div className="detail-pane" style={{ flex: `${monthlyRatio} 1 0%` }}>
                  <div
                    className="detail-chart detail-chart-focusable"
                    onDoubleClick={() => toggleFocus("monthly")}
                  >
                    {secondaryChartsReady ? (
                      <DetailChart
                        ref={monthlyChartRef}
                        candles={monthlyCandles}
                        volume={monthlyVolume}
                        maLines={monthlyChartMaLines}
                        showVolume={false}
                        boxes={boxes}
                        showBoxes={showBoxes}
                        gapBands={gapBandsOverride}
                        drawingEnabled={activeDrawTool != null}
                        timeZones={monthlyDrawings.timeZones}
                        priceBands={monthlyDrawings.priceBands}
                        drawBoxes={monthlyDrawings.drawBoxes}
                        horizontalLines={monthlyDrawings.horizontalLines}
                        showPriceBands
                        meeMeeDetailChrome={DETAIL_CHROME_MONTHLY}
                        meeMeeDetailChromeTerminalDates={monthlyChromeTerminalDates}
                        selectionEnabled={!annotationMode && drawingSelectionMode}
                        activeTool={activeDrawTool}
                        onDrawCommit={handleDrawCommit}
                        activeDrawColor={activeDrawColor}
                        activeLineOpacity={activeLineOpacity}
                        activeLineWidth={activeLineWidth}
                        onSelectShape={handleSelectDrawing}
                        onAddTimeZone={addTimeZone(monthlyDrawingKey)}
                        onUpdateTimeZone={updateTimeZone(monthlyDrawingKey)}
                        onDeleteTimeZone={deleteTimeZone(monthlyDrawingKey)}
                        onAddPriceBand={addPriceBand(monthlyDrawingKey)}
                        onUpdatePriceBand={updatePriceBand(monthlyDrawingKey)}
                        onDeletePriceBand={deletePriceBand(monthlyDrawingKey)}
                        onAddDrawBox={addDrawBox(monthlyDrawingKey)}
                        onUpdateDrawBox={updateDrawBox(monthlyDrawingKey)}
                        onDeleteDrawBox={deleteDrawBox(monthlyDrawingKey)}
                        onAddHorizontalLine={addHorizontalLine(monthlyDrawingKey)}
                        onUpdateHorizontalLine={updateHorizontalLine(monthlyDrawingKey)}
                        onDeleteHorizontalLine={deleteHorizontalLine(monthlyDrawingKey)}
                        partialTimes={monthlyYearBoundaries}
                        visibleRange={resolvedMonthlyVisibleRange}
                        cursorTime={resolvedCursorAsOfTime}
                        selectedTime={selectedBarData?.time ?? null}
                        onCrosshairMove={handleMonthlyCrosshair}
                        onVisibleRangeChange={handleMonthlyVisibleRangeChange}
                      />
                    ) : (
                      <ProductStateNotice kind="loading" prefix="月足" className="detail-chart-empty">
                        準備中...
                      </ProductStateNotice>
                    )}
                    {monthlyEmptyMessage && (
                      <ProductStateNotice
                        kind={chartNoticeKind(chartLifecycle.monthly.status)}
                        prefix="月足"
                        className="detail-chart-empty"
                      >
                        {monthlyEmptyMessage}
                      </ProductStateNotice>
                    )}
                  </div>
                </div>
              </div>
            </>
          )}
        </div>
        {rightRailKind && (
          <aside className="detail-right-rail">
            {rightRailKind === "replay" && (
              <DetailReplayPanel
                runId={replayRunId}
                loading={replayRun.loading}
                error={replayRun.error}
                summary={replayRun.summary}
                selectedDate={selectedDate}
                selectedRow={replayRun.selectedTimelineRow}
                ledgerRows={replayRun.ledgerRows}
                formatNumber={formatNumber}
                formatSignedNumber={formatSignedNumber}
              />
            )}
            {rightRailKind === "analysis" && (
              <>
                <ScreenPanel title="ランキング指標" className="detail-analysis-panel">
                  <div className="detail-analysis-body">
                    <div className="detail-analysis-meta">
                      売買判定 {rankingJudgementSurfaceLabel}
                    </div>
                    <div className="detail-analysis-meta">
                      表示スコア {rankingDisplayScore != null ? `${formatNumber(rankingDisplayScore, 2)}点` : "--"}
                      {" / "}
                      根拠 {rankingDisplayScoreSourceLabel}
                    </div>
                    {activeTicker?.entryPriorityScore != null && (
                      <div className="detail-analysis-meta">
                        厳選優先度 {formatNumber(activeTicker.entryPriorityScore, 2)}
                      </div>
                    )}
                    {activeTicker?.hybridScore != null && (
                      <div className="detail-analysis-meta">
                        補完スコア {formatNumber(activeTicker.hybridScore, 2)}
                      </div>
                    )}
                  </div>
                </ScreenPanel>
                <Suspense fallback={null}>
                  <LazyDetailJudgementPanel
                    analysisDtLabel={selectedRankingJudgement.analysisDtLabel ?? selectedAnalysisDtLabel ?? analysisDtLabel}
                    analysisSummaryLoading={persistedMarkersLoading && selectedRankingAppearancesForSummary.length === 0}
                    analysisMissingDataVisible={selectedDate != null && !persistedMarkersLoading && selectedRankingAppearancesForSummary.length === 0}
                    dailyChartShape={dailyChartShape}
                    dailyChartShapeLoading={dailyChartShapeLoading}
                    maRoleReview={maRoleReview}
                    dailyCandles={dailyCandles}
                    analysisDecision={selectedRankingJudgement.analysisDecision}
                    analysisGuidance={selectedRankingJudgement.analysisGuidance}
                    analysisEntryPolicy={selectedRankingJudgement.analysisEntryPolicy}
                    patternSummary={selectedRankingJudgement.patternSummary}
                    analysisResearchPrior={selectedRankingJudgement.analysisResearchPrior}
                    formatPercentLabel={formatPercentLabel}
                    formatNumber={formatNumber}
                    formatSignedPercentLabel={formatSignedPercentLabel}
                  />
                </Suspense>
                <details
                  className="detail-judgement-details"
                  open={tradexAnalysisDetailsOpen}
                  onToggle={(event) => setTradexAnalysisDetailsOpen((event.currentTarget as HTMLDetailsElement).open)}
                >
                  <summary>TRADEX詳細を開く</summary>
                  {tradexAnalysisDetailsOpen && (
                    <Suspense fallback={null}>
                      <LazyTradexAnalysisMount
                        backendReady={backendReady}
                        readyToFetch={analysisNetworkReady}
                        analysisFetchEnabled={analysisFetchEnabled}
                        code={code ?? null}
                        asof={analysisAsOfTime}
                        formatPercentLabel={formatPercentLabel}
                        formatSignedPercentLabel={formatSignedPercentLabel}
                        formatNumber={formatNumber}
                      />
                    </Suspense>
                  )}
                </details>
              </>
            )}
            {rightRailKind === "annotation" && (
              <ScreenPanel title="注釈・日付情報" className="detail-annotation-panel">
                {selectedDate && selectedBarData && (
                  <DailyMemoPanel
                    code={code || ""}
                    selectedDate={selectedDate}
                    selectedBarData={selectedBarData}
                    {...(memoPanelData || {})}
                    title="選択日の情報"
                    cursorMode={cursorMode}
                    compact
                    onPrevDay={moveToPrevDay}
                    onNextDay={moveToNextDay}
                    onCopyForConsult={handleCopyForConsult}
                  />
                )}
                {annotationMode && (
                  <>
                    {maRoleReview && (
                      <section className="annotation-panel ma-role-review-panel" data-testid="ma-role-review-panel">
                        <div className="annotation-panel-header">
                          <div>
                            <div className="annotation-panel-title">MA/Candle Review</div>
                            <div className="annotation-panel-meta">
                              chart markers only / read-only / no ranking effect
                            </div>
                          </div>
                          <span className={`ma-role-review-status ${maRoleReview.available ? "is-available" : "is-missing"}`}>
                            {maRoleReview.available ? "available" : maRoleReview.reason || "unavailable"}
                          </span>
                        </div>
                        <div className="annotation-panel-meta" data-testid="ma-role-review-marker-summary">
                          {Array.isArray(maRoleReview.chart_markers) && maRoleReview.chart_markers.length > 0
                            ? `Showing ${maRoleReview.chart_markers.length} important historical MA/candle markers on the chart.`
                            : "No important historical MA/candle marker is visible for this chart window."}
                        </div>
                        <div className="annotation-panel-empty">
                          Only high-priority historical markers are shown on the chart. Research evidence stays out of the main UI.
                        </div>
                      </section>
                    )}
                    <ChartReadingPanel
                      loading={annotationsLoading}
                      timeframe={readingTimeframe}
                      targetType={readingTargetType}
                      commentType={readingCommentType}
                      noteText={readingNoteText}
                      tagsText={readingTagsText}
                      saving={readingSaving}
                      selectedDrawing={selectedDrawing}
                      selectedAnnotation={selectedAnnotation}
                      selectedBar={selectedBarForChartReading}
                      noteDate={chartReadingNoteDate}
                      onTimeframeChange={setReadingTimeframe}
                      onTargetTypeChange={setReadingTargetType}
                      onCommentTypeChange={setReadingCommentType}
                      onNoteTextChange={setReadingNoteText}
                      onTagsTextChange={setReadingTagsText}
                      onClearNote={handleClearChartReadingNote}
                      onAnnotateDrawing={handleAnnotateSelectedDrawing}
                      onAnnotationChange={handleAnnotationChange}
                      onAnnotationDelete={handleAnnotationDelete}
                    />
                    <section className="annotation-panel chart-note-disclosure">
                      <button
                        type="button"
                        className="annotation-save"
                        data-testid="chart-note-toggle"
                        aria-expanded={chartNoteExpanded}
                        onClick={() => setChartNoteExpanded((current) => !current)}
                      >
                        詳細ノート{chartNoteParagraphs.length ? ` (${chartNoteParagraphs.length})` : ""}
                      </button>
                    </section>
                    {chartNoteExpanded && (
                      <ChartNotePanel
                        title={chartNoteTitle}
                        timeframe={chartNoteTimeframe}
                        paragraphs={chartNoteParagraphs}
                        selectedAnnotation={selectedAnnotation}
                        saving={chartNoteSaving}
                        onTitleChange={setChartNoteTitle}
                        onTimeframeChange={setChartNoteTimeframe}
                        onAddParagraph={handleAddChartNoteParagraph}
                        onParagraphChange={handleChartNoteParagraphChange}
                        onLinkSelectedAnnotation={handleLinkSelectedAnnotationToParagraph}
                        onLinkMa20={handleLinkMa20ToParagraph}
                        onSave={handleSaveChartNote}
                      />
                    )}
                  </>
                )}
              </ScreenPanel>
            )}
            {rightRailKind === "financial" && (
              <Suspense fallback={null}>
                <LazyDetailFinancialPanel
                  financialPanelRef={financialPanelRef}
                  financialPanel={financialPanel}
                  financialFetchedLabel={financialFetchedLabel}
                  financialLoading={financialLoading}
                  financialSeries={financialSeries}
                  financialCards={financialDisplay.cards}
                  financialKeyStats={financialDisplay.stats}
                  tdnetHighlights={tdnetHighlights}
                  tdnetLoading={tdnetLoading}
                  tdnetStatusLabel={tdnetResolvedStatusLabel ?? tdnetStatusLabel}
                  taisyakuCards={taisyakuDisplay.cards}
                  taisyakuHistory={taisyakuDisplay.history}
                  taisyakuRestrictions={taisyakuSnapshot?.restrictions ?? []}
                  taisyakuLoading={taisyakuLoading}
                  taisyakuStatusLabel={taisyakuStatusLabel}
                  taisyakuWatchLabel={taisyakuDisplay.watchLabel}
                  formatNumber={formatNumber}
                  formatPercentLabel={formatPercentLabel}
                  formatFinancialAmountLabel={formatFinancialAmountLabel}
                />
              </Suspense>
            )}
          </aside>
        )}
      </div>
      {activeTdnetDisclosure && !compareCode && (
        <Suspense fallback={null}>
          <LazyDetailTdnetCard
            activeTdnetDisclosure={activeTdnetDisclosure}
            activeTdnetReaction={activeTdnetReaction}
            selectedTdnetDisclosures={selectedTdnetDisclosures}
            selectedTdnetDisclosureIndex={selectedTdnetDisclosureIndex}
            setSelectedTdnetDisclosures={setSelectedTdnetDisclosures}
            setSelectedTdnetDisclosureIndex={setSelectedTdnetDisclosureIndex}
            formatNumber={formatNumber}
            formatSignedPercentLabel={formatSignedPercentLabel}
          />
        </Suspense>
      )}
      {!focusPanel && (
        <div className="detail-footer" data-testid="detail-footer">
          <div className="detail-footer-left">
            <button className="load-more is-compact" onClick={loadMoreDailyAndMonthly} disabled={loadMoreDisabled}>
              追加読込
            </button>
          </div>
          <div className="detail-footer-right">
            <div className="detail-hint detail-hint-compact" title={loadMoreLabel}>
              日足 {dailyCandles.length}本 | 週足 {weeklyCandles.length}本 | 月足 {monthlyCandles.length}本
            </div>
            {aiExplainDockMounted && (
              <Suspense fallback={null}>
                <LazyAiExplainDock
                  screenType={compareCode ? "compare" : "detail"}
                  targetLabel={compareCode ? `${code ?? ""} vs ${compareCode}` : `${code ?? ""} ${tickerName}`.trim()}
                  compareLabel={compareCode ? `${compareCode}${compareTickerName ? ` ${compareTickerName}` : ""}` : null}
                  snapshot={aiExplainSnapshot}
                  images={aiExplainImages}
                  inline
                />
              </Suspense>
            )}
          </div>
        </div>
      )}
      <div className="detail-bottom-tools">
        <DetailDebugBanner
          hasIssues={hasIssues}
          bannerTone={bannerTone}
          bannerTitle={bannerTitle}
          debugSummary={debugSummary}
          debugOpen={debugOpen}
          showInfoDetails={showInfoDetails}
          debugLines={debugLines}
          copyFallbackText={copyFallbackText}
          inline
          onToggleOpen={() => setDebugOpen((prev) => !prev)}
          onCopy={handleCopyDebug}
          onToggleInfoDetails={() => setShowInfoDetails((prev) => !prev)}
          onClose={() => setDebugOpen(false)}
        />
        {overwriteLiveValidationMode && mainChartOverwriteObservability != null && (
          <div className="detail-debug-banner info is-inline" data-testid="detail-overwrite-observability">
            <div className="detail-debug-panel">
              <div className="detail-debug-header">
                <div className="detail-debug-title">Overwrite Observability</div>
              </div>
              <div className="detail-debug-lines">
                <pre style={{ margin: 0, whiteSpace: "pre-wrap", wordBreak: "break-word" }}>
{JSON.stringify(mainChartOverwriteObservability, null, 2)}
                </pre>
              </div>
            </div>
          </div>
        )}
      </div>
      <DetailPositionLedgerSheet
        isOpen={showPositionLedger}
        expanded={positionLedgerExpanded}
        ledgerViewMode={ledgerViewMode}
        ledgerEligible={ledgerEligible}
        ledgerIizukaGroups={ledgerIizukaGroups}
        ledgerStockGroups={ledgerStockGroups}
        onToggleExpanded={() => setPositionLedgerExpanded((prev) => !prev)}
        onClose={handleClosePositionLedger}
        onChangeLedgerViewMode={handleLedgerViewModeChange}
        formatLedgerDate={formatLedgerDate}
        formatNumber={formatNumber}
        formatSignedNumber={formatSignedNumber}
        exitLinePrice={dailyPositionRiskOverlayDrawings.exitLinePrice}
        exitLineSide={dailyPositionRiskOverlayDrawings.exitLineSide}
      />
      {showIndicators && (
        <Suspense fallback={null}>
          <LazyDetailIndicatorOverlay
            isOpen={showIndicators}
            compareCode={compareCode}
            maEditMode={maEditMode}
            activeMaSettings={activeMaSettings}
            onSetMaEditMode={setMaEditMode}
            onUpdateSetting={updateSetting}
            onResetSettings={resetSettings}
            onClose={() => setShowIndicators(false)}
          />
        </Suspense>
      )}
      <Toast
        message={toastMessage}
        onClose={() => { setToastMessage(null); setToastAction(null); }}
        action={toastAction}
        duration={toastAction ? 8000 : 4000}
      />
      {similarSearchMounted && (
        <Suspense fallback={null}>
          <LazySimilarSearchPanel
            isOpen={showSimilar}
            onClose={() => setShowSimilar(false)}
            queryTicker={code ?? null}
            queryAsOf={mainAsOf}
          />
        </Suspense>
      )}
    </div>
  );
}
