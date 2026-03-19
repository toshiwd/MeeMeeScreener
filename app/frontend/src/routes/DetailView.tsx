// @ts-nocheck
import { useEffect, useMemo, useRef, useState } from "react";
import type { MouseEvent as ReactMouseEvent, TouchEvent as ReactTouchEvent } from "react";
import { useCallback } from "react";
import { startTransition } from "react";
import { useLocation, useNavigate, useParams } from "react-router-dom";
import {
  IconAdjustments,
  IconArrowLeft,
  IconArrowRight,
  IconBox,
  IconCamera,
  IconHeart,
  IconHeartFilled,
  IconMinus,
  IconTrash,
  IconSparkles,
  IconChartArrows,
  IconRefresh,
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
import SimilarSearchPanel from "../components/SimilarSearchPanel";
import { Box, MaSetting, useStore } from "../store";
import { computeSignalMetrics } from "../utils/signals";
import type { TradeEvent, CurrentPosition, DailyPosition } from "../utils/positions";
import { buildCurrentPositions, buildDailyPositions, buildPositionLedger } from "../utils/positions";
import { captureAndCopyScreenshot, saveBlobToFile, getScreenType } from "../utils/windowScreenshot";
import { buildAIExport, copyToClipboard } from "../utils/aiExport";
import { formatEventBadgeDate, parseEventDateMs } from "../utils/events";
import DailyMemoPanel from "../components/DailyMemoPanel";
import { buildConsultCopyText, copyToClipboard as copyConsultToClipboard } from "../utils/consultCopy";
import { useChartSync } from "../hooks/useChartSync";
import { useDetailInfo } from "../hooks/useDetailInfo";
import { useExactDecisionRange, type ExactDecisionTone } from "./detail/hooks/useExactDecisionRange";
import { useAsOfItemFetch } from "./detail/hooks/useAsOfItemFetch";
import { DetailAnalysisPanel } from "./detail/DetailAnalysisPanel";
import { DetailFinancialPanel } from "./detail/DetailFinancialPanel";
import { TradexAnalysisMount } from "./detail/TradexAnalysisMount";
import { DetailTdnetCard } from "./detail/DetailTdnetCard";
import DetailDebugBanner from "./detail/components/DetailDebugBanner";
import DetailIndicatorOverlay from "./detail/components/DetailIndicatorOverlay";
import DetailPositionLedgerSheet from "./detail/components/DetailPositionLedgerSheet";
import { useDetailDrawings } from "./detail/hooks/useDetailDrawings";

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
  DAILY_ROW_RATIO,
  DEFAULT_WEEKLY_RATIO,
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
  computeMA,
  buildCandlesWithStats,
  buildVolume,
  buildWeekly,
  clamp,
  incrementBarLimit,
  computeEnvironmentTone,
  normalizeEdinetFinancialPanel,
  normalizeTaisyakuSnapshot,
  normalizeTdnetDisclosureItem,
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

const buildDetailMaLines = (candles: Candle[], settings: MaSetting[]) =>
  settings.map((setting) => {
    const data = computeMA(candles, setting.period);
    return {
      key: setting.key,
      label: setting.label,
      period: setting.period,
      color: setting.color,
      visible: setting.visible,
      lineWidth: setting.lineWidth,
      data,
      chartData: setting.visible ? data : []
    };
  });

const toDetailChartMaLines = (lines: ReturnType<typeof buildDetailMaLines>) =>
  lines.map(({ chartData, data, ...line }) => ({
    ...line,
    data: chartData ?? data
  }));

type BatchBarsFramePayload = {
  bars?: number[][];
  boxes?: Box[];
};

type BatchBarsV3Response = {
  items?: Record<string, Partial<Record<"daily" | "weekly" | "monthly", BatchBarsFramePayload>>>;
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

type ChartPrefetchEntry = {
  rows: number[][];
  boxes: Box[];
  fetchedAt: number;
};

const CHART_PREFETCH_TTL_MS = 60_000;
const chartPrefetchCache = new Map<string, ChartPrefetchEntry>();
const chartPrefetchInFlight = new Map<string, Promise<void>>();
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
const COMPARE_FOCUS_MONTHS = 12;
const RANGE_SETTLE_MS = 2_000;

const buildChartPrefetchKey = (
  symbol: string,
  timeframe: "daily" | "monthly",
  limit: number,
  asof?: string | null
) => `${symbol}|${timeframe}|${limit}|${asof ?? ""}`;

const readChartPrefetch = (
  symbol: string,
  timeframe: "daily" | "monthly",
  limit: number,
  asof?: string | null
) => {
  const key = buildChartPrefetchKey(symbol, timeframe, limit, asof);
  const cached = chartPrefetchCache.get(key);
  if (!cached) return null;
  if (Date.now() - cached.fetchedAt > CHART_PREFETCH_TTL_MS) {
    chartPrefetchCache.delete(key);
    return null;
  }
  return cached;
};

const prefetchChartFrame = async ({
  code,
  timeframe,
  limit,
  asof,
}: {
  code: string;
  timeframe: "daily" | "monthly";
  limit: number;
  asof?: string | null;
}) => {
  const key = buildChartPrefetchKey(code, timeframe, limit, asof);
  if (readChartPrefetch(code, timeframe, limit, asof) != null) return;
  if (chartPrefetchInFlight.has(key)) return;
  const payload: {
    codes: string[];
    timeframes: string[];
    limit: number;
    includeProvisional: boolean;
    includeBoxes?: boolean;
    asof?: string;
  } = {
    codes: [code],
    timeframes: [timeframe],
    limit,
    includeProvisional: true,
  };
  if (timeframe === "monthly") {
    payload.includeBoxes = false;
  }
  if (asof) {
    payload.asof = asof;
  }
  const request = api
    .post("/batch_bars_v3", payload)
    .then((res) => {
      const items = (res.data as BatchBarsV3Response | null)?.items ?? {};
      const item = items[code] ?? {};
      const frame = item[timeframe];
      const rows = Array.isArray(frame?.bars) ? frame.bars : [];
      const boxes = timeframe === "monthly" && Array.isArray(frame?.boxes) ? frame.boxes : [];
      chartPrefetchCache.set(key, {
        rows,
        boxes,
        fetchedAt: Date.now(),
      });
    })
    .catch(() => {
      // ignore prefetch failures
    })
    .finally(() => {
      chartPrefetchInFlight.delete(key);
    });
  chartPrefetchInFlight.set(key, request);
  await request;
};

const fetchMonthlyBoxesFrame = async ({
  code,
  limit,
  asof,
}: {
  code: string;
  limit: number;
  asof?: string | null;
}) => {
  const payload: {
    codes: string[];
    timeframes: string[];
    limit: number;
    includeProvisional: boolean;
    includeBoxes: boolean;
    asof?: string;
  } = {
    codes: [code],
    timeframes: ["monthly"],
    limit,
    includeProvisional: true,
    includeBoxes: true,
  };
  if (asof) {
    payload.asof = asof;
  }
  const res = await api.post("/batch_bars_v3", payload);
  const items = (res.data as BatchBarsV3Response | null)?.items ?? {};
  const item = items[code] ?? {};
  const frame = item.monthly;
  const rows = Array.isArray(frame?.bars) ? frame.bars : [];
  const boxes = Array.isArray(frame?.boxes) ? frame.boxes : [];
  chartPrefetchCache.set(buildChartPrefetchKey(code, "monthly", limit, asof), {
    rows,
    boxes,
    fetchedAt: Date.now(),
  });
  return { rows, boxes };
};

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

const isRetryableTradesError = (error: unknown) => {
  const status = (error as { response?: { status?: number } })?.response?.status;
  const payload = (error as { response?: { data?: { retryable?: boolean } } })?.response?.data;
  return status === 503 && payload?.retryable === true;
};

const parseStateEvalReasonTexts = (value: string | null | undefined) => {
  if (!value) return [];
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed.filter((item) => typeof item === "string") : [];
  } catch {
    return [];
  }
};

const classifyStateEvalPriorReason = (reason: string) => {
  if (/^Combo strength:/i.test(reason)) {
    return { label: reason.replace(/^Combo strength:\s*/i, ""), tone: "combo" as const };
  }
  if (/^Historically strong:/i.test(reason)) {
    return { label: reason.replace(/^Historically strong:\s*/i, ""), tone: "prior-strong" as const };
  }
  if (/^Historical caution:/i.test(reason)) {
    return { label: reason.replace(/^Historical caution:\s*/i, ""), tone: "prior-caution" as const };
  }
  return null;
};

const parseStateEvalStrategyTags = (value: string | null | undefined) => {
  if (!value) return [];
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed.filter((item) => typeof item === "string") : [];
  } catch {
    return [];
  }
};

const buildStateEvalTrendReason = (trend: { label: string } | null | undefined) => {
  if (!trend) return null;
  if (trend.label === "Improving") return "Trend improving";
  if (trend.label === "Weakening") return "Trend weakening";
  if (trend.label === "Persistent Risk") return "Persistent risk";
  return trend.label;
};

export default function DetailView() {
  const { code } = useParams();
  const location = useLocation();
  const navigate = useNavigate();
  const { ready: backendReady } = useBackendReadyState();
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
  const [monthlyData, setMonthlyData] = useState<number[][]>([]);
  const [boxes, setBoxes] = useState<Box[]>([]);
  const [compareBoxes, setCompareBoxes] = useState<Box[]>([]);
  const [headerMode, setHeaderMode] = useState<"chart" | "draw" | "positions" | "analysis" | "financial">("chart");
  const [displayOpen, setDisplayOpen] = useState(false);
  const [signalsOpen, setSignalsOpen] = useState(false);
  const [showGapBands, setShowGapBands] = useState(true);
  const [showVolumeEnabled, setShowVolumeEnabled] = useState(true);
  const [showDecisionMarkers, setShowDecisionMarkers] = useState(true);
  const [showTdnetMarkers, setShowTdnetMarkers] = useState(true);
  const [routeReadyPhase, setRouteReadyPhase] = useState<RouteReadyPhase>("chart");
  const [showTradeMarkers, setShowTradeMarkers] = useState(true);
  const [activeDrawTool, setActiveDrawTool] = useState<DrawTool | null>(null);
  const [, setSelectedDrawing] = useState<SelectedDrawingInfo | null>(null);
  const COLOR_PALETTE = ["#ef4444", "#22c55e", "#0ea5e9", "#f59e0b", "#64748b"];
  const [activeDrawColorIndex, setActiveDrawColorIndex] = useState(4);
  const activeDrawColor = COLOR_PALETTE[activeDrawColorIndex] ?? "#64748b";
  const [activeLineOpacity, setActiveLineOpacity] = useState(0.8);
  const [activeLineWidth, setActiveLineWidth] = useState(2);
  const selectDrawTool = (tool: DrawTool) => {
    setActiveDrawTool(tool);
  };
  const [trades, setTrades] = useState<TradeEvent[]>([]);
  const [compareTrades, setCompareTrades] = useState<TradeEvent[]>([]);
  const [tradeWarnings, setTradeWarnings] = useState<ApiWarnings>({ items: [] });
  const [tradeErrors, setTradeErrors] = useState<string[]>([]);
  const [currentPositionsFromApi, setCurrentPositionsFromApi] = useState<CurrentPosition[] | null>(null);
  const [dailyErrors, setDailyErrors] = useState<string[]>([]);
  const [monthlyErrors, setMonthlyErrors] = useState<string[]>([]);
  const [dailyBarsMeta, setDailyBarsMeta] = useState<BarsMeta | null>(null);
  const [monthlyBarsMeta, setMonthlyBarsMeta] = useState<BarsMeta | null>(null);
  const [dailyFetch, setDailyFetch] = useState<FetchState>({
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
  const [hasMoreDaily, setHasMoreDaily] = useState(true);
  const [hasMoreMonthly, setHasMoreMonthly] = useState(true);
  const [showIndicators, setShowIndicators] = useState(false);
  const [maEditMode, setMaEditMode] = useState<"main" | "compare">("main");
  const [weeklyRatio, setWeeklyRatio] = useState(DEFAULT_WEEKLY_RATIO);
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
  const [taisyakuSnapshot, setTaisyakuSnapshot] = useState<TaisyakuSnapshot | null>(null);
  const [taisyakuLoading, setTaisyakuLoading] = useState(false);
  const [taisyakuFetchedOnce, setTaisyakuFetchedOnce] = useState(false);
  const [tdnetDisclosures, setTdnetDisclosures] = useState<TdnetDisclosureItem[]>([]);
  const [tdnetLoading, setTdnetLoading] = useState(false);
  const [tdnetFetchedOnce, setTdnetFetchedOnce] = useState(false);
  const [selectedTdnetDisclosures, setSelectedTdnetDisclosures] = useState<TdnetDisclosureItem[]>([]);
  const [selectedTdnetDisclosureIndex, setSelectedTdnetDisclosureIndex] = useState(0);
  const [taisyakuRefreshToken, setTaisyakuRefreshToken] = useState(0);
  const [tdnetRefreshToken, setTdnetRefreshToken] = useState(0);
  const [positionLedgerExpanded, setPositionLedgerExpanded] = useState(false);
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
  const prevShowAnalysisPanelRef = useRef(false);

  const syncRangesRef = useRef(syncRanges);
  const [showSimilar, setShowSimilar] = useState(false);
  const [stateEvalRow, setStateEvalRow] = useState<any | null>(null);
  const [stateEvalTrend, setStateEvalTrend] = useState<{ label: string; tone: "improving" | "weakening" | "risk" } | null>(null);
  const resetMainChartState = useCallback(() => {
    setDailyLimit(DEFAULT_LIMITS.daily);
    setMonthlyLimit(DEFAULT_LIMITS.monthly);
    setDailyData([]);
    setMonthlyData([]);
    setBoxes([]);
    setDailyErrors([]);
    setMonthlyErrors([]);
    setDailyBarsMeta(null);
    setMonthlyBarsMeta(null);
    setDailyFetch({ status: "idle", responseCount: 0, errorMessage: null });
    setMonthlyFetch({ status: "idle", responseCount: 0, errorMessage: null });
    setLoadingDaily(false);
    setLoadingMonthly(false);
    setHasMoreDaily(true);
    setHasMoreMonthly(true);
  }, []);
  const resetCompareChartState = useCallback(() => {
    setCompareDailyLimit(DEFAULT_LIMITS.daily);
    setCompareMonthlyData([]);
    setCompareMonthlyErrors([]);
    setCompareLoading(false);
    setCompareDailyData([]);
    setCompareDailyErrors([]);
    setCompareDailyLoading(false);
    setCompareBoxes([]);
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
  const analysisFetchEnabled = headerMode === "analysis" && !compareCode;
  const analysisNetworkReady = analysisFetchEnabled && routeReadyPhase === "analysis";
  const compareAsOf = useMemo(() => {
    const params = new URLSearchParams(location.search);
    const raw = params.get("compareAsOf");
    return raw ? raw.trim() : null;
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
  const [compareDailyLimit, setCompareDailyLimit] = useState(DEFAULT_LIMITS.daily);
  const [analysisHorizon] = useState<AnalysisHorizonKey>(20);
  const [analysisRiskMode, setAnalysisRiskMode] = useState<RankRiskMode>(() => resolveRiskModeFromSession());
  const [analysisAsOfTime, setAnalysisAsOfTime] = useState<number | null>(null);
  const displayRef = useRef<HTMLDivElement | null>(null);
  const signalsRef = useRef<HTMLDivElement | null>(null);
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
    if (!displayOpen && !signalsOpen) return;
    const handleClick = (event: MouseEvent) => {
      const target = event.target as HTMLElement;
      if (displayRef.current && displayRef.current.contains(target)) return;
      if (signalsRef.current && signalsRef.current.contains(target)) return;
      setDisplayOpen(false);
      setSignalsOpen(false);
    };
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [displayOpen, signalsOpen]);

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

  useEffect(() => {
    if (headerMode !== "draw") {
      setActiveDrawTool(null);
    } else {
      // entering draw mode: default to timeZone if nothing selected
      setActiveDrawTool((prev) => prev ?? "timeZone");
    }
  }, [headerMode]);

  useEffect(() => {
    if (headerMode !== "draw") {
      setSelectedDrawing(null);
    }
  }, [headerMode]);

  useEffect(() => {
    resetCompareChartState();
  }, [compareCode, resetCompareChartState]);

  useEffect(() => {
    if (!compareCode) return;
    manualCompareDailyRangeRef.current = null;
    manualCompareMonthlyRangeRef.current = null;
  }, [compareCode, compareAsOf]);

  useEffect(() => {
    setAnalysisAsOfTime(null);
    analysisBaseAsOfRef.current = null;
    setRouteReadyPhase("chart");
    resetMainChartState();
    // Reset cursor selection – will be re-initialized once dailyCandles load
    setSelectedBarIndex(null);
    setSelectedBarData(null);
    setAnalysisCursorTime(null);
    // Keep selectedDate so we can restore cursor position in new candle data
  }, [code, resetMainChartState]);

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
  const analysisPrefetchCandles = useMemo(
    () => filterCandlesByAsOf(sharedDailyParse.candles, mainAsOfTime),
    [sharedDailyParse.candles, mainAsOfTime]
  );
  const analysisPrefetchAsofs = useMemo(() => {
    if (!cursorMode || selectedBarIndex == null) return [];
    if (!analysisPrefetchCandles.length) return [];
    const offsets = [-2, -1, 1, 2];
    return offsets
      .map((offset) => analysisPrefetchCandles[selectedBarIndex + offset]?.time ?? null)
      .filter((value): value is number => value != null);
  }, [cursorMode, selectedBarIndex, analysisPrefetchCandles]);

  useEffect(() => {
    setAnalysisBackfillJob(null);
    setAnalysisFetchRefreshToken(0);
    analysisBackfillActiveRef.current = false;
    analysisAutoBackfillRequestKeyRef.current = null;
  }, [code]);

  const {
    item: phaseFallback,
    loading: phaseFallbackLoading,
  } = useAsOfItemFetch<PhaseFallback>({
    backendReady,
    code,
    asof: analysisAsOfTime,
    prefetchAsofs: analysisPrefetchAsofs,
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
    prefetchAsofs: analysisPrefetchAsofs,
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
    item: sellAnalysisFallback,
    loading: sellAnalysisLoading,
  } = useAsOfItemFetch<SellAnalysisFallback>({
    backendReady,
    code,
    asof: analysisAsOfTime,
    prefetchAsofs: analysisPrefetchAsofs,
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
      });
    return () => {
      cancelled = true;
    };
  }, [backendReady, code, headerMode]);

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
    if (!backendReady || !code || headerMode !== "financial") return;
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
  }, [backendReady, code, headerMode, taisyakuFetchedOnce, taisyakuLoading, taisyakuSnapshot]);

  useEffect(() => {
    if (!backendReady || !code) return;
    let cancelled = false;
    setTdnetLoading(true);
    setTdnetFetchedOnce(false);
    void api
      .get("/ticker/tdnet/disclosures", { params: { code, limit: 30 } })
      .then((response) => {
        if (cancelled) return;
        const items = response.data && typeof response.data === "object"
          ? (response.data as { items?: unknown }).items
          : [];
        const normalized = Array.isArray(items)
          ? items.map(normalizeTdnetDisclosureItem).filter((item): item is TdnetDisclosureItem => item !== null)
          : [];
        setTdnetDisclosures(normalized);
      })
      .catch(() => {
        if (!cancelled) setTdnetDisclosures([]);
      })
      .finally(() => {
        if (!cancelled) {
          setTdnetLoading(false);
          setTdnetFetchedOnce(true);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [backendReady, code, tdnetRefreshToken]);

  useEffect(() => {
    if (!backendReady || !code) return;
    if (!tdnetFetchedOnce || tdnetLoading) return;
    if (!shouldAutoRefreshTdnet(tdnetDisclosures)) return;
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
  }, [backendReady, code, tdnetDisclosures, tdnetFetchedOnce, tdnetLoading]);

  useEffect(() => {
    setSelectedTdnetDisclosures([]);
    setSelectedTdnetDisclosureIndex(0);
  }, [code]);

  const formatPhaseScore = (value: number | null | undefined) => {
    if (phaseFallbackLoading) return "読込中...";
    return Number.isFinite(value)
      ? String(Math.min(10, Math.max(0, Math.round(value! * 10))))
      : "--";
  };
  const getPhaseTone = (value: number | null | undefined) => {
    if (!Number.isFinite(value)) return "neutral";
    if (value! > 0) return "up";
    if (value! < 0) return "down";
    return "neutral";
  };
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
    ? new Date(financialPanel.fetchedAt).toLocaleDateString("ja-JP")
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
    return `貸借最終取得 ${new Date(taisyakuSnapshot.fetchedAt).toLocaleString("ja-JP")}`;
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
    return `TDNET最終取得 ${new Date(latestFetched).toLocaleString("ja-JP")}`;
  }, [tdnetDisclosures, tdnetLoading]);
  const showFinancialPanel = headerMode === "financial" && !compareCode;
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
  const showMemoPanel = cursorMode && !compareCode && headerMode !== "analysis" && headerMode !== "financial";
  const showRightPanel = showAnalysisPanel || showMemoPanel || showFinancialPanel;

  useEffect(() => {
    if (!backendReady || !showAnalysisPanel) {
      return;
    }

    let disposed = false;
    let timerId: number | null = null;
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

    void pollCurrentJob();
    return () => {
      disposed = true;
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
    if (!backendReady) return;
    if (!code) return;
    const prefetched = readChartPrefetch(code, "daily", dailyLimit, mainAsOf);
    if (prefetched) {
      setLoadingDaily(false);
      setDailyErrors([]);
      setDailyBarsMeta(null);
      setDailyData(prefetched.rows);
      setHasMoreDaily(prefetched.rows.length >= dailyLimit);
      setDailyFetch({ status: "success", responseCount: prefetched.rows.length, errorMessage: null });
      rangeSettleRef.current = Date.now() + RANGE_SETTLE_MS;
      return;
    }
    const controller = new AbortController();
    let active = true;
    setLoadingDaily(true);
    setDailyErrors([]);
    setDailyBarsMeta(null);
    setDailyFetch((prev) => ({ ...prev, status: "loading", errorMessage: null }));
    const payload: {
      codes: string[];
      timeframes: string[];
      limit: number;
      includeProvisional: boolean;
      asof?: string;
    } = {
      codes: [code],
      timeframes: ["daily"],
      limit: dailyLimit,
      includeProvisional: true
    };
    if (mainAsOf) {
      payload.asof = mainAsOf;
    }
    api
      .post("/batch_bars_v3", payload, { signal: controller.signal })
      .then((res) => {
        if (!active) return;
        const items = (res.data as BatchBarsV3Response | null)?.items ?? {};
        const item = items[code] ?? {};
        const dailyPayload = item.daily;
        const dailyRows = Array.isArray(dailyPayload?.bars) ? dailyPayload.bars : [];
        chartPrefetchCache.set(buildChartPrefetchKey(code, "daily", dailyLimit, mainAsOf), {
          rows: dailyRows,
          boxes: [],
          fetchedAt: Date.now(),
        });
        setDailyData(dailyRows);
        setDailyErrors(Array.isArray(dailyPayload?.bars) ? [] : ["daily_response_invalid"]);
        setHasMoreDaily(dailyRows.length >= dailyLimit);
        setDailyFetch({ status: "success", responseCount: dailyRows.length, errorMessage: null });
      })
      .catch((error) => {
        if (!active || isCanceledRequestError(error)) return;
        const message = error?.message || "Bars fetch failed";
        setDailyErrors([message]);
        setDailyFetch((prev) => ({
          status: "error",
          responseCount: prev.responseCount,
          errorMessage: message
        }));
      })
      .finally(() => {
        if (!active) return;
        setLoadingDaily(false);
        // Suppress programmatic range events after new data arrives
        rangeSettleRef.current = Date.now() + RANGE_SETTLE_MS;
      });
    return () => {
      active = false;
      controller.abort();
    };
  }, [backendReady, code, dailyLimit, mainAsOf]);

  useEffect(() => {
    if (!backendReady) return;
    if (!code) return;
    const prefetched = readChartPrefetch(code, "monthly", monthlyLimit, mainAsOf);
    if (prefetched) {
      setLoadingMonthly(false);
      setMonthlyErrors([]);
      setMonthlyBarsMeta(null);
      setMonthlyData(prefetched.rows);
      setBoxes(prefetched.boxes);
      setHasMoreMonthly(prefetched.rows.length >= monthlyLimit);
      setMonthlyFetch({ status: "success", responseCount: prefetched.rows.length, errorMessage: null });
      rangeSettleRef.current = Date.now() + RANGE_SETTLE_MS;
      return;
    }
    const controller = new AbortController();
    let active = true;
    setLoadingMonthly(true);
    setMonthlyErrors([]);
    setMonthlyBarsMeta(null);
    setMonthlyFetch((prev) => ({ ...prev, status: "loading", errorMessage: null }));
    const payload: {
      codes: string[];
      timeframes: string[];
      limit: number;
      includeProvisional: boolean;
      includeBoxes?: boolean;
      asof?: string;
    } = {
      codes: [code],
      timeframes: ["monthly"],
      limit: monthlyLimit,
      includeProvisional: true
    };
    payload.includeBoxes = false;
    if (mainAsOf) {
      payload.asof = mainAsOf;
    }
    api
      .post("/batch_bars_v3", payload, { signal: controller.signal })
      .then((res) => {
        if (!active) return;
        const items = (res.data as BatchBarsV3Response | null)?.items ?? {};
        const item = items[code] ?? {};
        const monthlyPayload = item.monthly;
        const monthlyRows = Array.isArray(monthlyPayload?.bars) ? monthlyPayload.bars : [];
        const nextBoxes = Array.isArray(monthlyPayload?.boxes) ? monthlyPayload.boxes : [];
        chartPrefetchCache.set(buildChartPrefetchKey(code, "monthly", monthlyLimit, mainAsOf), {
          rows: monthlyRows,
          boxes: nextBoxes,
          fetchedAt: Date.now(),
        });
        setMonthlyData(monthlyRows);
        setBoxes(nextBoxes);
        setMonthlyErrors(Array.isArray(monthlyPayload?.bars) ? [] : ["monthly_response_invalid"]);
        setHasMoreMonthly(monthlyRows.length >= monthlyLimit);
        setMonthlyFetch({ status: "success", responseCount: monthlyRows.length, errorMessage: null });
      })
      .catch((error) => {
        if (!active || isCanceledRequestError(error)) return;
        const message = error?.message || "Bars fetch failed";
        setMonthlyErrors([message]);
        setBoxes([]);
        setMonthlyFetch((prev) => ({
          status: "error",
          responseCount: prev.responseCount,
          errorMessage: message
        }));
      })
      .finally(() => {
        if (!active) return;
        setLoadingMonthly(false);
        rangeSettleRef.current = Date.now() + RANGE_SETTLE_MS;
      });
    return () => {
      active = false;
      controller.abort();
    };
  }, [backendReady, code, mainAsOf, monthlyLimit]);

  useEffect(() => {
    if (!backendReady) return;
    if (!code) return;
    if (!monthlyData.length) return;
    if (boxes.length > 0) return;
    let active = true;
    const timerId = window.setTimeout(() => {
      void fetchMonthlyBoxesFrame({ code, limit: monthlyLimit, asof: mainAsOf })
        .then((result) => {
          if (!active) return;
          setBoxes(result.boxes);
        })
        .catch(() => {
          // keep lightweight monthly result when box refresh fails
        });
    }, 120);
    return () => {
      active = false;
      window.clearTimeout(timerId);
    };
  }, [backendReady, boxes.length, code, mainAsOf, monthlyData.length, monthlyLimit]);

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
    const controller = new AbortController();
    let active = true;
    setCompareDailyLoading(true);
    setCompareDailyErrors([]);
    const payload: {
      codes: string[];
      timeframes: string[];
      limit: number;
      includeProvisional: boolean;
    } = {
      codes: [compareCode],
      timeframes: ["daily"],
      limit: compareDailyLimit,
      includeProvisional: true
    };
    api
      .post("/batch_bars_v3", payload, { signal: controller.signal })
      .then((res) => {
        if (!active) return;
        const items = (res.data as BatchBarsV3Response | null)?.items ?? {};
        const item = items[compareCode] ?? {};
        const dailyPayload = item.daily;
        const dailyRows = Array.isArray(dailyPayload?.bars) ? dailyPayload.bars : [];
        setCompareDailyData(dailyRows);
        setCompareDailyErrors(Array.isArray(dailyPayload?.bars) ? [] : ["daily_response_invalid"]);
      })
      .catch((error) => {
        if (!active || isCanceledRequestError(error)) return;
        const message = error?.message || "Bars fetch failed";
        setCompareDailyErrors([message]);
        setCompareDailyData([]);
      })
      .finally(() => {
        if (!active) return;
        setCompareDailyLoading(false);
        rangeSettleRef.current = Date.now() + RANGE_SETTLE_MS;
      });
    return () => {
      active = false;
      controller.abort();
    };
  }, [backendReady, compareCode, compareDailyLimit]);

  useEffect(() => {
    if (!backendReady) return;
    if (!compareCode) return;
    const controller = new AbortController();
    let active = true;
    setCompareLoading(true);
    setCompareMonthlyErrors([]);
    const payload: {
      codes: string[];
      timeframes: string[];
      limit: number;
      includeProvisional: boolean;
      includeBoxes?: boolean;
    } = {
      codes: [compareCode],
      timeframes: ["monthly"],
      limit: monthlyLimit,
      includeProvisional: true
    };
    payload.includeBoxes = false;
    api
      .post("/batch_bars_v3", payload, { signal: controller.signal })
      .then((res) => {
        if (!active) return;
        const items = (res.data as BatchBarsV3Response | null)?.items ?? {};
        const item = items[compareCode] ?? {};
        const monthlyPayload = item.monthly;
        const monthlyRows = Array.isArray(monthlyPayload?.bars) ? monthlyPayload.bars : [];
        const nextBoxes = Array.isArray(monthlyPayload?.boxes) ? monthlyPayload.boxes : [];
        setCompareMonthlyData(monthlyRows);
        setCompareBoxes(nextBoxes);
        setCompareMonthlyErrors(Array.isArray(monthlyPayload?.bars) ? [] : ["monthly_response_invalid"]);
      })
      .catch((error) => {
        if (!active || isCanceledRequestError(error)) return;
        const message = error?.message || "Bars fetch failed";
        setCompareMonthlyErrors([message]);
        setCompareMonthlyData([]);
        setCompareBoxes([]);
      })
      .finally(() => {
        if (!active) return;
        setCompareLoading(false);
        rangeSettleRef.current = Date.now() + RANGE_SETTLE_MS;
      });
    return () => {
      active = false;
      controller.abort();
    };
  }, [backendReady, compareCode, monthlyLimit]);

  useEffect(() => {
    if (!backendReady) return;
    if (!compareCode) return;
    if (!compareMonthlyData.length) return;
    if (compareBoxes.length > 0) return;
    let active = true;
    const timerId = window.setTimeout(() => {
      void fetchMonthlyBoxesFrame({ code: compareCode, limit: monthlyLimit })
        .then((result) => {
          if (!active) return;
          setCompareBoxes(result.boxes);
        })
        .catch(() => {
          // keep lightweight monthly result when box refresh fails
        });
    }, 120);
    return () => {
      active = false;
      window.clearTimeout(timerId);
    };
  }, [backendReady, compareBoxes.length, compareCode, compareMonthlyData.length, monthlyLimit]);

  useEffect(() => {
    if (!compareCode) return;
    setFocusPanel(null);
  }, [compareCode]);
  useEffect(() => {
    if (compareCode) return;
    setMaEditMode("main");
  }, [compareCode]);

  useEffect(() => {
    if (!backendReady) return;
    if (!code) return;
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
    let retryTimerId: number | null = null;
    const fetchTrades = (attempt: number) => {
      void api
        .get(`/trades/${code}`)
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
          if (isRetryableTradesError(error)) {
            if (attempt < 2) {
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
    fetchTrades(0);
    return () => {
      cancelled = true;
      if (retryTimerId != null) {
        window.clearTimeout(retryTimerId);
      }
    };
  }, [backendReady, code]);


  useEffect(() => {
    if (!backendReady) return;
    if (!compareCode) return;
    const cached = tradesCache.get(compareCode);
    if (cached) {
      setCompareTrades(cached.events);
    } else {
      setCompareTrades([]);
    }
    let cancelled = false;
    let retryTimerId: number | null = null;
    const fetchCompareTrades = (attempt: number) => {
      void api
        .get(`/trades/${compareCode}`)
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
          if (isRetryableTradesError(error)) {
            if (attempt < 2) {
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
    fetchCompareTrades(0);
    return () => {
      cancelled = true;
      if (retryTimerId != null) {
        window.clearTimeout(retryTimerId);
      }
    };
  }, [backendReady, compareCode]);

  const dailyParse = sharedDailyParse;
  const monthlyParse = useMemo(() => buildCandlesWithStats(monthlyData), [monthlyData]);
  const compareDailyParse = useMemo(() => buildCandlesWithStats(compareDailyData), [compareDailyData]);
  const compareMonthlyParse = useMemo(
    () => buildCandlesWithStats(compareMonthlyData),
    [compareMonthlyData]
  );
  const dailyCandles = useMemo(
    () => filterCandlesByAsOf(dailyParse.candles, mainAsOfTime),
    [dailyParse.candles, mainAsOfTime]
  );
  const monthlyCandles = useMemo(
    () => filterCandlesByAsOf(monthlyParse.candles, mainAsOfTime),
    [monthlyParse.candles, mainAsOfTime]
  );
  const compareDailyCandles = useMemo(
    () => compareDailyParse.candles,
    [compareDailyParse.candles]
  );
  const compareMonthlyCandles = useMemo(
    () => compareMonthlyParse.candles,
    [compareMonthlyParse.candles]
  );
  const dailyVolume = useMemo(
    () => filterVolumeByAsOf(buildVolume(dailyData), mainAsOfTime),
    [dailyData, mainAsOfTime]
  );
  const monthlyVolume = useMemo(
    () => filterVolumeByAsOf(buildVolume(monthlyData), mainAsOfTime),
    [monthlyData, mainAsOfTime]
  );
  const compareDailyVolume = useMemo(
    () => buildVolume(compareDailyData),
    [compareDailyData]
  );
  const weeklyData = useMemo(() => buildWeekly(dailyCandles, dailyVolume), [dailyCandles, dailyVolume]);
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

  const weeklyCandles = weeklyData.candles;
  const weeklyVolume = weeklyData.volume;
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
  const dailyPositions = positionData.dailyPositions;
  const tradeMarkers = positionData.tradeMarkers;
  const currentPositions = useMemo(
    () => (currentPositionsFromApi !== null ? currentPositionsFromApi : buildCurrentPositions(trades)),
    [currentPositionsFromApi, trades]
  );
  const latestTradeTime = useMemo(() => {
    if (trades.length === 0) return null;
    const times = trades
      .map((trade) => Date.parse(`${trade.date}T00:00:00Z`))
      .filter((value) => Number.isFinite(value))
      .map((value) => Math.floor(value / 1000));
    if (!times.length) return null;
    return Math.max(...times);
  }, [trades]);
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
  const monthlyInvalidCount =
    monthlyParse.stats.invalidRow + monthlyParse.stats.invalidTime + monthlyParse.stats.invalidValue;
  const dailyHasEmpty = dailyFetch.status === "success" && dailyFetch.responseCount === 0;
  const monthlyHasEmpty = monthlyFetch.status === "success" && monthlyFetch.responseCount === 0;
  const dailyHasParsedZero = dailyParse.stats.parsed === 0 && dailyParse.stats.total > 0;
  const monthlyHasParsedZero = monthlyParse.stats.parsed === 0 && monthlyParse.stats.total > 0;

  const dailyError =
    dailyErrors.length > 0
      ? dailyErrors[0]
      : dailyHasEmpty
        ? "No data"
        : dailyHasParsedZero
          ? `Date parse failed ${dailyParse.stats.invalidTime}`
          : null;

  const monthlyError =
    monthlyErrors.length > 0
      ? monthlyErrors[0]
      : monthlyHasEmpty
        ? "No data"
        : monthlyHasParsedZero
          ? `Date parse failed ${monthlyParse.stats.invalidTime}`
          : null;

  const weeklyHasEmpty = weeklyCandles.length === 0 && dailyCandles.length > 0;
  const weeklyError =
    dailyCandles.length === 0
      ? dailyError ?? "No data"
      : weeklyHasEmpty
        ? "No data"
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
    () => [...dailyErrors, ...monthlyErrors, ...tradeErrors],
    [dailyErrors, monthlyErrors, tradeErrors]
  );
  const otherWarningsCount = tradeWarningItems.length;
  const infoCount = tradeInfoItems.length;
  const warningCount = errors.length + unrecognizedCount + otherWarningsCount;
  const hasIssues = warningCount > 0 || infoCount > 0;
  const bannerTone = warningCount > 0 ? "warning" : "info";
  const bannerTitle = warningCount > 0 ? "Data issue detected" : "Data notice";

  const [debugOpen, setDebugOpen] = useState(false);
  const [showInfoDetails, setShowInfoDetails] = useState(false);
  const [copyFallbackText, setCopyFallbackText] = useState<string | null>(null);

  const debugSummary = useMemo(() => {
    const parts: string[] = [];
    if (errors.length) parts.push(`Errors ${errors.slice(0, 2).join(", ")}`);
    if (unrecognizedCount) parts.push(`Unrecognized labels ${unrecognizedCount}`);
    if (otherWarningsCount) parts.push(`Warnings ${otherWarningsCount}`);
    if (infoCount) parts.push(`Info ${infoCount}`);
    if (dailyHasEmpty) parts.push("Daily 0 bars");
    if (dailyHasParsedZero) parts.push("Daily parsed 0");
    if (dailyInvalidCount > 0) parts.push(`Daily invalid ${dailyInvalidCount}`);
    if (weeklyHasEmpty) parts.push("Weekly 0 bars");
    if (monthlyHasEmpty) parts.push("Monthly 0 bars");
    if (monthlyHasParsedZero) parts.push("Monthly parsed 0");
    if (monthlyInvalidCount > 0) parts.push(`Monthly invalid ${monthlyInvalidCount}`);
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
        return `Trades: OK (dedup ${count} rows for ${code})`;
      }
      return item;
    });
  }, [tradeInfoItems]);

  const debugLines = useMemo(() => {
    const lines: string[] = [];
    lines.push(
      `Daily(${dailyFetch.status}) API ${dailyFetch.responseCount} | Parsed ${dailyParse.stats.parsed} | Range ${dailyRangeCount} | InvalidRow ${dailyParse.stats.invalidRow} | InvalidTime ${dailyParse.stats.invalidTime} | InvalidValue ${dailyParse.stats.invalidValue} | Error ${dailyError ?? "-"}`
    );
    lines.push(`Weekly Parsed ${weeklyCandles.length} | Range ${weeklyRangeCount} | Error ${dailyError ?? "-"}`);
    lines.push(
      `Monthly(${monthlyFetch.status}) API ${monthlyFetch.responseCount} | Parsed ${monthlyParse.stats.parsed} | Range ${monthlyRangeCount} | InvalidRow ${monthlyParse.stats.invalidRow} | InvalidTime ${monthlyParse.stats.invalidTime} | InvalidValue ${monthlyParse.stats.invalidValue} | Error ${monthlyError ?? "-"}`
    );
    if (tradeWarningItems.length > 0) {
      lines.push(`Trades warnings: ${tradeWarningItems.slice(0, 5).join(", ")}`);
    }
    if (showInfoDetails && tradeInfoLines.length > 0) {
      lines.push(`Trades info: ${tradeInfoLines.slice(0, 5).join(", ")}`);
    }
    if (tradeWarnings.unrecognized_labels) {
      lines.push(
        `Unrecognized labels ${tradeWarnings.unrecognized_labels.count} samples: ${tradeWarnings.unrecognized_labels.samples.join(", ")}`
      );
    }
    if (tradeErrors.length > 0) {
      lines.push(`Trades errors: ${tradeErrors.slice(0, 3).join(", ")}`);
    }
    return lines;
  }, [
    dailyFetch.status,
    dailyFetch.responseCount,
    dailyParse.stats.parsed,
    dailyParse.stats.invalidRow,
    dailyParse.stats.invalidTime,
    dailyParse.stats.invalidValue,
    dailyRangeCount,
    dailyError,
    weeklyCandles.length,
    weeklyRangeCount,
    monthlyFetch.status,
    monthlyFetch.responseCount,
    monthlyParse.stats.parsed,
    monthlyParse.stats.invalidRow,
    monthlyParse.stats.invalidTime,
    monthlyParse.stats.invalidValue,
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
    if (!backendReady) return;
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
  }, [backendReady]);

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
    () => (rangeMonths ? buildRangeFromEndTime(rangeMonths, mainAsOfTime) : null),
    [rangeMonths, mainAsOfTime]
  );
  const compareDailyTargetRange = useMemo(
    () => (rangeMonths ? buildRangeFromEndTime(rangeMonths, compareAsOfTime) : null),
    [rangeMonths, compareAsOfTime]
  );
  const mainMonthlyTargetRange = useMemo(
    () => (rangeMonths ? buildRangeFromEndTime(rangeMonths, mainAsOfTime) : null),
    [rangeMonths, mainAsOfTime]
  );
  const compareMonthlyTargetRange = useMemo(
    () => (rangeMonths ? buildRangeFromEndTime(rangeMonths, compareAsOfTime) : null),
    [rangeMonths, compareAsOfTime]
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
    if (mainAsOfTime) {
      return buildRangeEndingAt(monthlyCandles, rangeMonths, mainAsOfTime);
    }
    return buildRange(monthlyCandles, rangeMonths);
  }, [monthlyCandles, rangeMonths, mainAsOfTime]);
  const resolvedDailyVisibleRange = rangeMonths ? dailyVisibleRange : manualDailyRangeRef.current;
  const resolvedWeeklyVisibleRange = rangeMonths ? weeklyVisibleRange : manualWeeklyRangeRef.current;
  const resolvedMonthlyVisibleRange = rangeMonths ? monthlyVisibleRange : manualMonthlyRangeRef.current;

  // Cursor mode functions
  const autoPanToBar = useCallback((time: number) => {
    if (!dailyChartRef.current) return;

    if (!resolvedDailyVisibleRange) return;

    const { from, to } = resolvedDailyVisibleRange;
    const rangeSize = to - from;
    const margin = rangeSize * 0.1;

    if (time < from + margin || time > to - margin) {
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
  }, [resolvedDailyVisibleRange, dailyCandles]);

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

  const handleDailyChartClick = (time: number | null) => {
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
    if (!cursorMode) return;
    if (nearestIndex != null) {
      updateSelectedBar(nearestIndex);
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



  const dailyMaLines = useMemo(() => {
    return buildDetailMaLines(dailyCandles, maSettings.daily);
  }, [dailyCandles, maSettings.daily]);
  const dailyChartMaLines = useMemo(() => toDetailChartMaLines(dailyMaLines), [dailyMaLines]);
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
  const holdDailyChartUntilDecisionReady = false;
  const shouldRenderCompareMonthlyChart = !compareLoading && compareMonthlyCandles.length > 0;
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
    const merged = [...dailyEventMarkers];
    if (showDecisionMarkers) {
      dailyCandles.forEach((candle) => {
        const tone = exactDecisionToneByDate.get(toDateKey(candle.time));
        if (tone === "up") {
          merged.push({ time: candle.time, kind: "decision-buy" });
        } else if (tone === "down") {
          merged.push({ time: candle.time, kind: "decision-sell" });
        }
      });
    }
    const deduped = new Map<string, (typeof merged)[number]>();
    merged.forEach((marker) => {
      const key =
        marker.kind === "earnings"
          ? `earnings:${marker.time}`
          : marker.kind?.startsWith("tdnet-")
            ? `${marker.kind}:${marker.time}:${marker.label ?? ""}`
            : `decision:${marker.time}`;
      deduped.set(key, marker);
    });
    return [...deduped.values()].sort((a, b) => a.time - b.time);
  }, [dailyEventMarkers, dailyCandles, exactDecisionToneByDate, showDecisionMarkers]);
  const compareMonthlyInitialRange = useMemo(() => {
    const months = rangeMonths ?? (compareAsOfTime ? COMPARE_FOCUS_MONTHS : null);
    if (!months) return null;
    return buildRangeEndingAt(compareMonthlyCandles, months, compareAsOfTime);
  }, [rangeMonths, compareMonthlyCandles, compareAsOfTime]);
  const compareMonthlyBaseRange = useMemo(() => {
    if (!rangeMonths) return null;
    if (mainMonthlyTargetRange) return mainMonthlyTargetRange;
    return buildRange(monthlyCandles, rangeMonths);
  }, [rangeMonths, mainMonthlyTargetRange, monthlyCandles]);
  const compareDailyInitialRange = useMemo(() => {
    if (!compareDailyCandles.length) return null;
    const months = rangeMonths ?? (compareAsOfTime ? COMPARE_FOCUS_MONTHS : null);
    if (!months) return null;
    return buildRangeEndingAt(compareDailyCandles, months, compareAsOfTime);
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
  const compareDailyNeedsMore = useMemo(() => {
    if (!compareDailyVisibleRange || !compareDailyCandles.length) return false;
    const earliest = compareDailyCandles[0]?.time;
    if (!earliest) return false;
    const hasMore = compareDailyData.length >= compareDailyLimit;
    return compareDailyVisibleRange.from < earliest && hasMore;
  }, [compareDailyVisibleRange, compareDailyCandles, compareDailyData.length, compareDailyLimit]);
  const shouldRenderCompareDailyChart =
    !compareDailyLoading &&
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

  // Cursor mode keyboard handler
  useEffect(() => {
    if (!cursorMode) return;

    const handleCursorKeyDown = (e: KeyboardEvent) => {
      // Don't handle if typing in textarea or input
      const target = e.target as HTMLElement;
      if (target.tagName === 'TEXTAREA' || target.tagName === 'INPUT') {
        return;
      }

      switch (e.key) {
        case 'ArrowLeft':
          e.preventDefault();
          moveToPrevDay();
          break;
        case 'ArrowRight':
          e.preventDefault();
          moveToNextDay();
          break;
        case 'c':
        case 'C':
          if (!e.ctrlKey && !e.metaKey) {
            e.preventDefault();
            toggleCursorMode();
          }
          break;
        case 'Escape':
          e.preventDefault();
          setCursorMode(false);
          break;
      }
    };

    window.addEventListener('keydown', handleCursorKeyDown);
    return () => window.removeEventListener('keydown', handleCursorKeyDown);
  }, [cursorMode, moveToNextDay, moveToPrevDay, toggleCursorMode]);


  const compareHasMoreDaily = compareDailyData.length >= compareDailyLimit;
  const compareHasMoreMonthly = compareMonthlyData.length >= monthlyLimit; // monthlyLimit is shared
  const canLoadMoreDaily = hasMoreDaily && dailyLimit < MAX_DAILY_BATCH_BARS_LIMIT;
  const canLoadMoreMonthly = hasMoreMonthly && monthlyLimit < MAX_MONTHLY_BATCH_BARS_LIMIT;
  const canLoadMoreCompareDaily = compareHasMoreDaily && compareDailyLimit < MAX_DAILY_BATCH_BARS_LIMIT;
  const canLoadMoreCompareMonthly = compareHasMoreMonthly && monthlyLimit < MAX_MONTHLY_BATCH_BARS_LIMIT;

  const mainSync = useChartSync(dailyChartRef, monthlyChartRef, weeklyChartRef, {
    enabled: syncRanges ?? true,
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
    enabled: syncRanges ?? true,
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
      ? "Loading..."
      : canLoadMoreDaily || canLoadMoreMonthly
        ? "Load more daily/monthly"
        : "All loaded";

  const toggleRange = (months: number) => {
    setRangeMonths((prev) => (prev === months ? null : months));
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
        navigate(`/detail/${nextCode}`, { state: { from: listBackPath } });
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

  const dailyEmptyMessage = loadingDaily
    ? "Loading..."
    : dailyCandles.length === 0
      ? dailyError ?? "No data"
      : null;
  const weeklyEmptyMessage = loadingDaily
    ? "Loading..."
    : weeklyCandles.length === 0
      ? weeklyError
      : null;
  const monthlyEmptyMessage = loadingMonthly
    ? "Loading..."
    : monthlyCandles.length === 0
      ? monthlyError ?? "No data"
      : null;

  const monthlyRatio = 1 - weeklyRatio;
  const focusTitle =
    focusPanel === "daily" ? "Daily (Focused)" : focusPanel === "weekly" ? "Weekly (Focused)" : "Monthly (Focused)";
  const listBackPath = useMemo(() => {
    const state = location.state as { from?: string } | null;
    const from = state?.from;
    let stored: string | null = null;
    if (typeof window !== "undefined") {
      try {
        stored = window.sessionStorage.getItem("detailListBack");
      } catch {
        stored = null;
      }
    }
    const candidate = from ?? stored;
    if (
      candidate === "/" ||
      candidate === "/ranking" ||
      candidate === "/favorites" ||
      candidate === "/candidates" ||
      candidate === "/tradex-tags"
    ) {
      return candidate;
    }
    return "/";
  }, [location.state]);
  useEffect(() => {
    if (!backendReady || !code) return;
    let active = true;
    const run = async () => {
      try {
        const [stateEvalResponse, trendResponse] = await Promise.all([
          api.get("/analysis-bridge/state-eval", {
            params: { code, limit: 5 }
          }),
          api.get("/analysis-bridge/internal/state-eval-trends", {
            params: { lookback: 14, limit: 20 }
          }),
        ]);
        if (!active) return;
        const rows = Array.isArray(stateEvalResponse.data?.rows) ? stateEvalResponse.data.rows : [];
        const first = rows[0] ?? null;
        setStateEvalRow(first);
        const trendMap = new Map<string, { label: string; tone: "improving" | "weakening" | "risk" }>();
        const improving = Array.isArray(trendResponse.data?.trends?.improving) ? trendResponse.data.trends.improving : [];
        const weakening = Array.isArray(trendResponse.data?.trends?.weakening) ? trendResponse.data.trends.weakening : [];
        const persistentRisk = Array.isArray(trendResponse.data?.trends?.persistent_risk) ? trendResponse.data.trends.persistent_risk : [];
        improving.forEach((row: { strategy_tag: string }) => trendMap.set(String(row.strategy_tag), { label: "Improving", tone: "improving" }));
        weakening.forEach((row: { strategy_tag: string }) => {
          if (!trendMap.has(String(row.strategy_tag))) trendMap.set(String(row.strategy_tag), { label: "Weakening", tone: "weakening" });
        });
        persistentRisk.forEach((row: { strategy_tag: string }) => {
          if (!trendMap.has(String(row.strategy_tag))) trendMap.set(String(row.strategy_tag), { label: "Persistent Risk", tone: "risk" });
        });
        const matchedTrend = parseStateEvalStrategyTags(first?.strategy_tags).map((tag) => trendMap.get(tag)).find(Boolean) ?? null;
        setStateEvalTrend(matchedTrend);
      } catch {
        if (active) {
          setStateEvalRow(null);
          setStateEvalTrend(null);
        }
      }
    };
    void run();
    return () => {
      active = false;
    };
  }, [backendReady, code]);
  const listCodes = useMemo(() => {
    if (typeof window === "undefined") return [];
    try {
      const stored = window.sessionStorage.getItem("detailListCodes");
      if (!stored) return [];
      const parsed = JSON.parse(stored);
      if (!Array.isArray(parsed)) return [];
      return parsed.filter((item) => typeof item === "string");
    } catch {
      return [];
    }
  }, []);
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
  }, [location.search]);
  const compareListItems = useMemo(() => compareList?.items ?? [], [compareList]);
  const compareListEligible = useMemo(() => {
    if (!compareList) return false;
    if (compareList.queryTicker !== code) return false;
    const storedMainAsOf = compareList.mainAsOf ?? null;
    const currentMainAsOf = mainAsOf ?? null;
    return storedMainAsOf === currentMainAsOf;
  }, [compareList, code, mainAsOf]);
  const nextCompareItem = useMemo(() => {
    if (!compareListEligible || !compareCode) return null;
    const index = compareListItems.findIndex(
      (item) => item.ticker === compareCode && (item.asof ?? null) === (compareAsOf ?? null)
    );
    if (index < 0) return null;
    return compareListItems[index + 1] ?? null;
  }, [compareListEligible, compareListItems, compareCode, compareAsOf]);
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
  useEffect(() => {
    if (!backendReady || compareCode) return;
    if (!nextCode) return;
    const timerId = window.setTimeout(() => {
      void prefetchChartFrame({
        code: nextCode,
        timeframe: "daily",
        limit: DEFAULT_LIMITS.daily,
      });
      void prefetchChartFrame({
        code: nextCode,
        timeframe: "monthly",
        limit: DEFAULT_LIMITS.monthly,
      });
    }, 150);
    return () => {
      window.clearTimeout(timerId);
    };
  }, [backendReady, compareCode, nextCode]);
  const stateEvalDisplayReasons = useMemo(() => {
    const reasons = parseStateEvalReasonTexts(stateEvalRow?.reason_text_top3).slice(0, 3);
    const trendReason = buildStateEvalTrendReason(stateEvalTrend);
    if (trendReason && !reasons.includes(trendReason)) {
      reasons.push(trendReason);
    }
    return reasons.slice(0, 3);
  }, [stateEvalRow, stateEvalTrend]);
  const stateEvalPriorReason = useMemo(
    () => stateEvalDisplayReasons.map(classifyStateEvalPriorReason).find(Boolean) ?? null,
    [stateEvalDisplayReasons]
  );
  const showDrawSettings = headerMode === "draw" && activeDrawTool !== null;
  const chartActionControls = (
    <div className="detail-controls-group detail-controls-icons">
      <IconButton
        label="スクショ"
        icon={<IconCamera size={18} />}
        disabled={screenshotBusy}
        tooltip="スクショ"
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

            const handleSaveSuccess = (saveResult: { success: boolean, savedPath?: string, savedDir?: string, error?: string }) => {
              if (saveResult.savedPath || saveResult.savedDir) {
                setToastMessage("スクショを保存しました");
                setToastAction({
                  label: "フォルダを開く",
                  onClick: async () => {
                    if (window.pywebview?.api?.open_path) {
                      const target = saveResult.savedPath || saveResult.savedDir;
                      if (target) {
                        await window.pywebview.api.open_path(target);
                      }
                    }
                  }
                });
              } else {
                // Fallback for browser download or missing path
                setToastMessage("スクショを保存しました（保存のみ）");
                setToastAction(null);
              }
            };

            if (result.copied) {
              // Clipboard copy succeeded - show toast with save action
              const blob = result.blob!;
              const filename = result.filename!;
              setToastMessage("スクショをクリップボードにコピーしました");
              setToastAction({
                label: "保存...",
                onClick: async () => {
                  const saveResult = await saveBlobToFile(blob, filename);
                  if (saveResult.success) {
                    handleSaveSuccess(saveResult);
                  } else {
                    setToastMessage(saveResult.error || "保存に失敗しました");
                    setToastAction(null);
                  }
                },
              });
            } else {
              // Clipboard failed - fallback to save
              setToastMessage("クリップボードにコピーできなかったため保存しました");
              setToastAction(null);
              if (result.blob && result.filename) {
                const saveResult = await saveBlobToFile(result.blob, result.filename);
                if (saveResult.success) {
                  handleSaveSuccess(saveResult);
                } else {
                  setToastMessage(saveResult.error || "保存に失敗しました");
                  setToastAction(null);
                }
              }
            }
          } finally {
            setScreenshotBusy(false);
          }
        }}
      />
      <IconButton
        label="AI出力"
        icon={<IconSparkles size={18} />}
        tooltip="AI出力"
        onClick={async () => {
          let dailyMemos: Record<string, string> = {};
          if (code) {
            try {
              const memoRes = await api.get("/memo/list", {
                params: { symbol: code, timeframe: "D" }
              });
              const items = memoRes.data?.items;
              if (Array.isArray(items)) {
                items.forEach((item: { date?: string; memo?: string }) => {
                  const rawDate = (item?.date ?? "").trim();
                  if (!rawDate) return;
                  const normalized = rawDate.replace(/\//g, "-");
                  dailyMemos[normalized] = item.memo ?? "";
                });
              }
            } catch {
              dailyMemos = {};
            }
          }

          const weeklyVolumeCounts = new Map<number, number>();
          dailyCandles.forEach((candle) => {
            if (!dailyVolumeByTime.has(candle.time)) return;
            const date = new Date(candle.time * 1000);
            const day = date.getUTCDay();
            const diff = (day + 6) % 7;
            const weekStart = Date.UTC(
              date.getUTCFullYear(),
              date.getUTCMonth(),
              date.getUTCDate() - diff
            );
            const key = Math.floor(weekStart / 1000);
            weeklyVolumeCounts.set(key, (weeklyVolumeCounts.get(key) ?? 0) + 1);
          });
          const weeklyVolumeMap = new Map<number, number | null>();
          weeklyVolume.forEach((item) => {
            const count = weeklyVolumeCounts.get(item.time) ?? 0;
            weeklyVolumeMap.set(item.time, count > 0 ? item.value : null);
          });
          const monthlyVolumeSums = new Map<number, number>();
          dailyCandles.forEach((candle) => {
            const volume = dailyVolumeByTime.get(candle.time);
            if (volume == null || !Number.isFinite(volume)) return;
            const date = new Date(candle.time * 1000);
            const monthStart = Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1);
            const key = Math.floor(monthStart / 1000);
            monthlyVolumeSums.set(key, (monthlyVolumeSums.get(key) ?? 0) + volume);
          });
          const monthlyVolumeMap = new Map<number, number>();
          monthlyCandles.forEach((candle) => {
            const sum = monthlyVolumeSums.get(candle.time);
            monthlyVolumeMap.set(candle.time, Number.isFinite(sum) ? Math.round(sum ?? 0) : 0);
          });
          const exportData = buildAIExport({
            code: code ?? "",
            name: tickerName,
            visibleTimeframe: "daily",
            rangeMonths: rangeMonths,
            dailyBars: dailyCandles.map((c) => ({
              time: c.time,
              open: c.open,
              high: c.high,
              low: c.low,
              close: c.close,
              volume: dailyVolumeByTime.get(c.time) ?? null
            })),
            weeklyBars: weeklyCandles.map((c) => ({
              time: c.time,
              open: c.open,
              high: c.high,
              low: c.low,
              close: c.close,
              volume: weeklyVolumeMap.get(c.time) ?? null
            })),
            monthlyBars: monthlyCandles.map((c) => ({
              time: c.time,
              open: c.open,
              high: c.high,
              low: c.low,
              close: c.close,
              volume: monthlyVolumeMap.get(c.time) ?? null
            })),
            maSettings,
            signals: dailySignals,
            showBoxes,
            showPositions: showTradesOverlay,
            boxes,
            dailyMemos,
            currentPositions,
          });
          const copied = await copyToClipboard(exportData.markdown);
          if (copied) {
            setToastMessage("AI用銘柄情報をクリップボードにコピーしました");
          } else {
            setToastMessage("クリップボードへのコピーに失敗しました");
          }
        }}
      />
      <IconButton
        label={analysisRecalcDisabled ? "判定更新" : "再計算"}
        icon={<IconRefresh size={18} />}
        disabled={
          (!analysisRecalcDisabled && analysisAsOfTime == null) ||
          analysisBackfillActive ||
          analysisRecalcSubmitting != null
        }
        tooltip={
          analysisRecalcDisabled
            ? "現在の基準日で売買判定を更新"
            : "基準日を中心に130本分の解析を再計算"
        }
        onClick={() => {
          void submitAnalysisRecalc();
        }}
      />
      <IconButton
        label="類似"
        icon={<IconChartArrows size={18} />}
        tooltip="類似チャート検索"
        onClick={() => setShowSimilar(true)}
      />
    </div>
  );
  const headerControls = (
    <>
      <div className="detail-controls-group">
        <div className="segmented detail-range">
          {RANGE_PRESETS.map((preset) => (
            <button
              key={preset.label}
              className={rangeMonths === preset.months ? "active" : ""}
              onClick={() => toggleRange(preset.months)}
            >
              {preset.label}
            </button>
          ))}
        </div>
        {canShowPhase && headerMode !== "analysis" && (
          <div className="detail-phase is-open">
            <div className="detail-phase-metrics">
              <span
                className={`detail-phase-score detail-phase-score--${getPhaseTone(
                  phaseScores?.bodyScore ?? null
                )}`}
              >
                中盤 {formatPhaseScore(phaseScores?.bodyScore ?? null)}
              </span>
              <span
                className={`detail-phase-score detail-phase-score--${getPhaseTone(
                  phaseScores?.earlyScore ?? null
                )}`}
              >
                序盤 {formatPhaseScore(phaseScores?.earlyScore ?? null)}
              </span>
              <span
                className={`detail-phase-score detail-phase-score--${getPhaseTone(
                  phaseScores?.lateScore ?? null
                )}`}
              >
                終盤 {formatPhaseScore(phaseScores?.lateScore ?? null)}
              </span>
              <div className="detail-phase-reasons">
                {phaseReasons.length ? (
                  phaseReasons.map((reason) => (
                    <span key={reason} className="detail-phase-reason">
                      {reason}
                    </span>
                  ))
                ) : (
                  <span className="detail-phase-empty">--</span>
                )}
              </div>
            </div>
          </div>
        )}
      </div>
      <div className="detail-controls-group">
        <div className="popover-anchor" ref={displayRef}>
          <IconButton
            icon={<IconAdjustments size={18} />}
            label="表示"
            variant="iconLabel"
            tooltip="表示設定"
            ariaLabel="表示設定メニューを開く"
            selected={displayOpen}
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
                  className={`popover-item ${showDecisionMarkers ? "active" : ""}`}
                  onClick={() => setShowDecisionMarkers((prev) => !prev)}
                >
                  <span className="popover-item-label">判定マーカー</span>
                  {showDecisionMarkers && <span className="popover-check">ON</span>}
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
                  <span className="popover-item-label">カーソル</span>
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
        <div className="segmented detail-mode">
          <button
            className={headerMode === "chart" ? "active" : ""}
            onClick={() => setHeaderMode("chart")}
          >
            チャート
          </button>
          <button
            className={headerMode === "analysis" ? "active" : ""}
            onClick={() => {
              setHeaderMode("analysis");
              if (!cursorMode) {
                setCursorMode(true);
                if (dailyCandles.length > 0) {
                  updateSelectedBar(dailyCandles.length - 1);
                }
              }
            }}
          >
            分析
          </button>
          <button
            className={headerMode === "financial" ? "active" : ""}
            onClick={() => setHeaderMode("financial")}
          >
            財務
          </button>
          <button
            className={headerMode === "draw" ? "active" : ""}
            onClick={() => setHeaderMode("draw")}
          >
            描画
          </button>
          <button
            onClick={() => {
              if (code) navigate(`/practice/${code}`);
            }}
          >
            練習          </button>
          <button
            className={headerMode === "positions" ? "active" : ""}
            onClick={() => setHeaderMode("positions")}
          >
            建玉          </button>
        </div>
      </div>
      {(headerMode === "chart" || headerMode === "analysis") && chartActionControls}
    </>
  );


  return (
    <div className={`detail-shell ${focusPanel ? "detail-shell-focus" : ""}`}>
      <div className="detail-header">
        <div className="detail-summary-row">
          <div className="detail-summary-back">
            <button
              className="back nav-button nav-primary"
              onClick={() => navigate(listBackPath)}
            >
              <span className="nav-icon">
                <IconArrowLeft size={16} />
              </span>
              <span className="nav-label">一覧に戻る</span>
            </button>
            <button className="back nav-button" onClick={() => navigate(-1)}>
              <span className="nav-icon">
                <IconArrowLeft size={16} />
              </span>
              <span className="nav-label">前画面</span>
            </button>
          </div>
          <div className="detail-summary-main">
            <div className="detail-summary-title">
              <div className="detail-summary-code">{code}</div>
              {tickerName && <div className="detail-summary-name">{tickerName}</div>}
            </div>
            <div className="detail-summary-status">
              {(rightsLabel || earningsLabel) && (
                <div className="detail-event-badges detail-event-badges-inline">
                  {rightsLabel && <span className="event-badge event-rights">権利 {rightsLabel}</span>}
                  {earningsLabel && <span className="event-badge event-earnings">決算 {earningsLabel}</span>}
                </div>
              )}
              {dailySignals.length > 0 && (
                <div className="detail-signals-inline summary-signals">
                  <div className="popover-anchor" ref={signalsRef}>
                    <button
                      type="button"
                      className="signal-chip"
                      onClick={() => setSignalsOpen((prev) => !prev)}
                    >
                      シグナル {dailySignals.length}
                    </button>
                    {signalsOpen && (
                      <div className="popover-panel">
                        <div className="popover-section">
                          <div className="popover-title">シグナル</div>
                          <div className="detail-signals-inline">
                            {dailySignals.map((signal) => (
                              <span
                                key={signal.label}
                                className={`signal-chip ${signal.kind === 'warning' ? 'warning' : 'achieved'}`}
                              >
                                {signal.label}
                              </span>
                            ))}
                          </div>
                        </div>
                      </div>
                    )}
                  </div>
                </div>
              )}
            </div>
            {stateEvalRow && (
              <div className="detail-ai-state-panel">
                <div className="detail-ai-state-head">
                  <span className={`candidate-ai-badge is-${String(stateEvalRow.decision_3way || "wait")}`}>
                    {String(stateEvalRow.decision_3way || "wait").toUpperCase()}
                  </span>
                  <span className="detail-ai-state-meta">
                    AI {typeof stateEvalRow.confidence === "number" ? `${Math.round(stateEvalRow.confidence * 100)}%` : "--"}
                  </span>
                  <span className="detail-ai-state-meta">{stateEvalRow.holding_band ?? "--"}</span>
                  {stateEvalTrend ? (
                    <span className={`detail-ai-state-meta detail-ai-state-trend is-${stateEvalTrend.tone}`}>{stateEvalTrend.label}</span>
                  ) : null}
                  {stateEvalPriorReason ? (
                    <span className={`candidate-ai-prior-badge is-${stateEvalPriorReason.tone}`}>
                      {stateEvalPriorReason.tone === "combo"
                        ? `COMBO ${stateEvalPriorReason.label}`
                        : stateEvalPriorReason.tone === "prior-caution"
                          ? `CAUTION ${stateEvalPriorReason.label}`
                          : `PRIOR ${stateEvalPriorReason.label}`}
                    </span>
                  ) : null}
                </div>
                <div className="detail-ai-state-reasons">
                  {stateEvalDisplayReasons.map((reason) => (
                    <span key={reason} className="detail-ai-state-reason">
                      {reason}
                    </span>
                  ))}
                </div>
              </div>
            )}
          </div>
          {headerMode !== "draw" && (
            <div className="detail-controls detail-summary-inline-controls">{headerControls}</div>
          )}
          <div className="detail-summary-actions">
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
                navigate(`/detail/${prevCode}`, { state: { from: listBackPath } });
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
                navigate(`/detail/${nextCode}`, { state: { from: listBackPath } });
              }}
              disabled={!nextCode}
            >
              <span className="nav-icon">
                <IconArrowRight size={16} />
              </span>
              <span className="nav-label">次の銘柄</span>
            </button>
          </div>
        </div>
        {headerMode === "draw" && (
          <div className="detail-controls detail-header-toolbar">
            <div className="detail-controls-group">
              <div className="segmented detail-range">
                {RANGE_PRESETS.map((preset) => (
                  <button
                    key={preset.label}
                    className={rangeMonths === preset.months ? "active" : ""}
                    onClick={() => toggleRange(preset.months)}
                  >
                    {preset.label}
                  </button>
                ))}
              </div>
              {canShowPhase && headerMode !== "analysis" && (
                <div className="detail-phase is-open">
                  <div className="detail-phase-metrics">
                    <span
                      className={`detail-phase-score detail-phase-score--${getPhaseTone(
                        phaseScores?.bodyScore ?? null
                      )}`}
                    >
                      中盤 {formatPhaseScore(phaseScores?.bodyScore ?? null)}
                    </span>
                    <span
                      className={`detail-phase-score detail-phase-score--${getPhaseTone(
                        phaseScores?.earlyScore ?? null
                      )}`}
                    >
                      序盤 {formatPhaseScore(phaseScores?.earlyScore ?? null)}
                    </span>
                    <span
                      className={`detail-phase-score detail-phase-score--${getPhaseTone(
                        phaseScores?.lateScore ?? null
                      )}`}
                    >
                      終盤 {formatPhaseScore(phaseScores?.lateScore ?? null)}
                    </span>
                    <div className="detail-phase-reasons">
                      {phaseReasons.length ? (
                        phaseReasons.map((reason) => (
                          <span key={reason} className="detail-phase-reason">
                            {reason}
                          </span>
                        ))
                      ) : (
                        <span className="detail-phase-empty">--</span>
                      )}
                    </div>
                  </div>
                </div>
              )}
            </div>
            <div className="detail-controls-group">
              <IconButton
                icon={<IconChartArrows size={18} />}
                tooltip="時間ゾーン描画"
                ariaLabel="時間ゾーン描画"
                className="draw-tool-button"
                selected={activeDrawTool === "timeZone"}
                onClick={() => selectDrawTool("timeZone")}
              />
              <IconButton
                icon={<IconBox size={18} />}
                tooltip="四角描画"
                ariaLabel="四角描画"
                className="draw-tool-button"
                selected={activeDrawTool === "drawBox"}
                onClick={() => selectDrawTool("drawBox")}
              />
              <IconButton
                icon={<span style={{ fontSize: 18, lineHeight: 1 }}>▭</span>}
                tooltip="価格帯描画"
                ariaLabel="価格帯描画"
                className="draw-tool-button"
                selected={activeDrawTool === "priceBand"}
                onClick={() => selectDrawTool("priceBand")}
              />
              <IconButton
                icon={<IconMinus size={18} />}
                tooltip="水平線描画"
                ariaLabel="水平線描画"
                className="draw-tool-button"
                selected={activeDrawTool === "horizontalLine"}
                onClick={() => selectDrawTool("horizontalLine")}
              />
              <IconButton
                icon={<IconTrash size={18} />}
                tooltip="描画をリセット"
                ariaLabel="描画をリセット"
                onClick={resetAllDrawings}
              />
            </div>
            {showDrawSettings && (
              <div className="detail-controls-group">
                <IconButton
                  icon={
                    <span
                      style={{
                        width: 14,
                        height: 14,
                        borderRadius: 999,
                        background: activeDrawColor,
                        display: "inline-block",
                        border: "1px solid rgba(0,0,0,0.2)"
                      }}
                    />
                  }
                  tooltip="描画色を変更"
                  ariaLabel="描画色を変更"
                  onClick={() =>
                    setActiveDrawColorIndex((prev) => (prev + 1) % COLOR_PALETTE.length)
                  }
                />
                <input
                  type="range"
                  min={0.1}
                  max={1}
                  step={0.05}
                  value={activeLineOpacity}
                  title="不透明度"
                  style={{ width: 60 }}
                  onChange={(event) => setActiveLineOpacity(Number(event.target.value))}
                />
                <input
                  type="range"
                  min={1}
                  max={6}
                  step={0.5}
                  value={activeLineWidth}
                  title="太さ"
                  style={{ width: 60 }}
                  onChange={(event) => setActiveLineWidth(Number(event.target.value))}
                />
              </div>
            )}
            <div className="detail-controls-group">
              <div className="popover-anchor" ref={displayRef}>
                <IconButton
                  icon={<IconAdjustments size={18} />}
                  label="表示"
                  variant="iconLabel"
                  tooltip="表示設定"
                  ariaLabel="表示設定メニューを開く"
                  selected={displayOpen}
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
                        className={`popover-item ${showDecisionMarkers ? "active" : ""}`}
                        onClick={() => setShowDecisionMarkers((prev) => !prev)}
                      >
                        <span className="popover-item-label">判定マーカー</span>
                        {showDecisionMarkers && <span className="popover-check">ON</span>}
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
                        <span className="popover-item-label">カーソル</span>
                        {cursorMode && <span className="popover-check">ON</span>}
                      </button>
                    </div>
                    <div className="popover-section">
                      <button
                        type="button"
                        className="popover-item"
                        onClick={() => setShowIndicators(true)}
                      >
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
              <div className="segmented detail-mode">
                <button
                  className={headerMode === "chart" ? "active" : ""}
                  onClick={() => setHeaderMode("chart")}
                >
                  チャート
                </button>
                <button
                  className={headerMode === "analysis" ? "active" : ""}
                  onClick={() => setHeaderMode("analysis")}
                >
                  分析
                </button>
                <button
                  className={headerMode === "financial" ? "active" : ""}
                  onClick={() => setHeaderMode("financial")}
                >
                  財務
                </button>
                <button
                  className={headerMode === "draw" ? "active" : ""}
                  onClick={() => setHeaderMode("draw")}
                >
                  描画
                </button>
                <button
                  onClick={() => {
                    if (code) navigate(`/practice/${code}`);
                  }}
                >
                  練習                </button>
                <button
                  className={headerMode === "positions" ? "active" : ""}
                  onClick={() => setHeaderMode("positions")}
                >
                  建玉                </button>
              </div>
            </div>
          </div>
        )}
      </div>
      {marketDataStatusMessage && (
        <div className={`detail-market-data-status ${marketDataStatusDelayed ? "is-delayed" : ""}`}>
          {marketDataStatusMessage}
        </div>
      )}
      <div className={`detail-content ${showFinancialPanel ? "with-memo-panel detail-content-financial" : ""}`}>
        <div className={`detail-split ${focusPanel ? "detail-split-focus" : ""} ${showRightPanel ? "with-memo-panel" : ""}`}>
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
                      drawingEnabled={headerMode === "draw"}
                      timeZones={monthlyDrawings.timeZones}
                      priceBands={monthlyDrawings.priceBands}
                      drawBoxes={monthlyDrawings.drawBoxes}
                      horizontalLines={monthlyDrawings.horizontalLines}
                      showPriceBands
                      activeTool={activeDrawTool}
                      activeDrawColor={activeDrawColor}
                      activeLineOpacity={activeLineOpacity}
                      activeLineWidth={activeLineWidth}
                      onSelectShape={setSelectedDrawing}
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
                      <div className="detail-chart-empty">Monthly: {monthlyEmptyMessage}</div>
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
                        drawingEnabled={headerMode === "draw"}
                        timeZones={compareMonthlyDrawings.timeZones}
                        priceBands={compareMonthlyDrawings.priceBands}
                        drawBoxes={compareMonthlyDrawings.drawBoxes}
                        horizontalLines={compareMonthlyDrawings.horizontalLines}
                        showPriceBands
                        activeTool={activeDrawTool}
                        activeDrawColor={activeDrawColor}
                        activeLineOpacity={activeLineOpacity}
                        activeLineWidth={activeLineWidth}
                        onSelectShape={setSelectedDrawing}
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
                    {compareLoading && (
                      <div className="detail-chart-empty">Loading...</div>
                    )}
                    {!compareLoading && compareMonthlyErrors.length > 0 && (
                      <div className="detail-chart-empty">Monthly: {compareMonthlyErrors[0]}</div>
                    )}
                    {!compareLoading && compareMonthlyErrors.length === 0 && compareMonthlyCandles.length === 0 && (
                      <div className="detail-chart-empty">Monthly: データがありません</div>
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
                        drawingEnabled={headerMode === "draw"}
                        timeZones={dailyDrawings.timeZones}
                        priceBands={dailyDrawings.priceBands}
                        drawBoxes={dailyDrawings.drawBoxes}
                        horizontalLines={dailyDrawings.horizontalLines}
                        showPriceBands
                        activeTool={activeDrawTool}
                        activeDrawColor={activeDrawColor}
                        activeLineOpacity={activeLineOpacity}
                        activeLineWidth={activeLineWidth}
                        onSelectShape={setSelectedDrawing}
                        onAddTimeZone={addTimeZone(dailyDrawingKey)}
                        onUpdateTimeZone={updateTimeZone(dailyDrawingKey)}
                        onDeleteTimeZone={deleteTimeZone(dailyDrawingKey)}
                        onAddPriceBand={addPriceBand(dailyDrawingKey)}
                        onUpdatePriceBand={updatePriceBand(dailyDrawingKey)}
                        onDeletePriceBand={deletePriceBand(dailyDrawingKey)}
                        onAddDrawBox={addDrawBox(dailyDrawingKey)}
                        onUpdateDrawBox={updateDrawBox(dailyDrawingKey)}
                        onDeleteDrawBox={deleteDrawBox(dailyDrawingKey)}
                        onAddHorizontalLine={addHorizontalLine(dailyDrawingKey)}
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
                        onCrosshairMove={(time, point) =>
                          handleCompareDailyCrosshair(time, "left", point)
                        }
                        onVisibleRangeChange={handleDailyVisibleRangeChange}
                      />
                    )}
                    {holdDailyChartUntilDecisionReady && (
                      <div className="detail-chart-empty">判定マークを読み込み中...</div>
                    )}
                    {dailyEmptyMessage && (
                      <div className="detail-chart-empty">Daily: {dailyEmptyMessage}</div>
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
                        drawingEnabled={headerMode === "draw"}
                        timeZones={compareDailyDrawings.timeZones}
                        priceBands={compareDailyDrawings.priceBands}
                        drawBoxes={compareDailyDrawings.drawBoxes}
                        horizontalLines={compareDailyDrawings.horizontalLines}
                        showPriceBands
                        activeTool={activeDrawTool}
                        activeDrawColor={activeDrawColor}
                        activeLineOpacity={activeLineOpacity}
                        activeLineWidth={activeLineWidth}
                        onSelectShape={setSelectedDrawing}
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
                    {(compareDailyLoading || compareDailyNeedsMore) && (
                      <div className="detail-chart-empty">一致期間のデータを読み込み中...</div>
                    )}
                    {!compareDailyLoading && compareDailyErrors.length > 0 && (
                      <div className="detail-chart-empty">Daily: {compareDailyErrors[0]}</div>
                    )}
                    {!compareDailyLoading && compareDailyErrors.length === 0 && compareDailyCandles.length === 0 && (
                      <div className="detail-chart-empty">Daily: データがありません</div>
                    )}
                  </div>
                </div>
              </div>
            </div>
          )}
          {compareCode ? null : focusPanel ? (
            <div className="detail-row detail-row-focus">
              <div className="detail-pane-header">{focusTitle}</div>
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
                      drawingEnabled={headerMode === "draw"}
                      timeZones={dailyDrawings.timeZones}
                      priceBands={dailyDrawings.priceBands}
                      drawBoxes={dailyDrawings.drawBoxes}
                      horizontalLines={dailyDrawings.horizontalLines}
                      showPriceBands
                      activeTool={activeDrawTool}
                      activeDrawColor={activeDrawColor}
                      activeLineOpacity={activeLineOpacity}
                      activeLineWidth={activeLineWidth}
                      onSelectShape={setSelectedDrawing}
                      onAddTimeZone={addTimeZone(dailyDrawingKey)}
                      onUpdateTimeZone={updateTimeZone(dailyDrawingKey)}
                      onDeleteTimeZone={deleteTimeZone(dailyDrawingKey)}
                      onAddPriceBand={addPriceBand(dailyDrawingKey)}
                      onUpdatePriceBand={updatePriceBand(dailyDrawingKey)}
                      onDeletePriceBand={deletePriceBand(dailyDrawingKey)}
                      onAddDrawBox={addDrawBox(dailyDrawingKey)}
                      onUpdateDrawBox={updateDrawBox(dailyDrawingKey)}
                      onDeleteDrawBox={deleteDrawBox(dailyDrawingKey)}
                      onAddHorizontalLine={addHorizontalLine(dailyDrawingKey)}
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
                      onCrosshairMove={handleDailyCrosshair}
                      onVisibleRangeChange={handleDailyVisibleRangeChange}
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
                    drawingEnabled={headerMode === "draw"}
                    timeZones={weeklyDrawings.timeZones}
                    priceBands={weeklyDrawings.priceBands}
                    drawBoxes={weeklyDrawings.drawBoxes}
                    horizontalLines={weeklyDrawings.horizontalLines}
                    showPriceBands
                    activeTool={activeDrawTool}
                    activeDrawColor={activeDrawColor}
                    activeLineOpacity={activeLineOpacity}
                    activeLineWidth={activeLineWidth}
                    onSelectShape={setSelectedDrawing}
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
                    drawingEnabled={headerMode === "draw"}
                    timeZones={monthlyDrawings.timeZones}
                    priceBands={monthlyDrawings.priceBands}
                    drawBoxes={monthlyDrawings.drawBoxes}
                    horizontalLines={monthlyDrawings.horizontalLines}
                    showPriceBands
                    activeTool={activeDrawTool}
                    activeDrawColor={activeDrawColor}
                    activeLineOpacity={activeLineOpacity}
                    activeLineWidth={activeLineWidth}
                    onSelectShape={setSelectedDrawing}
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
                    onCrosshairMove={handleMonthlyCrosshair}
                    onVisibleRangeChange={handleMonthlyVisibleRangeChange}
                  />
                )}
                {focusPanel === "daily" && dailyEmptyMessage && (
                  <div className="detail-chart-empty">Daily: {dailyEmptyMessage}</div>
                )}
                {focusPanel === "daily" && holdDailyChartUntilDecisionReady && (
                  <div className="detail-chart-empty">判定マークを読み込み中...</div>
                )}
                {focusPanel === "weekly" && weeklyEmptyMessage && (
                  <div className="detail-chart-empty">Weekly: {weeklyEmptyMessage}</div>
                )}
                {focusPanel === "monthly" && monthlyEmptyMessage && (
                  <div className="detail-chart-empty">Monthly: {monthlyEmptyMessage}</div>
                )}
                <button
                  type="button"
                  className="detail-focus-back"
                  onClick={() => setFocusPanel(null)}
                >
                  Back to 3 charts
                </button>
              </div>
            </div>
          ) : (
            <>
              <div className="detail-row detail-row-top" style={{ flex: `${DAILY_ROW_RATIO} 1 0%` }}>
                <div className="detail-pane-header">Daily</div>
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
                      drawingEnabled={headerMode === "draw"}
                      timeZones={dailyDrawings.timeZones}
                      priceBands={dailyDrawings.priceBands}
                      drawBoxes={dailyDrawings.drawBoxes}
                      horizontalLines={dailyDrawings.horizontalLines}
                      showPriceBands
                      activeTool={activeDrawTool}
                      activeDrawColor={activeDrawColor}
                      activeLineOpacity={activeLineOpacity}
                      activeLineWidth={activeLineWidth}
                      onSelectShape={setSelectedDrawing}
                      onAddTimeZone={addTimeZone(dailyDrawingKey)}
                      onUpdateTimeZone={updateTimeZone(dailyDrawingKey)}
                      onDeleteTimeZone={deleteTimeZone(dailyDrawingKey)}
                      onAddPriceBand={addPriceBand(dailyDrawingKey)}
                      onUpdatePriceBand={updatePriceBand(dailyDrawingKey)}
                      onDeletePriceBand={deletePriceBand(dailyDrawingKey)}
                      onAddDrawBox={addDrawBox(dailyDrawingKey)}
                      onUpdateDrawBox={updateDrawBox(dailyDrawingKey)}
                      onDeleteDrawBox={deleteDrawBox(dailyDrawingKey)}
                      onAddHorizontalLine={addHorizontalLine(dailyDrawingKey)}
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
                      onCrosshairMove={handleDailyCrosshair}
                      onVisibleRangeChange={handleDailyVisibleRangeChange}
                      onChartClick={handleDailyChartClick}
                    />
                  )}
                  {holdDailyChartUntilDecisionReady && (
                    <div className="detail-chart-empty">判定マークを読み込み中...</div>
                  )}
                  {dailyEmptyMessage && (
                    <div className="detail-chart-empty">Daily: {dailyEmptyMessage}</div>
                  )}
                </div>
              </div>
              <div
                className="detail-row detail-row-bottom"
                style={{ flex: `${1 - DAILY_ROW_RATIO} 1 0%` }}
                ref={bottomRowRef}
              >
                <div className="detail-pane" style={{ flex: `${weeklyRatio} 1 0%` }}>
                  <div className="detail-pane-header">Weekly</div>
                  <div
                    className="detail-chart detail-chart-focusable"
                    onDoubleClick={() => toggleFocus("weekly")}
                  >
                    <DetailChart
                      ref={weeklyChartRef}
                      candles={weeklyCandles}
                      volume={weeklyVolume}
                      maLines={weeklyChartMaLines}
                      showVolume={false}
                      boxes={boxes}
                      showBoxes={showBoxes}
                      gapBands={gapBandsOverride}
                      drawingEnabled={headerMode === "draw"}
                      timeZones={weeklyDrawings.timeZones}
                      priceBands={weeklyDrawings.priceBands}
                      drawBoxes={weeklyDrawings.drawBoxes}
                      horizontalLines={weeklyDrawings.horizontalLines}
                      showPriceBands
                      activeTool={activeDrawTool}
                      activeDrawColor={activeDrawColor}
                      activeLineOpacity={activeLineOpacity}
                      activeLineWidth={activeLineWidth}
                      onSelectShape={setSelectedDrawing}
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
                      onCrosshairMove={handleWeeklyCrosshair}
                      onVisibleRangeChange={handleWeeklyVisibleRangeChange}
                    />
                    {weeklyEmptyMessage && (
                      <div className="detail-chart-empty">Weekly: {weeklyEmptyMessage}</div>
                    )}
                  </div>
                </div>
                <div
                  className="detail-divider detail-divider-vertical"
                  onMouseDown={startDrag()}
                  onTouchStart={startDrag()}
                />
                <div className="detail-pane" style={{ flex: `${monthlyRatio} 1 0%` }}>
                  <div className="detail-pane-header">Monthly</div>
                  <div
                    className="detail-chart detail-chart-focusable"
                    onDoubleClick={() => toggleFocus("monthly")}
                  >
                    <DetailChart
                      ref={monthlyChartRef}
                      candles={monthlyCandles}
                      volume={monthlyVolume}
                      maLines={monthlyChartMaLines}
                      showVolume={false}
                      boxes={boxes}
                      showBoxes={showBoxes}
                      gapBands={gapBandsOverride}
                      drawingEnabled={headerMode === "draw"}
                      timeZones={monthlyDrawings.timeZones}
                      priceBands={monthlyDrawings.priceBands}
                      drawBoxes={monthlyDrawings.drawBoxes}
                      horizontalLines={monthlyDrawings.horizontalLines}
                      showPriceBands
                      activeTool={activeDrawTool}
                      activeDrawColor={activeDrawColor}
                      activeLineOpacity={activeLineOpacity}
                      activeLineWidth={activeLineWidth}
                      onSelectShape={setSelectedDrawing}
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
                      onCrosshairMove={handleMonthlyCrosshair}
                      onVisibleRangeChange={handleMonthlyVisibleRangeChange}
                    />
                    {monthlyEmptyMessage && (
                      <div className="detail-chart-empty">Monthly: {monthlyEmptyMessage}</div>
                    )}
                  </div>
                </div>
              </div>
            </>
          )}
        </div>
        {showMemoPanel && (
          <DailyMemoPanel
            code={code || ''}
            selectedDate={selectedDate}
            selectedBarData={selectedBarData}
            {...(memoPanelData || {})}
            cursorMode={cursorMode}
            onToggleCursorMode={toggleCursorMode}
            onPrevDay={moveToPrevDay}
            onNextDay={moveToNextDay}
            onCopyForConsult={handleCopyForConsult}
          />
        )}
        {showAnalysisPanel && (
          <DetailAnalysisPanel
            analysisAsOfTime={analysisAsOfTime}
            analysisBackfillActive={analysisBackfillActive}
            analysisRecalcSubmitting={analysisRecalcSubmitting}
            analysisRecalcDisabled={analysisRecalcDisabled}
            analysisRecalcDisabledReason={analysisRecalcDisabledReason}
            submitAnalysisRecalc={submitAnalysisRecalc}
            analysisDtLabel={analysisDtLabel}
            cursorMode={cursorMode}
            analysisCursorDateLabel={analysisCursorDateLabel}
            canShowPhase={canShowPhase}
            phaseReasons={phaseReasons}
            canShowAnalysis={canShowAnalysis}
            analysisDecision={analysisDecision}
            analysisSummaryLoading={analysisSummaryLoading}
            analysisGuidance={analysisGuidance}
            analysisEntryPolicy={analysisEntryPolicy}
            patternSummary={patternSummary}
            analysisPreparationVisible={analysisPreparationVisible}
            analysisBackfillProgressLabel={analysisBackfillProgressLabel}
            analysisBackfillMessage={analysisBackfillMessage}
            sellAnalysisDtLabel={sellAnalysisDtLabel}
            sellPredDtLabel={sellPredDtLabel}
            researchPriorRunId={researchPriorRunId}
            analysisResearchPrior={analysisResearchPrior}
            researchPriorUpMeta={researchPriorUpMeta}
            researchPriorDownMeta={researchPriorDownMeta}
            edinetStatusMeta={edinetStatusMeta}
            edinetQualityMeta={edinetQualityMeta}
            edinetMetricsMeta={edinetMetricsMeta}
            edinetBonusMeta={edinetBonusMeta}
            hasSwingData={hasSwingData}
            swingPlan={swingPlan}
            swingSideLabel={swingSideLabel}
            swingReasonsLabel={swingReasonsLabel}
            swingDiagnostics={swingDiagnostics}
            swingSetupExpectancy={swingSetupExpectancy}
            analysisMissingDataVisible={analysisMissingDataVisible}
            formatPercentLabel={formatPercentLabel}
            formatNumber={formatNumber}
            formatSignedPercentLabel={formatSignedPercentLabel}
          />
        )}
        {showAnalysisPanel && (
          <TradexAnalysisMount
            backendReady={backendReady}
            readyToFetch={analysisNetworkReady}
            analysisFetchEnabled={analysisFetchEnabled}
            code={code}
            asof={analysisAsOfTime}
            formatPercentLabel={formatPercentLabel}
            formatSignedPercentLabel={formatSignedPercentLabel}
            formatNumber={formatNumber}
          />
        )}
        {showFinancialPanel && (
          <DetailFinancialPanel
            financialPanelRef={financialPanelRef}
            financialPanel={financialPanel}
            financialFetchedLabel={financialFetchedLabel}
            financialLoading={financialLoading}
            financialSeries={financialSeries}
            financialCards={financialDisplay.cards}
            financialKeyStats={financialDisplay.stats}
            tdnetHighlights={tdnetHighlights}
            tdnetLoading={tdnetLoading}
            tdnetStatusLabel={tdnetStatusLabel}
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
        )}
      </div>
      {activeTdnetDisclosure && !compareCode && (
        <DetailTdnetCard
          activeTdnetDisclosure={activeTdnetDisclosure}
          activeTdnetReaction={activeTdnetReaction}
          selectedTdnetDisclosures={selectedTdnetDisclosures}
          selectedTdnetDisclosureIndex={selectedTdnetDisclosureIndex}
          setSelectedTdnetDisclosures={setSelectedTdnetDisclosures}
          setSelectedTdnetDisclosureIndex={setSelectedTdnetDisclosureIndex}
          formatNumber={formatNumber}
          formatSignedPercentLabel={formatSignedPercentLabel}
        />
      )}
      {!focusPanel && (
        <div className="detail-footer">
          <div className="detail-footer-left">
            <button className="load-more" onClick={loadMoreDailyAndMonthly} disabled={loadMoreDisabled}>
              {loadMoreLabel}
            </button>
          </div>
          <div className="detail-hint">
            Daily {dailyCandles.length} bars | Weekly {weeklyCandles.length} bars | Monthly {monthlyCandles.length} bars
          </div>
        </div>
      )}
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
      />
      <DetailDebugBanner
        hasIssues={hasIssues}
        bannerTone={bannerTone}
        bannerTitle={bannerTitle}
        debugSummary={debugSummary}
        debugOpen={debugOpen}
        showInfoDetails={showInfoDetails}
        debugLines={debugLines}
        copyFallbackText={copyFallbackText}
        onToggleOpen={() => setDebugOpen((prev) => !prev)}
        onCopy={handleCopyDebug}
        onToggleInfoDetails={() => setShowInfoDetails((prev) => !prev)}
        onClose={() => setDebugOpen(false)}
      />
      <DetailIndicatorOverlay
        isOpen={showIndicators}
        compareCode={compareCode}
        maEditMode={maEditMode}
        activeMaSettings={activeMaSettings}
        onSetMaEditMode={setMaEditMode}
        onUpdateSetting={updateSetting}
        onResetSettings={resetSettings}
        onClose={() => setShowIndicators(false)}
      />
      <Toast
        message={toastMessage}
        onClose={() => { setToastMessage(null); setToastAction(null); }}
        action={toastAction}
        duration={toastAction ? 8000 : 4000}
      />
      <SimilarSearchPanel
        isOpen={showSimilar}
        onClose={() => setShowSimilar(false)}
        queryTicker={code ?? null}
      />
    </div>
  );
}



