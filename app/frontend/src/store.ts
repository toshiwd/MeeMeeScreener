import { create } from "zustand";
import { api, setApiErrorReporter } from "./api";
import type { ApiErrorInfo } from "./apiErrors";

export type Ticker = {
  code: string;
  name: string;
  sector33Code?: string | null;
  sector33Name?: string | null;
  stage: string;
  score: number | null;
  reason: string;
  scoreStatus?: string | null;
  missingReasons?: string[] | null;
  scoreBreakdown?: Record<string, number> | null;
  dataStatus?: "missing" | null;
  liquidity20d?: number | null;
  atr14?: number | null;
  lastClose?: number | null;
  chg1D?: number | null;
  chg1W?: number | null;
  chg1M?: number | null;
  chg1Q?: number | null;
  chg1Y?: number | null;
  prevWeekChg?: number | null;
  prevMonthChg?: number | null;
  prevQuarterChg?: number | null;
  prevYearChg?: number | null;
  counts?: {
    up7?: number | null;
    down7?: number | null;
    up20?: number | null;
    down20?: number | null;
    up60?: number | null;
    down60?: number | null;
    up100?: number | null;
    down100?: number | null;
  };
  boxState?: "NONE" | "IN_BOX" | "JUST_BREAKOUT" | "BREAKOUT_UP" | "BREAKOUT_DOWN";
  boxEndMonth?: string | null;
  breakoutMonth?: string | null;
  boxActive?: boolean;
  hasBox?: boolean;
  // Buy Fields
  buyState?: string | null;
  buyPatternName?: string | null;
  buyPatternCode?: string | null;
  entryPriorityScore?: number | null;
  entryPriorityTier?: "A" | "B" | "C" | null;
  entryPriorityLabel?: string | null;
  entryPriorityReasons?: string[] | null;
  buyHardExcluded?: boolean | null;
  buyHardExcludeReasons?: string[] | null;
  buyStateRank?: number | null;
  buyStateScore?: number | null;
  buyCandidateScore?: number | null;
  buyEnvScore?: number | null;
  buyTimingScore?: number | null;
  buyRiskScore?: number | null;
  buyStateReason?: string | null;
  buyEligible?: boolean;
  buyOverextended?: boolean | null;
  buySignalRecencyDays?: number | null;
  buyRiskAtr?: number | null;
  buyUpsideAtr?: number | null;
  buyRiskDistance?: number | null; // legacy
  buyStateDetails?: {
    monthly?: number | null;
    weekly?: number | null;
    daily?: number | null;
  } | null;
  scores?: {
    upScore?: number | null;
    downScore?: number | null;
    overheatUp?: number | null;
    overheatDown?: number | null;
  };
  mlPUp?: number | null;
  mlPUp5?: number | null;
  mlPUp10?: number | null;
  mlPUpShort?: number | null;
  mlPDown?: number | null;
  mlPDownShort?: number | null;
  mlPTurnDown?: number | null;
  mlPTurnDown5?: number | null;
  mlPTurnDown10?: number | null;
  mlPTurnDown20?: number | null;
  mlPTurnDownShort?: number | null;
  mlEv20Net?: number | null;
  mlEv5Net?: number | null;
  mlEv10Net?: number | null;
  mlEvShortNet?: number | null;
  mlModelVersion?: string | null;
  statusLabel?: string;
  reasons?: string[];
  earlyScore?: number | null;
  lateScore?: number | null;
  bodyScore?: number | null;
  phaseN?: number | null;
  phaseReasons?: string[] | null;
  phaseDt?: number | null;
  // Short-selling fields
  shortScore?: number | null; // legacy
  shortCandidateScore?: number | null;
  aScore?: number | null; // legacy
  bScore?: number | null; // legacy
  aCandidateScore?: number | null;
  bCandidateScore?: number | null;
  shortPriorityScore?: number | null;
  shortPriorityTier?: "A" | "B" | "C" | null;
  shortPriorityLabel?: string | null;
  shortPriorityReasons?: string[] | null;
  shortHardExcluded?: boolean | null;
  shortHardExcludeReasons?: string[] | null;
  shortEligible?: boolean;
  shortEnvScore?: number | null;
  shortRiskScore?: number | null;
  shortType?: "A" | "B" | null;
  shortBadges?: string[];
  shortReasons?: string[];
  shortProhibitReason?: string | null;
  sellStop?: number | null;
  sellTarget?: number | null;
  sellRiskAtr?: number | null;
  sellDownsideAtr?: number | null;
  eventEarningsDate?: string | null;
  eventRightsDate?: string | null;
  swingScore?: number | null;
  swingQualified?: boolean | null;
  swingSide?: "long" | "short" | "none" | null;
  swingReasons?: string[] | null;
  swingLongScore?: number | null;
  swingShortScore?: number | null;
};

export type EventsMeta = {
  earningsLastSuccessAt: string | null;
  rightsLastSuccessAt: string | null;
  isRefreshing: boolean;
  refreshJobId: string | null;
  lastError: string | null;
  lastAttemptAt: string | null;
  dataCoverage?: {
    rightsMaxDate?: string | null;
  };
};

type GridTimeframe = "monthly" | "weekly" | "daily";

export type MaTimeframe = "daily" | "weekly" | "monthly";

export type MaSetting = {
  key: string;
  label: string;
  period: number;
  visible: boolean;
  color: string;
  lineWidth: number;
};

export type Box = {
  startIndex: number;
  endIndex: number;
  startTime: number;
  endTime: number;
  lower: number;
  upper: number;
  breakout: "up" | "down" | null;
};

export type BarsPayload = {
  bars: number[][];
  ma: {
    ma7: number[][];
    ma20: number[][];
    ma60: number[][];
  };
  boxes?: Box[];
};

export type MultiTimeframeBarsPayload = {
  daily?: BarsPayload;
  weekly?: BarsPayload;
  monthly?: BarsPayload;
};

export type BarsCache = {
  monthly: Record<string, BarsPayload>;
  weekly: Record<string, BarsPayload>;
  daily: Record<string, BarsPayload>;
};

export type BoxesCache = {
  monthly: Record<string, Box[]>;
  weekly: Record<string, Box[]>;
  daily: Record<string, Box[]>;
};

type MaSettings = {
  daily: MaSetting[];
  weekly: MaSetting[];
  monthly: MaSetting[];
};

type LoadingMap = {
  monthly: Record<string, boolean>;
  weekly: Record<string, boolean>;
  daily: Record<string, boolean>;
};

type StatusMap = {
  monthly: Record<string, "idle" | "loading" | "success" | "empty" | "error">;
  weekly: Record<string, "idle" | "loading" | "success" | "empty" | "error">;
  daily: Record<string, "idle" | "loading" | "success" | "empty" | "error">;
};

type Settings = {
  columns: 1 | 2 | 3 | 4;
  rows: 1 | 2 | 3 | 4 | 5 | 6;
  search: string;
  gridScrollTop: number;
  gridTimeframe: GridTimeframe;
  listTimeframe: GridTimeframe;
  listRangeBars: 60 | 120 | 240 | 360;
  listColumns: 1 | 2 | 3 | 4;
  listRows: 1 | 2 | 3 | 4 | 5 | 6;
  showBoxes: boolean;
  showIndicators: boolean;
  // Legacy sort key (for backward compatibility during migration)
  sortKey: SortKey;
  sortDir: SortDir;
  // Separated sort states (new)
  candidateSortKey: CandidateSortKey;
  basicSortKey: BasicSortKey;
  basicSortDir: SortDir;
  performancePeriod: PerformancePeriod;
  sectorSortEnabled: boolean;
  sectorSortInnerKey: BasicSortKey;
};

type StoreState = {
  tickers: Ticker[];
  favorites: string[];
  favoritesLoaded: boolean;
  favoritesLoading: boolean;
  keepList: string[];
  barsCache: BarsCache;
  boxesCache: BoxesCache;
  barsLoading: LoadingMap;
  barsStatus: StatusMap;
  loadingList: boolean;
  backendReady: boolean;
  lastApiError: ApiErrorInfo | null;
  eventsMeta: EventsMeta | null;
  eventsMetaLoading: boolean;
  maSettings: MaSettings;
  compareMaSettings: MaSettings;
  settings: Settings;
  setLastApiError: (info: ApiErrorInfo | null) => void;
  loadList: () => Promise<void>;
  loadFavorites: () => Promise<void>;
  replaceFavorites: (codes: string[]) => void;
  setFavoriteLocal: (code: string, isFavorite: boolean) => void;
  addKeep: (code: string) => void;
  removeKeep: (code: string) => void;
  clearKeep: () => void;
  replaceKeep: (codes: string[]) => void;

  setBackendReady: (ready: boolean) => void;

  setCandidateSortKey: (key: CandidateSortKey) => void;
  setBasicSortKey: (key: BasicSortKey) => void;
  setBasicSortDir: (dir: SortDir) => void;
  setPerformancePeriod: (period: PerformancePeriod) => void;
  setSectorSortEnabled: (enabled: boolean) => void;
  setSectorSortInnerKey: (key: BasicSortKey) => void;

  updateMaSetting: (
    timeframe: MaTimeframe,
    index: number,
    patch: Partial<MaSetting>
  ) => void;
  updateCompareMaSetting: (timeframe: MaTimeframe, index: number, patch: Partial<MaSetting>) => void;
  resetMaSettings: (timeframe: MaTimeframe) => void;
  resetCompareMaSettings: (timeframe: MaTimeframe) => void;
  resetBarsCache: () => void;
  loadEventsMeta: () => Promise<EventsMeta | null>;
  refreshEventsIfStale: () => Promise<void>;
  refreshEvents: () => Promise<void>;
  loadBarsBatch: (timeframe: GridTimeframe, codes: string[], limitOverride?: number, reason?: string) => Promise<void>;
  loadBoxesBatch: (codes: string[]) => Promise<void>;
  ensureBarsForVisible: (timeframe: GridTimeframe, codes: string[], reason?: string) => Promise<void>;
  setColumns: (value: 1 | 2 | 3 | 4) => void;
  setRows: (value: 1 | 2 | 3 | 4 | 5 | 6) => void;
  setListTimeframe: (value: GridTimeframe) => void;
  setListRangeBars: (value: number) => void;
  setListColumns: (value: 1 | 2 | 3 | 4) => void;
  setListRows: (value: 1 | 2 | 3 | 4 | 5 | 6) => void;
  setSearch: (value: string) => void;
  setGridScrollTop: (value: number) => void;
  setGridTimeframe: (value: GridTimeframe) => void;
  setShowBoxes: (value: boolean) => void;
  setSortKey: (value: SortKey) => void;
  setSortDir: (value: SortDir) => void;
  toggleKeep: (code: string) => void;
};

// Candidate sort presets (for buy/sell candidate screens only)
export type CandidateSortKey =
  | "entryPriority"     // 仕込み優先度
  | "buyCandidate"      // 買い候補（総合）
  | "buyInitial"        // 買い候補（初動）
  | "buyBase"           // 買い候補（底がため）
  | "swingScore"        // スイング候補（総合）
  | "shortPriority"     // 売り精度優先
  | "shortScore"        // 売り候補（総合）
  | "aScore"            // 売り候補（反転確定）
  | "bScore";           // 売り候補（戻り売り）

// Basic sort keys (for non-candidate screens)
export type BasicSortKey =
  | "code"
  | "name"
  | "sector"
  | "ma20Dev"
  | "ma60Dev"
  | "ma20Slope"
  | "ma60Slope"
  | "performance"       // Single performance key with period selector
  | "upScore"
  | "downScore"
  | "overheatUp"
  | "overheatDown"
  | "swingScore"
  | "mlEv20Net"
  | "mlPUpShort"
  | "mlPDownShort"
  | "boxState";

// Performance period for unified performance sorting
export type PerformancePeriod = "1D" | "1W" | "1M" | "1Q" | "1Y";

// Legacy combined type for backward compatibility
export type SortKey =
  | "code"
  | "name"
  | "sector"
  | "entryPriority"
  | "buyCandidate"
  | "buyInitial"
  | "buyBase"
  | "ma20Dev"
  | "ma60Dev"
  | "ma20Slope"
  | "ma60Slope"
  | "chg1D"
  | "chg1W"
  | "chg1M"
  | "chg1Q"
  | "chg1Y"
  | "prevWeekChg"
  | "prevMonthChg"
  | "prevQuarterChg"
  | "prevYearChg"
  | "upScore"
  | "downScore"
  | "overheatUp"
  | "overheatDown"
  | "swingScore"
  | "mlEv20Net"
  | "mlPUpShort"
  | "mlPDownShort"
  | "boxState"
  | "shortPriority"
  | "shortScore"
  | "aScore"
  | "bScore"
  | "performance";

export type SortDir = "asc" | "desc";

const MA_COLORS = ["#ef4444", "#22c55e", "#3b82f6", "#a855f7", "#f59e0b"];
const THUMB_BARS = 60;
const MIN_BATCH_LIMIT = 60;
const MAX_BATCH_LIMIT = 2000;
const BATCH_TTL_MS = 60_000;
const BATCH_REQUEST_TIMEOUT_MS = 30_000;
const BATCH_RETRY_DELAYS_MS = [400, 1200] as const;
const ENSURE_COALESCE_MS = 16;
const EVENTS_POLL_INTERVAL_MS = 10_000;
const EVENTS_POLL_MAX_ATTEMPTS = 180;
const KEEP_STORAGE_KEY = "keepList";
const GRID_COLS_KEY = "gridCols";
const GRID_ROWS_KEY = "gridRows";
const LIST_TIMEFRAME_KEY = "listTimeframe";
const LIST_RANGE_KEY = "listRangeBars";
const LEGACY_LIST_RANGE_KEY = "listRangeMonths";
const LIST_COLS_KEY = "listCols";
const LIST_ROWS_KEY = "listRows";
const LIST_RANGE_VALUES = [60, 120, 240, 360] as const;
const WATCHLIST_AUTO_REPAIR_TS_KEY = "watchlistAutoRepairTs";
const WATCHLIST_AUTO_REPAIR_COOLDOWN_MS = 15 * 60 * 1000;
const WATCHLIST_AUTO_REPAIR_MIN_MISSING = 30;
const WATCHLIST_AUTO_REPAIR_MIN_RATIO = 0.2;
const LEGACY_RANGE_MONTHS_TO_BARS: Record<number, Settings["listRangeBars"]> = {
  3: 60,
  6: 120,
  12: 240,
  24: 360
};
const MA_STORAGE_PREFIX = "maSettings";
const COMPARE_MA_STORAGE_PREFIX = "compareMaSettings";
const inFlightBatchRequests = new Map<
  string,
  { promise: Promise<void>; controller: AbortController }
>();
const recentBatchRequests = new Map<string, number>();
const lastEnsureKeyByTimeframe: Record<GridTimeframe, string | null> = {
  monthly: null,
  weekly: null,
  daily: null
};
const barsFetchedLimit: Record<GridTimeframe, Record<string, number>> = {
  monthly: {},
  weekly: {},
  daily: {}
};
let batchRequestCount = 0;
let v3RequestCount = 0;
let coalescedRequestCount = 0;
let dedupHitCount = 0;
let eventsPollPromise: Promise<void> | null = null;
const ensurePendingCodes: Record<GridTimeframe, Set<string>> = {
  monthly: new Set<string>(),
  weekly: new Set<string>(),
  daily: new Set<string>()
};
const ensurePendingReason: Record<GridTimeframe, string | undefined> = {
  monthly: undefined,
  weekly: undefined,
  daily: undefined
};
const ensurePendingWaiters: Record<
  GridTimeframe,
  Array<{ resolve: () => void; reject: (error: unknown) => void }>
> = {
  monthly: [],
  weekly: [],
  daily: []
};
const ensureCoalesceTimers: Record<GridTimeframe, ReturnType<typeof setTimeout> | null> = {
  monthly: null,
  weekly: null,
  daily: null
};
const DEFAULT_PERIODS: Record<MaTimeframe, number[]> = {
  daily: [7, 20, 60, 100, 200],
  weekly: [7, 20, 60, 100, 200],
  monthly: [7, 20, 60, 100, 200]
};

const makeDefaultSettings = (timeframe: MaTimeframe): MaSetting[] =>
  DEFAULT_PERIODS[timeframe].map((period, index) => ({
    key: `ma${index + 1}`,
    label: `MA${index + 1}`,
    period,
    visible: true,
    color: MA_COLORS[index] ?? "#94a3b8",
    lineWidth: 1
  }));

const buildBatchKey = (timeframe: GridTimeframe, limit: number, codes: string[]) => {
  const sorted = [...new Set(codes.filter((code) => code))].sort();
  return `${timeframe}|${limit}|${sorted.join(",")}`;
};

const isAbortError = (error: unknown) => {
  if (!error || typeof error !== "object") return false;
  const err = error as { name?: string; code?: string };
  return err.name === "CanceledError" || err.code === "ERR_CANCELED";
};

const sleepMs = (ms: number) =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });

const resolveErrorStatusCode = (error: unknown): number | null => {
  if (!error || typeof error !== "object") return null;
  const status = (error as { response?: { status?: unknown } }).response?.status;
  return typeof status === "number" && Number.isFinite(status) ? status : null;
};

const isRetriableBatchError = (error: unknown) => {
  if (isAbortError(error)) return false;
  const status = resolveErrorStatusCode(error);
  if (status === 429) return true;
  if (status !== null && status >= 500) return true;
  if (!error || typeof error !== "object") return false;
  const code = (error as { code?: string }).code;
  return (
    code === "ECONNABORTED" ||
    code === "ERR_NETWORK" ||
    code === "ETIMEDOUT" ||
    code === "ECONNRESET"
  );
};

const markFetchedLimit = (timeframe: GridTimeframe, code: string, limit: number) => {
  const current = barsFetchedLimit[timeframe][code] ?? 0;
  barsFetchedLimit[timeframe][code] = Math.max(current, limit);
};

const getFetchedLimit = (timeframe: GridTimeframe, code: string) =>
  barsFetchedLimit[timeframe][code] ?? 0;

const abortInFlightForTimeframe = (timeframe: GridTimeframe) => {
  const keysToAbort: string[] = [];
  for (const key of inFlightBatchRequests.keys()) {
    if (key.startsWith(`${timeframe}|`)) {
      keysToAbort.push(key);
    }
  }
  keysToAbort.forEach((key) => {
    const entry = inFlightBatchRequests.get(key);
    if (!entry) return;
    entry.controller.abort();
    inFlightBatchRequests.delete(key);
  });
};

const normalizeColor = (value: unknown, fallback: string) => {
  if (typeof value !== "string") return fallback;
  const trimmed = value.trim();
  return /^#[0-9a-fA-F]{6}$/.test(trimmed) ? trimmed : fallback;
};

const normalizeLineWidth = (value: unknown, fallback: number) => {
  const width = Number(value);
  if (!Number.isFinite(width)) return fallback;
  return Math.min(6, Math.max(1, Math.round(width)));
};

const parseIsoMs = (value: string | null | undefined) => {
  if (!value) return null;
  const ms = Date.parse(value);
  return Number.isNaN(ms) ? null : ms;
};

const normalizeBool = (value: unknown) => {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value === 1;
  if (typeof value === "string") {
    const trimmed = value.trim().toLowerCase();
    if (trimmed === "true") return true;
    if (trimmed === "false") return false;
    if (trimmed === "1") return true;
    if (trimmed === "0") return false;
  }
  return false;
};

const normalizeEventsMeta = (payload: unknown): EventsMeta | null => {
  if (!payload || typeof payload !== "object") return null;
  const data = payload as Record<string, unknown>;
  return {
    earningsLastSuccessAt: (data.earnings_last_success_at as string | null) ?? null,
    rightsLastSuccessAt: (data.rights_last_success_at as string | null) ?? null,
    isRefreshing: normalizeBool(data.is_refreshing),
    refreshJobId: (data.refresh_job_id as string | null) ?? null,
    lastError: (data.last_error as string | null) ?? null,
    lastAttemptAt: (data.last_attempt_at as string | null) ?? null,
    dataCoverage:
      data.data_coverage && typeof data.data_coverage === "object"
        ? {
          rightsMaxDate:
            ((data.data_coverage as Record<string, unknown>).rights_max_date as string | null) ??
            null
        }
        : undefined
  };
};

const isEventsStale = (meta: EventsMeta | null) => {
  if (!meta) return true;
  const now = Date.now();
  const earningsMs = parseIsoMs(meta.earningsLastSuccessAt);
  const rightsMs = parseIsoMs(meta.rightsLastSuccessAt);
  if (earningsMs == null || rightsMs == null) return true;
  const oldest = Math.min(earningsMs, rightsMs);
  return now - oldest >= 4 * 24 * 60 * 60 * 1000;
};

const normalizeSettings = (timeframe: MaTimeframe, input: unknown): MaSetting[] => {
  const defaults = makeDefaultSettings(timeframe);
  if (!Array.isArray(input)) return defaults;
  return defaults.map((item, index) => {
    const candidate = input[index] as Partial<MaSetting> | undefined;
    const period = Number(candidate?.period);
    return {
      ...item,
      period: Number.isFinite(period) && period > 0 ? Math.floor(period) : item.period,
      visible: typeof candidate?.visible === "boolean" ? candidate.visible : item.visible,
      color: normalizeColor(candidate?.color, item.color),
      lineWidth: normalizeLineWidth(candidate?.lineWidth, item.lineWidth)
    };
  });
};

const loadSettings = (timeframe: MaTimeframe, storagePrefix = MA_STORAGE_PREFIX): MaSetting[] => {
  if (typeof window === "undefined") return makeDefaultSettings(timeframe);
  const raw = window.localStorage.getItem(`${storagePrefix}:${timeframe}`);
  if (!raw) return makeDefaultSettings(timeframe);
  try {
    return normalizeSettings(timeframe, JSON.parse(raw));
  } catch {
    return makeDefaultSettings(timeframe);
  }
};

const persistSettings = (
  timeframe: MaTimeframe,
  settings: MaSetting[],
  storagePrefix = MA_STORAGE_PREFIX
) => {
  if (typeof window === "undefined") return;
  const payload = settings.map((item) => ({
    period: item.period,
    visible: item.visible,
    color: item.color,
    lineWidth: item.lineWidth
  }));
  window.localStorage.setItem(`${storagePrefix}:${timeframe}`, JSON.stringify(payload));
};

const getMaxPeriod = (settings: MaSetting[]) =>
  settings.reduce((max, setting) => Math.max(max, Math.max(1, setting.period)), 1);

const getRequiredBars = (settings: MaSetting[]) => {
  const desired = getMaxPeriod(settings) + THUMB_BARS - 1;
  return Math.min(MAX_BATCH_LIMIT, Math.max(MIN_BATCH_LIMIT, desired));
};

const startEventsMetaPolling = (
  get: () => StoreState,
  set: (partial: Partial<StoreState> | ((state: StoreState) => Partial<StoreState>)) => void
) => {
  if (eventsPollPromise) return eventsPollPromise;
  eventsPollPromise = (async () => {
    let sawRefreshing = false;
    let attempts = 0;
    const maxAttempts = EVENTS_POLL_MAX_ATTEMPTS;
    const intervalMs = EVENTS_POLL_INTERVAL_MS;

    while (attempts < maxAttempts) {
      const meta = await get().loadEventsMeta();

      if (meta?.refreshJobId) {
        try {
          const res = await api.get(`/events/refresh/${meta.refreshJobId}`);
          const payload = res.data as { status?: string; error?: string | null } | null;
          const status = payload?.status;
          if (status && status !== "running") {
            set((prev) => ({
              eventsMeta: {
                ...(prev.eventsMeta ?? {
                  earningsLastSuccessAt: null,
                  rightsLastSuccessAt: null,
                  lastAttemptAt: null,
                  lastError: null,
                  isRefreshing: false,
                  refreshJobId: null
                }),
                isRefreshing: false,
                lastError: payload?.error ?? prev.eventsMeta?.lastError ?? null
              }
            }));
            if (status === "success") {
              try {
                await get().loadList();
              } catch {
                // ignore list reload failures after events refresh
              }
            }
            try {
              await get().loadEventsMeta();
            } catch {
              // ignore meta reload failures after events refresh
            }
            break;
          }
        } catch {
          // ignore status fetch failures and retry on next loop
        }
      }

      if (meta?.isRefreshing) {
        sawRefreshing = true;
      } else {
        if (sawRefreshing) {
          try {
            await get().loadList();
          } catch {
            // ignore list reload failures after refresh completes
          }
        }
        break;
      }

      await new Promise((resolve) => setTimeout(resolve, intervalMs));
      attempts += 1;
    }

    if (attempts >= maxAttempts) {
      set((prev) => ({
        eventsMeta: {
          ...(prev.eventsMeta ?? {
            earningsLastSuccessAt: null,
            rightsLastSuccessAt: null,
            lastAttemptAt: null,
            lastError: null,
            isRefreshing: false,
            refreshJobId: null
          }),
          isRefreshing: false,
          lastError: "refresh_timeout"
        }
      }));
    }

    eventsPollPromise = null;
  })().catch(() => {
    eventsPollPromise = null;
  });
  return eventsPollPromise;
};

const getInitialTimeframe = (): Settings["gridTimeframe"] => {
  if (typeof window === "undefined") return "monthly";
  const saved = window.localStorage.getItem("gridTimeframe");
  return saved === "daily" || saved === "weekly" ? (saved as Settings["gridTimeframe"]) : "monthly";
};

const getInitialListTimeframe = (): Settings["listTimeframe"] => {
  if (typeof window === "undefined") return "daily";
  const saved = window.localStorage.getItem(LIST_TIMEFRAME_KEY);
  return saved === "monthly" || saved === "weekly" || saved === "daily"
    ? (saved as Settings["listTimeframe"])
    : "daily";
};

const getInitialColumns = (): Settings["columns"] => {
  if (typeof window === "undefined") return 3;
  const saved = Number(window.localStorage.getItem(GRID_COLS_KEY));
  if (saved >= 1 && saved <= 4) {
    return saved as Settings["columns"];
  }
  return 3;
};

const getInitialRows = (): Settings["rows"] => {
  if (typeof window === "undefined") return 3;
  const saved = Number(window.localStorage.getItem(GRID_ROWS_KEY));
  if (saved >= 1 && saved <= 6) {
    return saved as Settings["rows"];
  }
  return 3;
};

const getInitialListColumns = (): Settings["listColumns"] => {
  if (typeof window === "undefined") return 3;
  const saved = Number(window.localStorage.getItem(LIST_COLS_KEY));
  if (saved >= 1 && saved <= 4) {
    return saved as Settings["listColumns"];
  }
  return 3;
};

const getInitialListRows = (): Settings["listRows"] => {
  if (typeof window === "undefined") return 3;
  const saved = Number(window.localStorage.getItem(LIST_ROWS_KEY));
  if (saved >= 1 && saved <= 6) {
    return saved as Settings["listRows"];
  }
  return 3;
};

const getInitialListRangeBars = (): Settings["listRangeBars"] => {
  if (typeof window === "undefined") return 120;
  const saved = Number(window.localStorage.getItem(LIST_RANGE_KEY));
  if (LIST_RANGE_VALUES.includes(saved as Settings["listRangeBars"])) {
    return saved as Settings["listRangeBars"];
  }
  const legacy = Number(window.localStorage.getItem(LEGACY_LIST_RANGE_KEY));
  const mapped = LEGACY_RANGE_MONTHS_TO_BARS[legacy];
  if (mapped) return mapped;
  return 120;
};

const getInitialSortKey = (): SortKey => {
  if (typeof window === "undefined") return "entryPriority";
  const saved = window.localStorage.getItem("sortKey");
  const options: SortKey[] = [
    "code",
    "name",
    "entryPriority",
    "buyCandidate",
    "buyInitial",
    "buyBase",
    "ma20Dev",
    "ma60Dev",
    "ma20Slope",
    "ma60Slope",
    "chg1D",
    "chg1W",
    "chg1M",
    "chg1Q",
    "chg1Y",
    "prevWeekChg",
    "prevMonthChg",
    "prevQuarterChg",
    "prevYearChg",
    "upScore",
    "downScore",
    "overheatUp",
    "overheatDown",
    "swingScore",
    "mlEv20Net",
    "mlPUpShort",
    "mlPDownShort",
    "boxState",
    "shortPriority",
    "shortScore",
    "aScore",
    "bScore"
  ];
  return options.includes(saved as SortKey) ? (saved as SortKey) : "entryPriority";
};

const getInitialSortDir = (): SortDir => {
  if (typeof window === "undefined") return "desc";
  const saved = window.localStorage.getItem("sortDir");
  return saved === "asc" ? "asc" : "desc";
};

const loadKeepList = (): string[] => {
  if (typeof window === "undefined") return [];
  const raw = window.localStorage.getItem(KEEP_STORAGE_KEY);
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed.filter((item) => typeof item === "string" && item.trim());
  } catch {
    return [];
  }
};

const getInitialPerformancePeriod = (): PerformancePeriod => {
  if (typeof window === "undefined") return "1M";
  const saved = window.localStorage.getItem("performancePeriod");
  const options: PerformancePeriod[] = ["1D", "1W", "1M", "1Q", "1Y"];
  if (saved && options.includes(saved as PerformancePeriod)) {
    return saved as PerformancePeriod;
  }
  return "1M";
};

const persistKeepList = (list: string[]) => {
  if (typeof window === "undefined") return;
  window.localStorage.setItem(KEEP_STORAGE_KEY, JSON.stringify(list));
};

const getInitialSectorSortEnabled = (): boolean => {
  if (typeof window === "undefined") return false;
  const saved = window.localStorage.getItem("sectorSortEnabled");
  return saved === "true";
};

const getInitialSectorSortInnerKey = (): BasicSortKey => {
  if (typeof window === "undefined") return "code";
  const saved = window.localStorage.getItem("sectorSortInnerKey");
  const options: BasicSortKey[] = [
    "code",
    "name",
    "sector",
    "ma20Dev",
    "ma60Dev",
    "ma20Slope",
    "ma60Slope",
    "performance",
    "upScore",
    "downScore",
    "overheatUp",
    "overheatDown",
    "swingScore",
    "mlEv20Net",
    "mlPUpShort",
    "mlPDownShort",
    "boxState"
  ];
  if (saved && options.includes(saved as BasicSortKey)) {
    return saved as BasicSortKey;
  }
  return "code";
};

export const useStore = create<StoreState>((set, get) => ({
  tickers: [],
  favorites: [],
  favoritesLoaded: false,
  favoritesLoading: false,
  keepList: loadKeepList(),
  barsCache: { monthly: {}, weekly: {}, daily: {} },
  boxesCache: { monthly: {}, weekly: {}, daily: {} },
  barsLoading: { monthly: {}, weekly: {}, daily: {} },
  barsStatus: { monthly: {}, weekly: {}, daily: {} },
  loadingList: false,
  backendReady: false,
  lastApiError: null,
  eventsMeta: {
    earningsLastSuccessAt: null,
    rightsLastSuccessAt: null,
    lastAttemptAt: null,
    lastError: null,
    refreshJobId: null,
    isRefreshing: false
  },
  eventsMetaLoading: false,
  maSettings: {
    daily: loadSettings("daily"),
    weekly: loadSettings("weekly"),
    monthly: loadSettings("monthly")
  },
  compareMaSettings: {
    daily: loadSettings("daily", COMPARE_MA_STORAGE_PREFIX),
    weekly: loadSettings("weekly", COMPARE_MA_STORAGE_PREFIX),
    monthly: loadSettings("monthly", COMPARE_MA_STORAGE_PREFIX)
  },
  settings: {
    columns: getInitialColumns(),
    rows: getInitialRows(),
    listColumns: getInitialListColumns(),
    listRows: getInitialListRows(),
    listRangeBars: getInitialListRangeBars(),
    search: "",
    gridScrollTop: 0,
    gridTimeframe: getInitialTimeframe(),
    listTimeframe: "daily",
    showBoxes: true,
    showIndicators: false,
    sortKey: getInitialSortKey(),
    sortDir: getInitialSortDir(),
    candidateSortKey: "entryPriority",
    basicSortKey: "code",
    basicSortDir: "asc",
    performancePeriod: "1M",
    sectorSortEnabled: false,
    sectorSortInnerKey: "code"
  },
  setLastApiError: (info) => set({ lastApiError: info }),
  loadFavorites: async () => {
    if (get().favoritesLoading) return;
    set({ favoritesLoading: true });
    try {
      const res = await api.get("/favorites");
      const payload = res.data as { items?: { code?: string }[] } | { code?: string }[];
      const items = Array.isArray(payload) ? payload : payload.items ?? [];
      const codes = items
        .map((item) => (typeof item.code === "string" ? item.code : ""))
        .filter((code) => code);
      set({ favorites: codes, favoritesLoaded: true });
    } catch (error) {
      const err = error as {
        message?: string;
        response?: { status?: number; data?: unknown };
      };
      console.error("[favorites] load failed", {
        status: err?.response?.status ?? null,
        data: err?.response?.data ?? null,
        message: err?.message ?? null
      });
      set({ favorites: [], favoritesLoaded: true });
    } finally {
      set({ favoritesLoading: false });
    }
  },
  replaceFavorites: (codes) =>
    set({ favorites: [...new Set(codes.filter((code) => code))], favoritesLoaded: true }),
  setFavoriteLocal: (code, isFavorite) =>
    set((state) => {
      const normalized = code?.trim();
      if (!normalized) return state;
      const exists = state.favorites.includes(normalized);
      if (isFavorite && !exists) {
        return { favorites: [...state.favorites, normalized], favoritesLoaded: true };
      }
      if (!isFavorite && exists) {
        return {
          favorites: state.favorites.filter((item) => item !== normalized),
          favoritesLoaded: true
        };
      }
      return state;
    }),
  addKeep: (code) =>
    set((state) => {
      const normalized = code?.trim();
      if (!normalized) return state;
      if (state.keepList.includes(normalized)) return state;
      const next = [...state.keepList, normalized];
      persistKeepList(next);
      return { keepList: next };
    }),
  removeKeep: (code) =>
    set((state) => {
      const normalized = code?.trim();
      if (!normalized) return state;
      const next = state.keepList.filter((item) => item !== normalized);
      persistKeepList(next);
      return { keepList: next };
    }),
  toggleKeep: (code) =>
    set((state) => {
      const normalized = code?.trim();
      if (!normalized) return state;
      const exists = state.keepList.includes(normalized);
      const next = exists
        ? state.keepList.filter((item) => item !== normalized)
        : [...state.keepList, normalized];
      persistKeepList(next);
      return { keepList: next };
    }),
  clearKeep: () =>
    set((state) => {
      if (!state.keepList.length) return state;
      persistKeepList([]);
      return { keepList: [] };
    }),
  replaceKeep: (codes) => {
    persistKeepList(codes);
    set({ keepList: codes });
  },
  setBackendReady: (ready) => set({ backendReady: ready }),
  loadList: async () => {
    if (get().loadingList) return;
    set({ loadingList: true });
    try {
      const res = await api.get("/grid/screener");
      const payload = res.data as { items?: Ticker[] } | Ticker[];
      const items = Array.isArray(payload) ? payload : payload.items ?? [];
      if (!items.length) {
        throw new Error("Empty screener payload");
      }
      const parseReasons = (value: unknown): string[] => {
        if (Array.isArray(value)) {
          return value.filter((item) => typeof item === "string") as string[];
        }
        if (typeof value === "string" && value.trim()) {
          try {
            const parsed = JSON.parse(value);
            if (Array.isArray(parsed)) {
              return parsed.filter((item) => typeof item === "string") as string[];
            }
          } catch {
            return value.split(",").map((item) => item.trim()).filter(Boolean);
          }
        }
        return [];
      };
      const tickers: Ticker[] = items.map((rawItem) => {
        const item = rawItem as Record<string, any>;
        const statusLabel = item.statusLabel ?? null;
        const stageRaw = item.stage ?? statusLabel ?? "UNKNOWN";
        const stage =
          typeof stageRaw === "string" && stageRaw.toUpperCase() === "UNKNOWN" && statusLabel
            ? statusLabel
            : stageRaw;
        const nameRaw = typeof item.name === "string" ? item.name.trim() : "";
        return {
          code: item.code,
          name: nameRaw || item.code,
          sector33Code: item.sector33Code ?? item.sector33_code ?? null,
          sector33Name: item.sector33Name ?? item.sector33_name ?? null,
          stage,
          score: Number.isFinite(item.score) ? item.score : null,
          reason: item.reason ?? "",
          scoreStatus:
            item.scoreStatus ??
            item.score_status ??
            (Number.isFinite(item.score) ? "OK" : "INSUFFICIENT_DATA"),
          missingReasons: parseReasons(item.missingReasons ?? item.missing_reasons ?? item.missing_reasons_json),
          scoreBreakdown:
            (item.scoreBreakdown as Record<string, number> | null) ??
            (item.score_breakdown as Record<string, number> | null) ??
            null,
          lastClose: item.lastClose ?? null,
          chg1D: item.chg1D ?? null,
          chg1W: item.chg1W ?? null,
          chg1M: item.chg1M ?? null,
          chg1Q: item.chg1Q ?? null,
          chg1Y: item.chg1Y ?? null,
          prevWeekChg: item.prevWeekChg ?? null,
          prevMonthChg: item.prevMonthChg ?? null,
          prevQuarterChg: item.prevQuarterChg ?? null,
          prevYearChg: item.prevYearChg ?? null,
          counts: item.counts,
          boxState: item.boxState ?? item.box_state ?? "NONE",
          boxEndMonth: item.boxEndMonth ?? item.box_end_month ?? null,
          breakoutMonth: item.breakoutMonth ?? item.breakout_month ?? null,
          boxActive:
            typeof item.boxActive === "boolean"
              ? item.boxActive
              : typeof item.box_active === "boolean"
                ? item.box_active
                : null,
          hasBox:
            typeof item.hasBox === "boolean"
              ? item.hasBox
              : typeof item.boxActive === "boolean"
                ? item.boxActive
                : typeof item.box_active === "boolean"
                  ? item.box_active
                  : (item.boxState ?? item.box_state ?? "NONE") !== "NONE",
          buyState: item.buyState ?? item.buy_state ?? null,
          entryPriorityScore:
            typeof item.entryPriorityScore === "number"
              ? item.entryPriorityScore
              : typeof item.entry_priority_score === "number"
                ? item.entry_priority_score
                : null,
          entryPriorityTier: item.entryPriorityTier ?? item.entry_priority_tier ?? null,
          entryPriorityLabel: item.entryPriorityLabel ?? item.entry_priority_label ?? null,
          entryPriorityReasons: parseReasons(
            item.entryPriorityReasons ?? item.entry_priority_reasons
          ),
          buyHardExcluded:
            typeof item.buyHardExcluded === "boolean"
              ? item.buyHardExcluded
              : typeof item.buy_hard_excluded === "boolean"
                ? item.buy_hard_excluded
                : null,
          buyHardExcludeReasons: parseReasons(
            item.buyHardExcludeReasons ?? item.buy_hard_exclude_reasons
          ),
          buyPatternName: item.buyPatternName ?? item.buy_pattern_name ?? null,
          buyPatternCode: item.buyPatternCode ?? item.buy_pattern_code ?? null,
          buyStateRank:
            typeof item.buyStateRank === "number"
              ? item.buyStateRank
              : typeof item.buy_state_rank === "number"
                ? item.buy_state_rank
                : null,
          buyStateScore:
            typeof item.buyStateScore === "number"
              ? item.buyStateScore
              : typeof item.buy_state_score === "number"
                ? item.buy_state_score
                : null,
          buyStateReason: item.buyStateReason ?? item.buy_state_reason ?? null,
          buyOverextended:
            typeof item.buyOverextended === "boolean"
              ? item.buyOverextended
              : typeof item.buy_overextended === "boolean"
                ? item.buy_overextended
                : null,
          buyRiskDistance:
            typeof item.buyRiskDistance === "number"
              ? item.buyRiskDistance
              : typeof item.buy_risk_distance === "number"
                ? item.buy_risk_distance
                : null,
          buyStateDetails: item.buyStateDetails ?? null,
          scores: item.scores,
          mlPUp: Number.isFinite(item.mlPUp) ? item.mlPUp : Number.isFinite(item.ml_p_up) ? item.ml_p_up : null,
          mlPUp5:
            Number.isFinite(item.mlPUp5)
              ? item.mlPUp5
              : Number.isFinite(item.ml_p_up_5)
                ? item.ml_p_up_5
                : null,
          mlPUp10:
            Number.isFinite(item.mlPUp10)
              ? item.mlPUp10
              : Number.isFinite(item.ml_p_up_10)
                ? item.ml_p_up_10
                : null,
          mlPUpShort:
            Number.isFinite(item.mlPUpShort)
              ? item.mlPUpShort
              : Number.isFinite(item.ml_p_up_short)
                ? item.ml_p_up_short
                : null,
          mlPDown: Number.isFinite(item.mlPDown) ? item.mlPDown : Number.isFinite(item.ml_p_down) ? item.ml_p_down : null,
          mlPDownShort:
            Number.isFinite(item.mlPDownShort)
              ? item.mlPDownShort
              : Number.isFinite(item.ml_p_down_short)
                ? item.ml_p_down_short
                : null,
          mlPTurnDown:
            Number.isFinite(item.mlPTurnDown)
              ? item.mlPTurnDown
              : Number.isFinite(item.ml_p_turn_down)
                ? item.ml_p_turn_down
                : null,
          mlPTurnDown5:
            Number.isFinite(item.mlPTurnDown5)
              ? item.mlPTurnDown5
              : Number.isFinite(item.ml_p_turn_down_5)
                ? item.ml_p_turn_down_5
                : null,
          mlPTurnDown10:
            Number.isFinite(item.mlPTurnDown10)
              ? item.mlPTurnDown10
              : Number.isFinite(item.ml_p_turn_down_10)
                ? item.ml_p_turn_down_10
                : null,
          mlPTurnDown20:
            Number.isFinite(item.mlPTurnDown20)
              ? item.mlPTurnDown20
              : Number.isFinite(item.ml_p_turn_down_20)
                ? item.ml_p_turn_down_20
                : null,
          mlPTurnDownShort:
            Number.isFinite(item.mlPTurnDownShort)
              ? item.mlPTurnDownShort
              : Number.isFinite(item.ml_p_turn_down_short)
                ? item.ml_p_turn_down_short
                : null,
          mlEv20Net:
            Number.isFinite(item.mlEv20Net)
              ? item.mlEv20Net
              : Number.isFinite(item.ml_ev20_net)
                ? item.ml_ev20_net
                : null,
          mlEv5Net:
            Number.isFinite(item.mlEv5Net)
              ? item.mlEv5Net
              : Number.isFinite(item.ml_ev5_net)
                ? item.ml_ev5_net
                : null,
          mlEv10Net:
            Number.isFinite(item.mlEv10Net)
              ? item.mlEv10Net
              : Number.isFinite(item.ml_ev10_net)
                ? item.ml_ev10_net
                : null,
          mlEvShortNet:
            Number.isFinite(item.mlEvShortNet)
              ? item.mlEvShortNet
              : Number.isFinite(item.ml_ev_short_net)
                ? item.ml_ev_short_net
                : null,
          mlModelVersion:
            typeof item.mlModelVersion === "string"
              ? item.mlModelVersion
              : typeof item.ml_model_version === "string"
                ? item.ml_model_version
                : null,
          statusLabel: item.statusLabel,
          reasons: item.reasons,
          earlyScore: Number.isFinite(item.earlyScore) ? item.earlyScore : item.early_score ?? null,
          lateScore: Number.isFinite(item.lateScore) ? item.lateScore : item.late_score ?? null,
          bodyScore: Number.isFinite(item.bodyScore) ? item.bodyScore : item.body_score ?? null,
          phaseN:
            typeof item.phaseN === "number"
              ? item.phaseN
              : typeof item.phase_n === "number"
                ? item.phase_n
                : typeof item.n === "number"
                  ? item.n
                  : null,
          phaseReasons: parseReasons(
            item.phaseReasons ?? item.phase_reasons ?? item.reasons_top3 ?? item.reasonsTop3
          ),
          phaseDt:
            typeof item.phaseDt === "number"
              ? item.phaseDt
              : typeof item.phase_dt === "number"
                ? item.phase_dt
                : null,
          // Short-selling fields
          shortScore:
            typeof item.shortScore === "number"
              ? item.shortScore
              : typeof item.short_score === "number"
                ? item.short_score
                : null,
          shortCandidateScore:
            typeof item.shortCandidateScore === "number"
              ? item.shortCandidateScore
              : typeof item.short_candidate_score === "number"
                ? item.short_candidate_score
                : null,
          aScore:
            typeof item.aScore === "number"
              ? item.aScore
              : typeof item.a_score === "number"
                ? item.a_score
                : null,
          bScore:
            typeof item.bScore === "number"
              ? item.bScore
              : typeof item.b_score === "number"
                ? item.b_score
                : null,
          aCandidateScore:
            typeof item.aCandidateScore === "number"
              ? item.aCandidateScore
              : typeof item.a_candidate_score === "number"
                ? item.a_candidate_score
                : null,
          bCandidateScore:
            typeof item.bCandidateScore === "number"
              ? item.bCandidateScore
              : typeof item.b_candidate_score === "number"
                ? item.b_candidate_score
                : null,
          shortPriorityScore:
            typeof item.shortPriorityScore === "number"
              ? item.shortPriorityScore
              : typeof item.short_priority_score === "number"
                ? item.short_priority_score
                : null,
          shortPriorityTier: item.shortPriorityTier ?? item.short_priority_tier ?? null,
          shortPriorityLabel: item.shortPriorityLabel ?? item.short_priority_label ?? null,
          shortPriorityReasons: parseReasons(
            item.shortPriorityReasons ?? item.short_priority_reasons
          ),
          shortHardExcluded:
            typeof item.shortHardExcluded === "boolean"
              ? item.shortHardExcluded
              : typeof item.short_hard_excluded === "boolean"
                ? item.short_hard_excluded
                : null,
          shortHardExcludeReasons: parseReasons(
            item.shortHardExcludeReasons ?? item.short_hard_exclude_reasons
          ),
          shortEligible:
            typeof item.shortEligible === "boolean"
              ? item.shortEligible
              : typeof item.short_eligible === "boolean"
                ? item.short_eligible
                : null,
          shortEnvScore:
            typeof item.shortEnvScore === "number"
              ? item.shortEnvScore
              : typeof item.short_env_score === "number"
                ? item.short_env_score
                : null,
          shortRiskScore:
            typeof item.shortRiskScore === "number"
              ? item.shortRiskScore
              : typeof item.short_risk_score === "number"
                ? item.short_risk_score
                : null,
          shortType: item.shortType ?? null,
          shortBadges: Array.isArray(item.shortBadges) ? item.shortBadges : [],
          shortReasons: Array.isArray(item.shortReasons) ? item.shortReasons : [],
          shortProhibitReason: item.shortProhibitReason ?? item.short_prohibit_reason ?? null,
          sellStop:
            typeof item.sellStop === "number"
              ? item.sellStop
              : typeof item.sell_stop === "number"
                ? item.sell_stop
                : null,
          sellTarget:
            typeof item.sellTarget === "number"
              ? item.sellTarget
              : typeof item.sell_target === "number"
                ? item.sell_target
                : null,
          sellRiskAtr:
            typeof item.sellRiskAtr === "number"
              ? item.sellRiskAtr
              : typeof item.sell_risk_atr === "number"
                ? item.sell_risk_atr
                : null,
          sellDownsideAtr:
            typeof item.sellDownsideAtr === "number"
              ? item.sellDownsideAtr
              : typeof item.sell_downside_atr === "number"
                ? item.sell_downside_atr
                : null,
          eventEarningsDate: item.eventEarningsDate ?? item.event_earnings_date ?? null,
          eventRightsDate: item.eventRightsDate ?? item.event_rights_date ?? null,
          swingScore:
            typeof item.swingScore === "number"
              ? item.swingScore
              : typeof item.swing_score === "number"
                ? item.swing_score
                : null,
          swingQualified:
            typeof item.swingQualified === "boolean"
              ? item.swingQualified
              : typeof item.swing_qualified === "boolean"
                ? item.swing_qualified
                : null,
          swingSide: (() => {
            const raw = item.swingSide ?? item.swing_side;
            return raw === "long" || raw === "short" || raw === "none" ? raw : null;
          })(),
          swingReasons: parseReasons(item.swingReasons ?? item.swing_reasons),
          swingLongScore:
            typeof item.swingLongScore === "number"
              ? item.swingLongScore
              : typeof item.swing_long_score === "number"
                ? item.swing_long_score
                : null,
          swingShortScore:
            typeof item.swingShortScore === "number"
              ? item.swingShortScore
              : typeof item.swing_short_score === "number"
                ? item.swing_short_score
                : null,
        };
      });
      try {
        const resWatch = await api.get("/watchlist");
        const watchlistCodes = (resWatch.data?.codes || []) as string[];
        if (watchlistCodes.length) {
          const existing = new Set(tickers.map((item) => item.code));
          const missingWatchlistCodes = watchlistCodes.filter((code) => !existing.has(code));
          const missingRatio = missingWatchlistCodes.length / Math.max(1, watchlistCodes.length);
          const shouldAutoRepair =
            missingWatchlistCodes.length >= WATCHLIST_AUTO_REPAIR_MIN_MISSING &&
            missingRatio >= WATCHLIST_AUTO_REPAIR_MIN_RATIO;

          if (shouldAutoRepair && typeof window !== "undefined") {
            const now = Date.now();
            const lastAutoRepairTs = Number(window.localStorage.getItem(WATCHLIST_AUTO_REPAIR_TS_KEY) || "0");
            if (!Number.isFinite(lastAutoRepairTs) || now - lastAutoRepairTs >= WATCHLIST_AUTO_REPAIR_COOLDOWN_MS) {
              window.localStorage.setItem(WATCHLIST_AUTO_REPAIR_TS_KEY, String(now));
              // Auto-repair missing watchlist coverage in the background.
              void api.post("/jobs/force-sync").catch(() => undefined);
            }
          }

          const watchlistOnlyCodes = shouldAutoRepair ? [] : missingWatchlistCodes;
          watchlistOnlyCodes.forEach((code) => {
            if (existing.has(code)) return;
            tickers.push({
              code,
              name: code,
              stage: "",
              score: null,
              reason: "WATCHLIST_ONLY",
              scoreStatus: "INSUFFICIENT_DATA",
              missingReasons: [],
              scoreBreakdown: null,
              dataStatus: "missing"
            } as Ticker);
          });
        }
      } catch {
        // ignore watchlist failures for now
      }
      set({ tickers });
    } catch {
      const res = await api.get("/list");
      const items = (res.data || []) as [string, string, string, number | null, string][];
      const tickers = items.map(([code, name, stage, score, reason]) => ({
        code,
        name,
        stage,
        score: Number.isFinite(score) ? score : null,
        reason,
        scoreStatus: Number.isFinite(score) ? "OK" : "INSUFFICIENT_DATA",
        missingReasons: null,
        scoreBreakdown: null
      }));
      set({ tickers });
    } finally {
      set({ loadingList: false });
    }
  },
  loadBarsBatch: async (timeframe, codes, limitOverride, reason) => {
    const state = get();
    const loadingMap = state.barsLoading[timeframe];
    const uniqueCodes = [...new Set(codes.filter((code) => code))];
    const trimmed = uniqueCodes.filter((code) => !loadingMap[code]);
    if (!trimmed.length) return;

    if (timeframe === "weekly") {
      const weeklyRequired = Math.max(
        limitOverride ?? 0,
        getRequiredBars(get().maSettings.weekly),
        get().settings.listRangeBars
      );
      const requestCodes = [...new Set(trimmed)].sort();
      const requestKey = buildBatchKey(timeframe, weeklyRequired, requestCodes);
      const cachedAt = recentBatchRequests.get(requestKey);
      if (cachedAt && Date.now() - cachedAt < BATCH_TTL_MS) return;

      const inFlight = inFlightBatchRequests.get(requestKey);
      if (inFlight) {
        dedupHitCount += 1;
        return inFlight.promise;
      }

      batchRequestCount += 1;
      v3RequestCount += 1;
      console.debug("[batch_bars_v3]", {
        count: batchRequestCount,
        v3_request_count: v3RequestCount,
        coalesced_request_count: coalescedRequestCount,
        dedup_hit_count: dedupHitCount,
        key: requestKey,
        reason: reason ?? "unknown",
        timeframe,
        limit: weeklyRequired,
        codes: requestCodes.length
      });

      const controller = new AbortController();
      const requestPromise = (async () => {
        set((prev) => {
          const nextLoading = { ...prev.barsLoading.weekly };
          requestCodes.forEach((code) => {
            nextLoading[code] = true;
          });
          return {
            barsLoading: { ...prev.barsLoading, weekly: nextLoading },
            barsStatus: {
              ...prev.barsStatus,
              weekly: {
                ...prev.barsStatus.weekly,
                ...requestCodes.reduce((acc, code) => {
                  acc[code] = "loading";
                  return acc;
                }, {} as Record<string, "idle" | "loading" | "success" | "empty" | "error">)
              }
            }
          };
        });

        try {
          const requestPayload = {
            codes: requestCodes,
            timeframes: ["weekly"],
            limit: weeklyRequired,
            includeProvisional: true
          };
          let res: { status: number; data?: any } | null = null;
          let attempt = 0;
          while (true) {
            try {
              res = await api.post("/batch_bars_v3", requestPayload, {
                signal: controller.signal,
                timeout: BATCH_REQUEST_TIMEOUT_MS
              });
              break;
            } catch (error) {
              const canRetry =
                attempt < BATCH_RETRY_DELAYS_MS.length && isRetriableBatchError(error);
              if (!canRetry) throw error;
              const retryDelay =
                BATCH_RETRY_DELAYS_MS[attempt] ??
                BATCH_RETRY_DELAYS_MS[BATCH_RETRY_DELAYS_MS.length - 1] ??
                0;
              attempt += 1;
              await sleepMs(retryDelay);
            }
          }
          if (!res) {
            throw new Error("batch_bars_v3 failed without response");
          }
          if (res.status !== 200) {
            throw new Error(`batch_bars_v3 failed with status ${res.status}`);
          }

          const rawItems = (res.data?.items || {}) as Record<
            string,
            MultiTimeframeBarsPayload | undefined
          >;
          const weeklyItems: Record<string, BarsPayload> = {};
          const weeklyBoxes: Record<string, Box[]> = {};
          requestCodes.forEach((code) => {
            const payload = rawItems[code]?.weekly;
            if (payload && Array.isArray(payload.bars)) {
              weeklyItems[code] = payload;
              weeklyBoxes[code] = payload.boxes ?? [];
            } else {
              weeklyItems[code] = {
                bars: [],
                ma: { ma7: [], ma20: [], ma60: [] },
                boxes: []
              };
              weeklyBoxes[code] = [];
            }
            markFetchedLimit("weekly", code, weeklyRequired);
          });
          recentBatchRequests.set(requestKey, Date.now());
          set((prev) => ({
            barsCache: {
              ...prev.barsCache,
              weekly: { ...prev.barsCache.weekly, ...weeklyItems }
            },
            boxesCache: {
              ...prev.boxesCache,
              weekly: { ...prev.boxesCache.weekly, ...weeklyBoxes }
            },
            barsStatus: {
              ...prev.barsStatus,
              weekly: {
                ...prev.barsStatus.weekly,
                ...requestCodes.reduce((acc, code) => {
                  const payload = weeklyItems[code];
                  acc[code] = payload && payload.bars.length ? "success" : "empty";
                  return acc;
                }, {} as Record<string, "idle" | "loading" | "success" | "empty" | "error">)
              }
            }
          }));
        } catch (error) {
          if (isAbortError(error)) return;
          set((prev) => ({
            barsStatus: {
              ...prev.barsStatus,
              weekly: {
                ...prev.barsStatus.weekly,
                ...requestCodes.reduce((acc, code) => {
                  const cached = prev.barsCache.weekly[code];
                  acc[code] = cached ? (cached.bars.length ? "success" : "empty") : "error";
                  return acc;
                }, {} as Record<string, "idle" | "loading" | "success" | "empty" | "error">)
              }
            }
          }));
          throw error;
        } finally {
          set((prev) => {
            const cleared = { ...prev.barsLoading.weekly };
            requestCodes.forEach((code) => {
              delete cleared[code];
            });
            return { barsLoading: { ...prev.barsLoading, weekly: cleared } };
          });
        }
      })();

      inFlightBatchRequests.set(requestKey, { promise: requestPromise, controller });
      requestPromise.finally(() => {
        const entry = inFlightBatchRequests.get(requestKey);
        if (entry?.controller === controller) {
          inFlightBatchRequests.delete(requestKey);
        }
      });
      return requestPromise;
    }

    const maSettings =
      timeframe === "daily" ? get().maSettings.daily : get().maSettings.monthly;
    const limit = Math.max(limitOverride ?? 0, getRequiredBars(maSettings));
    const requestCodes = [...new Set(trimmed)].sort();
    const requestKey = buildBatchKey(timeframe, limit, requestCodes);
    const cachedAt = recentBatchRequests.get(requestKey);
    if (cachedAt && Date.now() - cachedAt < BATCH_TTL_MS) return;

    const inFlight = inFlightBatchRequests.get(requestKey);
    if (inFlight) {
      dedupHitCount += 1;
      return inFlight.promise;
    }

    batchRequestCount += 1;
    v3RequestCount += 1;
    console.debug("[batch_bars_v3]", {
      count: batchRequestCount,
      v3_request_count: v3RequestCount,
      coalesced_request_count: coalescedRequestCount,
      dedup_hit_count: dedupHitCount,
      key: requestKey,
      reason: reason ?? "unknown",
      timeframe,
      limit,
      codes: requestCodes.length
    });

    const controller = new AbortController();
    const requestPromise = (async () => {
      set((prev) => {
        const nextLoading = { ...prev.barsLoading[timeframe] };
        requestCodes.forEach((code) => {
          nextLoading[code] = true;
        });
        return {
          barsLoading: { ...prev.barsLoading, [timeframe]: nextLoading },
          barsStatus: {
            ...prev.barsStatus,
            [timeframe]: {
              ...prev.barsStatus[timeframe],
              ...requestCodes.reduce((acc, code) => {
                acc[code] = "loading";
                return acc;
              }, {} as Record<string, "idle" | "loading" | "success" | "empty" | "error">)
            }
          }
        };
      });

      try {
        const requestPayload = {
          timeframes: [timeframe],
          codes: requestCodes,
          limit,
          includeProvisional: true
        };
        let res: { status: number; data?: any } | null = null;
        let attempt = 0;
        while (true) {
          try {
            res = await api.post("/batch_bars_v3", requestPayload, {
              signal: controller.signal,
              timeout: BATCH_REQUEST_TIMEOUT_MS
            });
            break;
          } catch (error) {
            const canRetry =
              attempt < BATCH_RETRY_DELAYS_MS.length && isRetriableBatchError(error);
            if (!canRetry) throw error;
            const retryDelay =
              BATCH_RETRY_DELAYS_MS[attempt] ??
              BATCH_RETRY_DELAYS_MS[BATCH_RETRY_DELAYS_MS.length - 1] ??
              0;
            attempt += 1;
            await sleepMs(retryDelay);
          }
        }
        if (!res) {
          throw new Error("batch_bars_v3 failed without response");
        }
        if (res.status !== 200) {
          throw new Error(`batch_bars_v3 failed with status ${res.status}`);
        }
        const rawItems = (res.data?.items || {}) as Record<string, MultiTimeframeBarsPayload | undefined>;
        const items: Record<string, BarsPayload> = {};
        const boxesMonthly: Record<string, Box[]> = {};
        const boxesDaily: Record<string, Box[]> = {};
        requestCodes.forEach((code) => {
          const framePayload = rawItems[code]?.[timeframe];
          const payload: BarsPayload =
            framePayload && Array.isArray(framePayload.bars)
              ? framePayload
              : {
                  bars: [],
                  ma: { ma7: [], ma20: [], ma60: [] },
                  boxes: []
                };
          items[code] = payload;
          const boxes = payload.boxes ?? [];
          if (timeframe === "monthly") {
            boxesMonthly[code] = boxes;
            boxesDaily[code] = boxes;
          } else if (timeframe === "daily") {
            boxesDaily[code] = boxes;
          }
        });
        requestCodes.forEach((code) => markFetchedLimit(timeframe, code, limit));
        recentBatchRequests.set(requestKey, Date.now());
        set((prev) => ({
          barsCache: {
            ...prev.barsCache,
            [timeframe]: { ...prev.barsCache[timeframe], ...items }
          },
          boxesCache: {
            monthly: { ...prev.boxesCache.monthly, ...boxesMonthly },
            weekly: prev.boxesCache.weekly,
            daily: { ...prev.boxesCache.daily, ...boxesDaily }
          },
          barsStatus: {
            ...prev.barsStatus,
            [timeframe]: {
              ...prev.barsStatus[timeframe],
              ...requestCodes.reduce((acc, code) => {
                const payload = items[code];
                acc[code] = payload && payload.bars.length ? "success" : "empty";
                return acc;
              }, {} as Record<string, "idle" | "loading" | "success" | "empty" | "error">)
            }
          }
        }));
      } catch (error) {
        if (isAbortError(error)) return;
        set((prev) => ({
          barsStatus: {
            ...prev.barsStatus,
            [timeframe]: {
              ...prev.barsStatus[timeframe],
              ...requestCodes.reduce((acc, code) => {
                const cached = prev.barsCache[timeframe][code];
                acc[code] = cached ? (cached.bars.length ? "success" : "empty") : "error";
                return acc;
              }, {} as Record<string, "idle" | "loading" | "success" | "empty" | "error">)
            }
          }
        }));
        throw error;
      } finally {
        set((prev) => {
          const cleared = { ...prev.barsLoading[timeframe] };
          requestCodes.forEach((code) => {
            delete cleared[code];
          });
          return { barsLoading: { ...prev.barsLoading, [timeframe]: cleared } };
        });
      }
    })();

    inFlightBatchRequests.set(requestKey, { promise: requestPromise, controller });
    requestPromise.finally(() => {
      const entry = inFlightBatchRequests.get(requestKey);
      if (entry?.controller === controller) {
        inFlightBatchRequests.delete(requestKey);
      }
    });
    return requestPromise;
  },
  loadBoxesBatch: async (codes) => {
    if (!codes.length) return;
    await get().loadBarsBatch("monthly", codes, undefined, "boxes");
  },
  ensureBarsForVisible: async (timeframe, codes, reason) => {
    const uniqueCodes = [...new Set(codes.filter((code) => code))];
    if (!uniqueCodes.length) return;
    const pending = ensurePendingCodes[timeframe];
    uniqueCodes.forEach((code) => pending.add(code));
    if (reason) {
      ensurePendingReason[timeframe] = reason;
    }

    return new Promise<void>((resolve, reject) => {
      ensurePendingWaiters[timeframe].push({ resolve, reject });
      if (ensureCoalesceTimers[timeframe] !== null) {
        coalescedRequestCount += 1;
        return;
      }
      ensureCoalesceTimers[timeframe] = setTimeout(async () => {
        ensureCoalesceTimers[timeframe] = null;
        const mergedCodes = [...ensurePendingCodes[timeframe]];
        ensurePendingCodes[timeframe].clear();
        const mergedReason = ensurePendingReason[timeframe];
        ensurePendingReason[timeframe] = undefined;
        const waiters = ensurePendingWaiters[timeframe].splice(0);

        try {
          const state = get();
          const cache = state.barsCache[timeframe];
          const maSettings = state.maSettings;
          const requiredBars =
            timeframe === "daily"
              ? getRequiredBars(maSettings.daily)
              : timeframe === "weekly"
                ? getRequiredBars(maSettings.weekly)
                : getRequiredBars(maSettings.monthly);
          const requiredWithRange = Math.max(requiredBars, state.settings.listRangeBars);
          const listKey = buildBatchKey(timeframe, requiredWithRange, mergedCodes);
          if (lastEnsureKeyByTimeframe[timeframe] !== listKey) {
            abortInFlightForTimeframe(timeframe);
            lastEnsureKeyByTimeframe[timeframe] = listKey;
          }
          const missing = mergedCodes.filter((code) => {
            const payload = cache[code];
            const fetchedLimit = getFetchedLimit(timeframe, code);
            if (!payload) return fetchedLimit < requiredWithRange;
            if (payload.bars.length >= requiredWithRange) return false;
            if (fetchedLimit >= requiredWithRange) return false;
            return true;
          });
          if (!missing.length) {
            waiters.forEach((w) => w.resolve());
            return;
          }

          const batchSize = 48;
          for (let i = 0; i < missing.length; i += batchSize) {
            const batch = missing.slice(i, i + batchSize);
            await get().loadBarsBatch(
              timeframe,
              batch,
              requiredWithRange,
              mergedReason
            );
          }
          waiters.forEach((w) => w.resolve());
        } catch (error) {
          waiters.forEach((w) => w.reject(error));
        }
      }, ENSURE_COALESCE_MS);
    });
  },
  setColumns: (columns) => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem(GRID_COLS_KEY, String(columns));
    }
    set((state) => ({ settings: { ...state.settings, columns } }));
  },
  setRows: (rows) => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem(GRID_ROWS_KEY, String(rows));
    }
    set((state) => ({ settings: { ...state.settings, rows } }));
  },
  setListTimeframe: (value) => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem(LIST_TIMEFRAME_KEY, value);
    }
    set((state) => ({ settings: { ...state.settings, listTimeframe: value } }));
  },
  setListRangeBars: (value) => {
    const normalized = LIST_RANGE_VALUES.includes(value as Settings["listRangeBars"])
      ? (value as Settings["listRangeBars"])
      : 120;
    if (typeof window !== "undefined") {
      window.localStorage.setItem(LIST_RANGE_KEY, String(normalized));
    }
    set((state) => ({ settings: { ...state.settings, listRangeBars: normalized } }));
  },
  setListColumns: (value) => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem(LIST_COLS_KEY, String(value));
    }
    set((state) => ({ settings: { ...state.settings, listColumns: value } }));
  },
  setListRows: (value) => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem(LIST_ROWS_KEY, String(value));
    }
    set((state) => ({ settings: { ...state.settings, listRows: value } }));
  },
  setSearch: (search) => {
    set((state) => ({ settings: { ...state.settings, search } }));
  },
  setGridScrollTop: (value) => {
    set((state) => ({ settings: { ...state.settings, gridScrollTop: value } }));
  },
  setGridTimeframe: (value) => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem("gridTimeframe", value);
    }
    set((state) => ({ settings: { ...state.settings, gridTimeframe: value } }));
  },
  setShowBoxes: (value) => {
    set((state) => ({ settings: { ...state.settings, showBoxes: value } }));
  },
  setSortKey: (value) => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem("sortKey", value);
    }
    set((state) => ({ settings: { ...state.settings, sortKey: value } }));
  },
  setSortDir: (value) => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem("sortDir", value);
    }
    set((state) => ({ settings: { ...state.settings, sortDir: value } }));
  },
  // New separated sort setters
  setCandidateSortKey: (value) => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem("candidateSortKey", value);
    }
    set((state) => ({ settings: { ...state.settings, candidateSortKey: value } }));
  },
  setBasicSortKey: (value) => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem("basicSortKey", value);
    }
    set((state) => ({ settings: { ...state.settings, basicSortKey: value } }));
  },
  setBasicSortDir: (value) => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem("basicSortDir", value);
    }
    set((state) => ({ settings: { ...state.settings, basicSortDir: value } }));
  },
  setPerformancePeriod: (value) => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem("performancePeriod", value);
    }
    set((state) => ({ settings: { ...state.settings, performancePeriod: value } }));
  },
  setSectorSortEnabled: (value) => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem("sectorSortEnabled", String(value));
    }
    set((state) => ({ settings: { ...state.settings, sectorSortEnabled: value } }));
  },
  setSectorSortInnerKey: (value) => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem("sectorSortInnerKey", value);
    }
    set((state) => ({ settings: { ...state.settings, sectorSortInnerKey: value } }));
  },
  updateMaSetting: (timeframe, index, patch) => {
    set((state) => {
      const current = state.maSettings[timeframe][index];
      if (!current) return state;
      const next = [...state.maSettings[timeframe]];
      const updated: MaSetting = {
        ...current,
        ...patch,
        period:
          Number.isFinite(Number(patch.period)) && Number(patch.period) > 0
            ? Math.floor(Number(patch.period))
            : current.period,
        color: normalizeColor(patch.color ?? current.color, current.color),
        lineWidth: normalizeLineWidth(patch.lineWidth ?? current.lineWidth, current.lineWidth),
        visible: typeof patch.visible === "boolean" ? patch.visible : current.visible
      };
      next[index] = updated;
      persistSettings(timeframe, next);
      return { maSettings: { ...state.maSettings, [timeframe]: next } };
    });
  },
  updateCompareMaSetting: (timeframe, index, patch) => {
    set((state) => {
      const current = state.compareMaSettings[timeframe][index];
      if (!current) return state;
      const next = [...state.compareMaSettings[timeframe]];
      const updated: MaSetting = {
        ...current,
        ...patch,
        period:
          Number.isFinite(Number(patch.period)) && Number(patch.period) > 0
            ? Math.floor(Number(patch.period))
            : current.period,
        color: normalizeColor(patch.color ?? current.color, current.color),
        lineWidth: normalizeLineWidth(patch.lineWidth ?? current.lineWidth, current.lineWidth),
        visible: typeof patch.visible === "boolean" ? patch.visible : current.visible
      };
      next[index] = updated;
      persistSettings(timeframe, next, COMPARE_MA_STORAGE_PREFIX);
      return { compareMaSettings: { ...state.compareMaSettings, [timeframe]: next } };
    });
  },
  resetMaSettings: (timeframe) => {
    set((state) => {
      const next = makeDefaultSettings(timeframe);
      persistSettings(timeframe, next);
      return { maSettings: { ...state.maSettings, [timeframe]: next } };
    });
  },
  resetCompareMaSettings: (timeframe) => {
    set((state) => {
      const next = makeDefaultSettings(timeframe);
      persistSettings(timeframe, next, COMPARE_MA_STORAGE_PREFIX);
      return { compareMaSettings: { ...state.compareMaSettings, [timeframe]: next } };
    });
  },
  resetBarsCache: () => {
    abortInFlightForTimeframe("daily");
    abortInFlightForTimeframe("weekly");
    abortInFlightForTimeframe("monthly");
    recentBatchRequests.clear();
    barsFetchedLimit.daily = {};
    barsFetchedLimit.weekly = {};
    barsFetchedLimit.monthly = {};
    lastEnsureKeyByTimeframe.daily = null;
    lastEnsureKeyByTimeframe.weekly = null;
    lastEnsureKeyByTimeframe.monthly = null;
    set(() => ({
      barsCache: { monthly: {}, weekly: {}, daily: {} },
      boxesCache: { monthly: {}, weekly: {}, daily: {} },
      barsStatus: { monthly: {}, weekly: {}, daily: {} },
      barsLoading: { monthly: {}, weekly: {}, daily: {} }
    }));
  },
  loadEventsMeta: async () => {
    if (get().eventsMetaLoading) return get().eventsMeta;
    set({ eventsMetaLoading: true });
    try {
      const res = await api.get("/events/meta");
      const meta = normalizeEventsMeta(res.data);
      if (meta) {
        set({ eventsMeta: meta });
      }
      return meta;
    } catch {
      return get().eventsMeta;
    } finally {
      set({ eventsMetaLoading: false });
    }
  },
  refreshEventsIfStale: async () => {
    const meta = await get().loadEventsMeta();
    if (!isEventsStale(meta)) return;
    if (meta?.isRefreshing) return;
    try {
      const res = await api.post("/events/refresh", null, {
        params: { reason: "startup_stale" }
      });
      const jobId =
        (res.data as { jobId?: string; refresh_job_id?: string } | null)?.jobId ??
        (res.data as { refresh_job_id?: string } | null)?.refresh_job_id ??
        null;
      if (jobId) {
        set((prev) => ({
          eventsMeta: {
            ...(prev.eventsMeta ?? {
              earningsLastSuccessAt: null,
              rightsLastSuccessAt: null,
              lastAttemptAt: null,
              lastError: null,
              isRefreshing: false,
              refreshJobId: null
            }),
            isRefreshing: true,
            refreshJobId: jobId
          }
        }));
        void startEventsMetaPolling(get, set);
      } else {
        set((prev) => ({
          eventsMeta: {
            ...(prev.eventsMeta ?? {
              earningsLastSuccessAt: null,
              rightsLastSuccessAt: null,
              lastAttemptAt: null,
              lastError: null,
              isRefreshing: false,
              refreshJobId: null
            }),
            isRefreshing: false,
            lastError: "refresh_job_missing",
            refreshJobId: null
          }
        }));
      }
    } catch {
      // ignore refresh failures
    }
  },
  refreshEvents: async () => {
    if (get().eventsMeta?.isRefreshing) return;
    try {
      const res = await api.post("/events/refresh", null, {
        params: { reason: "manual" }
      });
      const jobId =
        (res.data as { jobId?: string; refresh_job_id?: string } | null)?.jobId ??
        (res.data as { refresh_job_id?: string } | null)?.refresh_job_id ??
        null;
      if (!jobId) {
        set((prev) => ({
          eventsMeta: {
            ...(prev.eventsMeta ?? {
              earningsLastSuccessAt: null,
              rightsLastSuccessAt: null,
              lastAttemptAt: null,
              lastError: null,
              isRefreshing: false,
              refreshJobId: null
            }),
            isRefreshing: false,
            lastError: "refresh_job_missing",
            refreshJobId: null
          }
        }));
        return;
      }
      set((prev) => ({
        eventsMeta: {
          ...(prev.eventsMeta ?? {
            earningsLastSuccessAt: null,
            rightsLastSuccessAt: null,
            lastAttemptAt: null,
            lastError: null,
            isRefreshing: false,
            refreshJobId: null
          }),
          isRefreshing: true,
          refreshJobId: jobId
        }
      }));
      void startEventsMetaPolling(get, set);
    } catch {
      set((prev) => ({
        eventsMeta: {
          ...(prev.eventsMeta ?? {
            earningsLastSuccessAt: null,
            rightsLastSuccessAt: null,
            lastAttemptAt: null,
            lastError: null,
            isRefreshing: false,
            refreshJobId: null
          }),
          isRefreshing: false,
          lastError: "refresh_failed"
        }
      }));
    } finally {
      void get().loadEventsMeta();
    }
  },

}));

setApiErrorReporter((info) => {
  useStore.getState().setLastApiError(info);
});
