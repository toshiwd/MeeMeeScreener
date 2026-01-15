import { create } from "zustand";
import { api, setApiErrorReporter } from "./api";
import type { ApiErrorInfo } from "./apiErrors";

export type Ticker = {
  code: string;
  name: string;
  stage: string;
  score: number | null;
  reason: string;
  scoreStatus?: string | null;
  missingReasons?: string[] | null;
  scoreBreakdown?: Record<string, number> | null;
  dataStatus?: "missing" | null;
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
  buyState?: string | null;
  buyStateRank?: number | null;
  buyStateScore?: number | null;
  buyStateReason?: string | null;
  buyRiskDistance?: number | null;
  buyStateDetails?: {
    monthly?: number | null;
    weekly?: number | null;
    daily?: number | null;
  };
  scores?: {
    upScore?: number | null;
    downScore?: number | null;
    overheatUp?: number | null;
    overheatDown?: number | null;
  };
  statusLabel?: string;
  reasons?: string[];
  // Short-selling fields
  shortScore?: number | null;
  aScore?: number | null;
  bScore?: number | null;
  shortType?: "A" | "B" | null;
  shortBadges?: string[];
  shortReasons?: string[];
  shortProhibition?: "Z1" | "Z2" | "Z3" | null;
  eventEarningsDate?: string | null;
  eventRightsDate?: string | null;
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
  listRangeMonths: 3 | 6 | 12 | 24;
  listColumns: 1 | 2 | 3 | 4;
  listRows: 1 | 2 | 3 | 4 | 5 | 6;
  showBoxes: boolean;
  // Legacy sort key (for backward compatibility during migration)
  sortKey: SortKey;
  sortDir: SortDir;
  // Separated sort states (new)
  candidateSortKey: CandidateSortKey;
  basicSortKey: BasicSortKey;
  basicSortDir: SortDir;
  performancePeriod: PerformancePeriod;
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
  toggleKeep: (code: string) => void;
  clearKeep: () => void;
  loadBarsBatch: (
    timeframe: GridTimeframe,
    codes: string[],
    limitOverride?: number,
    reason?: string
  ) => Promise<void>;
  loadBoxesBatch: (codes: string[]) => Promise<void>;
  ensureBarsForVisible: (
    timeframe: GridTimeframe,
    codes: string[],
    reason?: string
  ) => Promise<void>;
  setColumns: (columns: Settings["columns"]) => void;
  setRows: (rows: Settings["rows"]) => void;
  setListTimeframe: (value: Settings["listTimeframe"]) => void;
  setListRangeMonths: (value: Settings["listRangeMonths"]) => void;
  setListColumns: (value: Settings["listColumns"]) => void;
  setListRows: (value: Settings["listRows"]) => void;
  setSearch: (search: string) => void;
  setGridScrollTop: (value: number) => void;
  setGridTimeframe: (value: Settings["gridTimeframe"]) => void;
  setShowBoxes: (value: boolean) => void;
  setSortKey: (value: SortKey) => void;
  setSortDir: (value: SortDir) => void;
  // New separated sort setters
  setCandidateSortKey: (value: CandidateSortKey) => void;
  setBasicSortKey: (value: BasicSortKey) => void;
  setBasicSortDir: (value: SortDir) => void;
  setPerformancePeriod: (value: PerformancePeriod) => void;
  updateMaSetting: (timeframe: MaTimeframe, index: number, patch: Partial<MaSetting>) => void;
  updateCompareMaSetting: (timeframe: MaTimeframe, index: number, patch: Partial<MaSetting>) => void;
  resetMaSettings: (timeframe: MaTimeframe) => void;
  resetCompareMaSettings: (timeframe: MaTimeframe) => void;
  resetBarsCache: () => void;
  loadEventsMeta: () => Promise<EventsMeta | null>;
  refreshEventsIfStale: () => Promise<void>;
};

// Candidate sort presets (for buy/sell candidate screens only)
export type CandidateSortKey =
  | "buyCandidate"      // 買い候補（総合）
  | "buyInitial"        // 買い候補（初動）
  | "buyBase"           // 買い候補（底がため）
  | "shortScore"        // 売り候補（総合）
  | "aScore"            // 売り候補（反転確定）
  | "bScore";           // 売り候補（戻り売り）

// Basic sort keys (for non-candidate screens)
export type BasicSortKey =
  | "code"
  | "name"
  | "ma20Dev"
  | "ma60Dev"
  | "ma20Slope"
  | "ma60Slope"
  | "performance"       // Single performance key with period selector
  | "upScore"
  | "downScore"
  | "overheatUp"
  | "overheatDown"
  | "boxState";

// Performance period for unified performance sorting
export type PerformancePeriod = "1D" | "1W" | "1M" | "1Q" | "1Y";

// Legacy combined type for backward compatibility
export type SortKey =
  | "code"
  | "name"
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
  | "boxState"
  | "shortScore"
  | "aScore"
  | "bScore"
  | "performance";

export type SortDir = "asc" | "desc";

const MA_COLORS = ["#ef4444", "#22c55e", "#3b82f6", "#a855f7", "#f59e0b"];
const THUMB_BARS = 60;
const MIN_BATCH_LIMIT = 60;
const MAX_BATCH_LIMIT = 2000;
const WEEKLY_DAILY_FACTOR = 7;
const BATCH_TTL_MS = 60_000;
const KEEP_STORAGE_KEY = "keepList";
const GRID_COLS_KEY = "gridCols";
const GRID_ROWS_KEY = "gridRows";
const LIST_TIMEFRAME_KEY = "listTimeframe";
const LIST_RANGE_KEY = "listRangeMonths";
const LIST_COLS_KEY = "listCols";
const LIST_ROWS_KEY = "listRows";
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
const DEFAULT_PERIODS: Record<MaTimeframe, number[]> = {
  daily: [7, 20, 60, 120, 200],
  weekly: [7, 20, 60, 120, 200],
  monthly: [7, 20, 60, 120, 200]
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

const normalizeEventsMeta = (payload: unknown): EventsMeta | null => {
  if (!payload || typeof payload !== "object") return null;
  const data = payload as Record<string, unknown>;
  return {
    earningsLastSuccessAt: (data.earnings_last_success_at as string | null) ?? null,
    rightsLastSuccessAt: (data.rights_last_success_at as string | null) ?? null,
    isRefreshing: Boolean(data.is_refreshing),
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

const getDailyLimitForWeekly = (settings: MaSetting[]) => {
  const weeklyBars = getRequiredBars(settings);
  return Math.min(MAX_BATCH_LIMIT, Math.max(MIN_BATCH_LIMIT, weeklyBars * WEEKLY_DAILY_FACTOR));
};

const normalizeDateParts = (year: number, month: number, day: number) => {
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return null;
  if (year < 1900 || month < 1 || month > 12 || day < 1 || day > 31) return null;
  return Math.floor(Date.UTC(year, month - 1, day) / 1000);
};

const normalizeBarTime = (value: unknown) => {
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

const buildWeeklyBars = (bars: number[][]) => {
  const groups = new Map<number, { o: number; h: number; l: number; c: number }>();
  for (const row of bars) {
    if (!Array.isArray(row) || row.length < 5) continue;
    const time = normalizeBarTime(row[0]);
    if (time == null) continue;
    const date = new Date(time * 1000);
    const day = date.getUTCDay();
    const diff = (day + 6) % 7;
    const weekStart = Date.UTC(
      date.getUTCFullYear(),
      date.getUTCMonth(),
      date.getUTCDate() - diff
    );
    const key = Math.floor(weekStart / 1000);
    const open = Number(row[1]);
    const high = Number(row[2]);
    const low = Number(row[3]);
    const close = Number(row[4]);
    const existing = groups.get(key);
    if (!existing) {
      groups.set(key, { o: open, h: high, l: low, c: close });
    } else {
      existing.h = Math.max(existing.h, high);
      existing.l = Math.min(existing.l, low);
      existing.c = close;
    }
  }
  return [...groups.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([time, bar]) => [time, bar.o, bar.h, bar.l, bar.c]);
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

const getInitialListRangeMonths = (): Settings["listRangeMonths"] => {
  if (typeof window === "undefined") return 6;
  const saved = Number(window.localStorage.getItem(LIST_RANGE_KEY));
  return saved === 3 || saved === 6 || saved === 12 || saved === 24
    ? (saved as Settings["listRangeMonths"])
    : 6;
};

const getInitialSortKey = (): SortKey => {
  if (typeof window === "undefined") return "chg1D";
  const saved = window.localStorage.getItem("sortKey");
  const options: SortKey[] = [
    "code",
    "name",
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
    "boxState",
    "shortScore",
    "aScore",
    "bScore"
  ];
  return options.includes(saved as SortKey) ? (saved as SortKey) : "buyCandidate";
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

const persistKeepList = (list: string[]) => {
  if (typeof window === "undefined") return;
  window.localStorage.setItem(KEEP_STORAGE_KEY, JSON.stringify(list));
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
  lastApiError: null,
  eventsMeta: null,
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
    search: "",
    gridScrollTop: 0,
    gridTimeframe: getInitialTimeframe(),
    listTimeframe: getInitialListTimeframe(),
    listRangeMonths: getInitialListRangeMonths(),
    listColumns: getInitialListColumns(),
    listRows: getInitialListRows(),
    showBoxes: true,
    sortKey: getInitialSortKey(),
    sortDir: getInitialSortDir(),
    // New separated sort states
    candidateSortKey: "buyCandidate" as CandidateSortKey,
    basicSortKey: "code" as BasicSortKey,
    basicSortDir: "desc" as SortDir,
    performancePeriod: "1M" as PerformancePeriod
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
    } catch {
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
  loadList: async () => {
    if (get().loadingList) return;
    set({ loadingList: true });
    try {
      const res = await api.get("/screener");
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
      const tickers = items.map((item) => {
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
          buyRiskDistance:
            typeof item.buyRiskDistance === "number"
              ? item.buyRiskDistance
              : typeof item.buy_risk_distance === "number"
                ? item.buy_risk_distance
                : null,
          buyStateDetails: item.buyStateDetails ?? null,
          scores: item.scores,
          statusLabel: item.statusLabel,
          reasons: item.reasons,
          // Short-selling fields
          shortScore: typeof item.shortScore === "number" ? item.shortScore : null,
          aScore: typeof item.aScore === "number" ? item.aScore : null,
          bScore: typeof item.bScore === "number" ? item.bScore : null,
          shortType: item.shortType ?? null,
          shortBadges: Array.isArray(item.shortBadges) ? item.shortBadges : [],
          shortReasons: Array.isArray(item.shortReasons) ? item.shortReasons : [],
          shortProhibition: item.shortProhibition ?? null,
          eventEarningsDate: item.eventEarningsDate ?? item.event_earnings_date ?? null,
          eventRightsDate: item.eventRightsDate ?? item.event_rights_date ?? null
        };
      });
      try {
        const resWatch = await api.get("/watchlist");
        const watchlistCodes = (resWatch.data?.codes || []) as string[];
        if (watchlistCodes.length) {
          const existing = new Set(tickers.map((item) => item.code));
          watchlistCodes.forEach((code) => {
            if (existing.has(code)) return;
            tickers.push({
              code,
              name: code,
              stage: "",
              score: null,
              reason: "WATCHLIST_ONLY",
              scoreStatus: "INSUFFICIENT_DATA",
              missingReasons: null,
              scoreBreakdown: null,
              dataStatus: "missing"
            });
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
      const dailyLimit = Math.max(
        limitOverride ?? 0,
        getDailyLimitForWeekly(get().maSettings.weekly)
      );
      const weeklyRequired = getRequiredBars(get().maSettings.weekly);
      const reasonLabel = reason ? `${reason}:weekly` : "weekly";
      try {
        const dailyCache = get().barsCache.daily;
        const dailyMissing = trimmed.filter((code) => {
          const payload = dailyCache[code];
          return !payload || payload.bars.length < dailyLimit;
        });
        if (dailyMissing.length) {
          await get().loadBarsBatch("daily", dailyMissing, dailyLimit, reasonLabel);
        }
      } catch (error) {
        set((prev) => ({
          barsStatus: {
            ...prev.barsStatus,
            weekly: {
              ...prev.barsStatus.weekly,
              ...trimmed.reduce((acc, code) => {
                acc[code] = "error";
                return acc;
              }, {} as Record<string, "idle" | "loading" | "success" | "empty" | "error">)
            }
          }
        }));
        throw error;
      }
      set((prev) => {
        const weeklyItems: Record<string, BarsPayload> = {};
        const weeklyBoxes: Record<string, Box[]> = {};
        trimmed.forEach((code) => {
          const dailyPayload = prev.barsCache.daily[code];
          if (!dailyPayload) return;
          weeklyItems[code] = {
            bars: buildWeeklyBars(dailyPayload.bars),
            ma: { ma7: [], ma20: [], ma60: [] }
          };
          weeklyBoxes[code] = prev.boxesCache.daily[code] ?? [];
        });
        trimmed.forEach((code) => markFetchedLimit("weekly", code, weeklyRequired));
        return {
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
              ...trimmed.reduce((acc, code) => {
                const payload = weeklyItems[code];
                acc[code] = payload && payload.bars.length ? "success" : "empty";
                return acc;
              }, {} as Record<string, "idle" | "loading" | "success" | "empty" | "error">)
            }
          }
        };
      });
      return;
    }

    const maSettings =
      timeframe === "daily" ? get().maSettings.daily : get().maSettings.monthly;
    const limit = Math.max(limitOverride ?? 0, getRequiredBars(maSettings));
    const requestCodes = [...new Set(trimmed)].sort();
    const requestKey = buildBatchKey(timeframe, limit, requestCodes);
    const cachedAt = recentBatchRequests.get(requestKey);
    if (cachedAt && Date.now() - cachedAt < BATCH_TTL_MS) return;

    const inFlight = inFlightBatchRequests.get(requestKey);
    if (inFlight) return inFlight.promise;

    batchRequestCount += 1;
    console.debug("[batch_bars]", {
      count: batchRequestCount,
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
        const res = await api.post(
          "/batch_bars",
          {
            timeframe,
            codes: requestCodes,
            limit
          },
          { signal: controller.signal }
        );
        if (res.status !== 200) {
          throw new Error(`batch_bars failed with status ${res.status}`);
        }
        const items = (res.data?.items || {}) as Record<string, BarsPayload>;
        const boxesMonthly: Record<string, Box[]> = {};
        const boxesDaily: Record<string, Box[]> = {};
        Object.entries(items).forEach(([code, payload]) => {
          const boxes = payload.boxes ?? [];
          boxesMonthly[code] = boxes;
          boxesDaily[code] = boxes;
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
                acc[code] = "error";
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
    const state = get();
    const cache = state.barsCache[timeframe];
    const maSettings = state.maSettings;
    const requiredBars =
      timeframe === "daily"
        ? getRequiredBars(maSettings.daily)
        : timeframe === "weekly"
          ? getRequiredBars(maSettings.weekly)
          : getRequiredBars(maSettings.monthly);
    const dailyLimitForWeekly =
      timeframe === "weekly" ? getDailyLimitForWeekly(maSettings.weekly) : null;
    const uniqueCodes = [...new Set(codes.filter((code) => code))];
    const listKey = buildBatchKey(timeframe, requiredBars, uniqueCodes);
    if (lastEnsureKeyByTimeframe[timeframe] !== listKey) {
      abortInFlightForTimeframe(timeframe);
      lastEnsureKeyByTimeframe[timeframe] = listKey;
    }
    const missing = uniqueCodes.filter((code) => {
      const payload = cache[code];
      const fetchedLimit = getFetchedLimit(timeframe, code);
      if (!payload) return fetchedLimit < requiredBars;
      if (payload.bars.length >= requiredBars) return false;
      if (fetchedLimit >= requiredBars) return false;
      return true;
    });
    if (!missing.length) return;

    const batchSize = 48;
    for (let i = 0; i < missing.length; i += batchSize) {
      const batch = missing.slice(i, i + batchSize);
      await get().loadBarsBatch(
        timeframe,
        batch,
        timeframe === "weekly" ? dailyLimitForWeekly ?? undefined : requiredBars,
        reason
      );
    }
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
  setListRangeMonths: (value) => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem(LIST_RANGE_KEY, String(value));
    }
    set((state) => ({ settings: { ...state.settings, listRangeMonths: value } }));
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
      const jobId = (res.data as { refresh_job_id?: string } | null)?.refresh_job_id ?? null;
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
      }
    } catch {
      // ignore refresh failures
    }
  }
}));

setApiErrorReporter((info) => {
  useStore.getState().setLastApiError(info);
});
