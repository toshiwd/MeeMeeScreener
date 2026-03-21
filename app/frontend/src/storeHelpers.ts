import { api } from "./api";
import type {
  EventsMeta,
  GridTimeframe,
  MaSetting,
  MaTimeframe,
  PerformancePeriod,
  Settings,
  SortDir,
  SortKey,
  StoreState
} from "./storeTypes";
import {
  DEFAULT_DENSITY_PRESET,
  normalizeDensityPreset,
  type DensityPreset
} from "./density";

export const MA_COLORS = ["#ef4444", "#22c55e", "#3b82f6", "#a855f7", "#f59e0b"];
export const THUMB_BARS = 60;
export const MIN_BATCH_LIMIT = 60;
export const MAX_BATCH_LIMIT = 2000;
export const BATCH_TTL_MS = 60_000;
export const BATCH_REQUEST_TIMEOUT_MS = 30_000;
export const BATCH_RETRY_DELAYS_MS = [400, 1200] as const;
export const ENSURE_COALESCE_MS = 16;
export const EVENTS_POLL_INTERVAL_MS = 10_000;
export const EVENTS_POLL_MAX_ATTEMPTS = 180;
export const KEEP_STORAGE_KEY = "keepList";
export const GRID_PRESET_KEY = "gridPreset";
export const GRID_COLS_KEY = "gridCols";
export const GRID_ROWS_KEY = "gridRows";
export const LIST_TIMEFRAME_KEY = "listTimeframe";
export const LIST_RANGE_KEY = "listRangeBars";
export const LEGACY_LIST_RANGE_KEY = "listRangeMonths";
export const LIST_COLS_KEY = "listCols";
export const LIST_ROWS_KEY = "listRows";
export const LIST_RANGE_VALUES = [30, 45, 60, 90, 120, 180, 240, 360] as const;
export const WATCHLIST_AUTO_REPAIR_TS_KEY = "watchlistAutoRepairTs";
export const WATCHLIST_AUTO_REPAIR_COOLDOWN_MS = 15 * 60 * 1000;
export const WATCHLIST_AUTO_REPAIR_MIN_MISSING = 30;
export const WATCHLIST_AUTO_REPAIR_MIN_RATIO = 0.2;
export const LEGACY_RANGE_MONTHS_TO_BARS: Record<number, Settings["listRangeBars"]> = {
  3: 60,
  6: 120,
  12: 240,
  24: 360
};
export const MA_STORAGE_PREFIX = "maSettings";
export const COMPARE_MA_STORAGE_PREFIX = "compareMaSettings";
export const inFlightBatchRequests = new Map<
  string,
  { promise: Promise<void>; controller: AbortController }
>();
export const recentBatchRequests = new Map<string, number>();
export const lastEnsureKeyByTimeframe: Record<GridTimeframe, string | null> = {
  monthly: null,
  weekly: null,
  daily: null
};
export const barsFetchedLimit: Record<GridTimeframe, Record<string, number>> = {
  monthly: {},
  weekly: {},
  daily: {}
};
export const counters = {
  batchRequestCount: 0,
  v3RequestCount: 0,
  coalescedRequestCount: 0,
  dedupHitCount: 0
};
export let eventsPollPromise: Promise<void> | null = null;
export const setEventsPollPromise = (promise: Promise<void> | null) => {
  eventsPollPromise = promise;
};
export const ensurePendingCodes: Record<GridTimeframe, Set<string>> = {
  monthly: new Set<string>(),
  weekly: new Set<string>(),
  daily: new Set<string>()
};
export const ensurePendingReason: Record<GridTimeframe, string | undefined> = {
  monthly: undefined,
  weekly: undefined,
  daily: undefined
};
export const ensurePendingWaiters: Record<
  GridTimeframe,
  Array<{ resolve: () => void; reject: (error: unknown) => void }>
> = {
  monthly: [],
  weekly: [],
  daily: []
};
export const ensureCoalesceTimers: Record<GridTimeframe, ReturnType<typeof setTimeout> | null> = {
  monthly: null,
  weekly: null,
  daily: null
};
export const DEFAULT_PERIODS: Record<MaTimeframe, number[]> = {
  daily: [7, 20, 60, 100, 200],
  weekly: [7, 20, 60, 100, 200],
  monthly: [7, 20, 60, 100, 200]
};

export const makeDefaultSettings = (timeframe: MaTimeframe): MaSetting[] =>
  DEFAULT_PERIODS[timeframe].map((period, index) => ({
    key: `ma${index + 1}`,
    label: `MA${index + 1}`,
    period,
    visible: true,
    color: MA_COLORS[index] ?? "#94a3b8",
    lineWidth: 1
  }));

export const buildBatchKey = (timeframe: GridTimeframe, limit: number, codes: string[]) => {
  const sorted = [...new Set(codes.filter((code) => code))].sort();
  return `${timeframe}|${limit}|${sorted.join(",")}`;
};

export const isAbortError = (error: unknown) => {
  if (!error || typeof error !== "object") return false;
  const err = error as { name?: string; code?: string };
  return err.name === "CanceledError" || err.code === "ERR_CANCELED";
};

export const sleepMs = (ms: number) =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });

export const resolveErrorStatusCode = (error: unknown): number | null => {
  if (!error || typeof error !== "object") return null;
  const status = (error as { response?: { status?: unknown } }).response?.status;
  return typeof status === "number" && Number.isFinite(status) ? status : null;
};

export const isRetriableBatchError = (error: unknown) => {
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

export const markFetchedLimit = (timeframe: GridTimeframe, code: string, limit: number) => {
  const current = barsFetchedLimit[timeframe][code] ?? 0;
  barsFetchedLimit[timeframe][code] = Math.max(current, limit);
};

export const getFetchedLimit = (timeframe: GridTimeframe, code: string) =>
  barsFetchedLimit[timeframe][code] ?? 0;

export const abortInFlightForTimeframe = (timeframe: GridTimeframe) => {
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

export const normalizeColor = (value: unknown, fallback: string) => {
  if (typeof value !== "string") return fallback;
  const trimmed = value.trim();
  return /^#[0-9a-fA-F]{6}$/.test(trimmed) ? trimmed : fallback;
};

export const normalizeLineWidth = (value: unknown, fallback: number) => {
  const width = Number(value);
  if (!Number.isFinite(width)) return fallback;
  return Math.min(6, Math.max(1, Math.round(width)));
};

export const parseIsoMs = (value: string | null | undefined) => {
  if (!value) return null;
  const ms = Date.parse(value);
  return Number.isNaN(ms) ? null : ms;
};

export const normalizeBool = (value: unknown) => {
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

export const normalizeEventsMeta = (payload: unknown): EventsMeta | null => {
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

export const isEventsStale = (meta: EventsMeta | null) => {
  if (!meta) return true;
  const now = Date.now();
  const earningsMs = parseIsoMs(meta.earningsLastSuccessAt);
  const rightsMs = parseIsoMs(meta.rightsLastSuccessAt);
  if (earningsMs == null || rightsMs == null) return true;
  const oldest = Math.min(earningsMs, rightsMs);
  return now - oldest >= 4 * 24 * 60 * 60 * 1000;
};

export const normalizeSettings = (timeframe: MaTimeframe, input: unknown): MaSetting[] => {
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

export const loadSettings = (
  timeframe: MaTimeframe,
  storagePrefix = MA_STORAGE_PREFIX
): MaSetting[] => {
  if (typeof window === "undefined") return makeDefaultSettings(timeframe);
  const raw = window.localStorage.getItem(`${storagePrefix}:${timeframe}`);
  if (!raw) return makeDefaultSettings(timeframe);
  try {
    return normalizeSettings(timeframe, JSON.parse(raw));
  } catch {
    return makeDefaultSettings(timeframe);
  }
};

export const persistSettings = (
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

export const getMaxPeriod = (settings: MaSetting[]) =>
  settings.reduce((max, setting) => Math.max(max, Math.max(1, setting.period)), 1);

export const getRequiredBars = (settings: MaSetting[]) => {
  const desired = getMaxPeriod(settings) + THUMB_BARS - 1;
  return Math.min(MAX_BATCH_LIMIT, Math.max(MIN_BATCH_LIMIT, desired));
};

export const startEventsMetaPolling = (
  get: () => StoreState,
  set: (partial: Partial<StoreState> | ((state: StoreState) => Partial<StoreState>)) => void
) => {
  if (eventsPollPromise) return eventsPollPromise;
  setEventsPollPromise(
    (async () => {
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

        if (!meta?.isRefreshing) {
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

      setEventsPollPromise(null);
    })().catch(() => {
      setEventsPollPromise(null);
    })
  );
  return eventsPollPromise;
};

export const getInitialTimeframe = (): Settings["gridTimeframe"] => {
  if (typeof window === "undefined") return "monthly";
  const saved = window.localStorage.getItem("gridTimeframe");
  return saved === "daily" || saved === "weekly" ? (saved as Settings["gridTimeframe"]) : "monthly";
};

export const getInitialListTimeframe = (): Settings["listTimeframe"] => {
  if (typeof window === "undefined") return "daily";
  const saved = window.localStorage.getItem(LIST_TIMEFRAME_KEY);
  return saved === "monthly" || saved === "weekly" || saved === "daily"
    ? (saved as Settings["listTimeframe"])
    : "daily";
};

export const getInitialColumns = (): Settings["columns"] => {
  if (typeof window === "undefined") return DEFAULT_DENSITY_PRESET;
  return getStoredGridPreset();
};

export const getInitialRows = (): Settings["rows"] => {
  if (typeof window === "undefined") return DEFAULT_DENSITY_PRESET;
  return getStoredGridPreset();
};

export const getInitialListRangeBars = (): Settings["listRangeBars"] => {
  if (typeof window === "undefined") return 120;
  const saved = Number(window.localStorage.getItem(LIST_RANGE_KEY));
  if (LIST_RANGE_VALUES.includes(saved as Settings["listRangeBars"])) {
    return saved as Settings["listRangeBars"];
  }
  const legacy = Number(window.localStorage.getItem(LEGACY_LIST_RANGE_KEY));
  const mapped = LEGACY_RANGE_MONTHS_TO_BARS[legacy];
  if (mapped) return mapped;
  return 60;
};

export const getInitialSortKey = (): SortKey => {
  if (typeof window === "undefined") return "code";
  const saved = window.localStorage.getItem("sortKey");
  const options: SortKey[] = [
    "code",
    "name",
    "entryPriority",
    "buyCandidate",
    "buySignalLatest",
    "sellSignalLatest",
    "volumeSurge",
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
  return options.includes(saved as SortKey) ? (saved as SortKey) : "code";
};

export const getInitialSortDir = (): SortDir => {
  if (typeof window === "undefined") return "asc";
  const saved = window.localStorage.getItem("sortDir");
  return saved === "desc" ? "desc" : "asc";
};

export const loadKeepList = (): string[] => {
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

export const getInitialPerformancePeriod = (): PerformancePeriod => {
  if (typeof window === "undefined") return "1M";
  const saved = window.localStorage.getItem("performancePeriod");
  const options: PerformancePeriod[] = ["1D", "1W", "1M", "1Q", "1Y"];
  if (saved && options.includes(saved as PerformancePeriod)) {
    return saved as PerformancePeriod;
  }
  return "1M";
};

export const persistKeepList = (list: string[]) => {
  if (typeof window === "undefined") return;
  window.localStorage.setItem(KEEP_STORAGE_KEY, JSON.stringify(list));
};

const readLegacyDensityPair = (columnsKey: string, rowsKey: string): DensityPreset | null => {
  if (typeof window === "undefined") return null;
  const rawColumns = window.localStorage.getItem(columnsKey);
  const rawRows = window.localStorage.getItem(rowsKey);
  const hasColumns = rawColumns != null;
  const hasRows = rawRows != null;
  if (!hasColumns && !hasRows) return null;
  if (hasColumns && hasRows) {
    const columns = normalizeDensityPreset(rawColumns);
    const rows = normalizeDensityPreset(rawRows);
    return normalizeDensityPreset(Math.min(columns, rows));
  }
  const columns = normalizeDensityPreset(rawColumns ?? rawRows);
  const rows = normalizeDensityPreset(rawRows ?? rawColumns);
  return normalizeDensityPreset(Math.min(columns, rows));
};

export const getStoredGridPreset = () => {
  if (typeof window === "undefined") return DEFAULT_DENSITY_PRESET;
  const stored = window.localStorage.getItem(GRID_PRESET_KEY);
  if (stored != null) return normalizeDensityPreset(stored);
  const legacyGrid = readLegacyDensityPair(GRID_COLS_KEY, GRID_ROWS_KEY);
  if (legacyGrid != null) return legacyGrid;
  const legacyList = readLegacyDensityPair(LIST_COLS_KEY, LIST_ROWS_KEY);
  if (legacyList != null) return legacyList;
  return DEFAULT_DENSITY_PRESET;
};

export const persistGridPreset = (preset: Settings["columns"]) => {
  if (typeof window === "undefined") return;
  const normalized = normalizeDensityPreset(preset);
  window.localStorage.setItem(GRID_PRESET_KEY, String(normalized));
  window.localStorage.removeItem(GRID_COLS_KEY);
  window.localStorage.removeItem(GRID_ROWS_KEY);
  window.localStorage.removeItem(LIST_COLS_KEY);
  window.localStorage.removeItem(LIST_ROWS_KEY);
};
