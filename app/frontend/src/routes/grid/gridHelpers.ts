import type { HealthDeepResponse, HealthReadyResponse } from "../../backendReady";
import { getSignalDirectionSummary, type SignalMetrics } from "../../utils/signals";
import type { TechnicalFilterState } from "../../utils/technicalFilter";
import type { HealthStatus, Timeframe, WalkforwardParams } from "./gridTypes";

export const GRID_GAP = 12;
export const KP_LIMIT = 24;
export const BARS_ERROR_RETRY_INTERVAL_MS = 4000;
export const BARS_ERROR_RETRY_COOLDOWN_MS = 8000;

export const rangeOptions = [
  { label: "60本", count: 60 },
  { label: "120本", count: 120 },
  { label: "240本", count: 240 },
  { label: "360本", count: 360 }
];

export const gridRowOptions: Array<1 | 2 | 3 | 4 | 5 | 6> = [1, 2, 3, 4, 5, 6];
export const gridColumnOptions: Array<1 | 2 | 3 | 4> = [1, 2, 3, 4];

export const APP_VERSION_LABEL = `MeeMee v${__APP_VERSION__}`;
export const GRID_REFACTOR_FLAG_RAW = String(import.meta.env.VITE_GRID_REFACTOR ?? "1")
  .trim()
  .toLowerCase();
export const GRID_REFACTOR_ENABLED = ["1", "true", "yes", "on"].includes(GRID_REFACTOR_FLAG_RAW);

export const createDefaultTechFilter = (defaultTimeframe: Timeframe): TechnicalFilterState => ({
  defaultTimeframe,
  anchorMode: "latest",
  anchorDate: null,
  conditions: [],
  boxThisMonth: false
});

export const normalizeHealthStatus = (
  payload: HealthReadyResponse | HealthDeepResponse | null | undefined
): HealthStatus => ({
  txt_count: typeof payload?.txt_count === "number" ? payload.txt_count : null,
  code_count: typeof payload?.code_count === "number" ? payload.code_count : undefined,
  last_updated: typeof payload?.last_updated === "string" ? payload.last_updated : null,
  code_txt_missing: payload?.code_txt_missing === true,
  pan_out_txt_dir: typeof payload?.pan_out_txt_dir === "string" ? payload.pan_out_txt_dir : null
});

export const mergeHealthStatus = (
  prev: HealthStatus | null,
  payload: HealthReadyResponse | HealthDeepResponse | null | undefined
): HealthStatus => {
  const next = normalizeHealthStatus(payload);
  if (!prev) return next;
  const codeTxtMissing =
    payload?.code_txt_missing === true
      ? true
      : payload?.code_txt_missing === false
        ? false
        : prev.code_txt_missing;
  return {
    txt_count: next.txt_count ?? prev.txt_count,
    code_count: next.code_count ?? prev.code_count,
    last_updated: next.last_updated ?? prev.last_updated,
    code_txt_missing: codeTxtMissing,
    pan_out_txt_dir: next.pan_out_txt_dir ?? prev.pan_out_txt_dir
  };
};

export const resolveGridSignalSortScore = (
  metrics: SignalMetrics | null,
  liquidity20d: number | null | undefined,
  direction: "up" | "down"
) => {
  if (!metrics) return Number.NEGATIVE_INFINITY;
  const summary = getSignalDirectionSummary(metrics);
  const directionalTrend = direction === "up" ? metrics.trendStrength : -metrics.trendStrength;
  const signalCount = metrics.signals.length;
  const directionMatched = direction === "up" ? summary.hasBuySignal : summary.hasSellSignal;
  const oppositeMatched = direction === "up" ? summary.hasSellSignal : summary.hasBuySignal;
  return (
    (directionMatched ? 10_000 : 0)
    - (oppositeMatched ? 2_000 : 0)
    + directionalTrend * 10
    + signalCount * 20
    + ((Number.isFinite(liquidity20d ?? NaN) ? Number(liquidity20d) : 0) / 1_000_000)
  );
};

export const WALKFORWARD_PRESETS_STORAGE_KEY = "walkforwardPresetsV1";
export const WALKFORWARD_PRESETS_LIMIT = 20;

export const createDefaultWalkforwardParams = (): WalkforwardParams => ({
  trainMonths: 24,
  testMonths: 3,
  stepMonths: 1,
  minWindows: 1,
  maxCodes: 500,
  allowedSides: "both",
  minLongScore: 1.0,
  minShortScore: 1.0,
  maxNewEntriesPerDay: 3,
  maxNewEntriesPerMonth: "",
  minMlPUpLong: "",
  useRegimeFilter: false,
  regimeBreadthLookbackDays: 20,
  regimeLongMinBreadthAbove60: "0.52",
  regimeShortMaxBreadthAbove60: "0.48",
  allowedLongSetups: "",
  allowedShortSetups: ""
});

export const toWalkforwardParams = (value: unknown): WalkforwardParams => {
  const defaults = createDefaultWalkforwardParams();
  if (!value || typeof value !== "object") return defaults;
  const raw = value as Partial<WalkforwardParams>;
  const allowedSides = raw.allowedSides;
  return {
    trainMonths:
      typeof raw.trainMonths === "number" && Number.isFinite(raw.trainMonths)
        ? Math.max(1, Math.floor(raw.trainMonths))
        : defaults.trainMonths,
    testMonths:
      typeof raw.testMonths === "number" && Number.isFinite(raw.testMonths)
        ? Math.max(1, Math.floor(raw.testMonths))
        : defaults.testMonths,
    stepMonths:
      typeof raw.stepMonths === "number" && Number.isFinite(raw.stepMonths)
        ? Math.max(1, Math.floor(raw.stepMonths))
        : defaults.stepMonths,
    minWindows:
      typeof raw.minWindows === "number" && Number.isFinite(raw.minWindows)
        ? Math.max(1, Math.floor(raw.minWindows))
        : defaults.minWindows,
    maxCodes:
      typeof raw.maxCodes === "number" && Number.isFinite(raw.maxCodes)
        ? Math.max(20, Math.floor(raw.maxCodes))
        : defaults.maxCodes,
    allowedSides:
      allowedSides === "both" || allowedSides === "long" || allowedSides === "short"
        ? allowedSides
        : defaults.allowedSides,
    minLongScore:
      typeof raw.minLongScore === "number" && Number.isFinite(raw.minLongScore)
        ? raw.minLongScore
        : defaults.minLongScore,
    minShortScore:
      typeof raw.minShortScore === "number" && Number.isFinite(raw.minShortScore)
        ? raw.minShortScore
        : defaults.minShortScore,
    maxNewEntriesPerDay:
      typeof raw.maxNewEntriesPerDay === "number" && Number.isFinite(raw.maxNewEntriesPerDay)
        ? Math.max(1, Math.floor(raw.maxNewEntriesPerDay))
        : defaults.maxNewEntriesPerDay,
    maxNewEntriesPerMonth:
      typeof raw.maxNewEntriesPerMonth === "string"
        ? raw.maxNewEntriesPerMonth
        : defaults.maxNewEntriesPerMonth,
    minMlPUpLong:
      typeof raw.minMlPUpLong === "string" ? raw.minMlPUpLong : defaults.minMlPUpLong,
    useRegimeFilter:
      typeof raw.useRegimeFilter === "boolean" ? raw.useRegimeFilter : defaults.useRegimeFilter,
    regimeBreadthLookbackDays:
      typeof raw.regimeBreadthLookbackDays === "number"
      && Number.isFinite(raw.regimeBreadthLookbackDays)
        ? Math.max(1, Math.floor(raw.regimeBreadthLookbackDays))
        : defaults.regimeBreadthLookbackDays,
    regimeLongMinBreadthAbove60:
      typeof raw.regimeLongMinBreadthAbove60 === "string"
        ? raw.regimeLongMinBreadthAbove60
        : defaults.regimeLongMinBreadthAbove60,
    regimeShortMaxBreadthAbove60:
      typeof raw.regimeShortMaxBreadthAbove60 === "string"
        ? raw.regimeShortMaxBreadthAbove60
        : defaults.regimeShortMaxBreadthAbove60,
    allowedLongSetups:
      typeof raw.allowedLongSetups === "string" ? raw.allowedLongSetups : defaults.allowedLongSetups,
    allowedShortSetups:
      typeof raw.allowedShortSetups === "string"
        ? raw.allowedShortSetups
        : defaults.allowedShortSetups
  };
};

export const TERMINAL_JOB_STATUS = new Set(["success", "failed", "canceled"]);
export const ACTIVE_JOB_STATUS = new Set(["queued", "running", "cancel_requested"]);

export const extractErrorDetail = (err: unknown, fallback = "不明なエラー"): string => {
  if (!err || typeof err !== "object") return fallback;
  const maybeErr = err as {
    message?: unknown;
    response?: {
      data?: {
        error?: unknown;
        detail?: unknown;
        message?: unknown;
      };
    };
  };
  const responseData = maybeErr.response?.data;
  if (typeof responseData?.error === "string" && responseData.error.trim()) return responseData.error;
  if (typeof responseData?.detail === "string" && responseData.detail.trim()) return responseData.detail;
  if (typeof responseData?.message === "string" && responseData.message.trim()) return responseData.message;
  if (typeof maybeErr.message === "string" && maybeErr.message.trim()) return maybeErr.message;
  return fallback;
};
