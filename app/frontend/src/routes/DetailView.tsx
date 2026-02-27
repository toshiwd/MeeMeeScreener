import { useEffect, useMemo, useRef, useState } from "react";
import type { MouseEvent as ReactMouseEvent, TouchEvent as ReactTouchEvent } from "react";
import { useLocation, useNavigate, useParams } from "react-router-dom";
import {
  IconAdjustments,
  IconArrowBackUp,
  IconArrowLeft,
  IconArrowRight,
  IconBox,
  IconCamera,
  IconCopy,
  IconCurrencyYen,
  IconHeart,
  IconHeartFilled,
  IconMinus,
  IconTrash,
  IconSparkles,
  IconChartArrows,
} from "@tabler/icons-react";
import { api } from "../api";
import { useBackendReadyState } from "../backendReady";
import DetailChart, {
  DetailChartHandle,
  type DrawBox,
  type DrawTool,
  type HorizontalLine,
  type PriceBand,
  type SelectedDrawingInfo,
  type TimeZone
} from "../components/DetailChart";
import Toast from "../components/Toast";
import IconButton from "../components/IconButton";
import SimilarSearchPanel from "../components/SimilarSearchPanel";
import { Box, MaSetting, useStore } from "../store";
import { computeSignalMetrics } from "../utils/signals";
import type { TradeEvent, CurrentPosition, DailyPosition } from "../utils/positions";
import { buildCurrentPositions, buildDailyPositions, buildPositionLedger } from "../utils/positions";
import { captureAndCopyScreenshot, saveBlobToFile, getScreenType } from "../utils/windowScreenshot";
import { buildAIExport, copyToClipboard, saveAsFile } from "../utils/aiExport";
import { formatEventBadgeDate, formatEventDateYmd, parseEventDateMs } from "../utils/events";
import DailyMemoPanel from "../components/DailyMemoPanel";
import { buildConsultCopyText, copyToClipboard as copyConsultToClipboard } from "../utils/consultCopy";
import { useChartSync } from "../hooks/useChartSync";
import { useDetailInfo } from "../hooks/useDetailInfo";


type Timeframe = "daily" | "weekly" | "monthly";
type FocusPanel = Timeframe | null;

type ChartDrawings = {
  timeZones: TimeZone[];
  priceBands: PriceBand[];
  drawBoxes: DrawBox[];
  horizontalLines: HorizontalLine[];
};

const DRAWING_STORAGE_PREFIX = "drawings:v1";

const createEmptyDrawings = (): ChartDrawings => ({
  timeZones: [],
  priceBands: [],
  drawBoxes: [],
  horizontalLines: []
});

const normalizeDrawings = (value: any): ChartDrawings => {
  if (!value || typeof value !== "object") return createEmptyDrawings();
  return {
    timeZones: Array.isArray(value.timeZones) ? value.timeZones : [],
    priceBands: Array.isArray(value.priceBands) ? value.priceBands : [],
    drawBoxes: Array.isArray(value.drawBoxes) ? value.drawBoxes : [],
    horizontalLines: Array.isArray(value.horizontalLines) ? value.horizontalLines : []
  };
};

const loadDrawingsFromStorage = (key: string): ChartDrawings => {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return createEmptyDrawings();
    return normalizeDrawings(JSON.parse(raw));
  } catch {
    return createEmptyDrawings();
  }
};

const saveDrawingsToStorage = (key: string, drawings: ChartDrawings) => {
  try {
    localStorage.setItem(key, JSON.stringify(drawings));
  } catch {
    // ignore storage errors
  }
};

type Candle = {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
};

type VolumePoint = {
  time: number;
  value: number;
};

type ParseStats = {
  total: number;
  parsed: number;
  invalidRow: number;
  invalidTime: number;
  invalidValue: number;
};

type FetchState = {
  status: "idle" | "loading" | "success" | "error";
  responseCount: number;
  errorMessage: string | null;
};

type ApiWarnings = {
  items: string[];
  info?: string[];
  unrecognized_labels?: { count: number; samples: string[] };
};

type BarsResponse = {
  data?: number[][];
  errors?: string[];
};

type CompareListItem = {
  ticker: string;
  asof: string | null;
};

type CompareListPayload = {
  queryTicker: string;
  mainAsOf: string | null;
  items: CompareListItem[];
};

type AnalysisHorizonKey = 5 | 10 | 20;

type AnalysisHorizonEntry = {
  horizon: AnalysisHorizonKey;
  pUp: number | null;
  pDown: number | null;
  evNet: number | null;
  pTurnDown: number | null;
  pTurnUp: number | null;
  pUpProjected: boolean;
  evProjected: boolean;
  turnProjected: boolean;
};

type AnalysisHorizonPayload = {
  defaultHorizon: AnalysisHorizonKey;
  turnBaseHorizon: number | null;
  projectionMethod: string | null;
  items: Partial<Record<`${AnalysisHorizonKey}`, AnalysisHorizonEntry>>;
};

type RankRiskMode = "defensive" | "balanced" | "aggressive";

type AnalysisAdditiveSignals = {
  trendUpStrict: boolean;
  mtfStrongAligned: boolean;
  boxBottomAligned: boolean;
  shootingStarLike: boolean;
  threeWhiteSoldiers: boolean;
  bullEngulfing: boolean;
  reclaim60: boolean;
  v60Core: boolean;
  v60Strong: boolean;
  v60StrongPenalty: boolean;
  candlestickPatternBonus: number | null;
  bonusEstimate: number | null;
  weeklyBreakoutUpProb: number | null;
  monthlyBreakoutUpProb: number | null;
  monthlyRangeProb: number | null;
  monthlyRangePos: number | null;
};

type AnalysisEntryPolicySide = {
  setupType: string | null;
  recommendedHoldDays: number | null;
  recommendedHoldMinDays: number | null;
  recommendedHoldMaxDays: number | null;
  recommendedHoldReason: string | null;
  invalidationTrigger: string | null;
  invalidationConservativeAction: string | null;
  invalidationAggressiveAction: string | null;
  invalidationRecommendedAction: string | null;
  invalidationDotenRecommended: boolean;
  invalidationOppositeHoldDays: number | null;
  invalidationExpectedDeltaMean: number | null;
  invalidationPolicyNote: string | null;
  playbookScoreBonus: number | null;
};

type AnalysisEntryPolicy = {
  riskMode: RankRiskMode;
  up: AnalysisEntryPolicySide | null;
  down: AnalysisEntryPolicySide | null;
};

type BuyStagePrecisionEntry = {
  precision: number | null;
  samples: number;
  wins: number;
};

type BuyStrategyBacktest = {
  precision: number | null;
  samples: number;
  wins: number;
  probeShares: number | null;
  addShares: number | null;
  coreShares: number | null;
  topupShares: number | null;
  targetShares: number | null;
  takeProfitPct: number | null;
};

type BuyStagePrecisionPayload = {
  horizon: number | null;
  lookbackBars: number | null;
  probe: BuyStagePrecisionEntry;
  add: BuyStagePrecisionEntry;
  core: BuyStagePrecisionEntry;
  strategy: BuyStrategyBacktest | null;
};

type SellAnalysisFallback = {
  dt: number | string | null;
  close: number | null;
  dayChangePct: number | null;
  pDown: number | null;
  pTurnDown: number | null;
  ev20Net: number | null;
  rankDown20: number | null;
  predDt: number | string | null;
  pUp5: number | null;
  pUp10: number | null;
  pUp20: number | null;
  shortScore: number | null;
  aScore: number | null;
  bScore: number | null;
  ma20: number | null;
  ma60: number | null;
  ma20Slope: number | null;
  ma60Slope: number | null;
  distMa20Signed: number | null;
  distMa60Signed: number | null;
  trendDown: boolean | null;
  trendDownStrict: boolean | null;
  fwdClose5: number | null;
  fwdClose10: number | null;
  fwdClose20: number | null;
  shortRet5: number | null;
  shortRet10: number | null;
  shortRet20: number | null;
  shortWin5: boolean | null;
  shortWin10: boolean | null;
  shortWin20: boolean | null;
};

type PhaseFallback = {
  dt: number | null;
  earlyScore: number | null;
  lateScore: number | null;
  bodyScore: number | null;
  n: number | null;
  reasons: string[];
};

type AnalysisFallback = {
  dt: number | string | null;
  pUp: number | null;
  pDown: number | null;
  pTurnUp: number | null;
  pTurnDown: number | null;
  pTurnDownHorizon: number | null;
  retPred20: number | null;
  ev20: number | null;
  ev20Net: number | null;
  horizonAnalysis: AnalysisHorizonPayload | null;
  additiveSignals: AnalysisAdditiveSignals | null;
  entryPolicy: AnalysisEntryPolicy | null;
  riskMode: RankRiskMode | null;
  buyStagePrecision: BuyStagePrecisionPayload | null;
  modelVersion: string | null;
};

const DEFAULT_LIMITS = {
  daily: 2000,
  monthly: 240
};

const LIMIT_STEP = {
  daily: 1000,
  monthly: 120
};

const RANGE_PRESETS = [
  { label: "3M", months: 3 },
  { label: "6M", months: 6 },
  { label: "1Y", months: 12 },
  { label: "2Y", months: 24 }
];

const ANALYSIS_HORIZONS = [5, 10, 20] as const;
const RANK_VIEW_STATE_KEY = "rankingViewState";
const RISK_MODE_LABEL: Record<RankRiskMode, string> = {
  defensive: "守り",
  balanced: "中立",
  aggressive: "攻め"
};

const buildMonthBoundaries = (candles: Candle[]) => {
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

const buildYearBoundaries = (candles: Candle[]) => {
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

const DAILY_ROW_RATIO = 12 / 16;
const DEFAULT_WEEKLY_RATIO = 3 / 4;
const MIN_WEEKLY_RATIO = 0.2;
const MIN_MONTHLY_RATIO = 0.1;
const MAX_EVENT_OFFSET_SEC = 3 * 24 * 60 * 60;

const normalizeDateParts = (year: number, month: number, day: number) => {
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return null;
  if (year < 1900 || month < 1 || month > 12 || day < 1 || day > 31) return null;
  return Math.floor(Date.UTC(year, month - 1, day) / 1000);
};

const formatNumber = (value: number | null | undefined, digits = 0) => {
  if (value == null || !Number.isFinite(value)) return "--";
  return value.toLocaleString("ja-JP", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits
  });
};

const formatSignedNumber = (value: number | null | undefined, digits = 0) => {
  if (value == null || !Number.isFinite(value)) return "--";
  return value.toLocaleString("ja-JP", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
    signDisplay: "always"
  });
};

const formatPercentLabel = (value: number | null | undefined, digits = 1) => {
  if (value == null || !Number.isFinite(value)) return "--";
  return `${(value * 100).toFixed(digits)}%`;
};

const formatSignedPercentLabel = (value: number | null | undefined, digits = 1) => {
  if (value == null || !Number.isFinite(value)) return "--";
  const scaled = value * 100;
  const sign = scaled > 0 ? "+" : "";
  return `${sign}${scaled.toFixed(digits)}%`;
};

const toFiniteNumber = (value: unknown): number | null => {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
};

const toBoolean = (value: unknown): boolean => {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return Number.isFinite(value) && value !== 0;
  if (typeof value === "string") {
    const trimmed = value.trim().toLowerCase();
    return trimmed === "1" || trimmed === "true" || trimmed === "yes";
  }
  return false;
};

const normalizeRiskMode = (value: unknown): RankRiskMode => {
  if (value === "defensive" || value === "aggressive" || value === "balanced") {
    return value;
  }
  return "balanced";
};

const resolveRiskModeFromSession = (): RankRiskMode => {
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

const formatInvalidationTrigger = (value: string | null | undefined): string => {
  if (!value) return "--";
  if (value === "box_break") return "Box下限割れ";
  if (value === "box_reclaim") return "Box回復";
  if (value === "stop3") return "3日否定";
  if (value === "stop5") return "5日否定";
  return value;
};

const formatInvalidationAction = (value: string | null | undefined): string => {
  if (!value) return "--";
  if (value === "exit") return "撤退";
  if (value === "hold") return "継続";
  if (value === "doten_opt") return "ドテン検討";
  return value;
};

const normalizeEntryPolicySide = (value: unknown): AnalysisEntryPolicySide | null => {
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

const normalizeEntryPolicy = (value: unknown): AnalysisEntryPolicy | null => {
  if (!value || typeof value !== "object") return null;
  const payload = value as Record<string, unknown>;
  return {
    riskMode: normalizeRiskMode(payload.riskMode),
    up: normalizeEntryPolicySide(payload.up),
    down: normalizeEntryPolicySide(payload.down)
  };
};

const normalizeAnalysisHorizonEntry = (horizon: AnalysisHorizonKey, value: unknown): AnalysisHorizonEntry => {
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

const normalizeHorizonAnalysis = (value: unknown): AnalysisHorizonPayload | null => {
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

const normalizeAdditiveSignals = (value: unknown): AnalysisAdditiveSignals | null => {
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

const normalizeBuyStagePrecisionEntry = (value: unknown): BuyStagePrecisionEntry => {
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

const normalizeBuyStrategyBacktest = (value: unknown): BuyStrategyBacktest | null => {
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

const normalizeBuyStagePrecision = (value: unknown): BuyStagePrecisionPayload | null => {
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

const formatLotValue = (value: number) => {
  if (!Number.isFinite(value)) return "0";
  return Number.isInteger(value) ? `${value}` : value.toFixed(1);
};

const formatSignedLot = (value: number) => {
  if (value === 0) return "0";
  const sign = value > 0 ? "+" : "-";
  return `${sign}${formatLotValue(Math.abs(value))}`;
};

const formatShares = (shares: number | null | undefined) => {
  if (shares == null || !Number.isFinite(shares)) return "--";
  return formatNumber(shares, 0);
};


const formatLedgerDate = (value: string) => {
  const trimmed = value?.trim();
  if (!trimmed) return "--";
  const match = trimmed.match(/^(\d{4})[/-](\d{1,2})[/-](\d{1,2})$/);
  if (!match) return trimmed;
  const year = match[1];
  const month = match[2].padStart(2, "0");
  const day = match[3].padStart(2, "0");
  return `${year}-${month}-${day}`;
};

const normalizeTime = (value: unknown) => {
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

const computeMA = (candles: Candle[], period: number) => {
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

const buildCandlesWithStats = (rows: number[][]) => {
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

const buildVolume = (rows: number[][]): VolumePoint[] => {
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

const buildWeekly = (candles: Candle[], volume: VolumePoint[]) => {
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

const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value));

const buildRange = (candles: Candle[], months: number) => {
  if (!candles.length) return null;
  const end = candles[candles.length - 1].time;
  const endDate = new Date(end * 1000);
  const startDate = new Date(endDate);
  startDate.setMonth(endDate.getMonth() - months);
  return { from: Math.floor(startDate.getTime() / 1000), to: end };
};

const buildRangeEndingAt = (candles: Candle[], months: number, endTime: number | null) => {
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

const buildRangeFromEndTime = (months: number, endTime: number | null) => {
  if (!endTime) return null;
  const endDate = new Date(endTime * 1000);
  const startDate = new Date(endDate);
  startDate.setMonth(endDate.getMonth() - months);
  return { from: Math.floor(startDate.getTime() / 1000), to: endTime };
};

const formatDateLabel = (value: number | null) => {
  if (!value) return "";
  const date = new Date(value * 1000);
  if (Number.isNaN(date.getTime())) return "";
  const yyyy = date.getUTCFullYear();
  const mm = String(date.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(date.getUTCDate()).padStart(2, "0");
  return `${yyyy}/${mm}/${dd}`;
};

const countInRange = (candles: Candle[], months: number | null) => {
  if (!months) return candles.length;
  const range = buildRange(candles, months);
  if (!range) return 0;
  return candles.filter((c) => c.time >= range.from && c.time <= range.to).length;
};

const filterCandlesByAsOf = (candles: Candle[], asOf: number | null) => {
  if (!asOf) return candles;
  return candles.filter((candle) => candle.time <= asOf);
};

const filterVolumeByAsOf = (volume: VolumePoint[], asOf: number | null) => {
  if (!asOf) return volume;
  return volume.filter((point) => point.time <= asOf);
};

const findNearestCandleTime = (candles: Candle[], time: number) => {
  if (!candles.length) return null;
  let left = 0;
  let right = candles.length - 1;
  while (left <= right) {
    const mid = Math.floor((left + right) / 2);
    const midTime = candles[mid].time;
    if (midTime === time) return midTime;
    if (midTime < time) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  const lower = candles[Math.max(0, Math.min(candles.length - 1, right))];
  const upper = candles[Math.max(0, Math.min(candles.length - 1, left))];
  if (!lower) return upper?.time ?? null;
  if (!upper) return lower.time;
  return Math.abs(time - lower.time) <= Math.abs(upper.time - time) ? lower.time : upper.time;
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
  const draggingRef = useRef(false);
  const hoverTimeRef = useRef<number | null>(null);
  const hoverTimePendingRef = useRef<number | null>(null);
  const hoverRafRef = useRef<number | null>(null);
  const manualDailyRangeRef = useRef<{ from: number; to: number } | null>(null);
  const manualWeeklyRangeRef = useRef<{ from: number; to: number } | null>(null);
  const manualMonthlyRangeRef = useRef<{ from: number; to: number } | null>(null);
  const manualCompareDailyRangeRef = useRef<{ from: number; to: number } | null>(null);

  const tickers = useStore((state) => state.tickers);
  const loadList = useStore((state) => state.loadList);
  const loadingList = useStore((state) => state.loadingList);
  const eventsMeta = useStore((state) => state.eventsMeta);
  const favorites = useStore((state) => state.favorites);
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
  const [headerMode, setHeaderMode] = useState<"chart" | "draw" | "positions" | "analysis">("chart");
  const [displayOpen, setDisplayOpen] = useState(false);
  const [signalsOpen, setSignalsOpen] = useState(false);
  const [showGapBands, setShowGapBands] = useState(true);
  const [showVolumeEnabled, setShowVolumeEnabled] = useState(true);
  const [activeDrawTool, setActiveDrawTool] = useState<DrawTool | null>(null);
  const [, setSelectedDrawing] = useState<SelectedDrawingInfo | null>(null);
  const [drawingsByKey, setDrawingsByKey] = useState<Record<string, ChartDrawings>>({});
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
  const [showTradesOverlay, setShowTradesOverlay] = useState(true);
  const [showPnLPanel, setShowPnLPanel] = useState(true);
  const [syncRanges, setSyncRanges] = useState(true);
  const [hoverTime, setHoverTime] = useState<number | null>(null);
  const [focusPanel, setFocusPanel] = useState<FocusPanel>(null);
  const [toastMessage, setToastMessage] = useState<string | null>(null);
  const [toastAction, setToastAction] = useState<{ label: string; onClick: () => void } | null>(null);
  const [screenshotBusy, setScreenshotBusy] = useState(false);
  const [deleteBusy, setDeleteBusy] = useState(false);
  const [showPositionLedger, setShowPositionLedger] = useState(false);
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

  const syncRangesRef = useRef(syncRanges);
  const pendingRangeRef = useRef<{ from: number; to: number } | null>(null);
  const syncRafRef = useRef<number | null>(null);
  const [showSimilar, setShowSimilar] = useState(false);
  const compareCode = useMemo(() => {
    const params = new URLSearchParams(location.search);
    const raw = params.get("compare");
    if (!raw) return null;
    const trimmed = raw.trim();
    if (!trimmed || trimmed === code) return null;
    return trimmed;
  }, [location.search, code]);
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
  const [compareMonthlyData, setCompareMonthlyData] = useState<number[][]>([]);
  const [compareMonthlyErrors, setCompareMonthlyErrors] = useState<string[]>([]);
  const [compareLoading, setCompareLoading] = useState(false);
  const [compareDailyData, setCompareDailyData] = useState<number[][]>([]);
  const [compareDailyErrors, setCompareDailyErrors] = useState<string[]>([]);
  const [compareDailyLoading, setCompareDailyLoading] = useState(false);
  const [compareDailyLimit, setCompareDailyLimit] = useState(DEFAULT_LIMITS.daily);
  const [phaseFallback, setPhaseFallback] = useState<PhaseFallback | null>(null);
  const [phaseFallbackLoading, setPhaseFallbackLoading] = useState(false);
  const lastPhaseAttemptAsOfRef = useRef<number | null>(null);
  const phaseFallbackRequestKeyRef = useRef<string | null>(null);
  const [analysisFallback, setAnalysisFallback] = useState<AnalysisFallback | null>(null);
  const [analysisHorizon] = useState<AnalysisHorizonKey>(20);
  const [analysisRiskMode, setAnalysisRiskMode] = useState<RankRiskMode>(() => resolveRiskModeFromSession());
  const [analysisLoading, setAnalysisLoading] = useState(false);
  const lastAnalysisAttemptAsOfRef = useRef<number | null>(null);
  const analysisRequestKeyRef = useRef<string | null>(null);
  const [sellAnalysisFallback, setSellAnalysisFallback] = useState<SellAnalysisFallback | null>(null);
  const [sellAnalysisLoading, setSellAnalysisLoading] = useState(false);
  const lastSellAnalysisAttemptAsOfRef = useRef<number | null>(null);
  const sellAnalysisRequestKeyRef = useRef<string | null>(null);
  const phaseFallbackCacheRef = useRef<Map<string, PhaseFallback | null>>(new Map());
  const analysisFallbackCacheRef = useRef<Map<string, AnalysisFallback | null>>(new Map());
  const sellAnalysisFallbackCacheRef = useRef<Map<string, SellAnalysisFallback | null>>(new Map());
  const [analysisAsOfTime, setAnalysisAsOfTime] = useState<number | null>(null);
  const displayRef = useRef<HTMLDivElement | null>(null);
  const signalsRef = useRef<HTMLDivElement | null>(null);
  const emptyDrawingsRef = useRef<ChartDrawings>(createEmptyDrawings());

  const buildDrawingKey = (symbol: string | null | undefined, timeframe: Timeframe) =>
    symbol ? `${DRAWING_STORAGE_PREFIX}:${symbol}:${timeframe}` : null;

  const dailyDrawingKey = useMemo(() => buildDrawingKey(code, "daily"), [code]);
  const weeklyDrawingKey = useMemo(() => buildDrawingKey(code, "weekly"), [code]);
  const monthlyDrawingKey = useMemo(() => buildDrawingKey(code, "monthly"), [code]);
  const compareDailyDrawingKey = useMemo(() => buildDrawingKey(compareCode, "daily"), [compareCode]);
  const compareMonthlyDrawingKey = useMemo(
    () => buildDrawingKey(compareCode, "monthly"),
    [compareCode]
  );

  const updateDrawings = (key: string | null, updater: (prev: ChartDrawings) => ChartDrawings) => {
    if (!key) return;
    setDrawingsByKey((prev) => {
      const current = prev[key] ?? emptyDrawingsRef.current;
      const nextValue = updater(current);
      const next = { ...prev, [key]: nextValue };
      saveDrawingsToStorage(key, nextValue);
      return next;
    });
  };

  const resolveDrawings = (key: string | null) =>
    key ? drawingsByKey[key] ?? emptyDrawingsRef.current : emptyDrawingsRef.current;

  const addTimeZone = (key: string | null) => (zone: TimeZone) =>
    updateDrawings(key, (prev) => ({ ...prev, timeZones: [...prev.timeZones, zone] }));
  const updateTimeZone = (key: string | null) => (index: number, zone: TimeZone) =>
    updateDrawings(key, (prev) => {
      const next = [...prev.timeZones];
      if (!next[index]) return prev;
      next[index] = zone;
      return { ...prev, timeZones: next };
    });

  const addPriceBand = (key: string | null) => (band: PriceBand) =>
    updateDrawings(key, (prev) => ({ ...prev, priceBands: [...prev.priceBands, band] }));
  const updatePriceBand = (key: string | null) => (index: number, band: PriceBand) =>
    updateDrawings(key, (prev) => {
      const next = [...prev.priceBands];
      if (!next[index]) return prev;
      next[index] = band;
      return { ...prev, priceBands: next };
    });

  const addDrawBox = (key: string | null) => (box: DrawBox) =>
    updateDrawings(key, (prev) => ({ ...prev, drawBoxes: [...prev.drawBoxes, box] }));
  const updateDrawBox = (key: string | null) => (index: number, box: DrawBox) =>
    updateDrawings(key, (prev) => {
      const next = [...prev.drawBoxes];
      if (!next[index]) return prev;
      next[index] = box;
      return { ...prev, drawBoxes: next };
    });

  const addHorizontalLine = (key: string | null) => (line: HorizontalLine) =>
    updateDrawings(key, (prev) => ({
      ...prev,
      horizontalLines: [...prev.horizontalLines, line]
    }));
  const updateHorizontalLine = (key: string | null) => (index: number, line: HorizontalLine) =>
    updateDrawings(key, (prev) => {
      const next = [...prev.horizontalLines];
      if (!next[index]) return prev;
      next[index] = line;
      return { ...prev, horizontalLines: next };
    });
  const deleteTimeZone = (key: string | null) => (index: number) =>
    updateDrawings(key, (prev) => ({
      ...prev,
      timeZones: prev.timeZones.filter((_, i) => i !== index)
    }));
  const deletePriceBand = (key: string | null) => (index: number) =>
    updateDrawings(key, (prev) => ({
      ...prev,
      priceBands: prev.priceBands.filter((_, i) => i !== index)
    }));
  const deleteDrawBox = (key: string | null) => (index: number) =>
    updateDrawings(key, (prev) => ({
      ...prev,
      drawBoxes: prev.drawBoxes.filter((_, i) => i !== index)
    }));
  const deleteHorizontalLine = (key: string | null) => (index: number) =>
    updateDrawings(key, (prev) => ({
      ...prev,
      horizontalLines: prev.horizontalLines.filter((_, i) => i !== index)
    }));

  const resetAllDrawings = () => {
    const keys = [
      dailyDrawingKey,
      weeklyDrawingKey,
      monthlyDrawingKey,
      compareDailyDrawingKey,
      compareMonthlyDrawingKey
    ].filter(Boolean) as string[];
    if (!keys.length) return;
    setDrawingsByKey((prev) => {
      const next = { ...prev };
      keys.forEach((key) => {
        const empty = createEmptyDrawings();
        next[key] = empty;
        saveDrawingsToStorage(key, empty);
      });
      return next;
    });
    setSelectedDrawing(null);
  };

  useEffect(() => {
    const keys = [
      dailyDrawingKey,
      weeklyDrawingKey,
      monthlyDrawingKey,
      compareDailyDrawingKey,
      compareMonthlyDrawingKey
    ].filter(Boolean) as string[];
    if (!keys.length) return;
    setDrawingsByKey((prev) => {
      let next = prev;
      let changed = false;
      keys.forEach((key) => {
        if (next[key]) return;
        const loaded = loadDrawingsFromStorage(key);
        if (!changed) {
          next = { ...prev };
          changed = true;
        }
        next[key] = loaded;
      });
      return changed ? next : prev;
    });
  }, [dailyDrawingKey, weeklyDrawingKey, monthlyDrawingKey, compareDailyDrawingKey, compareMonthlyDrawingKey]);

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
    lastAnalysisAttemptAsOfRef.current = null;
  }, [analysisRiskMode]);

  useEffect(() => {
    if (headerMode !== "draw" && activeDrawTool !== null) {
      setActiveDrawTool(null);
    }
  }, [headerMode, activeDrawTool]);

  useEffect(() => {
    if (headerMode === "draw" && activeDrawTool == null) {
      setActiveDrawTool("timeZone");
    }
  }, [headerMode, activeDrawTool]);

  useEffect(() => {
    if (headerMode !== "draw") {
      setSelectedDrawing(null);
    }
  }, [headerMode]);

  const dailyDrawings = resolveDrawings(dailyDrawingKey);
  const weeklyDrawings = resolveDrawings(weeklyDrawingKey);
  const monthlyDrawings = resolveDrawings(monthlyDrawingKey);
  const compareDailyDrawings = resolveDrawings(compareDailyDrawingKey);
  const compareMonthlyDrawings = resolveDrawings(compareMonthlyDrawingKey);

  useEffect(() => {
    if (compareCode) return;
    setCompareMonthlyData([]);
    setCompareMonthlyErrors([]);
    setCompareLoading(false);
    setCompareDailyData([]);
    setCompareDailyErrors([]);
    setCompareDailyLoading(false);
    setCompareBoxes([]);
    setCompareTrades([]);
    setCompareDailyLimit(DEFAULT_LIMITS.daily);
  }, [compareCode]);

  useEffect(() => {
    if (!compareCode) return;
    setCompareMonthlyData([]);
    setCompareMonthlyErrors([]);
    setCompareLoading(false);
    setCompareDailyData([]);
    setCompareDailyErrors([]);
    setCompareDailyLoading(false);
    setCompareBoxes([]);
    setCompareTrades([]);
    setCompareDailyLimit(DEFAULT_LIMITS.daily);
  }, [compareCode]);

  useEffect(() => {
    if (!compareCode) return;
    manualCompareDailyRangeRef.current = null;
  }, [compareCode, compareAsOf]);

  useEffect(() => {
    setPhaseFallback(null);
    setPhaseFallbackLoading(false);
    lastPhaseAttemptAsOfRef.current = null;
    phaseFallbackRequestKeyRef.current = null;
    phaseFallbackCacheRef.current.clear();
    setAnalysisFallback(null);
    setAnalysisLoading(false);
    lastAnalysisAttemptAsOfRef.current = null;
    analysisRequestKeyRef.current = null;
    analysisFallbackCacheRef.current.clear();
    setSellAnalysisFallback(null);
    setSellAnalysisLoading(false);
    lastSellAnalysisAttemptAsOfRef.current = null;
    sellAnalysisRequestKeyRef.current = null;
    sellAnalysisFallbackCacheRef.current.clear();
    setAnalysisAsOfTime(null);
    setDailyData([]);
  }, [code]);

  useEffect(() => {
    setRangeMonths(12);
    manualDailyRangeRef.current = null;
    manualWeeklyRangeRef.current = null;
    manualMonthlyRangeRef.current = null;
    manualCompareDailyRangeRef.current = null;
  }, [code]);

  const tickerName = useMemo(() => {
    if (!code) return "";
    const raw = tickers.find((item) => item.code === code)?.name ?? "";
    const cleaned = raw.replace(/\s*\?\s*$/, "").trim();
    return cleaned === "?" ? "" : cleaned;
  }, [tickers, code]);
  const activeTicker = useMemo(() => tickers.find((item) => item.code === code) ?? null, [tickers, code]);
  const earningsLabel = useMemo(
    () => formatEventBadgeDate(activeTicker?.eventEarningsDate),
    [activeTicker?.eventEarningsDate]
  );
  const rightsLabel = useMemo(
    () => formatEventBadgeDate(activeTicker?.eventRightsDate),
    [activeTicker?.eventRightsDate]
  );
  const rightsCoverageLabel = useMemo(() => {
    const rightsMaxDate = eventsMeta?.dataCoverage?.rightsMaxDate ?? null;
    const maxMs = parseEventDateMs(rightsMaxDate);
    if (!rightsMaxDate || maxMs == null) return null;
    const thresholdMs = Date.now() + 30 * 24 * 60 * 60 * 1000;
    if (maxMs >= thresholdMs) return null;
    const formatted = formatEventDateYmd(rightsMaxDate);
    return formatted ? `権利データ範囲: ～${formatted}` : null;
  }, [eventsMeta]);
  const compareTickerName = useMemo(() => {
    if (!compareCode) return "";
    const raw = tickers.find((item) => item.code === compareCode)?.name ?? "";
    const cleaned = raw.replace(/\s*\?\s*$/, "").trim();
    return cleaned === "?" ? "" : cleaned;
  }, [tickers, compareCode]);
  const formatPhaseScore = (value: number | null | undefined) => {
    if (phaseFallbackLoading) return "読込中...";
    return Number.isFinite(value)
      ? String(Math.min(10, Math.max(0, Math.round(value! * 10))))
      : "--";
  };
  const formatPhaseN = (value: number | null | undefined) => {
    if (phaseFallbackLoading) return "読込中...";
    return typeof value === "number" ? String(value) : "--";
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
  const needsPhaseReasons = !(activeTicker?.phaseReasons?.length);
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
  const detailAsOfTime = useMemo(() => {
    if (cursorMode && selectedBarData?.time != null) {
      return selectedBarData.time;
    }
    if (mainAsOfTime != null) return mainAsOfTime;
    return dailyData.reduce<number | null>((maxValue, row) => {
      if (!Array.isArray(row) || row.length === 0) return maxValue;
      const normalized = normalizeTime(row[0]);
      if (normalized == null) return maxValue;
      if (maxValue == null || normalized > maxValue) return normalized;
      return maxValue;
    }, null);
  }, [cursorMode, selectedBarData?.time, mainAsOfTime, dailyData]);
  useEffect(() => {
    if (detailAsOfTime == null) {
      setAnalysisAsOfTime(null);
      return;
    }
    if (!cursorMode) {
      setAnalysisAsOfTime(detailAsOfTime);
      return;
    }
    const timerId = window.setTimeout(() => {
      setAnalysisAsOfTime(detailAsOfTime);
    }, 160);
    return () => {
      window.clearTimeout(timerId);
    };
  }, [detailAsOfTime, cursorMode]);
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
    sellAnalysisFallback?.trendDown != null ||
    sellAnalysisFallback?.trendDownStrict != null ||
    sellAnalysisFallback?.shortRet5 != null ||
    sellAnalysisFallback?.shortRet10 != null ||
    sellAnalysisFallback?.shortRet20 != null;
  const patternSummary = useMemo(() => {
    const entryPolicy = analysisFallback?.entryPolicy ?? null;
    const playbookUpBonus = toFiniteNumber(entryPolicy?.up?.playbookScoreBonus) ?? 0;
    const playbookDownBonus = toFiniteNumber(entryPolicy?.down?.playbookScoreBonus) ?? 0;
    const playbookUpBias = clamp(playbookUpBonus / 0.08, -0.4, 0.4);
    const playbookDownBias = clamp(playbookDownBonus / 0.08, -0.4, 0.4);
    const upProb = clamp(analysisPUp ?? 0.5, 0, 1);
    const downProbMl = analysisPDown ?? (analysisPUp != null ? 1 - analysisPUp : 0.5);
    const downProb = clamp(
      ((downProbMl ?? 0.5) * 0.6) + ((sellAnalysisFallback?.pDown ?? downProbMl ?? 0.5) * 0.4),
      0,
      1
    );
    const turnUp = clamp(analysisPTurnUp ?? 0.5, 0, 1);
    const turnDown = clamp(
      ((analysisPTurnDown ?? 0.5) * 0.5) + ((sellAnalysisFallback?.pTurnDown ?? analysisPTurnDown ?? 0.5) * 0.5),
      0,
      1
    );
    const evBias = analysisEvNet == null ? 0 : clamp(analysisEvNet / 0.06, -1, 1);
    const additiveBias =
      analysisAdditive?.bonusEstimate == null
        ? 0
        : clamp(analysisAdditive.bonusEstimate / 0.06, -1, 1);
    const trendDownPenalty =
      sellAnalysisFallback?.trendDownStrict ? 0.08 : sellAnalysisFallback?.trendDown ? 0.04 : 0;
    const trendDownBoost =
      sellAnalysisFallback?.trendDownStrict ? 1.0 : sellAnalysisFallback?.trendDown ? 0.7 : 0.3;
    const trendDownFlag = sellAnalysisFallback?.trendDown === true;
    const trendDownStrictFlag = sellAnalysisFallback?.trendDownStrict === true;
    const shortScoreNorm = clamp(
      ((sellAnalysisFallback?.shortScore ?? 70) - 70) / 90,
      0,
      1
    );
    const bullishStructure = Boolean(
      !trendDownFlag &&
      (sellAnalysisFallback?.distMa20Signed ?? 0) > 0 &&
      (sellAnalysisFallback?.ma20Slope ?? 0) >= 0 &&
      (sellAnalysisFallback?.ma60Slope ?? 0) >= 0
    );
    const strongUpContext = Boolean(
      analysisAdditive?.trendUpStrict &&
      (analysisAdditive?.monthlyBreakoutUpProb ?? 0) >= 0.8
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
      0.14 * clamp((-analysisEvNet + 0.005) / 0.04, 0, 1) +
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
      analysisAdditive?.mtfStrongAligned &&
      upScore >= downScore &&
      turnUp >= turnDown - 0.10
    );
    const forceDownConfirm = Boolean(
      (trendDownStrictFlag && downProb >= 0.58 && turnDown >= 0.56 && (analysisEvNet ?? 0) <= 0) ||
      (downProb >= 0.68 && turnDown >= 0.64 && (analysisEvNet ?? 0) <= -0.01)
    );
    const downConfirm = Boolean(
      trendDownFlag ||
      trendDownStrictFlag ||
      downProb - upProb >= 0.10 ||
      downProb >= 0.62
    );
    const downThreshold = strongUpContext ? 0.66 : 0.56;

    const scenarios = [
      {
        key: "up",
        label: "上昇継続（押し目再開）",
        tone: "up" as const,
        score: upScore,
        reasons: [
          `上昇 ${formatPercentLabel(upProb)} / 下落 ${formatPercentLabel(downProb)}`,
          `期待値 ${formatSignedPercentLabel(analysisEvNet)}`,
          `転換 上 ${formatPercentLabel(turnUp)}`
        ]
      },
      {
        key: "down",
        label: "下落継続（戻り売り優位）",
        tone: "down" as const,
        score: downScore,
        reasons: [
          `下落 ${formatPercentLabel(downProb)} / 上昇 ${formatPercentLabel(upProb)}`,
          `転換 下 ${formatPercentLabel(turnDown)}`,
          `売り品質 ${formatPercentLabel(sellSignalQuality)}`,
          `下向きトレンド判定 ${sellAnalysisFallback?.trendDownStrict ? "強い" : sellAnalysisFallback?.trendDown ? "あり" : "弱い"
          }`
        ]
      },
      {
        key: "range",
        label: "往復レンジ（上下振れ）",
        tone: "neutral" as const,
        score: rangeScore,
        reasons: [
          `確率差 ${formatPercentLabel(Math.abs(upProb - downProb), 1)}`,
          `転換 上 ${formatPercentLabel(turnUp)} / 下 ${formatPercentLabel(turnDown)}`,
          `方向感弱め ${(1 - Math.abs(evBias)).toFixed(2)}`
        ]
      }
    ].sort((a, b) => b.score - a.score);

    const top = scenarios[0];
    let environmentLabel = "方向感拮抗";
    let environmentTone: "up" | "down" | "neutral" = "neutral";
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
      environmentLabel = "レンジ優位";
      environmentTone = "neutral";
    }

    return {
      environmentLabel,
      environmentTone,
      scenarios
    };
  }, [
    analysisPUp,
    analysisPDown,
    analysisEvNet,
    analysisPTurnUp,
    analysisPTurnDown,
    analysisFallback?.entryPolicy?.up?.playbookScoreBonus,
    analysisFallback?.entryPolicy?.down?.playbookScoreBonus,
    analysisAdditive?.bonusEstimate,
    analysisAdditive?.trendUpStrict,
    analysisAdditive?.mtfStrongAligned,
    analysisAdditive?.monthlyBreakoutUpProb,
    sellAnalysisFallback?.shortScore,
    sellAnalysisFallback?.distMa20Signed,
    sellAnalysisFallback?.ma20Slope,
    sellAnalysisFallback?.ma60Slope,
    sellAnalysisFallback?.pDown,
    sellAnalysisFallback?.pTurnDown,
    sellAnalysisFallback?.trendDown,
    sellAnalysisFallback?.trendDownStrict
  ]);
  const analysisDecision = useMemo(() => {
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
  }, [patternSummary.environmentTone, patternSummary.scenarios]);
  const analysisGuidance = useMemo(() => {
    const buyProb = clamp(analysisDecision.buyProb ?? 0, 0, 1);
    const sellProb = clamp(analysisDecision.sellProb ?? 0, 0, 1);
    const neutralProb = clamp(analysisDecision.neutralProb ?? 0, 0, 1);
    const entryPolicy = analysisFallback?.entryPolicy ?? null;
    const upPolicy = entryPolicy?.up ?? null;
    const downPolicy = entryPolicy?.down ?? null;
    const activePolicy =
      analysisDecision.tone === "up"
        ? upPolicy
        : analysisDecision.tone === "down"
          ? downPolicy
          : buyProb >= sellProb
            ? upPolicy
            : downPolicy;
    const policyRiskMode = entryPolicy?.riskMode ?? analysisFallback?.riskMode ?? null;
    const stagePrecision = analysisFallback?.buyStagePrecision ?? null;
    const strategyBacktest = stagePrecision?.strategy ?? null;
    const stageHorizonRaw = stagePrecision?.horizon;
    const stageHorizon =
      stageHorizonRaw == null || !Number.isFinite(stageHorizonRaw) ? 20 : Math.max(1, Math.round(stageHorizonRaw));
    const strategySamples = strategyBacktest?.samples ?? 0;
    const coreSamples = stagePrecision?.core.samples ?? 0;
    const stagePrecisionModeLabel =
      strategySamples > 0
        ? `${stageHorizon}D実測`
        : coreSamples > 0
          ? `${stageHorizon}D実測(本玉)`
          : "推定";
    const confidence = clamp(analysisDecision.confidence ?? 0, 0, 1);
    const spread = Math.abs(buyProb - sellProb);
    const turnUp = clamp(analysisPTurnUp ?? 0.5, 0, 1);
    const turnDown = clamp(analysisPTurnDown ?? 0.5, 0, 1);
    const evBias = analysisEvNet == null ? 0 : clamp(analysisEvNet / 0.06, -1, 1);
    const confidenceRank = confidence >= 0.66 ? "高" : confidence >= 0.56 ? "中" : "低";

    let action = "様子見";
    let watchpoint = "方向が固まるまでポジションを小さく維持。";

    const tonePenalty = analysisDecision.tone === "down" ? 0.08 : analysisDecision.tone === "neutral" ? 0.03 : 0;
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
    const corePrecisionResolved = stagePrecision?.core.precision ?? corePrecision;
    const strategyPrecisionResolved = strategyBacktest?.precision ?? corePrecisionResolved;
    const strategyWins = Math.max(0, Math.trunc(strategyBacktest?.wins ?? 0));
    const coreWins = Math.max(0, Math.trunc(stagePrecision?.core.wins ?? 0));
    const strategyPrecisionLabel = (() => {
      if (strategySamples > 0) {
        return `${formatPercentLabel(strategyPrecisionResolved)} (n${strategySamples})`;
      }
      if (coreSamples > 0) {
        return `${formatPercentLabel(corePrecisionResolved)} (本玉 n${coreSamples})`;
      }
      return `${formatPercentLabel(strategyPrecisionResolved)} (推定)`;
    })();
    const probeShares = Math.max(0, Math.round(strategyBacktest?.probeShares ?? 100));
    const addShares = Math.max(0, Math.round(strategyBacktest?.addShares ?? 300));
    const coreShares = Math.max(0, Math.round(strategyBacktest?.coreShares ?? 500));
    const topupShares = Math.max(0, Math.round(strategyBacktest?.topupShares ?? 100));
    const targetShares = Math.max(0, Math.round(strategyBacktest?.targetShares ?? probeShares + addShares + coreShares + topupShares));
    const takeProfitPct = clamp(strategyBacktest?.takeProfitPct ?? 0.06, 0, 1);

    const probeReady = buyProb > sellProb;
    const addReady = buyProb >= 0.54 && spread >= 0.06;
    const coreReady = buyProb >= 0.58 && spread >= 0.1 && turnUp >= turnDown;
    const currentStageLabel = coreReady
      ? "本玉成立"
      : addReady
        ? "追加成立（本玉待ち）"
        : probeReady
          ? "打診成立（追加待ち）"
          : "未成立（打診待ち）";
    const backtestSummary = strategySamples > 0
      ? `過去検証 ${formatPercentLabel(strategyPrecisionResolved)}（勝ち ${strategyWins}/${strategySamples}）`
      : coreSamples > 0
        ? `過去検証(本玉) ${formatPercentLabel(corePrecisionResolved)}（勝ち ${coreWins}/${coreSamples}）`
        : "過去検証データ不足（推定）";

    let probeRule = "買い確率が売り確率を上回ったら";
    let addRule = "押し目でも買い優位を維持したら";
    let coreRule = "買い優位の拡大（買い-売り差 10pt 以上）で";
    let buyTimingTitle = `3分割買いタイミング（主精度 ${strategyPrecisionLabel}）`;
    if (analysisDecision.tone === "up") {
      action = spread >= 0.12 ? "押し目買い優先" : "買い寄りだが慎重";
      watchpoint = "売り確率が買い確率に接近したら撤退を優先。";
      buyTimingTitle = `3分割買いタイミング（${stagePrecisionModeLabel} 主精度 ${strategyPrecisionLabel}）`;
    } else if (analysisDecision.tone === "down") {
      action = spread >= 0.12 ? "戻り売り優先" : "売り寄りだが慎重";
      watchpoint = "買い確率が売り確率に接近したら戻しを警戒。";
      buyTimingTitle = `買いは条件付き（主精度 ${strategyPrecisionLabel}）`;
      probeRule = "買い確率が 50% を回復してから";
      addRule = "売り確率を上回る状態が2日続いたら";
      coreRule = "想定パターンが上昇側へ転じてから";
    } else {
      buyTimingTitle = `買いは段階的に（主精度 ${strategyPrecisionLabel}）`;
      probeRule = "買い確率が売り確率を上回ってから";
      addRule = "押し目で買い優位が崩れないことを確認してから";
      coreRule = "方向が明確化（買い-売り差 10pt 以上）してから";
    }
    if (coreReady) {
      buyTimingTitle = "本玉成立";
    }
    if (activePolicy?.invalidationPolicyNote) {
      watchpoint = `${watchpoint} ${activePolicy.invalidationPolicyNote}`;
    }
    const buyTimingPlan = coreReady
      ? [
        `本玉成立: ${coreShares}株エントリー優先 / 主精度 ${strategyPrecisionLabel} / ${backtestSummary}`
      ]
      : [
        `現在判定: ${currentStageLabel} / 主精度 ${strategyPrecisionLabel}（${probeShares}/${addShares}/${coreShares}株 + 追撃${topupShares}株 = ${targetShares}株 / 利確 ${formatPercentLabel(takeProfitPct)}）`,
        `打診 ${probeShares}株: ${probeRule}打診 / ${probeReady ? "成立" : "待機"}`,
        `追加 ${addShares}株: ${addRule}追加 / ${addReady ? "成立" : "待機"}`,
        `本玉 ${coreShares}株: ${coreRule}本玉 / ${coreReady ? "成立" : "待機"}`,
        backtestSummary
      ];

    const trendLine =
      analysisDecision.tone === "down"
        ? `下向きトレンド ${sellAnalysisFallback?.trendDownStrict ? "強い" : sellAnalysisFallback?.trendDown ? "あり" : "弱い"
        }`
        : null;
    const reasonLines = [
      `買い ${formatPercentLabel(buyProb)} / 売り ${formatPercentLabel(sellProb)}（差 ${formatPercentLabel(spread)}）`,
      `転換 上 ${formatPercentLabel(analysisPTurnUp)} / 下 ${formatPercentLabel(analysisPTurnDown)}`,
      analysisEvNet == null ? null : `期待値 ${formatSignedPercentLabel(analysisEvNet)}`,
      policyRiskMode ? `運用モード ${RISK_MODE_LABEL[policyRiskMode]}` : null,
      activePolicy?.recommendedHoldDays != null
        ? `保有目安 ${Math.round(activePolicy.recommendedHoldDays)}日`
        : null,
      activePolicy?.invalidationTrigger
        ? `否定 ${formatInvalidationTrigger(activePolicy.invalidationTrigger)} / 推奨 ${formatInvalidationAction(activePolicy.invalidationRecommendedAction)}`
        : null,
      activePolicy?.playbookScoreBonus != null
        ? `Playbook補正 ${formatSignedPercentLabel(activePolicy.playbookScoreBonus)}`
        : null,
      trendLine
    ].filter((value): value is string => typeof value === "string" && value.length > 0);

    return {
      confidenceRank,
      action,
      watchpoint,
      buyTimingTitle,
      buyTimingPlan,
      buyWidth: Math.round(buyProb * 100),
      sellWidth: Math.round(sellProb * 100),
      neutralWidth: Math.round(neutralProb * 100),
      reasonLines
    };
  }, [
    analysisDecision.buyProb,
    analysisDecision.sellProb,
    analysisDecision.neutralProb,
    analysisDecision.confidence,
    analysisDecision.tone,
    analysisFallback?.entryPolicy,
    analysisFallback?.riskMode,
    analysisFallback?.buyStagePrecision,
    analysisPTurnUp,
    analysisPTurnDown,
    analysisEvNet,
    sellAnalysisFallback?.trendDown,
    sellAnalysisFallback?.trendDownStrict
  ]);
  const canShowPhase = hasPhasePanelData;
  const showBuyAnalysis = hasAnalysisData || analysisLoading;
  const showSellAnalysis = hasSellAnalysisData || sellAnalysisLoading;
  const canShowAnalysis = showBuyAnalysis || showSellAnalysis;
  const analysisLoadingText = analysisLoading ? "読込中..." : null;
  const sellAnalysisLoadingText = sellAnalysisLoading ? "読込中..." : null;
  const analysisSummaryLoading = analysisLoadingText != null || sellAnalysisLoadingText != null;
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
  const showAnalysisPanel = headerMode === "analysis" && !compareCode;
  const showMemoPanel = cursorMode && !compareCode && !showAnalysisPanel;
  const showRightPanel = showAnalysisPanel || showMemoPanel;

  const favoritesSet = useMemo(() => new Set(favorites), [favorites]);
  const isFavorite = useMemo(() => (code ? favoritesSet.has(code) : false), [favoritesSet, code]);

  useEffect(() => {
    if (!backendReady) return;
    if (!tickers.length && !loadingList) {
      loadList();
    }
  }, [backendReady, tickers.length, loadingList, loadList]);

  useEffect(() => {
    if (!backendReady) return;
    if (!favoritesLoaded) {
      loadFavorites();
    }
  }, [backendReady, favoritesLoaded, loadFavorites]);

  useEffect(() => {
    if (!backendReady) return;
    if (!code) return;
    if (hasPhaseScores && !needsPhaseReasons) {
      setPhaseFallback(null);
      setPhaseFallbackLoading(false);
      lastPhaseAttemptAsOfRef.current = null;
      return;
    }
    const asof = analysisAsOfTime;
    if (asof == null) {
      setPhaseFallbackLoading(false);
      return;
    }
    const requestKey = `${code}|${asof}`;
    if (phaseFallbackCacheRef.current.has(requestKey)) {
      setPhaseFallback(phaseFallbackCacheRef.current.get(requestKey) ?? null);
      setPhaseFallbackLoading(false);
      lastPhaseAttemptAsOfRef.current = asof;
      return;
    }
    if (lastPhaseAttemptAsOfRef.current === asof) return;
    setPhaseFallbackLoading(true);
    lastPhaseAttemptAsOfRef.current = asof;
    phaseFallbackRequestKeyRef.current = requestKey;
    api
      .get("/ticker/phase", { params: { code, asof }, timeout: 10000 })
      .then((res) => {
        if (phaseFallbackRequestKeyRef.current !== requestKey) return;
        const item = res.data?.item ?? null;
        if (!item) {
          phaseFallbackCacheRef.current.set(requestKey, null);
          setPhaseFallback(null);
          return;
        }
        const reasons = Array.isArray(item.reasonsTop3)
          ? (item.reasonsTop3 as string[])
          : typeof item.reasonsTop3 === "string"
            ? item.reasonsTop3.split(",").map((part: string) => part.trim()).filter(Boolean)
            : [];
        const payload: PhaseFallback = {
          dt: typeof item.dt === "number" ? item.dt : null,
          earlyScore: Number.isFinite(item.earlyScore) ? item.earlyScore : null,
          lateScore: Number.isFinite(item.lateScore) ? item.lateScore : null,
          bodyScore: Number.isFinite(item.bodyScore) ? item.bodyScore : null,
          n: typeof item.n === "number" ? item.n : null,
          reasons
        };
        phaseFallbackCacheRef.current.set(requestKey, payload);
        setPhaseFallback(payload);
      })
      .catch(() => {
        if (phaseFallbackRequestKeyRef.current !== requestKey) return;
        phaseFallbackCacheRef.current.set(requestKey, null);
        setPhaseFallback(null);
      })
      .finally(() => {
        if (phaseFallbackRequestKeyRef.current !== requestKey) return;
        setPhaseFallbackLoading(false);
      });
  }, [
    backendReady,
    code,
    hasPhaseScores,
    needsPhaseReasons,
    analysisAsOfTime
  ]);

  useEffect(() => {
    if (!backendReady) return;
    if (!code) return;
    const asof = analysisAsOfTime;
    if (asof == null) {
      setAnalysisLoading(false);
      return;
    }
    const requestKey = `${code}|${asof}|${analysisRiskMode}`;
    if (analysisFallbackCacheRef.current.has(requestKey)) {
      setAnalysisFallback(analysisFallbackCacheRef.current.get(requestKey) ?? null);
      setAnalysisLoading(false);
      lastAnalysisAttemptAsOfRef.current = asof;
      return;
    }
    if (lastAnalysisAttemptAsOfRef.current === asof) return;
    setAnalysisLoading(true);
    lastAnalysisAttemptAsOfRef.current = asof;
    analysisRequestKeyRef.current = requestKey;
    api
      .get("/ticker/analysis", { params: { code, asof, risk_mode: analysisRiskMode }, timeout: 30000 })
      .then((res) => {
        if (analysisRequestKeyRef.current !== requestKey) return;
        const item = res.data?.item ?? null;
        if (!item) {
          analysisFallbackCacheRef.current.set(requestKey, null);
          setAnalysisFallback(null);
          return;
        }
        const payload: AnalysisFallback = {
          dt: item.dt ?? null,
          pUp: toFiniteNumber(item.pUp),
          pDown: toFiniteNumber(item.pDown),
          pTurnUp: toFiniteNumber(item.pTurnUp),
          pTurnDown: toFiniteNumber(item.pTurnDown),
          pTurnDownHorizon: toFiniteNumber(item.pTurnDownHorizon),
          retPred20: toFiniteNumber(item.retPred20),
          ev20: toFiniteNumber(item.ev20),
          ev20Net: toFiniteNumber(item.ev20Net),
          horizonAnalysis: normalizeHorizonAnalysis(item.horizonAnalysis),
          additiveSignals: normalizeAdditiveSignals(item.additiveSignals),
          entryPolicy: normalizeEntryPolicy(item.entryPolicy),
          riskMode: item.riskMode == null ? null : normalizeRiskMode(item.riskMode),
          buyStagePrecision: normalizeBuyStagePrecision(item.buyStagePrecision),
          modelVersion: typeof item.modelVersion === "string" ? item.modelVersion : null
        };
        analysisFallbackCacheRef.current.set(requestKey, payload);
        setAnalysisFallback(payload);
      })
      .catch(() => {
        if (analysisRequestKeyRef.current !== requestKey) return;
        analysisFallbackCacheRef.current.set(requestKey, null);
        setAnalysisFallback(null);
      })
      .finally(() => {
        if (analysisRequestKeyRef.current !== requestKey) return;
        setAnalysisLoading(false);
      });
  }, [backendReady, code, analysisAsOfTime, analysisRiskMode]);

  useEffect(() => {
    if (!backendReady) return;
    if (!code) return;
    const asof = analysisAsOfTime;
    if (asof == null) {
      setSellAnalysisLoading(false);
      return;
    }
    const requestKey = `${code}|${asof}`;
    if (sellAnalysisFallbackCacheRef.current.has(requestKey)) {
      setSellAnalysisFallback(sellAnalysisFallbackCacheRef.current.get(requestKey) ?? null);
      setSellAnalysisLoading(false);
      lastSellAnalysisAttemptAsOfRef.current = asof;
      return;
    }
    if (lastSellAnalysisAttemptAsOfRef.current === asof) return;
    setSellAnalysisLoading(true);
    lastSellAnalysisAttemptAsOfRef.current = asof;
    sellAnalysisRequestKeyRef.current = requestKey;
    api
      .get("/ticker/analysis/sell", { params: { code, asof }, timeout: 30000 })
      .then((res) => {
        if (sellAnalysisRequestKeyRef.current !== requestKey) return;
        const item = res.data?.item ?? null;
        if (!item) {
          sellAnalysisFallbackCacheRef.current.set(requestKey, null);
          setSellAnalysisFallback(null);
          return;
        }
        const payload: SellAnalysisFallback = {
          dt: item.dt ?? null,
          close: toFiniteNumber(item.close),
          dayChangePct: toFiniteNumber(item.dayChangePct),
          pDown: toFiniteNumber(item.pDown),
          pTurnDown: toFiniteNumber(item.pTurnDown),
          ev20Net: toFiniteNumber(item.ev20Net),
          rankDown20: toFiniteNumber(item.rankDown20),
          predDt: item.predDt ?? null,
          pUp5: toFiniteNumber(item.pUp5),
          pUp10: toFiniteNumber(item.pUp10),
          pUp20: toFiniteNumber(item.pUp20),
          shortScore: toFiniteNumber(item.shortScore),
          aScore: toFiniteNumber(item.aScore),
          bScore: toFiniteNumber(item.bScore),
          ma20: toFiniteNumber(item.ma20),
          ma60: toFiniteNumber(item.ma60),
          ma20Slope: toFiniteNumber(item.ma20Slope),
          ma60Slope: toFiniteNumber(item.ma60Slope),
          distMa20Signed: toFiniteNumber(item.distMa20Signed),
          distMa60Signed: toFiniteNumber(item.distMa60Signed),
          trendDown: item.trendDown == null ? null : toBoolean(item.trendDown),
          trendDownStrict: item.trendDownStrict == null ? null : toBoolean(item.trendDownStrict),
          fwdClose5: toFiniteNumber(item.fwdClose5),
          fwdClose10: toFiniteNumber(item.fwdClose10),
          fwdClose20: toFiniteNumber(item.fwdClose20),
          shortRet5: toFiniteNumber(item.shortRet5),
          shortRet10: toFiniteNumber(item.shortRet10),
          shortRet20: toFiniteNumber(item.shortRet20),
          shortWin5: item.shortWin5 == null ? null : toBoolean(item.shortWin5),
          shortWin10: item.shortWin10 == null ? null : toBoolean(item.shortWin10),
          shortWin20: item.shortWin20 == null ? null : toBoolean(item.shortWin20)
        };
        sellAnalysisFallbackCacheRef.current.set(requestKey, payload);
        setSellAnalysisFallback(payload);
      })
      .catch(() => {
        if (sellAnalysisRequestKeyRef.current !== requestKey) return;
        sellAnalysisFallbackCacheRef.current.set(requestKey, null);
        setSellAnalysisFallback(null);
      })
      .finally(() => {
        if (sellAnalysisRequestKeyRef.current !== requestKey) return;
        setSellAnalysisLoading(false);
      });
  }, [backendReady, code, analysisAsOfTime]);

  useEffect(() => {
    if (!backendReady) return;
    if (!code) return;
    setLoadingDaily(true);
    setDailyErrors([]);
    setDailyFetch((prev) => ({ ...prev, status: "loading", errorMessage: null }));
    const params: Record<string, string | number> = { code, limit: dailyLimit };
    if (mainAsOf) {
      params.asof = mainAsOf;
    }
    api
      .get("/ticker/daily", { params })
      .then((res) => {
        const { rows, errors } = parseBarsResponse(res.data as BarsResponse | number[][], "daily");
        setDailyData(rows);
        setDailyErrors(errors);
        setHasMoreDaily(rows.length >= dailyLimit);
        setDailyFetch({ status: "success", responseCount: rows.length, errorMessage: null });
      })
      .catch((error) => {
        const message = error?.message || "Daily fetch failed";
        setDailyErrors([message]);
        setDailyFetch((prev) => ({
          status: "error",
          responseCount: prev.responseCount,
          errorMessage: message
        }));
      })
      .finally(() => setLoadingDaily(false));
  }, [backendReady, code, dailyLimit, mainAsOf]);

  useEffect(() => {
    if (!backendReady) return;
    if (!code) return;
    setLoadingMonthly(true);
    setMonthlyErrors([]);
    setMonthlyFetch((prev) => ({ ...prev, status: "loading", errorMessage: null }));
    const params: Record<string, string | number> = { code, limit: monthlyLimit };
    if (mainAsOf) {
      params.asof = mainAsOf;
    }
    api
      .get("/ticker/monthly", { params })
      .then((res) => {
        const { rows, errors } = parseBarsResponse(res.data as BarsResponse | number[][], "monthly");
        setMonthlyData(rows);
        setMonthlyErrors(errors);
        setHasMoreMonthly(rows.length >= monthlyLimit);
        setMonthlyFetch({ status: "success", responseCount: rows.length, errorMessage: null });
      })
      .catch((error) => {
        const message = error?.message || "Monthly fetch failed";
        setMonthlyErrors([message]);
        setMonthlyFetch((prev) => ({
          status: "error",
          responseCount: prev.responseCount,
          errorMessage: message
        }));
      })
      .finally(() => setLoadingMonthly(false));
  }, [backendReady, code, monthlyLimit, mainAsOf]);

  useEffect(() => {
    if (!backendReady) return;
    if (!compareCode) return;
    setCompareLoading(true);
    setCompareMonthlyErrors([]);
    const params: Record<string, string | number> = { code: compareCode, limit: monthlyLimit };
    api
      .get("/ticker/monthly", { params })
      .then((res) => {
        const { rows, errors } = parseBarsResponse(res.data as BarsResponse | number[][], "monthly");
        setCompareMonthlyData(rows);
        setCompareMonthlyErrors(errors);
      })
      .catch((error) => {
        const message = error?.message || "Monthly fetch failed";
        setCompareMonthlyErrors([message]);
        setCompareMonthlyData([]);
      })
      .finally(() => setCompareLoading(false));
  }, [backendReady, compareCode, monthlyLimit]);

  useEffect(() => {
    if (!backendReady) return;
    if (!compareCode) return;
    setCompareDailyLoading(true);
    setCompareDailyErrors([]);
    const params: Record<string, string | number> = {
      code: compareCode,
      limit: compareDailyLimit
    };
    api
      .get("/ticker/daily", { params })
      .then((res) => {
        const { rows, errors } = parseBarsResponse(res.data as BarsResponse | number[][], "daily");
        setCompareDailyData(rows);
        setCompareDailyErrors(errors);
      })
      .catch((error) => {
        const message = error?.message || "Daily fetch failed";
        setCompareDailyErrors([message]);
        setCompareDailyData([]);
      })
      .finally(() => setCompareDailyLoading(false));
  }, [backendReady, compareCode, compareDailyLimit]);

  useEffect(() => {
    if (!backendReady) return;
    if (!code) return;
    const params: Record<string, string> = { code };
    if (mainAsOf) {
      params.asof = mainAsOf;
    }
    api
      .get("/ticker/boxes", { params })
      .then((res) => {
        const rows = (res.data || []) as Box[];
        setBoxes(rows);
      })
      .catch(() => {
        setBoxes([]);
      });
  }, [backendReady, code, mainAsOf]);

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
    if (!compareCode) return;
    const params: Record<string, string> = { code: compareCode };
    if (compareAsOf) {
      params.asof = compareAsOf;
    }
    api
      .get("/ticker/boxes", { params })
      .then((res) => {
        const rows = (res.data || []) as Box[];
        setCompareBoxes(rows);
      })
      .catch(() => {
        setCompareBoxes([]);
      });
  }, [backendReady, compareCode, compareAsOf]);

  useEffect(() => {
    if (!backendReady) return;
    if (!code) return;
    setTradeErrors([]);
    setTradeWarnings({ items: [] });
    setCurrentPositionsFromApi(null);
    api
      .get(`/trades/${code}`)
      .then((res) => {
        const payload = res.data as {
          events?: TradeEvent[];
          warnings?: ApiWarnings;
          errors?: string[];
          currentPosition?: { longLots: number; shortLots: number };
          currentPositions?: CurrentPosition[];
        };
        if (!payload || !Array.isArray(payload.events)) {
          throw new Error("Trades response is invalid");
        }
        setTrades(payload.events ?? []);
        if (Array.isArray(payload.currentPositions)) {
          setCurrentPositionsFromApi(payload.currentPositions);
        } else {
          setCurrentPositionsFromApi(null);
        }
        setTradeWarnings(normalizeWarnings(payload.warnings));
        setTradeErrors(Array.isArray(payload.errors) ? payload.errors : []);
      })
      .catch((error) => {
        const message = error?.message || "Trades fetch failed";
        setTradeErrors([message]);
        setTrades([]);
        setTradeWarnings({ items: [] });
        setCurrentPositionsFromApi(null);
      });
  }, [backendReady, code]);


  useEffect(() => {
    if (!backendReady) return;
    if (!compareCode) return;
    api
      .get(`/trades/${compareCode}`)
      .then((res) => {
        const payload = res.data as {
          events?: TradeEvent[];
          errors?: string[];
        };
        if (!payload || !Array.isArray(payload.events)) {
          throw new Error("Trades response is invalid");
        }
        setCompareTrades(payload.events ?? []);
      })
      .catch((error) => {
        const message = error?.message || "Trades fetch failed";
        setCompareTrades([]);
      });
  }, [backendReady, compareCode]);

  const dailyParse = useMemo(() => buildCandlesWithStats(dailyData), [dailyData]);
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

  const dailyEventMarkers = useMemo(() => {
    const eventMs = parseEventDateMs(activeTicker?.eventEarningsDate);
    if (eventMs == null || dailyCandles.length === 0) return [];
    const eventTime = Math.floor(eventMs / 1000);
    const nearestTime = findNearestCandleTime(dailyCandles, eventTime);
    if (nearestTime == null) return [];
    if (Math.abs(nearestTime - eventTime) > MAX_EVENT_OFFSET_SEC) return [];
    return [{ time: nearestTime, kind: "earnings", label: "E" }];
  }, [activeTicker?.eventEarningsDate, dailyCandles]);

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
  const tradeWarningItems = tradeWarnings.items ?? [];
  const tradeInfoItems = tradeWarnings.info ?? [];
  const unrecognizedCount = tradeWarnings.unrecognized_labels?.count ?? 0;
  const errors = [...dailyErrors, ...monthlyErrors, ...tradeErrors];
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

  // Cursor mode functions
  const toggleCursorMode = () => {
    setCursorMode(prev => !prev);
    if (!cursorMode && dailyCandles.length > 0) {
      // Initialize with last bar when turning on
      updateSelectedBar(dailyCandles.length - 1);
    }
  };

  const updateSelectedBar = (index: number) => {
    if (index < 0 || index >= dailyCandles.length) return;

    const bar = dailyCandles[index];
    setSelectedBarIndex(index);
    setSelectedBarData(bar);

    // Convert time to date string (YYYY-MM-DD)
    const date = new Date(bar.time * 1000);
    const dateStr = date.toISOString().split('T')[0];
    setSelectedDate(dateStr);

    // Auto-pan if needed
    autoPanToBar(bar.time);
  };

  const autoPanToBar = (time: number) => {
    if (!dailyChartRef.current) return;

    // Get current visible range from resolvedDailyVisibleRange
    if (!resolvedDailyVisibleRange) return;

    const { from, to } = resolvedDailyVisibleRange;
    const rangeSize = to - from;
    const margin = rangeSize * 0.1; // 10% margin

    // Check if time is outside visible range
    if (time < from + margin || time > to - margin) {
      // Pan to center the selected bar
      const newFrom = time - rangeSize / 2;
      const newTo = time + rangeSize / 2;
      dailyChartRef.current.setVisibleRange({ from: newFrom, to: newTo });
    }
  };

  const moveToPrevDay = () => {
    if (selectedBarIndex === null || selectedBarIndex <= 0) return;
    updateSelectedBar(selectedBarIndex - 1);
  };

  const moveToNextDay = () => {
    if (selectedBarIndex === null || selectedBarIndex >= dailyCandles.length - 1) return;
    updateSelectedBar(selectedBarIndex + 1);
  };

  const handleDailyChartClick = (time: number | null) => {
    if (!cursorMode || time === null) return;

    // Find nearest bar index
    let nearestIndex = -1;
    let minDiff = Infinity;

    for (let i = 0; i < dailyCandles.length; i++) {
      const diff = Math.abs(dailyCandles[i].time - time);
      if (diff < minDiff) {
        minDiff = diff;
        nearestIndex = i;
      }
    }

    if (nearestIndex >= 0) {
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
    const ma7Line = dailyMaLines.find(line => line.period === 7);
    const ma20Line = dailyMaLines.find(line => line.period === 20);
    const ma60Line = dailyMaLines.find(line => line.period === 60);

    const getMaTrend = (maLine: typeof ma7Line, barIndex: number | null) => {
      if (!maLine || barIndex == null || barIndex < 1) return "--";
      const currentValue = maLine.data.find(d => d.time === selectedBarData.time)?.value;
      const prevBar = dailyCandles[barIndex - 1];
      const prevValue = prevBar ? maLine.data.find(d => d.time === prevBar.time)?.value : null;
      if (currentValue == null || prevValue == null) return "--";
      if (selectedBarData.close > currentValue && prevBar.close > prevValue) return "UP";
      if (selectedBarData.close < currentValue && prevBar.close < prevValue) return "DOWN";
      return "FLAT";
    };

    const barIndex = dailyCandles.findIndex(c => c.time === selectedTime);

    if (ma7Line?.visible) {
      const value = ma7Line.data.find(d => d.time === selectedTime)?.value;
      if (value != null) {
        maData.ma7 = { value, trend: getMaTrend(ma7Line, barIndex) };
      }
    }
    if (ma20Line?.visible) {
      const value = ma20Line.data.find(d => d.time === selectedTime)?.value;
      if (value != null) {
        maData.ma20 = { value, trend: getMaTrend(ma20Line, barIndex) };
      }
    }
    if (ma60Line?.visible) {
      const value = ma60Line.data.find(d => d.time === selectedTime)?.value;
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
      volume: dailyVolume.find(v => v.time === selectedBarData.time)?.value,
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
    return maSettings.daily.map((setting) => ({
      key: setting.key,
      label: setting.label,
      period: setting.period,
      color: setting.color,
      visible: setting.visible,
      lineWidth: setting.lineWidth,
      data: computeMA(dailyCandles, setting.period)
    }));
  }, [dailyCandles, maSettings.daily]);
  const compareDailyMaLines = useMemo(() => {
    return compareMaSettings.daily.map((setting) => ({
      key: setting.key,
      label: setting.label,
      period: setting.period,
      color: setting.color,
      visible: setting.visible,
      lineWidth: setting.lineWidth,
      data: computeMA(compareDailyCandles, setting.period)
    }));
  }, [compareDailyCandles, compareMaSettings.daily]);
  const memoPanelData = useDetailInfo(
    selectedBarData,
    selectedBarIndex ?? -1,
    dailyCandles,
    dailyPositions,
    dailyMaLines
  );

  const weeklyMaLines = useMemo(() => {
    return maSettings.weekly.map((setting) => ({
      key: setting.key,
      label: setting.label,
      period: setting.period,
      color: setting.color,
      visible: setting.visible,
      lineWidth: setting.lineWidth,
      data: computeMA(weeklyCandles, setting.period)
    }));
  }, [weeklyCandles, maSettings.weekly]);

  const monthlyMaLines = useMemo(() => {
    return maSettings.monthly.map((setting) => ({
      key: setting.key,
      label: setting.label,
      period: setting.period,
      color: setting.color,
      visible: setting.visible,
      lineWidth: setting.lineWidth,
      data: computeMA(monthlyCandles, setting.period)
    }));
  }, [monthlyCandles, maSettings.monthly]);
  const compareMonthlyMaLines = useMemo(() => {
    return compareMaSettings.monthly.map((setting) => ({
      key: setting.key,
      label: setting.label,
      period: setting.period,
      color: setting.color,
      visible: setting.visible,
      lineWidth: setting.lineWidth,
      data: computeMA(compareMonthlyCandles, setting.period)
    }));
  }, [compareMonthlyCandles, compareMaSettings.monthly]);

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
  const compareMonthlyVisibleRange = useMemo(() => {
    if (!rangeMonths) return null;
    if (compareMonthlyTargetRange) return compareMonthlyTargetRange;
    return buildRange(compareMonthlyCandles, rangeMonths);
  }, [rangeMonths, compareMonthlyTargetRange, compareMonthlyCandles]);
  const compareMonthlyBaseRange = useMemo(() => {
    if (!rangeMonths) return null;
    if (mainMonthlyTargetRange) return mainMonthlyTargetRange;
    return buildRange(monthlyCandles, rangeMonths);
  }, [rangeMonths, mainMonthlyTargetRange, monthlyCandles]);
  const compareRequiredFrom = useMemo(
    () => compareDailyTargetRange?.from ?? null,
    [compareDailyTargetRange]
  );
  const compareDailyVisibleRange = useMemo(() => {
    if (manualCompareDailyRangeRef.current) return manualCompareDailyRangeRef.current;
    if (!compareDailyTargetRange) return null;
    if (!compareDailyCandles.length) return null;
    return compareDailyTargetRange;
  }, [compareDailyTargetRange, compareDailyCandles]);
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
    if (compareDailyTargetRange) {
      return `一致期間: ${formatDateLabel(compareDailyTargetRange.from)} - ${formatDateLabel(compareDailyTargetRange.to)}`;
    }
    if (compareAsOfTime) {
      return `一致日: ${formatDateLabel(compareAsOfTime)}`;
    }
    return "一致期間: --";
  }, [compareDailyTargetRange, compareAsOfTime]);
  const leftMonthlyRangeLabel = useMemo(() => {
    if (mainMonthlyTargetRange) {
      return `対象期間: ${formatDateLabel(mainMonthlyTargetRange.from)} - ${formatDateLabel(mainMonthlyTargetRange.to)}`;
    }
    return `表示期間: ${dailyRangeLabel}`;
  }, [mainMonthlyTargetRange, dailyRangeLabel]);
  const rightMonthlyRangeLabel = useMemo(() => {
    if (compareMonthlyVisibleRange) {
      return `一致期間: ${formatDateLabel(compareMonthlyVisibleRange.from)} - ${formatDateLabel(compareMonthlyVisibleRange.to)}`;
    }
    return `表示期間: ${dailyRangeLabel}`;
  }, [compareMonthlyVisibleRange, dailyRangeLabel]);
  const compareDailyNeedsMore = useMemo(() => {
    if (!compareDailyTargetRange || !compareDailyCandles.length) return false;
    const earliest = compareDailyCandles[0]?.time;
    if (!earliest) return false;
    const hasMore = compareDailyData.length >= compareDailyLimit;
    return compareDailyTargetRange.from < earliest && hasMore;
  }, [compareDailyTargetRange, compareDailyCandles, compareDailyData.length, compareDailyLimit]);
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
    setCompareDailyLimit((prev) => prev + LIMIT_STEP.daily);
  }, [compareCode, compareDailyLoading, compareDailyNeedsMore]);
  useEffect(() => {
    if (!compareCode) return;
    if (loadingMonthly || compareLoading) return;
    if (!mainMonthlyNeedsMore && !compareMonthlyNeedsMore) return;
    setMonthlyLimit((prev) => prev + LIMIT_STEP.monthly);
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
  }, [cursorMode, selectedBarIndex, dailyCandles]);


  const compareHasMoreDaily = compareDailyData.length >= compareDailyLimit;
  const compareHasMoreMonthly = compareMonthlyData.length >= monthlyLimit; // monthlyLimit is shared

  const mainSync = useChartSync(dailyChartRef, monthlyChartRef, weeklyChartRef, {
    enabled: syncRanges ?? true,
    cursorEnabled: true,
    onLoadMoreDaily: () => setDailyLimit((prev) => prev + LIMIT_STEP.daily),
    onLoadMoreMonthly: () => setMonthlyLimit((prev) => prev + LIMIT_STEP.monthly),
    hasMoreDaily,
    loadingDaily,
    hasMoreMonthly,
    loadingMonthly,
    dailyCandles,
    monthlyCandles
  });

  const compareSync = useChartSync(compareDailyChartRef, compareMonthlyChartRef, undefined, {
    enabled: syncRanges ?? true,
    cursorEnabled: true,
    onLoadMoreDaily: () => setCompareDailyLimit((prev) => prev + LIMIT_STEP.daily),
    // compare monthly load more is implicitly handled by shared monthlyLimit, but comparing data length:
    onLoadMoreMonthly: () => setMonthlyLimit((prev) => prev + LIMIT_STEP.monthly),
    hasMoreDaily: compareHasMoreDaily,
    loadingDaily: compareDailyLoading,
    hasMoreMonthly: compareHasMoreMonthly,
    loadingMonthly: compareLoading, // compareLoading is for monthly
    dailyCandles: compareDailyCandles,
    monthlyCandles: compareMonthlyCandles
  });

  // Removed scheduleHoverTime

  const showVolumeDaily = dailyVolume.length > 0 && showVolumeEnabled;
  const gapBandsOverride = showGapBands ? undefined : [];

  const handleDailyVisibleRangeChange = (range: { from: number; to: number } | null) => {
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
    if (rangeMonths) return;
    compareSync.handleMonthlyVisibleRangeChange(range);
  };

  const handleCompareDailyVisibleRangeChange = (range: { from: number; to: number } | null) => {
    compareSync.handleDailyVisibleRangeChange(range);
    if (!rangeMonths && range) {
      manualCompareDailyRangeRef.current = range;
    }
  };

  const loadMoreDailyAndMonthly = () => {
    if (hasMoreDaily) {
      setDailyLimit((prev) => prev + LIMIT_STEP.daily);
    }
    if (hasMoreMonthly) {
      setMonthlyLimit((prev) => prev + LIMIT_STEP.monthly);
    }
  };
  const loadMoreDisabled = loadingDaily || loadingMonthly || (!hasMoreDaily && !hasMoreMonthly);
  const loadMoreLabel =
    loadingDaily || loadingMonthly
      ? "Loading..."
      : hasMoreDaily || hasMoreMonthly
        ? "Load more daily/monthly"
        : "All loaded";

  const toggleRange = (months: number) => {
    setRangeMonths((prev) => (prev === months ? null : months));
  };

  // Visible range sync is handled by hook; wrapper keeps manual range for load-more.

  const parseBarsResponse = (payload: BarsResponse | number[][], label: string) => {
    if (Array.isArray(payload)) {
      return { rows: payload, errors: [] as string[] };
    }
    if (payload && Array.isArray(payload.data)) {
      return {
        rows: payload.data,
        errors: Array.isArray(payload.errors) ? payload.errors : []
      };
    }
    return { rows: [], errors: [`${label}_response_invalid`] };
  };

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
    } catch (error) {
      setToastMessage("削除に失敗しました");
    } finally {
      setDeleteBusy(false);
    }
  };

  /* Handlers replaced by hooks */
  const handleDailyCrosshair = mainSync.handleDailyCrosshair;
  const handleWeeklyCrosshair = mainSync.handleWeeklyCrosshair;
  const handleMonthlyCrosshair = mainSync.handleMonthlyCrosshair;

  const handleCompareMonthlyCrosshair = (
    time: number | null,
    source: "left" | "right",
    point?: { x: number; y: number } | null
  ) => {
    if (source === "left") {
      // Main chart (Left)
      mainSync.handleMonthlyCrosshair(time, point ?? null);
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
      mainSync.handleDailyCrosshair(time, point ?? null);
    } else {
      compareSync.handleDailyCrosshair(time, point ?? null);
    }
  };

  const dailyEmptyMessage = dailyCandles.length === 0 ? dailyError ?? "No data" : null;
  const weeklyEmptyMessage = weeklyCandles.length === 0 ? dailyError ?? "No data" : null;
  const monthlyEmptyMessage = monthlyCandles.length === 0 ? monthlyError ?? "No data" : null;

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
      candidate === "/candidates"
    ) {
      return candidate;
    }
    return "/";
  }, [location.state]);
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
  }, [code]);
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
  }, [code, compareCode, mainAsOf]);
  const compareListItems = compareList?.items ?? [];
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

          const dailyVolumeMap = new Map(dailyVolume.map((item) => [item.time, item.value]));
          const weeklyVolumeCounts = new Map<number, number>();
          dailyCandles.forEach((candle) => {
            if (!dailyVolumeMap.has(candle.time)) return;
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
            const volume = dailyVolumeMap.get(candle.time);
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
              volume: dailyVolumeMap.get(c.time) ?? null
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
      <div className="detail-content">
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
                      maLines={monthlyMaLines}
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
                    <DetailChart
                      ref={compareMonthlyChartRef}
                      candles={compareMonthlyCandles}
                      volume={[]}
                      maLines={compareMonthlyMaLines}
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
                      visibleRange={compareMonthlyCandles.length ? compareMonthlyVisibleRange : null}
                      onCrosshairMove={(time, point) =>
                        handleCompareMonthlyCrosshair(time, "right", point)
                      }
                      onVisibleRangeChange={handleCompareMonthlyVisibleRangeChange}
                    />
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
                    <DetailChart
                      ref={dailyChartRef}
                      candles={dailyCandles}
                      volume={dailyVolume}
                      maLines={dailyMaLines}
                      showVolume={showVolumeDaily}
                      eventMarkers={dailyEventMarkers}
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
                        showMarkers: true,
                        showPnL: showPnLPanel,
                        hoverTime: mainSync.hoverTime,
                        currentPositions,
                        latestTradeTime
                      }}
                      onCrosshairMove={(time, point) =>
                        handleCompareDailyCrosshair(time, "left", point)
                      }
                      onVisibleRangeChange={handleDailyVisibleRangeChange}
                    />
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
                    <DetailChart
                      ref={compareDailyChartRef}
                      candles={compareDailyCandles}
                      volume={compareDailyVolume}
                      maLines={compareDailyMaLines}
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
                        showMarkers: true,
                        showPnL: showPnLPanel,
                        hoverTime: compareSync.hoverTime
                      }}
                      onCrosshairMove={(time, point) =>
                        handleCompareDailyCrosshair(time, "right", point)
                      }
                      onVisibleRangeChange={handleCompareDailyVisibleRangeChange}
                    />
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
                  <DetailChart
                    ref={dailyChartRef}
                    candles={dailyCandles}
                    volume={dailyVolume}
                    maLines={dailyMaLines}
                    showVolume={showVolumeDaily}
                    eventMarkers={dailyEventMarkers}
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
                      showMarkers: true,
                      showPnL: showPnLPanel,
                      hoverTime: mainSync.hoverTime,
                      currentPositions,
                      latestTradeTime
                    }}
                    onCrosshairMove={handleDailyCrosshair}
                    onVisibleRangeChange={handleDailyVisibleRangeChange}
                  />
                )}
                {focusPanel === "weekly" && (
                  <DetailChart
                    ref={weeklyChartRef}
                    candles={weeklyCandles}
                    volume={weeklyVolume}
                    maLines={weeklyMaLines}
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
                      hoverTime,
                      currentPositions,
                      latestTradeTime
                    }}
                    onCrosshairMove={handleWeeklyCrosshair}
                    onVisibleRangeChange={handleWeeklyVisibleRangeChange}
                  />
                )}
                {focusPanel === "monthly" && (
                  <DetailChart
                    ref={monthlyChartRef}
                    candles={monthlyCandles}
                    volume={monthlyVolume}
                    maLines={monthlyMaLines}
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
                      hoverTime,
                      currentPositions,
                      latestTradeTime
                    }}
                    onCrosshairMove={handleMonthlyCrosshair}
                    onVisibleRangeChange={handleMonthlyVisibleRangeChange}
                  />
                )}
                {focusPanel === "daily" && dailyEmptyMessage && (
                  <div className="detail-chart-empty">Daily: {dailyEmptyMessage}</div>
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
                  <DetailChart
                    ref={dailyChartRef}
                    candles={dailyCandles}
                    volume={dailyVolume}
                    maLines={dailyMaLines}
                    showVolume={showVolumeDaily}
                    eventMarkers={dailyEventMarkers}
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
                      showMarkers: true,
                      showPnL: showPnLPanel,
                      hoverTime: mainSync.hoverTime,
                      currentPositions,
                      latestTradeTime
                    }}
                    cursorTime={cursorMode && selectedBarData ? selectedBarData.time : null}
                    onCrosshairMove={handleDailyCrosshair}
                    onVisibleRangeChange={handleDailyVisibleRangeChange}
                    onChartClick={handleDailyChartClick}
                  />
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
                      maLines={weeklyMaLines}
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
                      maLines={monthlyMaLines}
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
          <div className="daily-memo-panel detail-analysis-panel">
            <div className="memo-panel-header">
              <h3>解析結果</h3>
            </div>
            <div className="detail-analysis-body">
              {analysisDtLabel && (
                <div className="detail-analysis-meta">基準日 {analysisDtLabel}</div>
              )}
              {cursorMode && selectedDate && (
                <div className="detail-analysis-meta">カーソル日 {selectedDate}</div>
              )}
              {canShowPhase && phaseReasons[0] && (
                <div className="detail-analysis-meta">局面メモ {phaseReasons[0]}</div>
              )}
              {canShowAnalysis ? (
                <>
                  <div className="detail-analysis-section">
                    <div className="detail-analysis-section-title">売買サマリー</div>
                    <div className={`detail-analysis-regime detail-analysis-regime--${analysisDecision.tone}`}>
                      <div className="detail-analysis-call-head">
                        <span className={`detail-analysis-call-badge detail-analysis-call-badge--${analysisDecision.tone}`}>
                          判定 {analysisDecision.sideLabel}
                        </span>
                        <span className="detail-analysis-call-confidence">
                          確信度 {analysisSummaryLoading ? "読込中..." : analysisGuidance.confidenceRank}
                        </span>
                      </div>
                      <div className="detail-analysis-call-action">{analysisGuidance.action}</div>
                      <div className="detail-analysis-regime-title">{patternSummary.environmentLabel}</div>
                      <div className="detail-analysis-call-pattern">
                        想定チャートパターン {analysisDecision.patternLabel}
                      </div>
                      <div className="detail-analysis-regime-text">{analysisGuidance.watchpoint}</div>
                      <div className="detail-analysis-entry-plan">
                        <div className="detail-analysis-entry-plan-title">{analysisGuidance.buyTimingTitle}</div>
                        {analysisGuidance.buyTimingPlan.map((step, index) => (
                          <div key={`analysis-entry-plan-${index}`} className="detail-analysis-entry-plan-item">
                            {step}
                          </div>
                        ))}
                      </div>
                      <div className="detail-analysis-prob-meter-list">
                        <div className="detail-analysis-prob-meter-row tone-up">
                          <div className="detail-analysis-prob-meter-label">
                            買い {analysisSummaryLoading ? "読込中..." : formatPercentLabel(analysisDecision.buyProb)}
                          </div>
                          <div className="detail-analysis-prob-meter-track">
                            <div className="detail-analysis-prob-meter-fill" style={{ width: `${analysisGuidance.buyWidth}%` }} />
                          </div>
                        </div>
                        <div className="detail-analysis-prob-meter-row tone-down">
                          <div className="detail-analysis-prob-meter-label">
                            売り {analysisSummaryLoading ? "読込中..." : formatPercentLabel(analysisDecision.sellProb)}
                          </div>
                          <div className="detail-analysis-prob-meter-track">
                            <div className="detail-analysis-prob-meter-fill" style={{ width: `${analysisGuidance.sellWidth}%` }} />
                          </div>
                        </div>
                        <div className="detail-analysis-prob-meter-row tone-neutral">
                          <div className="detail-analysis-prob-meter-label">
                            中立 {analysisSummaryLoading ? "読込中..." : formatPercentLabel(analysisDecision.neutralProb)}
                          </div>
                          <div className="detail-analysis-prob-meter-track">
                            <div className="detail-analysis-prob-meter-fill" style={{ width: `${analysisGuidance.neutralWidth}%` }} />
                          </div>
                        </div>
                      </div>
                      <div className="detail-analysis-call-reason-list">
                        {analysisGuidance.reasonLines.length ? (
                          analysisGuidance.reasonLines.map((reason, index) => (
                            <div key={`analysis-call-reason-${index}`} className="detail-analysis-call-reason">
                              {reason}
                            </div>
                          ))
                        ) : (
                          <div className="detail-analysis-empty">根拠データがありません。</div>
                        )}
                      </div>
                    </div>
                    {analysisSummaryLoading && (
                      <div className="detail-analysis-meta">一部データ読込中のため、確率は暫定表示です。</div>
                    )}
                    {analysisDtLabel && (
                      <div className="detail-analysis-meta">買い基準日 {analysisDtLabel}</div>
                    )}
                    {sellAnalysisDtLabel && (
                      <div className="detail-analysis-meta">売り基準日 {sellAnalysisDtLabel}</div>
                    )}
                    {sellPredDtLabel && (
                      <div className="detail-analysis-meta">予測スナップショット {sellPredDtLabel}</div>
                    )}
                  </div>
                </>
              ) : (
                <div className="detail-analysis-empty">分析データがありません。</div>
              )}
            </div>
          </div>
        )}
      </div>
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
      {showPositionLedger && (
        <div
          className={`position-ledger-sheet ${positionLedgerExpanded ? "is-expanded" : "is-mini"
            }`}
        >
          <button
            type="button"
            className="position-ledger-handle"
            onClick={() => setPositionLedgerExpanded((prev) => !prev)}
            aria-label={positionLedgerExpanded ? "建玉推移を折りたたむ" : "建玉推移を展開する"}
          />
          <div className="position-ledger-header">
            <div className="position-ledger-header-main">
              <div>
                <div className="position-ledger-title">建玉推移（証券会社別）</div>
                <div className="position-ledger-sub">証券会社別に集計</div>
              </div>
              <div className="position-ledger-toggle" role="tablist" aria-label="表示モード">
                <span className="position-ledger-toggle-label">表示モード:</span>
                <button
                  type="button"
                  className={ledgerViewMode === "iizuka" ? "is-active" : ""}
                  onClick={() => {
                    setLedgerViewMode("iizuka");
                    try {
                      window.localStorage.setItem("positionLedgerMode", "iizuka");
                    } catch {
                      // ignore storage errors
                    }
                  }}
                >
                  飯塚式（玉）
                </button>
                <button
                  type="button"
                  className={ledgerViewMode === "stock" ? "is-active" : ""}
                  onClick={() => {
                    setLedgerViewMode("stock");
                    try {
                      window.localStorage.setItem("positionLedgerMode", "stock");
                    } catch {
                      // ignore storage errors
                    }
                  }}
                >
                  株式（株）
                </button>
              </div>
            </div>
            <button
              type="button"
              className="position-ledger-close"
              onClick={() => {
                setHeaderMode("chart");
                setPositionLedgerExpanded(false);
              }}
              aria-label="建玉推移を閉じる"
            >
              x
            </button>
          </div>
          {!ledgerEligible ? (
            <div className="position-ledger-empty">
              建玉推移の対象データがありません。
            </div>
          ) : (
            <div className="position-ledger-group-list">
              {ledgerViewMode === "iizuka"
                ? ledgerIizukaGroups.map((group) => (
                  <div
                    key={`${group.brokerKey}-${group.account}`}
                    className={`position-ledger-group broker-${group.brokerKey}`}
                  >
                    <div className="position-ledger-group-header">
                      <span className="broker-badge">{group.brokerLabel}</span>
                      {group.account && (
                        <span className="position-ledger-account">{group.account}</span>
                      )}
                    </div>
                    <div className="position-ledger-table is-iizuka">
                      <div className="position-ledger-row position-ledger-head">
                        <span className="position-ledger-cell position-ledger-sticky-left" title="日付">
                          日付
                        </span>
                        <span className="position-ledger-cell position-ledger-sticky-left second" title="取引種別">
                          区分
                        </span>
                        <span className="position-ledger-cell align-right" title="売玉の増減">
                          当日Δ（売玉）
                        </span>
                        <span className="position-ledger-cell align-right" title="買玉の増減">
                          当日Δ（買玉）
                        </span>
                        <span className="position-ledger-cell align-right" title="当日引けの売玉">
                          当日引け（売玉）
                        </span>
                        <span className="position-ledger-cell align-right" title="当日引けの買玉">
                          当日引け（買玉）
                        </span>
                        <span className="position-ledger-cell align-right" title="建玉表記（売-買）">
                          建玉表記
                        </span>
                        <span className="position-ledger-cell align-right" title="買い単価（玉）">
                          買い単価
                        </span>
                        <span className="position-ledger-cell align-right" title="売り単価（玉）">
                          売り単価
                        </span>
                        <span
                          className="position-ledger-cell align-right"
                          title="実現損益（返済・現渡などで確定した分）"
                        >
                          損益（実現）
                        </span>
                      </div>
                      {group.rows.map((row, index) => {
                        const realizedClass =
                          row.realizedDelta === 0
                            ? "position-ledger-pnl"
                            : row.realizedDelta > 0
                              ? "position-ledger-pnl up"
                              : "position-ledger-pnl down";
                        return (
                          <div
                            className={`position-ledger-row ${index % 2 === 0 ? "is-even" : "is-odd"}`}
                            key={`${row.date}-${index}`}
                          >
                            <span className="position-ledger-cell position-ledger-sticky-left">
                              {formatLedgerDate(row.date)}
                            </span>
                            <span className="position-ledger-cell position-ledger-sticky-left second position-ledger-kind">
                              {row.kindLabel}
                            </span>
                            <span className="position-ledger-cell align-right">
                              {formatSignedLot(row.deltaShort)}
                            </span>
                            <span className="position-ledger-cell align-right">
                              {formatSignedLot(row.deltaLong)}
                            </span>
                            <span className="position-ledger-cell align-right">
                              {formatLotValue(row.shortLots)}
                            </span>
                            <span className="position-ledger-cell align-right">
                              {formatLotValue(row.longLots)}
                            </span>
                            <span className="position-ledger-cell align-right">
                              {`${formatLotValue(row.shortLots)}-${formatLotValue(row.longLots)}`}
                            </span>
                            <span className="position-ledger-cell align-right">
                              {row.avgLongPrice != null ? formatNumber(row.avgLongPrice, 2) : "--"}
                            </span>
                            <span className="position-ledger-cell align-right">
                              {row.avgShortPrice != null ? formatNumber(row.avgShortPrice, 2) : "--"}
                            </span>
                            <span className={`position-ledger-cell align-right ${realizedClass}`}>
                              {formatSignedNumber(row.realizedDelta, 0)}
                            </span>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                ))
                : ledgerStockGroups.map((group) => (
                  <div
                    key={`${group.brokerKey}-${group.account}`}
                    className={`position-ledger-group broker-${group.brokerKey}`}
                  >
                    <div className="position-ledger-group-header">
                      <span className="broker-badge">{group.brokerLabel}</span>
                      {group.account && (
                        <span className="position-ledger-account">{group.account}</span>
                      )}
                    </div>
                    <div className="position-ledger-table is-stock">
                      <div className="position-ledger-row position-ledger-head">
                        <span className="position-ledger-cell position-ledger-sticky-left" title="日付">
                          日付
                        </span>
                        <span className="position-ledger-cell position-ledger-sticky-left second" title="取引種別">
                          区分
                        </span>
                        <span className="position-ledger-cell align-right" title="約定数量（株）。100株=1玉。">
                          数量（株）
                        </span>
                        <span className="position-ledger-cell align-right" title="売株の増減">
                          当日Δ（売株）
                        </span>
                        <span className="position-ledger-cell align-right" title="買株の増減">
                          当日Δ（買株）
                        </span>
                        <span className="position-ledger-cell align-right" title="当日引けの売株">
                          当日引け（売株）
                        </span>
                        <span className="position-ledger-cell align-right" title="当日引けの買株">
                          当日引け（買株）
                        </span>
                        <span className="position-ledger-cell align-right" title="買い単価（株）">
                          買い単価
                        </span>
                        <span className="position-ledger-cell align-right" title="売り単価（株）">
                          売り単価
                        </span>
                        <span
                          className="position-ledger-cell align-right"
                          title="実現損益（返済・現渡などで確定した分）"
                        >
                          損益（実現）
                        </span>
                      </div>
                      {group.rows.map((row, index) => {
                        const realizedClass =
                          row.realizedDelta === 0
                            ? "position-ledger-pnl"
                            : row.realizedDelta > 0
                              ? "position-ledger-pnl up"
                              : "position-ledger-pnl down";
                        return (
                          <div
                            className={`position-ledger-row ${index % 2 === 0 ? "is-even" : "is-odd"}`}
                            key={`${row.date}-${index}`}
                          >
                            <span className="position-ledger-cell position-ledger-sticky-left">
                              {formatLedgerDate(row.date)}
                            </span>
                            <span className="position-ledger-cell position-ledger-sticky-left second position-ledger-kind">
                              {row.kindLabel}
                            </span>
                            <span className="position-ledger-cell align-right">
                              {formatShares(row.qtyShares)}
                            </span>
                            <span className="position-ledger-cell align-right">
                              {formatSignedNumber(row.deltaSellShares, 0)}
                            </span>
                            <span className="position-ledger-cell align-right">
                              {formatSignedNumber(row.deltaBuyShares, 0)}
                            </span>
                            <span className="position-ledger-cell align-right">
                              {formatShares(row.closeSellShares)}
                            </span>
                            <span className="position-ledger-cell align-right">
                              {formatShares(row.closeBuyShares)}
                            </span>
                            <span className="position-ledger-cell align-right">
                              {row.buyAvgPrice != null ? formatNumber(row.buyAvgPrice, 2) : "--"}
                            </span>
                            <span className="position-ledger-cell align-right">
                              {row.sellAvgPrice != null ? formatNumber(row.sellAvgPrice, 2) : "--"}
                            </span>
                            <span className={`position-ledger-cell align-right ${realizedClass}`}>
                              {formatSignedNumber(row.realizedDelta, 0)}
                            </span>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                ))}
            </div>
          )}
        </div>
      )}
      {hasIssues && (
        <div className={`detail-debug-banner ${bannerTone}`}>
          <button
            type="button"
            className="detail-debug-toggle"
            onClick={() => setDebugOpen((prev) => !prev)}
          >
            {`${bannerTitle}${debugSummary.length ? ` (${debugSummary.join(", ")})` : ""}`}
          </button>
          {debugOpen && (
            <div className="detail-debug-panel">
              <div className="detail-debug-header">
                <div className="detail-debug-title">Debug Details</div>
                <div className="detail-debug-actions">
                  <button
                    type="button"
                    className="detail-debug-copy"
                    onClick={handleCopyDebug}
                    title="Copy"
                    aria-label="Copy"
                  >
                    <IconCopy size={16} />
                  </button>
                  <button
                    type="button"
                    className="detail-debug-info-toggle"
                    onClick={() => setShowInfoDetails((prev) => !prev)}
                  >
                    {showInfoDetails ? "Info: ON" : "Info: OFF"}
                  </button>
                  <button
                    type="button"
                    className="detail-debug-close"
                    onClick={() => setDebugOpen(false)}
                  >
                    Close
                  </button>
                </div>
              </div>
              <div className="detail-debug-lines">
                {debugLines.map((line, index) => (
                  <div key={`${line}-${index}`}>{line}</div>
                ))}
              </div>
              {copyFallbackText && (
                <div className="detail-debug-fallback">
                  <div className="detail-debug-fallback-title">Copy failed</div>
                  <textarea readOnly value={copyFallbackText} />
                </div>
              )}
            </div>
          )}
        </div>
      )}
      {showIndicators && (
        <div className="indicator-overlay" onClick={() => setShowIndicators(false)}>
          <div className="indicator-panel" onClick={(event) => event.stopPropagation()}>
            <div className="indicator-header">
              <div className="indicator-title">Indicators</div>
              {compareCode && (
                <div className="ma-toggle">
                  <button
                    type="button"
                    className={`indicator-button${maEditMode === "main" ? " active" : ""}`}
                    onClick={() => setMaEditMode("main")}
                  >
                    株式
                  </button>
                  <button
                    type="button"
                    className={`indicator-button${maEditMode === "compare" ? " active" : ""}`}
                    onClick={() => setMaEditMode("compare")}
                  >
                    比較
                  </button>
                </div>
              )}
              <button className="indicator-close" onClick={() => setShowIndicators(false)}>
                Close
              </button>
            </div>
            {(["daily", "weekly", "monthly"] as Timeframe[]).map((frame) => (
              <div className="indicator-section" key={frame}>
                <div className="indicator-subtitle">Moving Averages ({frame})</div>
                <div className="indicator-rows">
                  {activeMaSettings[frame].map((setting, index) => (
                    <div className="indicator-row" key={setting.key}>
                      <input
                        type="checkbox"
                        checked={setting.visible}
                        onChange={() => updateSetting(frame, index, { visible: !setting.visible })}
                      />
                      <div className="indicator-label">{setting.label}</div>
                      <input
                        className="indicator-input"
                        type="number"
                        min={1}
                        value={setting.period}
                        onChange={(event) =>
                          updateSetting(frame, index, { period: Number(event.target.value) || 1 })
                        }
                      />
                      <input
                        className="indicator-input indicator-width"
                        type="number"
                        min={1}
                        max={6}
                        value={setting.lineWidth}
                        onChange={(event) =>
                          updateSetting(frame, index, { lineWidth: Number(event.target.value) })
                        }
                      />
                      <input
                        className="indicator-color-input"
                        type="color"
                        value={setting.color}
                        onChange={(event) => updateSetting(frame, index, { color: event.target.value })}
                      />
                    </div>
                  ))}
                </div>
                <button className="indicator-reset" onClick={() => resetSettings(frame)}>
                  Reset {frame}
                </button>
              </div>
            ))}
          </div>
        </div>
      )}
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



