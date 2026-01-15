import { useEffect, useMemo, useRef, useState } from "react";
import type { MouseEvent as ReactMouseEvent, TouchEvent as ReactTouchEvent } from "react";
import { useLocation, useNavigate, useParams } from "react-router-dom";
import {
  IconAdjustments,
  IconArrowBackUp,
  IconArrowLeft,
  IconArrowRight,
  IconCamera,
  IconCopy,
  IconHeart,
  IconHeartFilled,
  IconTrash,
  IconSparkles,
  IconChartArrows
} from "@tabler/icons-react";
import { api } from "../api";
import { useBackendReadyState } from "../backendReady";
import DetailChart, { DetailChartHandle } from "../components/DetailChart";
import Toast from "../components/Toast";
import IconButton from "../components/IconButton";
import SimilarSearchPanel from "../components/SimilarSearchPanel";
import { Box, MaSetting, useStore } from "../store";
import { computeSignalMetrics } from "../utils/signals";
import type { TradeEvent } from "../utils/positions";
import { buildDailyPositions, buildPositionLedger } from "../utils/positions";
import { captureAndCopyScreenshot, saveBlobToFile, getScreenType } from "../utils/windowScreenshot";
import { buildAIExport, copyToClipboard, saveAsFile } from "../utils/aiExport";
import { formatEventBadgeDate, formatEventDateYmd, parseEventDateMs } from "../utils/events";

type Timeframe = "daily" | "weekly" | "monthly";
type FocusPanel = Timeframe | null;

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

const DAILY_ROW_RATIO = 12 / 16;
const DEFAULT_WEEKLY_RATIO = 3 / 4;
const MIN_WEEKLY_RATIO = 0.2;
const MIN_MONTHLY_RATIO = 0.1;

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
  const [trades, setTrades] = useState<TradeEvent[]>([]);
  const [compareTrades, setCompareTrades] = useState<TradeEvent[]>([]);
  const [tradeWarnings, setTradeWarnings] = useState<ApiWarnings>({ items: [] });
  const [tradeErrors, setTradeErrors] = useState<string[]>([]);
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
  const eventsLastSuccessLabel = useMemo(() => {
    const earningsMs = parseEventDateMs(eventsMeta?.earningsLastSuccessAt);
    const rightsMs = parseEventDateMs(eventsMeta?.rightsLastSuccessAt);
    const candidates = [
      { value: eventsMeta?.earningsLastSuccessAt ?? null, ms: earningsMs },
      { value: eventsMeta?.rightsLastSuccessAt ?? null, ms: rightsMs }
    ].filter((item) => item.value && item.ms != null) as { value: string; ms: number }[];
    if (!candidates.length) return null;
    const oldest = candidates.reduce((prev, next) => (next.ms < prev.ms ? next : prev));
    return formatEventDateYmd(oldest.value);
  }, [eventsMeta]);
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

  const subtitle = "Daily / Weekly / Monthly";

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
    setLoadingDaily(true);
    setDailyErrors([]);
    setDailyFetch((prev) => ({ ...prev, status: "loading", errorMessage: null }));
    api
      .get("/ticker/daily", { params: { code, limit: dailyLimit } })
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
  }, [backendReady, code, dailyLimit]);

  useEffect(() => {
    if (!backendReady) return;
    if (!code) return;
    setLoadingMonthly(true);
    setMonthlyErrors([]);
    setMonthlyFetch((prev) => ({ ...prev, status: "loading", errorMessage: null }));
    api
      .get("/ticker/monthly", { params: { code, limit: monthlyLimit } })
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
  }, [backendReady, code, monthlyLimit]);

  useEffect(() => {
    if (!backendReady) return;
    if (!compareCode) return;
    setCompareLoading(true);
    setCompareMonthlyErrors([]);
    api
      .get("/ticker/monthly", { params: { code: compareCode, limit: monthlyLimit } })
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
    api
      .get("/ticker/daily", { params: { code: compareCode, limit: compareDailyLimit } })
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
    api
      .get("/ticker/boxes", { params: { code } })
      .then((res) => {
        const rows = (res.data || []) as Box[];
        setBoxes(rows);
      })
      .catch(() => {
        setBoxes([]);
      });
  }, [backendReady, code]);

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
    api
      .get("/ticker/boxes", { params: { code: compareCode } })
      .then((res) => {
        const rows = (res.data || []) as Box[];
        setCompareBoxes(rows);
      })
      .catch(() => {
        setCompareBoxes([]);
      });
  }, [backendReady, compareCode]);

  useEffect(() => {
    if (!backendReady) return;
    if (!code) return;
    setTradeErrors([]);
    setTradeWarnings({ items: [] });
    api
      .get(`/trades/${code}`)
      .then((res) => {
        const payload = res.data as {
          events?: TradeEvent[];
          warnings?: ApiWarnings;
          errors?: string[];
          currentPosition?: { buyUnits: number; sellUnits: number; text?: string };
        };
        if (!payload || !Array.isArray(payload.events)) {
          throw new Error("Trades response is invalid");
        }
        setTrades(payload.events ?? []);
        setTradeWarnings(normalizeWarnings(payload.warnings));
        setTradeErrors(Array.isArray(payload.errors) ? payload.errors : []);
      })
      .catch((error) => {
        const message = error?.message || "Trades fetch failed";
        setTradeErrors([message]);
        setTrades([]);
        setTradeWarnings({ items: [] });
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
  const dailyCandles = dailyParse.candles;
  const monthlyCandles = monthlyParse.candles;
  const compareDailyCandles = compareDailyParse.candles;
  const compareMonthlyCandles = compareMonthlyParse.candles;
  const dailyVolume = useMemo(() => buildVolume(dailyData), [dailyData]);
  const monthlyVolume = useMemo(() => buildVolume(monthlyData), [monthlyData]);
  const compareDailyVolume = useMemo(() => buildVolume(compareDailyData), [compareDailyData]);
  const weeklyData = useMemo(() => buildWeekly(dailyCandles, dailyVolume), [dailyCandles, dailyVolume]);

  const weeklyCandles = weeklyData.candles;
  const weeklyVolume = weeklyData.volume;
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
  const comparePositionData = useMemo(
    () => buildDailyPositions(compareDailyCandles, compareTrades),
    [compareDailyCandles, compareTrades]
  );
  const compareDailyPositions = comparePositionData.dailyPositions;
  const compareTradeMarkers = comparePositionData.tradeMarkers;
  const positionLedger = useMemo(() => buildPositionLedger(trades), [trades]);
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

    showShortToast("コピー失敗");
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
    () => (mainAsOfTime ? buildRangeFromEndTime(24, mainAsOfTime) : null),
    [mainAsOfTime]
  );
  const compareMonthlyTargetRange = useMemo(
    () => (compareAsOfTime ? buildRangeFromEndTime(24, compareAsOfTime) : null),
    [compareAsOfTime]
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
  const compareMonthlyVisibleRange = useMemo(() => {
    if (compareMonthlyTargetRange) return compareMonthlyTargetRange;
    return buildRange(compareMonthlyCandles, 24);
  }, [compareMonthlyTargetRange, compareMonthlyCandles]);
  const compareMonthlyBaseRange = useMemo(() => {
    if (mainMonthlyTargetRange) return mainMonthlyTargetRange;
    return buildRange(monthlyCandles, 24);
  }, [mainMonthlyTargetRange, monthlyCandles]);
  const compareRequiredFrom = useMemo(
    () => compareDailyTargetRange?.from ?? null,
    [compareDailyTargetRange]
  );
  const compareDailyVisibleRange = useMemo(() => {
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
    return "24本";
  }, [mainMonthlyTargetRange]);
  const rightMonthlyRangeLabel = useMemo(() => {
    if (compareMonthlyVisibleRange) {
      return `一致期間: ${formatDateLabel(compareMonthlyVisibleRange.from)} - ${formatDateLabel(compareMonthlyVisibleRange.to)}`;
    }
    return "24本";
  }, [compareMonthlyVisibleRange]);
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
          setShowPositionLedger(false);
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

  useEffect(() => {
    if (hoverRafRef.current !== null) {
      window.cancelAnimationFrame(hoverRafRef.current);
      hoverRafRef.current = null;
    }
    hoverTimePendingRef.current = null;
    hoverTimeRef.current = null;
    setHoverTime(null);
    dailyChartRef.current?.clearCrosshair();
    weeklyChartRef.current?.clearCrosshair();
    monthlyChartRef.current?.clearCrosshair();
  }, [focusPanel]);

  useEffect(() => {
    return () => {
      if (hoverRafRef.current !== null) {
        window.cancelAnimationFrame(hoverRafRef.current);
        hoverRafRef.current = null;
      }
    };
  }, []);

  useEffect(() => {
    return () => {
      if (syncRafRef.current !== null) {
        window.cancelAnimationFrame(syncRafRef.current);
        syncRafRef.current = null;
      }
    };
  }, []);

  const scheduleHoverTime = (time: number | null) => {
    hoverTimePendingRef.current = time;
    if (hoverRafRef.current !== null) return;
    hoverRafRef.current = window.requestAnimationFrame(() => {
      hoverRafRef.current = null;
      const next = hoverTimePendingRef.current ?? null;
      if (hoverTimeRef.current === next) return;
      hoverTimeRef.current = next;
      setHoverTime(next);
    });
  };

  const showVolumeDaily = dailyVolume.length > 0;

  const loadMoreDaily = () => {
    setDailyLimit((prev) => prev + LIMIT_STEP.daily);
  };

  const loadMoreMonthly = () => {
    setMonthlyLimit((prev) => prev + LIMIT_STEP.monthly);
  };

  const toggleRange = (months: number) => {
    setRangeMonths((prev) => (prev === months ? null : months));
  };

  const syncRangeToSecondary = (range: { from: number; to: number }) => {
    if (!syncRangesRef.current) return;
    const weeklyMin = weeklyCandles[0]?.time;
    const monthlyMin = monthlyCandles[0]?.time;
    if (weeklyMin && range.from < weeklyMin && hasMoreDaily && !loadingDaily) {
      loadMoreDaily();
    }
    if (monthlyMin && range.from < monthlyMin && hasMoreMonthly && !loadingMonthly) {
      loadMoreMonthly();
    }
    weeklyChartRef.current?.setVisibleRange(range);
    monthlyChartRef.current?.setVisibleRange(range);
  };

  const handleDailyVisibleRangeChange = (range: { from: number; to: number } | null) => {
    if (!range) return;
    pendingRangeRef.current = range;
    if (syncRafRef.current !== null) return;
    syncRafRef.current = window.requestAnimationFrame(() => {
      syncRafRef.current = null;
      const pending = pendingRangeRef.current;
      if (!pending) return;
      syncRangeToSecondary(pending);
    });
  };

  useEffect(() => {
    const pending = pendingRangeRef.current;
    if (!pending || !syncRangesRef.current) return;
    syncRangeToSecondary(pending);
  }, [weeklyCandles, monthlyCandles, loadingDaily, loadingMonthly]);

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
    if (!code) return;
    const next = !isFavorite;
    setFavoriteLocal(code, next);
    try {
      if (next) {
        await api.post(`/favorites/${encodeURIComponent(code)}`);
      } else {
        await api.delete(`/favorites/${encodeURIComponent(code)}`);
      }
    } catch {
      setFavoriteLocal(code, !next);
      setToastMessage("お気に入りの更新に失敗しました。");
    }
  };

  const handleDeleteTicker = async () => {
    if (!code || deleteBusy) return;
    const confirmed =
      typeof window === "undefined"
        ? false
        : window.confirm(
          `${code} を完全に削除しますか？\ncode.txt、data/txt、DB、お気に入り、練習セッションも削除します。`
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

  const handleDailyCrosshair = (time: number | null, point?: { x: number; y: number } | null) => {
    weeklyChartRef.current?.setCrosshair(time, null);
    monthlyChartRef.current?.setCrosshair(time, null);
    if (focusPanel === null || focusPanel === "daily") {
      scheduleHoverTime(time);
    }
  };

  const handleWeeklyCrosshair = (time: number | null, point?: { x: number; y: number } | null) => {
    dailyChartRef.current?.setCrosshair(time, null);
    monthlyChartRef.current?.setCrosshair(time, null);
    if (focusPanel === "weekly") {
      scheduleHoverTime(time);
    }
  };

  const handleMonthlyCrosshair = (time: number | null, point?: { x: number; y: number } | null) => {
    dailyChartRef.current?.setCrosshair(time, null);
    weeklyChartRef.current?.setCrosshair(time, null);
    if (focusPanel === "monthly") {
      scheduleHoverTime(time);
    }
  };

  const handleCompareMonthlyCrosshair = (time: number | null, source: "left" | "right") => {
    if (syncRangesRef.current) {
      if (source === "left") {
        compareMonthlyChartRef.current?.setCrosshair(time, null);
      } else {
        monthlyChartRef.current?.setCrosshair(time, null);
      }
    }
  };

  const handleCompareDailyCrosshair = (time: number | null, source: "left" | "right") => {
    if (syncRangesRef.current) {
      if (source === "left") {
        compareDailyChartRef.current?.setCrosshair(time, null);
      } else {
        dailyChartRef.current?.setCrosshair(time, null);
      }
    }
    scheduleHoverTime(time);
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
  const nextCode = useMemo(() => {
    if (!code) return null;
    const index = listCodes.indexOf(code);
    if (index < 0) return null;
    return listCodes[index + 1] ?? null;
  }, [listCodes, code]);

  return (
    <div className={`detail-shell ${focusPanel ? "detail-shell-focus" : ""}`}>
      <div className="detail-header">
        <div className="detail-header-nav">
          <button className="back nav-button nav-primary" onClick={() => navigate(listBackPath)}>
            <span className="nav-icon">
              <IconArrowLeft size={16} />
            </span>
            <span className="nav-label">一覧に戻る</span>
          </button>
          <button className="back nav-button" onClick={() => navigate(-1)}>
            <span className="nav-icon">
              <IconArrowBackUp size={16} />
            </span>
            <span className="nav-label">前の画面</span>
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
        <div className="detail-title">
          <div className="detail-title-text">
            <div className="detail-title-top">
              <div className="detail-title-code">{code}</div>
              <div className="detail-title-name">{tickerName || "?????"}</div>
            </div>
            <div className="subtitle">{subtitle}</div>
            {(rightsLabel || earningsLabel) && (
              <div className="detail-event-badges">
                {rightsLabel && <span className="event-badge event-rights">権利 {rightsLabel}</span>}
                {earningsLabel && <span className="event-badge event-earnings">決算 {earningsLabel}</span>}
              </div>
            )}
            <div className="detail-event-meta">
              {eventsMeta?.isRefreshing && (
                <span className="event-meta-refreshing">イベント更新中...</span>
              )}
              {eventsMeta?.lastError && (
                <span className="event-meta-error" title={eventsMeta.lastError}>
                  更新失敗
                </span>
              )}
              <span className="event-meta-last">
                イベント最終更新: {eventsLastSuccessLabel ?? "--"}
              </span>
              {rightsCoverageLabel && <span className="event-meta-rights">{rightsCoverageLabel}</span>}
            </div>
          </div>
          <div className="detail-title-actions">
            <button
              type="button"
              className={isFavorite ? "favorite-toggle active" : "favorite-toggle"}
              aria-pressed={isFavorite}
              aria-label={isFavorite ? "\u304a\u6c17\u306b\u5165\u308a\u89e3\u9664" : "\u304a\u6c17\u306b\u5165\u308a\u8ffd\u52a0"}
              onClick={handleToggleFavorite}
              title={isFavorite ? "\u304a\u6c17\u306b\u5165\u308a\u89e3\u9664" : "\u304a\u6c17\u306b\u5165\u308a\u8ffd\u52a0"}
            >
              {isFavorite ? <IconHeartFilled size={18} /> : <IconHeart size={18} />}
            </button>
            {dailySignals.length > 0 && (
              <div className="detail-signals-inline">
                {dailySignals.map((signal) => (
                  <span
                    key={signal.label}
                    className={`signal-chip ${signal.kind === "warning" ? "warning" : "achieved"}`}
                  >
                    {signal.label}
                  </span>
                ))}
              </div>
            )}
          </div>
        </div>
        <div className="detail-controls">
          <div className="detail-controls-group">
            <button
              className="indicator-button is-primary"
              onClick={() => {
                if (code) navigate(`/practice/${code}`);
              }}
            >
              練習
            </button>
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
          </div>
          <div className="detail-controls-group">
            <button
              className={showBoxes ? "indicator-button active" : "indicator-button"}
              onClick={() => setShowBoxes(!showBoxes)}
            >
              Boxes
            </button>
            <button
              className={showTradesOverlay ? "indicator-button active" : "indicator-button"}
              onClick={() => setShowTradesOverlay((prev) => !prev)}
            >
              Positions
            </button>
            <button
              className={showPositionLedger ? "indicator-button active" : "indicator-button"}
              onClick={() =>
                setShowPositionLedger((prev) => {
                  const next = !prev;
                  if (!next) {
                    setPositionLedgerExpanded(false);
                  }
                  return next;
                })
              }
            >
              建玉推移
            </button>
            <button
              className={showPnLPanel ? "indicator-button active" : "indicator-button"}
              onClick={() => setShowPnLPanel(!showPnLPanel)}
            >
              PnL
            </button>
            <button
              className={syncRanges ? "indicator-button active" : "indicator-button"}
              onClick={() => setSyncRanges((prev) => !prev)}
            >
              連動: {syncRanges ? "ON" : "OFF"}
            </button>
          </div>
          <div className="detail-controls-group detail-controls-icons">
            <IconButton
              label="Indicators"
              icon={<IconAdjustments size={18} />}
              onClick={() => setShowIndicators(true)}
              title="Indicators"
            />
            <IconButton
              label="スクショ"
              icon={<IconCamera size={18} />}
              disabled={screenshotBusy}
              title="スクショ"
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
                  if (result.copied) {
                    // Clipboard copy succeeded - show toast with save action
                    const blob = result.blob!;
                    const filename = result.filename!;
                    setToastMessage("スクショをクリップボードにコピーしました");
                    setToastAction({
                      label: "保存...",
                      onClick: async () => {
                        await saveBlobToFile(blob, filename);
                        setToastMessage("スクショを保存しました");
                        setToastAction(null);
                      },
                    });
                  } else {
                    // Clipboard failed - fallback to save
                    setToastMessage("クリップボードにコピーできなかったため保存しました");
                    setToastAction(null);
                    if (result.blob && result.filename) {
                      await saveBlobToFile(result.blob, result.filename);
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
              title="AI出力"
              onClick={async () => {
                const exportData = buildAIExport({
                  code: code ?? "",
                  name: tickerName,
                  visibleTimeframe: "daily",
                  rangeMonths: rangeMonths,
                  dailyBars: dailyCandles.map((c) => ({ time: c.time, open: c.open, high: c.high, low: c.low, close: c.close })),
                  weeklyBars: weeklyCandles.map((c) => ({ time: c.time, open: c.open, high: c.high, low: c.low, close: c.close })),
                  monthlyBars: monthlyCandles.map((c) => ({ time: c.time, open: c.open, high: c.high, low: c.low, close: c.close })),
                  maSettings,
                  signals: dailySignals,
                  showBoxes,
                  showPositions: showTradesOverlay,
                  boxes,
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
              title="類似チャート検索"
              onClick={() => setShowSimilar(true)}
            />
            <IconButton
              label="削除"
              icon={<IconTrash size={18} />}
              title="削除"
              disabled={deleteBusy || !code}
              onClick={handleDeleteTicker}
            />
          </div>
        </div>
      </div>
      <div className={`detail-split ${focusPanel ? "detail-split-focus" : ""}`}>
        {compareCode && (
          <div className="detail-compare">
            <div className="detail-compare-header">
              <div>
                <div className="detail-compare-title">
                  比較: {code} / {compareCode}
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
                    visibleRange={monthlyCandles.length ? compareMonthlyBaseRange : null}
                    onCrosshairMove={(time) => handleCompareMonthlyCrosshair(time, "left")}
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
                    visibleRange={compareMonthlyCandles.length ? compareMonthlyVisibleRange : null}
                    onCrosshairMove={(time) => handleCompareMonthlyCrosshair(time, "right")}
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
                    boxes={boxes}
                    showBoxes={showBoxes}
                    visibleRange={dailyCandles.length ? dailyVisibleRange : null}
                    positionOverlay={{
                      dailyPositions,
                      tradeMarkers,
                      showOverlay: showTradesOverlay,
                      showMarkers: true,
                      showPnL: showPnLPanel,
                      hoverTime
                    }}
                    onCrosshairMove={(time) => handleCompareDailyCrosshair(time, "left")}
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
                    showVolume={compareDailyVolume.length > 0}
                    boxes={compareBoxes}
                    showBoxes={showBoxes}
                    visibleRange={compareDailyVisibleRange}
                    positionOverlay={{
                      dailyPositions: compareDailyPositions,
                      tradeMarkers: compareTradeMarkers,
                      showOverlay: showTradesOverlay,
                      showMarkers: true,
                      showPnL: showPnLPanel,
                      hoverTime
                    }}
                    onCrosshairMove={(time) => handleCompareDailyCrosshair(time, "right")}
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
                  boxes={boxes}
                  showBoxes={showBoxes}
                  visibleRange={dailyVisibleRange}
                  positionOverlay={{
                    dailyPositions,
                    tradeMarkers,
                    showOverlay: showTradesOverlay,
                    showMarkers: true,
                    showPnL: showPnLPanel,
                    hoverTime
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
                  visibleRange={weeklyVisibleRange}
                  positionOverlay={{
                    dailyPositions,
                    tradeMarkers,
                    showOverlay: showTradesOverlay,
                    showMarkers: false,
                    showPnL: showPnLPanel,
                    hoverTime
                  }}
                  onCrosshairMove={handleWeeklyCrosshair}
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
                  visibleRange={monthlyVisibleRange}
                  positionOverlay={{
                    dailyPositions,
                    tradeMarkers,
                    showOverlay: showTradesOverlay,
                    showMarkers: false,
                    showPnL: showPnLPanel,
                    hoverTime
                  }}
                  onCrosshairMove={handleMonthlyCrosshair}
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
                  boxes={boxes}
                  showBoxes={showBoxes}
                  visibleRange={dailyVisibleRange}
                  positionOverlay={{
                    dailyPositions,
                    tradeMarkers,
                    showOverlay: showTradesOverlay,
                    showMarkers: true,
                    showPnL: showPnLPanel,
                    hoverTime
                  }}
                  onCrosshairMove={handleDailyCrosshair}
                  onVisibleRangeChange={handleDailyVisibleRangeChange}
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
                    visibleRange={weeklyVisibleRange}
                    onCrosshairMove={handleWeeklyCrosshair}
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
                    visibleRange={monthlyVisibleRange}
                    onCrosshairMove={handleMonthlyCrosshair}
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
      {!focusPanel && (
        <div className="detail-footer">
          <div className="detail-footer-left">
            <button className="load-more" onClick={loadMoreDaily} disabled={loadingDaily || !hasMoreDaily}>
              {loadingDaily ? "Loading daily..." : hasMoreDaily ? "Load more daily" : "Daily all loaded"}
            </button>
            <button
              className="load-more"
              onClick={loadMoreMonthly}
              disabled={loadingMonthly || !hasMoreMonthly}
            >
              {loadingMonthly
                ? "Loading monthly..."
                : hasMoreMonthly
                  ? "Load more monthly"
                  : "Monthly all loaded"}
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
            aria-label={positionLedgerExpanded ? "Collapse position ledger" : "Expand position ledger"}
          />
          <div className="position-ledger-header">
            <div>
              <div className="position-ledger-title">Position Ledger (Per Broker)</div>
              <div className="position-ledger-sub">Grouped by broker</div>
            </div>
            <button
              type="button"
              className="position-ledger-close"
              onClick={() => {
                setShowPositionLedger(false);
                setPositionLedgerExpanded(false);
              }}
              aria-label="Close position ledger"
            >
              x
            </button>
          </div>
          {!ledgerEligible ? (
            <div className="position-ledger-empty">
              No eligible position ledger data.
            </div>
          ) : (
            <div className="position-ledger-group-list">
              {ledgerGroups.map((group) => (
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
                  <div className="position-ledger-table">
                    <div className="position-ledger-row position-ledger-head">
                      <span>Date</span>
                      <span>Type</span>
                      <span>Qty</span>
                      <span>Price</span>
                      <span>Long</span>
                      <span>Short</span>
                      <span>PnL</span>
                      <span>Total</span>
                    </div>
                    {group.rows.map((row, index) => (
                      <div className="position-ledger-row" key={`${row.date}-${index}`}>
                        <span>{row.date}</span>
                        <span className="position-ledger-kind">{row.kindLabel}</span>
                        <span>{formatNumber(row.qtyShares, 0)}</span>
                        <span>{formatNumber(row.price, 2)}</span>
                        <span>{formatNumber(row.buyShares, 0)}</span>
                        <span>{formatNumber(row.sellShares, 0)}</span>
                        <span
                          className={
                            row.realizedPnL == null
                              ? "position-ledger-pnl"
                              : row.realizedPnL >= 0
                                ? "position-ledger-pnl up"
                                : "position-ledger-pnl down"
                          }
                        >
                          {row.realizedPnL == null ? "--" : formatNumber(row.realizedPnL, 0)}
                        </span>
                        <span
                          className={
                            row.totalPnL >= 0 ? "position-ledger-pnl up" : "position-ledger-pnl down"
                          }
                        >
                          {formatNumber(row.totalPnL, 0)}
                        </span>
                      </div>
                    ))}
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
                    通常
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
