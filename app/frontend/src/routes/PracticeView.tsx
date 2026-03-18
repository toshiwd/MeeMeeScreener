import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { MouseEvent as ReactMouseEvent, TouchEvent as ReactTouchEvent } from "react";
import { useLocation, useNavigate, useParams } from "react-router-dom";
import { api } from "../api";
import { useBackendReadyState } from "../backendReady";
import DetailChart, { DetailChartHandle } from "../components/DetailChart";
import Toast from "../components/Toast";
import { useStore } from "../store";
import type { MaSetting } from "../storeTypes";
import { computeSignalMetrics } from "../utils/signals";
import { captureAndCopyScreenshot, saveBlobToFile, getScreenType } from "../utils/windowScreenshot";
import {
  IconArrowLeft,
  IconArrowBackUp,
  IconCamera,
  IconFileDownload,
  IconSparkles,
  IconRefresh,
  IconChevronLeft,
  IconChevronRight,
  IconPlus
} from "@tabler/icons-react";
import IconButton from "../components/IconButton";
import { buildAIExport, copyToClipboard } from "../utils/aiExport";
import type {
  BarsResponse,
  DailyBar,
  PracticeSession,
  PracticeTrade,
  PracticeUiState,
  Timeframe
} from "./practice/practiceTypes";
import {
  buildAggregatedBars,
  buildCandles,
  buildDailyBars,
  buildPracticeLedger,
  buildPracticePositions,
  buildVolume,
  clampValue,
  computeMA,
  createSessionId,
  DAILY_ROW_RATIO,
  DEFAULT_LIMITS,
  DEFAULT_LOT_SIZE,
  DEFAULT_RANGE_MONTHS,
  DEFAULT_WEEKLY_RATIO,
  exportFile,
  EXPORT_ATR_PERIOD,
  EXPORT_MA_PERIODS,
  EXPORT_SLOPE_LOOKBACK,
  EXPORT_VOLUME_PERIOD,
  formatDate,
  formatDateSlash,
  formatNumber,
  getMonthStartTime,
  getWeekStartTime,
  MIN_MONTHLY_RATIO,
  MIN_WEEKLY_RATIO,
  parseBarsResponse,
  parseDateString,
  PositionDonutChart,
  RANGE_PRESETS,
  resolveCursorIndex,
  resolveExactIndex,
  resolveIndexOnOrBefore,
  subtractMonths
} from "./practice/practiceHelpers";

const buildChartSeries = (bars: DailyBar[]) => ({
  candles: buildCandles(bars),
  volume: buildVolume(bars)
});

const filterSeriesByTime = <T extends { time: number }>(
  series: T[],
  startTime: number,
  endTime: number
) => series.filter((item) => item.time >= startTime && item.time <= endTime);

const buildPracticeMaLines = (
  candles: Parameters<typeof computeMA>[0],
  settings: MaSetting[],
  startTime: number,
  endTime: number
) =>
  settings.map((setting) => ({
    key: setting.key,
    label: setting.label,
    period: setting.period,
    color: setting.color,
    lineWidth: setting.lineWidth,
    visible: setting.visible,
    data: setting.visible ? filterSeriesByTime(computeMA(candles, setting.period), startTime, endTime) : []
  }));

export default function PracticeView() {
  const { code } = useParams();
  const navigate = useNavigate();
  const location = useLocation();
  const { ready: backendReady } = useBackendReadyState();
  const dailyChartRef = useRef<DetailChartHandle | null>(null);
  const weeklyChartRef = useRef<DetailChartHandle | null>(null);
  const monthlyChartRef = useRef<DetailChartHandle | null>(null);
  const cursorTimeRef = useRef<number | null>(null);
  const sessionChangeRef = useRef(0);
  const bottomRowRef = useRef<HTMLDivElement | null>(null);
  const resizingRef = useRef(false);
  const hoverRafRef = useRef<number | null>(null);
  const hoverTimePendingRef = useRef<number | null>(null);
  const hoverTimeRef = useRef<number | null>(null);
  const crosshairSyncRef = useRef<{ source: Timeframe; time: number | null } | null>(null);
  const crosshairSyncRafRef = useRef<number | null>(null);
  const startDateInputRef = useRef<HTMLInputElement | null>(null);

  const tickers = useStore((state) => state.tickers);
  const ensureListLoaded = useStore((state) => state.ensureListLoaded);
  const loadingList = useStore((state) => state.loadingList);
  const maSettings = useStore((state) => state.maSettings);

  const [sessions, setSessions] = useState<PracticeSession[]>([]);
  const [sessionsLoading, setSessionsLoading] = useState(false);
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [sessionManagerOpen, setSessionManagerOpen] = useState(false);
  const [startDate, setStartDate] = useState<string | null>(null);
  const [endDate, setEndDate] = useState<string | null>(null);
  const [startDateDraft, setStartDateDraft] = useState<string>("");
  const [sessionNotes, setSessionNotes] = useState("");
  const [notesCollapsed, setNotesCollapsed] = useState(true);
  const [panelCollapsed, setPanelCollapsed] = useState(true);
  const [tradeLogCollapsed, setTradeLogCollapsed] = useState(true);
  const [cursorTime, setCursorTime] = useState<number | null>(null);
  const [maxUnlockedTime, setMaxUnlockedTime] = useState<number | null>(null);
  const [tradeNote, setTradeNote] = useState("");
  const [trades, setTrades] = useState<PracticeTrade[]>([]);
  const [editingTradeId, setEditingTradeId] = useState<string | null>(null);
  const [dailyLimit] = useState(DEFAULT_LIMITS.daily);
  const [dailyData, setDailyData] = useState<number[][]>([]);
  const [dailyErrors, setDailyErrors] = useState<string[]>([]);
  const [toastMessage, setToastMessage] = useState<string | null>(null);
  const [toastAction, setToastAction] = useState<{ label: string; onClick: () => void } | null>(null);
  const [screenshotBusy, setScreenshotBusy] = useState(false);
  const [hoverTime, setHoverTime] = useState<number | null>(null);
  const [weeklyRatio, setWeeklyRatio] = useState(DEFAULT_WEEKLY_RATIO);
  const [lotSize, setLotSize] = useState(DEFAULT_LOT_SIZE);
  const [rangeMonths, setRangeMonths] = useState(DEFAULT_RANGE_MONTHS);
  const [, setHasMoreDaily] = useState(true);
  const [, setLoadingDaily] = useState(false);

  const tickerByCode = useMemo(() => new Map(tickers.map((item) => [item.code, item])), [tickers]);
  const tickerName = useMemo(() => {
    if (!code) return "";
    const raw = tickerByCode.get(code)?.name ?? "";
    const cleaned = raw.replace(/\s*\?\s*$/, "").trim();
    return cleaned === "?" ? "" : cleaned;
  }, [tickerByCode, code]);
  const sessionStorageKey = code ? `practice_session_id_${code}` : null;

  useEffect(() => {
    if (!backendReady) return;
    if (!tickers.length && !loadingList) {
      void ensureListLoaded();
    }
  }, [backendReady, tickers.length, loadingList, ensureListLoaded]);

  const refreshSessions = useCallback((selectId?: string | null) => {
    if (!backendReady || !code) return;
    setSessionsLoading(true);
    api
      .get("/practice/sessions", { params: { code } })
      .then((res) => {
        const payload = res.data as { sessions?: PracticeSession[] };
        const items = Array.isArray(payload.sessions) ? payload.sessions : [];
        setSessions(items);

        if (selectId) {
          setSessionId(selectId);
          return;
        }

        if (!sessionId || !items.some((item) => item.session_id === sessionId)) {
          const stored = sessionStorageKey ? localStorage.getItem(sessionStorageKey) : null;
          const fallback = items.find((item) => item.session_id === stored) ?? items[0] ?? null;
          if (fallback) {
            setSessionId(fallback.session_id);
          } else {
            setSessionId(null);
          }
        }
      })
      .finally(() => setSessionsLoading(false));
  }, [backendReady, code, sessionId, sessionStorageKey]);

  useEffect(() => {
    refreshSessions();
  }, [refreshSessions]);

  useEffect(() => {
    if (!sessionStorageKey || !sessionId) return;
    localStorage.setItem(sessionStorageKey, sessionId);
  }, [sessionStorageKey, sessionId]);

  useEffect(() => {
    if (!backendReady || !sessionId) {
      if (!sessionId) {
        setTrades([]);
        setStartDate(null);
        setEndDate(null);
        setStartDateDraft("");
        setSessionNotes("");
        setCursorTime(null);
        setMaxUnlockedTime(null);
      }
      return;
    }
    api
      .get("/practice/session", { params: { session_id: sessionId } })
      .then((res) => {
        const payload = res.data as { session?: PracticeSession | null };
        const session = payload.session;
        if (!session) {
          setToastMessage("セッションの読み込みに失敗しました。");
          setSessionId(null);
          return;
        }
        if (code && session.code !== code) return;
        const sessionTrades = Array.isArray(session.trades) ? session.trades : [];
        const sessionStart = session.start_date ?? null;
        const sessionEnd = session.end_date ?? null;
        const sessionNotesValue = session.notes ?? "";
        const nextLotSize =
          Number.isFinite(Number(session.lot_size)) && Number(session.lot_size) > 0
            ? Number(session.lot_size)
            : DEFAULT_LOT_SIZE;
        const nextRangeMonths =
          Number.isFinite(Number(session.range_months)) && Number(session.range_months) > 0
            ? Number(session.range_months)
            : DEFAULT_RANGE_MONTHS;
        const uiState = (session.ui_state ?? {}) as PracticeUiState;
        setTrades(sessionTrades);
        setStartDate(sessionStart);
        setEndDate(sessionEnd);
        setStartDateDraft(sessionStart ?? "");
        setSessionNotes(sessionNotesValue);
        setLotSize(nextLotSize);
        setRangeMonths(nextRangeMonths);
        setCursorTime(session.cursor_time ?? null);
        setMaxUnlockedTime(session.max_unlocked_time ?? null);
        setPanelCollapsed(Boolean(uiState.panelCollapsed ?? true));
        setNotesCollapsed(Boolean(uiState.notesCollapsed ?? true));
        setTradeLogCollapsed(Boolean(uiState.tradeLogCollapsed ?? true));
        sessionChangeRef.current += 1;
      })
      .catch(() => {
        setToastMessage("セッションの読み込みに失敗しました。");
        setSessionId(null);
      });
  }, [backendReady, sessionId, code]);


  const persistSession = useCallback((next: Partial<{
    startDate: string | null;
    endDate: string | null;
    cursorTime: number | null;
    maxUnlockedTime: number | null;
    trades: PracticeTrade[];
    notes: string;
    lotSize: number;
    rangeMonths: number;
    uiState: PracticeUiState;
  }>) => {
    if (!sessionId || !code) return;
    const payload = {
      session_id: sessionId,
      code,
      start_date: next.startDate !== undefined ? next.startDate : startDate,
      end_date: next.endDate !== undefined ? next.endDate : endDate,
      cursor_time: next.cursorTime !== undefined ? next.cursorTime : cursorTime,
      max_unlocked_time:
        next.maxUnlockedTime !== undefined ? next.maxUnlockedTime : maxUnlockedTime,
      lot_size: next.lotSize !== undefined ? next.lotSize : lotSize,
      range_months: next.rangeMonths !== undefined ? next.rangeMonths : rangeMonths,
      trades: next.trades !== undefined ? next.trades : trades,
      notes: next.notes !== undefined ? next.notes : sessionNotes,
      ui_state:
        next.uiState !== undefined
          ? next.uiState
          : { panelCollapsed, notesCollapsed, tradeLogCollapsed }
    };
    api.post("/practice/session", payload).catch(() => {
      setToastMessage("セッションの保存に失敗しました。");
    });
  }, [
    code,
    cursorTime,
    endDate,
    lotSize,
    maxUnlockedTime,
    notesCollapsed,
    panelCollapsed,
    rangeMonths,
    sessionId,
    sessionNotes,
    startDate,
    tradeLogCollapsed,
    trades
  ]);

  useEffect(() => {
    if (!backendReady || !code) return;
    setLoadingDaily(true);
    setDailyErrors([]);
    api
      .get("/practice/daily", {
        params: {
          code,
          limit: dailyLimit
        }
      })
      .then((res) => {
        const { rows, errors } = parseBarsResponse(res.data as BarsResponse | number[][], "daily");
        setDailyData(rows);
        setDailyErrors(errors);
        setHasMoreDaily(rows.length >= dailyLimit);
      })
      .catch((error) => {
        const message = error?.message || "Daily fetch failed";
        setDailyErrors([message]);
      })
      .finally(() => setLoadingDaily(false));
  }, [backendReady, code, dailyLimit]);

  const dailyBars = useMemo(() => buildDailyBars(dailyData), [dailyData]);
  const sessionStartTime = useMemo(() => parseDateString(startDate), [startDate]);
  const sessionEndTime = useMemo(() => parseDateString(endDate), [endDate]);

  useEffect(() => {
    if (!dailyBars.length) return;
    if (cursorTime == null) {
      const fallbackTime = sessionStartTime ?? dailyBars[dailyBars.length - 1].time;
      const idx = resolveCursorIndex(dailyBars, fallbackTime) ?? 0;
      const nextTime = dailyBars[idx]?.time ?? dailyBars[dailyBars.length - 1].time;
      setCursorTime(nextTime);
      if (maxUnlockedTime == null) {
        setMaxUnlockedTime(nextTime);
      }
      return;
    }
    const idx = resolveCursorIndex(dailyBars, cursorTime) ?? 0;
    const resolved = dailyBars[idx]?.time;
    if (resolved != null && resolved != cursorTime) {
      setCursorTime(resolved);
    }
    if (maxUnlockedTime != null) {
      const maxIdx = resolveCursorIndex(dailyBars, maxUnlockedTime) ?? 0;
      const resolvedMax = dailyBars[maxIdx]?.time;
      if (resolvedMax != null && resolvedMax != maxUnlockedTime) {
        setMaxUnlockedTime(resolvedMax);
      }
    }
  }, [dailyBars, cursorTime, maxUnlockedTime, sessionStartTime]);

  useEffect(() => {
    cursorTimeRef.current = cursorTime ?? null;
  }, [cursorTime]);

  useEffect(() => {
    if (!sessionId) return;
    if (cursorTime == null && maxUnlockedTime == null) return;
    persistSession({ cursorTime, maxUnlockedTime });
  }, [cursorTime, maxUnlockedTime, persistSession, sessionId]);

  useEffect(() => {
    if (!dailyBars.length || sessionEndTime == null) return;
    const endIdx = resolveIndexOnOrBefore(dailyBars, sessionEndTime);
    const endTime = endIdx != null ? dailyBars[endIdx]?.time : null;
    if (endTime == null) return;
    if (cursorTime != null && cursorTime > endTime) {
      setCursorTime(endTime);
    }
    if (maxUnlockedTime != null && maxUnlockedTime > endTime) {
      setMaxUnlockedTime(endTime);
    }
  }, [dailyBars, sessionEndTime, cursorTime, maxUnlockedTime]);

  const cursorIndex = useMemo(
    () => (cursorTime != null ? resolveCursorIndex(dailyBars, cursorTime) : null),
    [dailyBars, cursorTime]
  );
  const maxUnlockedIndex = useMemo(() => {
    if (maxUnlockedTime != null) {
      return resolveCursorIndex(dailyBars, maxUnlockedTime);
    }
    return cursorIndex;
  }, [dailyBars, maxUnlockedTime, cursorIndex]);

  const sessionStartIndex = useMemo(() => {
    if (!dailyBars.length) return null;
    if (sessionStartTime == null) return 0;
    return resolveCursorIndex(dailyBars, sessionStartTime);
  }, [dailyBars, sessionStartTime]);

  const sessionEndIndex = useMemo(() => {
    if (!dailyBars.length) return null;
    if (sessionEndTime == null) return dailyBars.length - 1;
    return resolveIndexOnOrBefore(dailyBars, sessionEndTime);
  }, [dailyBars, sessionEndTime]);

  const cursorCandle = cursorIndex != null ? dailyBars[cursorIndex] : null;

  const rangeStartTime = useMemo(() => {
    if (cursorTime == null || !dailyBars.length) return null;
    let start = subtractMonths(cursorTime, rangeMonths);
    const earliest = dailyBars[0]?.time;
    if (earliest != null) {
      start = Math.max(start, earliest);
    }
    return start;
  }, [cursorTime, rangeMonths, dailyBars]);

  const practiceChartData = useMemo(() => {
    const emptyWeeklyAggregate = buildAggregatedBars([], "weekly", cursorTime);
    const emptyMonthlyAggregate = buildAggregatedBars([], "monthly", cursorTime);
    if (!dailyBars.length) {
      return {
        trainingBars: [] as DailyBar[],
        weeklyAggregate: emptyWeeklyAggregate,
        monthlyAggregate: emptyMonthlyAggregate,
        weeklyBars: [] as DailyBar[],
        monthlyBars: [] as DailyBar[],
        trainingCandles: [] as ReturnType<typeof buildCandles>,
        dailySeries: buildChartSeries([]),
        weeklyCandlesAll: emptyWeeklyAggregate.candles,
        weeklySeries: {
          candles: [] as typeof emptyWeeklyAggregate.candles,
          volume: [] as typeof emptyWeeklyAggregate.volume
        },
        monthlyCandlesAll: emptyMonthlyAggregate.candles,
        monthlySeries: {
          candles: [] as typeof emptyMonthlyAggregate.candles,
          volume: [] as typeof emptyMonthlyAggregate.volume
        },
        practiceSignals: [] as ReturnType<typeof computeSignalMetrics>["signals"]
      };
    }

    const endTime = cursorTime ?? dailyBars[dailyBars.length - 1].time;
    const trainingBars = filterSeriesByTime(dailyBars, dailyBars[0].time, endTime);
    const startTime = rangeStartTime ?? trainingBars[0]?.time ?? endTime;
    const visibleDailyBars = filterSeriesByTime(trainingBars, startTime, endTime);
    const weeklyAggregate = buildAggregatedBars(trainingBars, "weekly", cursorTime);
    const monthlyAggregate = buildAggregatedBars(trainingBars, "monthly", cursorTime);
    const weeklyBars = filterSeriesByTime(weeklyAggregate.bars, startTime, endTime);
    const monthlyBars = filterSeriesByTime(monthlyAggregate.bars, startTime, endTime);
    const trainingCandles = buildCandles(trainingBars);
    const rows = trainingBars.map((bar) => [
      bar.time,
      bar.open,
      bar.high,
      bar.low,
      bar.close,
      bar.volume
    ]);

    return {
      trainingBars,
      weeklyAggregate,
      monthlyAggregate,
      weeklyBars,
      monthlyBars,
      trainingCandles,
      dailySeries: buildChartSeries(visibleDailyBars),
      weeklyCandlesAll: weeklyAggregate.candles,
      weeklySeries: {
        candles: filterSeriesByTime(weeklyAggregate.candles, startTime, endTime),
        volume: filterSeriesByTime(weeklyAggregate.volume, startTime, endTime)
      },
      monthlyCandlesAll: monthlyAggregate.candles,
      monthlySeries: {
        candles: filterSeriesByTime(monthlyAggregate.candles, startTime, endTime),
        volume: filterSeriesByTime(monthlyAggregate.volume, startTime, endTime)
      },
      practiceSignals: computeSignalMetrics(rows).signals
    };
  }, [dailyBars, cursorTime, rangeStartTime]);
  const trainingBars = practiceChartData.trainingBars;
  const weeklyAggregate = practiceChartData.weeklyAggregate;
  const monthlyAggregate = practiceChartData.monthlyAggregate;
  const weeklyBars = practiceChartData.weeklyBars;
  const monthlyBars = practiceChartData.monthlyBars;
  const trainingCandles = practiceChartData.trainingCandles;
  const dailySeries = practiceChartData.dailySeries;
  const weeklyCandlesAll = practiceChartData.weeklyCandlesAll;
  const weeklySeries = practiceChartData.weeklySeries;
  const monthlyCandlesAll = practiceChartData.monthlyCandlesAll;
  const monthlySeries = practiceChartData.monthlySeries;
  const practiceSignals = practiceChartData.practiceSignals;
  const dailyCandles = dailySeries.candles;
  const dailyVolume = dailySeries.volume;
  const weeklyCandles = weeklySeries.candles;
  const weeklyVolume = weeklySeries.volume;
  const monthlyCandles = monthlySeries.candles;
  const dailyMaLines = useMemo(() => {
    const start = rangeStartTime ?? (trainingCandles[0]?.time ?? 0);
    const end = cursorTime ?? (trainingCandles[trainingCandles.length - 1]?.time ?? 0);
    return buildPracticeMaLines(trainingCandles, maSettings.daily, start, end);
  }, [trainingCandles, maSettings.daily, rangeStartTime, cursorTime]);

  const weeklyMaLines = useMemo(() => {
    const start = rangeStartTime ?? (weeklyCandlesAll[0]?.time ?? 0);
    const end = cursorTime ?? (weeklyCandlesAll[weeklyCandlesAll.length - 1]?.time ?? 0);
    return buildPracticeMaLines(weeklyCandlesAll, maSettings.weekly, start, end);
  }, [weeklyCandlesAll, maSettings.weekly, rangeStartTime, cursorTime]);

  const monthlyMaLines = useMemo(() => {
    const start = rangeStartTime ?? (monthlyCandlesAll[0]?.time ?? 0);
    const end = cursorTime ?? (monthlyCandlesAll[monthlyCandlesAll.length - 1]?.time ?? 0);
    return buildPracticeMaLines(monthlyCandlesAll, maSettings.monthly, start, end);
  }, [monthlyCandlesAll, maSettings.monthly, rangeStartTime, cursorTime]);

  const visibleTrades = useMemo(
    () => trades.filter((trade) => (cursorTime == null ? true : trade.time <= cursorTime)),
    [trades, cursorTime]
  );

  const ledger = useMemo(() => buildPracticeLedger(visibleTrades, lotSize), [visibleTrades, lotSize]);
  const positionSummary = ledger.summary;
  const netLots = positionSummary.longLots - positionSummary.shortLots;

  const latestDailyClose = cursorCandle?.close ?? null;
  const longShares = positionSummary.longShares ?? positionSummary.longLots * lotSize;
  const shortShares = positionSummary.shortShares ?? positionSummary.shortLots * lotSize;
  const unrealizedPnL =
    latestDailyClose != null
      ? (latestDailyClose - positionSummary.avgLongPrice) * longShares +
      (positionSummary.avgShortPrice - latestDailyClose) * shortShares
      : null;

  const practicePositionData = useMemo(
    () => buildPracticePositions(trainingBars, visibleTrades, lotSize, code, tickerName),
    [trainingBars, visibleTrades, lotSize, code, tickerName]
  );
  const dailyPositions = practicePositionData.dailyPositions;
  const tradeMarkers = practicePositionData.tradeMarkers;

  const weeklyCursorTime = cursorTime != null ? getWeekStartTime(cursorTime) : null;
  const monthlyCursorTime = cursorTime != null ? getMonthStartTime(cursorTime) : null;

  const weeklyPartialTimes = useMemo(() => {
    const last = weeklyBars[weeklyBars.length - 1];
    return last?.isPartial ? [last.time] : [];
  }, [weeklyBars]);

  const monthlyPartialTimes = useMemo(() => {
    const last = monthlyBars[monthlyBars.length - 1];
    return last?.isPartial ? [last.time] : [];
  }, [monthlyBars]);

  const monthlyRatio = 1 - weeklyRatio;

  const isLocked = cursorTime != null && maxUnlockedTime != null && cursorTime < maxUnlockedTime;

  const progressIndex = useMemo(() => {
    if (cursorIndex == null || sessionStartIndex == null) return null;
    return Math.max(1, cursorIndex - sessionStartIndex + 1);
  }, [cursorIndex, sessionStartIndex]);

  const minStepIndex = sessionStartIndex ?? 0;
  const maxStepIndex = sessionEndIndex ?? (dailyBars.length ? dailyBars.length - 1 : 0);
  const currentStepIndex = cursorIndex ?? maxStepIndex;
  const frontierIndex = maxUnlockedIndex ?? currentStepIndex;
  const maxAdvanceIndex = currentStepIndex < frontierIndex ? frontierIndex : maxStepIndex;
  const canStepBack = dailyBars.length > 0 && currentStepIndex > minStepIndex;
  const canStepForward = dailyBars.length > 0 && currentStepIndex < maxAdvanceIndex;
  const headerDateLabel = cursorCandle ? formatDateSlash(cursorCandle.time) : "--";
  const headerDayLabel = progressIndex != null ? `${progressIndex}日目` : "--";
  const headerMetaLabel =
    cursorCandle && progressIndex != null ? `${headerDateLabel} (${headerDayLabel})` : headerDateLabel;
  const guideText = useMemo(() => {
    if (sessionsLoading) return "セッションを読み込んでいます...";
    if (!sessionId) return "「新規」で練習を開始するか、「管理」から過去の練習を読み込んでください。";
    if (!startDate) return "開始日を選んで「開始日を確定」を押してください";
    if (isLocked) return "過去日を表示中です。最新日に戻ると操作できます";
    return "建玉を操作して「翌日」で進めます（→キーでも可）";
  }, [sessionsLoading, sessionId, startDate, isLocked]);
  const sessionBadgeLabel = sessionId ? (endDate ? "完了" : "進行中") : "未作成";
  const sessionBadgeClass = sessionId ? (endDate ? "is-ended" : "is-active") : "is-empty";
  const sessionRangeLabel = sessionId
    ? `開始 ${startDate ?? "--"} / 終了 ${endDate ?? "--"}`
    : "セッション未選択";
  const canUndo =
    !isLocked &&
    cursorTime != null &&
    trades.length > 0 &&
    trades[trades.length - 1]?.time === cursorTime;
  const canResetDay =
    !isLocked && cursorTime != null && trades.some((trade) => trade.time === cursorTime);

  const handleStep = useCallback((direction: 1 | -1) => {
    if (!dailyBars.length) return;
    const minIndex = minStepIndex;
    const maxIndex = maxStepIndex;
    const currentIndex = cursorIndex ?? maxIndex;
    let nextIndex = currentIndex;
    let nextMaxUnlocked = maxUnlockedTime ?? null;

    if (direction < 0) {
      nextIndex = Math.max(currentIndex - 1, minIndex);
    } else {
      if (maxUnlockedIndex != null && currentIndex < maxUnlockedIndex) {
        nextIndex = Math.min(currentIndex + 1, maxUnlockedIndex);
      } else {
        nextIndex = Math.min(currentIndex + 1, maxIndex);
        const candidate = dailyBars[nextIndex]?.time ?? null;
        if (candidate != null && (nextMaxUnlocked == null || candidate > nextMaxUnlocked)) {
          nextMaxUnlocked = candidate;
        }
      }
    }

    const nextTime = dailyBars[nextIndex]?.time;
    if (nextTime == null) return;
    setCursorTime(nextTime);
    if (nextMaxUnlocked != null && nextMaxUnlocked !== maxUnlockedTime) {
      setMaxUnlockedTime(nextMaxUnlocked);
    }
    persistSession({ cursorTime: nextTime, maxUnlockedTime: nextMaxUnlocked });
  }, [
    cursorIndex,
    dailyBars,
    maxStepIndex,
    maxUnlockedIndex,
    maxUnlockedTime,
    minStepIndex,
    persistSession
  ]);

  const togglePanel = useCallback((force?: boolean) => {
    setPanelCollapsed((prev) => {
      const next = typeof force === "boolean" ? force : !prev;
      if (sessionId) {
        persistSession({
          uiState: { panelCollapsed: next, notesCollapsed, tradeLogCollapsed }
        });
      }
      return next;
    });
  }, [notesCollapsed, persistSession, sessionId, tradeLogCollapsed]);

  const toggleNotes = useCallback(() => {
    setNotesCollapsed((prev) => {
      const next = !prev;
      persistSession({
        uiState: { panelCollapsed, notesCollapsed: next, tradeLogCollapsed }
      });
      return next;
    });
  }, [panelCollapsed, persistSession, tradeLogCollapsed]);

  const toggleTradeLog = useCallback(() => {
    setTradeLogCollapsed((prev) => {
      const next = !prev;
      persistSession({
        uiState: { panelCollapsed, notesCollapsed, tradeLogCollapsed: next }
      });
      return next;
    });
  }, [notesCollapsed, panelCollapsed, persistSession]);

  useEffect(() => {
    const handler = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement | null;
      if (target && (target.tagName === "INPUT" || target.tagName === "TEXTAREA")) {
        return;
      }
      if (event.key === "ArrowRight") {
        event.preventDefault();
        handleStep(1);
        return;
      }
      if (event.key === "ArrowLeft") {
        event.preventDefault();
        handleStep(-1);
        return;
      }
      if (event.key.toLowerCase() === "p") {
        togglePanel();
        return;
      }
      if (event.key === "Escape") {
        setSessionManagerOpen(false);
        togglePanel(true);
        return;
      }
      if (event.key.toLowerCase() === "m") {
        toggleNotes();
      }
      if (event.key.toLowerCase() === "h") {
        toggleTradeLog();
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [handleStep, toggleNotes, togglePanel, toggleTradeLog]);

  useEffect(() => {
    if (!panelCollapsed && !startDate && startDateInputRef.current) {
      startDateInputRef.current.focus();
    }
  }, [panelCollapsed, startDate]);

  useEffect(() => {
    const handleMove = (event: MouseEvent | TouchEvent) => {
      if (!resizingRef.current || !bottomRowRef.current) return;
      let clientX = 0;
      if ("touches" in event) {
        if (!event.touches.length) return;
        event.preventDefault();
        clientX = event.touches[0].clientX;
      } else {
        clientX = event.clientX;
      }
      const rect = bottomRowRef.current.getBoundingClientRect();
      if (rect.width <= 0) return;
      const position = clampValue((clientX - rect.left) / rect.width, 0.05, 0.95);
      const nextWeekly = clampValue(position, MIN_WEEKLY_RATIO, 1 - MIN_MONTHLY_RATIO);
      setWeeklyRatio(nextWeekly);
    };

    const handleUp = () => {
      resizingRef.current = false;
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
    return () => {
      if (hoverRafRef.current !== null) {
        window.cancelAnimationFrame(hoverRafRef.current);
        hoverRafRef.current = null;
      }
      if (crosshairSyncRafRef.current !== null) {
        window.cancelAnimationFrame(crosshairSyncRafRef.current);
        crosshairSyncRafRef.current = null;
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

  const syncCrosshair = (
    source: Timeframe,
    time: number | null,
    point?: { x: number; y: number } | null
  ) => {
    const lock = crosshairSyncRef.current;
    if (lock && lock.source !== source) {
      return;
    }
    crosshairSyncRef.current = { source, time };
    scheduleHoverTime(time);
    const resolvedPoint = point ?? null;
    if (source !== "daily") {
      dailyChartRef.current?.setCrosshair(time, resolvedPoint);
    }
    if (source !== "weekly") {
      weeklyChartRef.current?.setCrosshair(time, resolvedPoint);
    }
    if (source !== "monthly") {
      monthlyChartRef.current?.setCrosshair(time, resolvedPoint);
    }
    if (crosshairSyncRafRef.current !== null) {
      window.cancelAnimationFrame(crosshairSyncRafRef.current);
    }
    crosshairSyncRafRef.current = window.requestAnimationFrame(() => {
      crosshairSyncRafRef.current = null;
      const latest = crosshairSyncRef.current;
      if (latest && latest.source === source && latest.time === time) {
        crosshairSyncRef.current = null;
      }
    });
  };

  const startResize = () => (event: ReactMouseEvent | ReactTouchEvent) => {
    event.preventDefault();
    resizingRef.current = true;
  };

  const resolveActiveCandle = () => {
    if (cursorCandle) return cursorCandle;
    if (dailyBars.length) return dailyBars[dailyBars.length - 1];
    return null;
  };

  const handleDailyCrosshair = (time: number | null, point?: { x: number; y: number } | null) => {
    syncCrosshair("daily", time, point);
  };

  const handleWeeklyCrosshair = (time: number | null, point?: { x: number; y: number } | null) => {
    syncCrosshair("weekly", time, point);
  };

  const handleMonthlyCrosshair = (time: number | null, point?: { x: number; y: number } | null) => {
    syncCrosshair("monthly", time, point);
  };

  const pushTrade = (trade: PracticeTrade | PracticeTrade[]) => {
    const newTrades = Array.isArray(trade) ? trade : [trade];
    if (newTrades.length === 0) return;
    const nextTrades = [...trades, ...newTrades];
    setTrades(nextTrades);
    persistSession({ trades: nextTrades });
  };

  const handleHudAction = (side: "buy" | "sell", delta: number) => {
    if (!code) return;
    if (isLocked) {
      setToastMessage("過去の日時では編集できません");
      return;
    }
    const candle = cursorCandle ?? resolveActiveCandle();
    if (!candle) {
      setToastMessage("チャートデータがありません");
      return;
    }
    const qty = Math.abs(delta);
    const isBuy = side === "buy";
    const action = delta > 0 ? "open" : "close";
    const book = isBuy ? "long" : "short";
    const actualSide =
      isBuy && action === "open"
        ? "buy"
        : isBuy
          ? "sell"
          : action === "open"
            ? "sell"
            : "buy";
    const available = book === "long" ? positionSummary.longLots : positionSummary.shortLots;
    const finalQty = action === "close" ? Math.min(qty, available) : qty;
    if (action === "close" && finalQty <= 0) {
      if (delta !== 0) setToastMessage("減玉できる建玉がありません");
      return;
    }
    const newTrade: PracticeTrade = {
      id: `${Date.now()}-${Math.random().toString(16).slice(2)}`,
      time: candle.time,
      side: actualSide,
      action,
      book,
      quantity: finalQty,
      price: candle.close,
      lotSize,
      note: tradeNote.trim() ? tradeNote.trim() : undefined
    };
    pushTrade(newTrade);
  };

  const handleUndo = () => {
    if (!trades.length || cursorTime == null || isLocked) return;
    const last = trades[trades.length - 1];
    if (!last || last.time !== cursorTime) return;
    const next = trades.slice(0, -1);
    setTrades(next);
    persistSession({ trades: next });
  };

  const handleCloseAllPositions = () => {
    if (isLocked || !cursorCandle) {
      setToastMessage("この操作は現在実行できません。");
      return;
    }
    const { longLots, shortLots } = positionSummary;
    if (longLots === 0 && shortLots === 0) {
      setToastMessage("決済できる建玉がありません。");
      return;
    }
    if (typeof window !== "undefined") {
      const ok = window.confirm("全ての建玉を決済しますか？（この操作は取り消せません）");
      if (!ok) return;
    }

    const tradesToPush: PracticeTrade[] = [];
    if (longLots > 0) {
      tradesToPush.push({
        id: `${Date.now()}-${Math.random().toString(16).slice(2)}-close-long`,
        time: cursorCandle.time,
        side: "sell",
        action: "close",
        book: "long",
        quantity: longLots,
        price: cursorCandle.close,
        lotSize,
        note: "全決済"
      });
    }
    if (shortLots > 0) {
      tradesToPush.push({
        id: `${Date.now()}-${Math.random().toString(16).slice(2)}-close-short`,
        time: cursorCandle.time,
        side: "buy",
        action: "close",
        book: "short",
        quantity: shortLots,
        price: cursorCandle.close,
        lotSize,
        note: "全決済"
      });
    }
    if (tradesToPush.length > 0) {
      pushTrade(tradesToPush);
      setToastMessage("全決済を実行しました。");
    }
  };

  const handleResetDay = () => {
    if (cursorTime == null || isLocked) return;
    const next = trades.filter((trade) => trade.time !== cursorTime);
    if (next.length === trades.length) return;
    setTrades(next);
    persistSession({ trades: next });
  };

  const handleDeleteTrade = (id: string) => {
    if (isLocked || cursorTime == null) return;
    const target = trades.find((trade) => trade.id === id);
    if (!target || target.time !== cursorTime) return;
    const next = trades.filter((trade) => trade.id !== id);
    setTrades(next);
    persistSession({ trades: next });
  };

  const handleEditTrade = (id: string, patch: Partial<PracticeTrade>) => {
    if (isLocked || cursorTime == null) return;
    const target = trades.find((trade) => trade.id === id);
    if (!target || target.time !== cursorTime) return;
    const next = trades.map((trade) => (trade.id === id ? { ...trade, ...patch } : trade));
    setTrades(next);
    persistSession({ trades: next });
  };

  useEffect(() => {
    if (!editingTradeId) return;
    const target = trades.find((trade) => trade.id === editingTradeId);
    if (!target || cursorTime == null || target.time !== cursorTime || isLocked) {
      setEditingTradeId(null);
    }
  }, [editingTradeId, trades, cursorTime, isLocked]);

  const handleInitiateNewSession = () => {
    setSessionId(null);
    setStartDate(null);
    setEndDate(null);
    setStartDateDraft("");
    setTrades([]);
    setSessionNotes("");
    setCursorTime(null);
    setMaxUnlockedTime(null);
    setPanelCollapsed(false);
  };

  const handleSelectSession = (nextId: string) => {
    if (nextId === sessionId) return;
    setSessionId(nextId);
    setSessionManagerOpen(false);
  };

  const _handleEndSession = () => {
    if (!cursorCandle || !sessionId || endDate) return;
    if (typeof window !== "undefined") {
      const ok = window.confirm("現在の練習を終了しますか？");
      if (!ok) return;
    }
    const date = formatDate(cursorCandle.time);
    setEndDate(date);
    persistSession({ endDate: date });
    setToastMessage("練習を終了しました。");
  };


  const handleDeleteSession = (targetId?: string) => {
    const id = targetId ?? sessionId;
    if (!id) return;
    if (typeof window !== "undefined") {
      const ok = window.confirm("このセッションを削除しますか？");
      if (!ok) return;
    }
    api
      .delete("/practice/session", { params: { session_id: id } })
      .finally(() => {
        if (sessionStorageKey) {
          localStorage.removeItem(sessionStorageKey);
        }
        if (id === sessionId) {
          setSessionId(null);
        }
        refreshSessions();
        setToastMessage("セッションを削除しました。");
      });
  };


  const buildExportPayload = () => {
    const exportedAt = new Date().toISOString();
    const cursorLabel = cursorTime != null ? formatDate(cursorTime) : null;
    const rangeStartLabel = rangeStartTime != null ? formatDate(rangeStartTime) : cursorLabel;
    const selectedRange =
      rangeMonths === 12 ? "1Y" : rangeMonths === 24 ? "2Y" : `${rangeMonths}M`;

    const lastIndex = dailyBars.length ? dailyBars.length - 1 : 0;
    const cursorIdx = cursorIndex != null ? cursorIndex : lastIndex;
    const warmupDepth = Math.max(
      EXPORT_MA_PERIODS[EXPORT_MA_PERIODS.length - 1],
      EXPORT_ATR_PERIOD,
      EXPORT_VOLUME_PERIOD
    );
    const warmupStartIndex = Math.max(0, cursorIdx - warmupDepth - 5);
    const warmupBars = dailyBars.slice(warmupStartIndex, cursorIdx + 1);
    const warmupStartTime = warmupBars[0]?.time ?? null;

    const buildSignalSeries = (bars: DailyBar[], timeframe: "D" | "W" | "M") => {
      const hits: {
        date: string;
        timeframe: "D" | "W" | "M";
        ruleId: string;
        label: string;
        value: string;
        tags: string[];
      }[] = [];
      const byTime = new Map<number, { labels: string[] }>();
      const rows = bars.map((bar) => [bar.time, bar.open, bar.high, bar.low, bar.close, bar.volume]);
      for (let i = 0; i < bars.length; i += 1) {
        const metrics = computeSignalMetrics(rows.slice(0, i + 1));
        const labels = metrics.signals.map((signal) => signal.label);
        byTime.set(bars[i].time, { labels });
        metrics.signals.forEach((signal) => {
          hits.push({
            date: formatDate(bars[i].time),
            timeframe,
            ruleId: signal.label,
            label: signal.label,
            value: signal.kind,
            tags: [signal.kind]
          });
        });
      }
      return { byTime, hits };
    };

    const buildExportBars = (
      bars: DailyBar[],
      signalMap: Map<number, { labels: string[] }>
    ) => {
      const candles = bars.map((bar) => ({
        time: bar.time,
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close
      }));
      const maMaps = new Map<number, Record<number, number>>();
      EXPORT_MA_PERIODS.forEach((period) => {
        computeMA(candles, period).forEach((point) => {
          const existing = maMaps.get(point.time) ?? {};
          existing[period] = point.value;
          maMaps.set(point.time, existing);
        });
      });

      const countState = new Map<number, { up: number; down: number }>();
      EXPORT_MA_PERIODS.forEach((period) => countState.set(period, { up: 0, down: 0 }));
      let prevClose: number | null = null;
      const trWindow: number[] = [];
      let trSum = 0;
      const volumeWindow: number[] = [];
      let volumeSum = 0;

      const rows = bars.map((bar, index) => {
        const maValues = maMaps.get(bar.time) ?? {};
        const count: Record<string, number | null> = {};
        let aboveCount = 0;
        let belowCount = 0;

        EXPORT_MA_PERIODS.forEach((period) => {
          const maValue = maValues[period];
          const state = countState.get(period) ?? { up: 0, down: 0 };
          if (maValue == null || !Number.isFinite(maValue)) {
            state.up = 0;
            state.down = 0;
          } else if (bar.close > maValue) {
            state.up += 1;
            state.down = 0;
            aboveCount += 1;
          } else if (bar.close < maValue) {
            state.down += 1;
            state.up = 0;
            belowCount += 1;
          } else {
            state.up = 0;
            state.down = 0;
          }
          countState.set(period, state);
          count[`up${period}`] = state.up;
          count[`down${period}`] = state.down;
        });

        const tr = prevClose == null
          ? bar.high - bar.low
          : Math.max(
            bar.high - bar.low,
            Math.abs(bar.high - prevClose),
            Math.abs(bar.low - prevClose)
          );
        prevClose = bar.close;
        trWindow.push(tr);
        trSum += tr;
        if (trWindow.length > EXPORT_ATR_PERIOD) {
          trSum -= trWindow.shift() ?? 0;
        }
        const atr14 = trWindow.length >= EXPORT_ATR_PERIOD ? trSum / EXPORT_ATR_PERIOD : null;

        const volume = Number.isFinite(bar.volume) ? bar.volume : 0;
        volumeWindow.push(volume);
        volumeSum += volume;
        if (volumeWindow.length > EXPORT_VOLUME_PERIOD) {
          volumeSum -= volumeWindow.shift() ?? 0;
        }
        const volumeRatio =
          volumeWindow.length >= EXPORT_VOLUME_PERIOD && volumeSum > 0
            ? volume / (volumeSum / volumeWindow.length)
            : null;

        const body = Math.abs(bar.close - bar.open);
        const range = Math.max(0, bar.high - bar.low);
        const upperWick = bar.high - Math.max(bar.open, bar.close);
        const lowerWick = Math.min(bar.open, bar.close) - bar.low;
        const bodyRatio = range > 0 ? body / range : 0;
        const direction = bar.close >= bar.open ? "up" : "down";

        const ma20 = maValues[20] ?? null;
        let slope20: number | null = null;
        if (index >= EXPORT_SLOPE_LOOKBACK && ma20 != null) {
          const pastBar = bars[index - EXPORT_SLOPE_LOOKBACK];
          const pastMa20 = maMaps.get(pastBar.time)?.[20] ?? null;
          if (pastMa20 != null) {
            slope20 = ma20 - pastMa20;
          }
        }

        return {
          date: formatDate(bar.time),
          o: bar.open,
          h: bar.high,
          l: bar.low,
          c: bar.close,
          v: bar.volume,
          ma: {
            ma7: maValues[7] ?? null,
            ma20: maValues[20] ?? null,
            ma60: maValues[60] ?? null,
            ma100: maValues[100] ?? null,
            ma200: maValues[200] ?? null
          },
          slope: {
            ma20: slope20
          },
          pos: {
            aboveCount,
            belowCount
          },
          count,
          candle: {
            body,
            range,
            upperWick,
            lowerWick,
            bodyRatio,
            direction
          },
          atr14,
          volumeRatio,
          isPartial: Boolean(bar.isPartial),
          signalsRaw: signalMap.get(bar.time) ?? { labels: [] }
        };
      });

      return rows.filter((row) => {
        if (rangeStartTime == null) return true;
        const time = parseDateString(row.date);
        return time != null && time >= rangeStartTime;
      });
    };

    const dailySignals = buildSignalSeries(warmupBars, "D");
    const weeklySignals = buildSignalSeries(weeklyAggregate.bars, "W");
    const monthlySignals = buildSignalSeries(monthlyAggregate.bars, "M");

    const dailyExportBars = buildExportBars(warmupBars, dailySignals.byTime);
    const weeklyExportBars = buildExportBars(weeklyAggregate.bars, weeklySignals.byTime);
    const monthlyExportBars = buildExportBars(monthlyAggregate.bars, monthlySignals.byTime);

    const rangeTrades = visibleTrades.filter((trade) => {
      if (cursorTime != null && trade.time > cursorTime) return false;
      if (rangeStartTime != null && trade.time < rangeStartTime) return false;
      return true;
    });

    const beforeRangeTrades = visibleTrades.filter((trade) =>
      rangeStartTime != null ? trade.time < rangeStartTime : false
    );
    const rangeSnapshot = buildPracticeLedger(beforeRangeTrades, lotSize).summary;

    const positionByDate = dailyPositions
      .filter((pos) => (rangeStartTime == null ? true : pos.time >= rangeStartTime))
      .map((pos) => ({
        date: pos.date,
        time: pos.time,
        longLots: pos.longLots,
        shortLots: pos.shortLots,
        avgLongPrice: pos.avgLongPrice,
        avgShortPrice: pos.avgShortPrice,
        realizedPnL: pos.realizedPnL,
        unrealizedPnL: pos.unrealizedPnL,
        totalPnL: pos.totalPnL
      }));

    const pnlSummary = {
      realized: positionSummary.realizedPnL,
      unrealized: unrealizedPnL,
      total:
        (positionSummary.realizedPnL ?? 0) + (unrealizedPnL ?? 0)
    };

    return {
      meta: {
        schemaVersion: 1,
        exportedAt,
        code,
        sessionId,
        scope: {
          rangeStartDate: rangeStartLabel,
          cursorDate: cursorLabel,
          selectedRange,
          calcWarmupStartDate: warmupStartTime != null ? formatDate(warmupStartTime) : null
        }
      },
      settings: {
        lotSizeDefault: DEFAULT_LOT_SIZE
      },
      series: {
        daily: { bars: dailyExportBars },
        weekly: { bars: weeklyExportBars },
        monthly: { bars: monthlyExportBars }
      },
      positions: {
        rangeStartSnapshot: {
          date: rangeStartLabel,
          longLots: rangeSnapshot.longLots,
          shortLots: rangeSnapshot.shortLots,
          avgLongPrice: rangeSnapshot.avgLongPrice,
          avgShortPrice: rangeSnapshot.avgShortPrice,
          realizedPnL: rangeSnapshot.realizedPnL
        },
        tradeLog: rangeTrades,
        positionByDate,
        pnlSummary
      },
      signals: {
        signalHits: [...dailySignals.hits, ...weeklySignals.hits, ...monthlySignals.hits]
      },
      summary: {}
    };
  };

  const handleApplyStartDate = () => {
    if (!code) return;
    const date = startDateDraft || (cursorCandle ? formatDate(cursorCandle.time) : "");
    if (!date) {
      setToastMessage("開始日を選択してください。");
      return;
    }
    if (!dailyBars.length) {
      setToastMessage("日足データが読み込まれていません。");
      return;
    }
    const nextTime = parseDateString(date);
    if (nextTime == null) {
      setToastMessage("開始日が正しくありません。");
      return;
    }
    const idx = resolveExactIndex(dailyBars, nextTime);
    if (idx == null) {
      setToastMessage("指定日が日足データにありません。");
      return;
    }

    const apply = (targetSessionId: string, isNew: boolean) => {
      let nextTrades = trades;
      if (!isNew && trades.length > 0 && date !== startDate) {
        const ok =
          typeof window === "undefined"
            ? false
            : window.confirm("開始日を変更し、既存の建玉をクリアしますか？");
        if (!ok) return;
        nextTrades = [];
        setTrades([]);
      }
      const resolved = dailyBars[idx]?.time;
      if (resolved != null) {
        setCursorTime(resolved);
        setMaxUnlockedTime(resolved);
      }
      setStartDate(date);
      setStartDateDraft(date);

      const payload = {
        session_id: targetSessionId,
        code,
        start_date: date,
        cursor_time: resolved ?? null,
        max_unlocked_time: resolved ?? null,
        trades: nextTrades,
        lot_size: lotSize,
        range_months: rangeMonths,
        notes: sessionNotes,
        ui_state: { panelCollapsed: false, notesCollapsed, tradeLogCollapsed }
      };
      api
        .post("/practice/session", payload)
        .then(() => {
          setToastMessage(isNew ? "練習を開始しました。" : "開始日を更新しました。");
          togglePanel(true);
          if (isNew) {
            refreshSessions(targetSessionId);
          } else {
            persistSession({
              startDate: date,
              cursorTime: resolved ?? null,
              maxUnlockedTime: resolved ?? null,
              trades: nextTrades
            });
          }
        })
        .catch(() => {
          setToastMessage("セッションの開始/更新に失敗しました。");
        });
    };

    if (sessionId) {
      apply(sessionId, false);
    } else {
      const nextId = createSessionId();
      setSessionId(nextId);
      apply(nextId, true);
    }
  };

  const handleScreenshot = async () => {
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
          setToastMessage("スクショを保存しました (保存先不明)");
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
  };

  const handleExport = () => {
    if (!code) return;
    const exportPayload = buildExportPayload();
    exportFile(
      `practice_${code}_${startDate ?? "unset"}.json`,
      JSON.stringify(exportPayload, null, 2),
      "application/json"
    );
  };

  const handleSaveNotes = () => {
    persistSession({ notes: sessionNotes });
    setToastMessage("メモを保存しました。");
  };

  const handleJumpToTrade = (time: number) => {
    syncCrosshair("daily", time);
  };

  const handleAIExport = async () => {
    const dailyMemos: Record<string, string> = {};
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
      signals: practiceSignals,
      showBoxes: false,
      showPositions: true,
      boxes: [],
      dailyMemos,
      currentPositions: [
        {
          brokerKey: "practice",
          brokerLabel: "practice",
          longLots: positionSummary.longLots,
          shortLots: positionSummary.shortLots,
          avgLongPrice: positionSummary.avgLongPrice,
          avgShortPrice: positionSummary.avgShortPrice,
          realizedPnL: positionSummary.realizedPnL
        }
      ],
      currentPrice: cursorCandle?.close ?? null
    });
    const copied = await copyToClipboard(exportData.markdown);
    if (copied) {
      setToastMessage("AI用銘柄情報をクリップボードにコピーしました");
    } else {
      setToastMessage("クリップボードへのコピーに失敗しました");
    }
  };

  const dailyEmptyMessage = dailyCandles.length === 0 ? dailyErrors[0] ?? "No data" : null;
  const weeklyEmptyMessage = weeklyCandles.length === 0 ? dailyErrors[0] ?? "No data" : null;
  const monthlyEmptyMessage = monthlyCandles.length === 0 ? dailyErrors[0] ?? "No data" : null;

  return (
    <div className="detail-shell practice-shell">
      <div className="detail-header practice-header">
        <div className="practice-header-left">
          <div className="practice-nav-group">
            <IconButton
              icon={<IconArrowLeft size={20} />}
              tooltip="一覧に戻る"
              onClick={() => navigate("/")}
            />
            <IconButton
              icon={<IconArrowBackUp size={20} />}
              tooltip="前の画面に戻る"
              onClick={() => navigate(-1)}
            />
          </div>
          <div className="detail-title">
            <div className="detail-title-main">
              <div className="title">{code}</div>
              {tickerName && <div className="title-name">{tickerName}</div>}
            </div>
            <div className="subtitle">練習</div>
          </div>
          <div className="practice-header-group">
            <div className="practice-session-controls">
              <select
                value={sessionId ?? ""}
                onChange={(event) => handleSelectSession(event.target.value)}
                disabled={sessionsLoading}
                className="practice-session-select"
              >
                {!sessionId && <option value="">セッションを選択...</option>}
                {sessions.map((session) => (
                  <option key={session.session_id} value={session.session_id}>
                    {session.start_date ?? "開始未設定"}
                    {session.end_date ? ` - ${session.end_date}` : " (進行中)"}
                  </option>
                ))}
              </select>
              <IconButton
                icon={<IconPlus size={18} />}
                tooltip="新規セッション"
                onClick={handleInitiateNewSession}
              />
              <IconButton
                icon={<IconPlus size={18} />}
                tooltip="新規セッション"
                onClick={handleInitiateNewSession}
              />
            </div>
            <div className="practice-session-meta">
              <span className={`practice-session-badge ${sessionBadgeClass}`}>
                {sessionBadgeLabel}
              </span>
              <span className="practice-session-range-text">
                {sessionRangeLabel}
              </span>
            </div>
          </div>
        </div>
        <div className="practice-header-actions">
          <div className="practice-header-group">
            <div className="segmented practice-range">
              {RANGE_PRESETS.map((preset) => (
                <button
                  key={preset.label}
                  className={rangeMonths === preset.months ? "active" : ""}
                  onClick={() => {
                    setRangeMonths(preset.months);
                    persistSession({ rangeMonths: preset.months });
                  }}
                >
                  {preset.label}
                </button>
              ))}
            </div>
            <div className="practice-view-meta">{headerMetaLabel}</div>
          </div>
          <div className="practice-header-group">
            <div className="practice-header-stack">
              <IconButton
                icon={<IconChevronLeft size={20} />}
                tooltip="1日戻る"
                disabled={!canStepBack}
                onClick={() => handleStep(-1)}
              />
              <IconButton
                icon={<IconChevronRight size={20} />}
                tooltip="1日進む"
                disabled={!canStepForward}
                onClick={() => handleStep(1)}
              />
              <IconButton
                icon={<IconRefresh size={18} />}
                tooltip="当日リセット"
                disabled={!canResetDay}
                onClick={handleResetDay}
              />
              <div className="practice-divider" />
              <IconButton
                icon={<IconCamera size={18} />}
                tooltip="スクショ"
                disabled={screenshotBusy}
                onClick={handleScreenshot}
              />
              <IconButton
                icon={<IconFileDownload size={18} />}
                tooltip="出力 (JSON)"
                onClick={handleExport}
              />
              <IconButton
                icon={<IconSparkles size={18} />}
                tooltip="AI出力"
                onClick={handleAIExport}
              />
              <button
                className="indicator-button practice-panel-toggle"
                onClick={() => togglePanel(panelCollapsed ? false : true)}
                aria-label={panelCollapsed ? "パネルを開く" : "パネルを閉じる"}
              >
                {panelCollapsed ? "← パネル" : "→ パネル"}
              </button>
            </div>
          </div>
        </div>
      </div>
      {sessionManagerOpen && (
        <div className="practice-session-list">
          <div className="practice-session-list-header">
            <span>過去のセッション</span>
            <button onClick={() => setSessionManagerOpen(false)}>&times;</button>
          </div>
          {sessions.length === 0 ? (
            <div className="practice-session-empty">まだセッションがありません。</div>
          ) : (
            sessions.map((session) => (
              <div
                key={session.session_id}
                className={`practice-session-row ${session.session_id === sessionId ? "is-active" : ""
                  }`}
              >
                <div className="practice-session-range">
                  <div className="practice-session-title">
                    {session.start_date ?? "開始未設定"}
                    {session.end_date ? ` - ${session.end_date}` : " (進行中)"}
                  </div>
                  <div className="practice-session-meta">
                    更新: {session.updated_at ?? "--"}
                  </div>
                </div>
                <div className="practice-session-actions">
                  <button onClick={() => handleSelectSession(session.session_id)}>開く</button>
                  <button onClick={() => handleDeleteSession(session.session_id)}>削除</button>
                </div>
              </div>
            ))
          )}
        </div>
      )}

      <div className={`practice-content-grid ${panelCollapsed ? "panel-collapsed" : ""}`}>
        <div className="practice-left-column">
          <div className="practice-charts">
            <div className="detail-split practice-split">
              <div className="detail-row detail-row-top" style={{ flex: `${DAILY_ROW_RATIO} 1 0%` }}>
                <div className="detail-pane-header">日足</div>
                <div className="detail-chart">
                  <DetailChart
                    ref={dailyChartRef}
                    candles={dailyCandles}
                    volume={dailyVolume}
                    maLines={dailyMaLines}
                    showVolume={dailyVolume.length > 0}
                    boxes={[]}
                    showBoxes={false}
                    cursorTime={hoverTime == null ? cursorCandle?.time ?? null : null}
                    positionOverlay={{
                      dailyPositions,
                      tradeMarkers,
                      showOverlay: true,
                      showPnL: false,
                      hoverTime: hoverTime ?? cursorCandle?.time ?? null,
                      showMarkers: true,
                      markerSuffix: lotSize !== DEFAULT_LOT_SIZE ? `x${lotSize}` : undefined
                    }}
                    onCrosshairMove={handleDailyCrosshair}
                  />
                  <div className="practice-hud-mini">
                    <div className="practice-hud-mini-title">建玉</div>
                    <div className="practice-hud-mini-row">
                      売{positionSummary.shortLots}-買{positionSummary.longLots}
                    </div>
                    <div className="practice-hud-mini-row">
                      {netLots === 0
                        ? "ネット0"
                        : netLots > 0
                          ? `ネット買い ${netLots}`
                          : `ネット売り ${Math.abs(netLots)}`}
                    </div>
                    <div className="practice-hud-mini-row">
                      実 {formatNumber(positionSummary.realizedPnL, 0)} / 評{" "}
                      {formatNumber(unrealizedPnL, 0)}
                    </div>
                    <div className="practice-hud-mini-row">株数 {lotSize}</div>
                    {isLocked && <div className="practice-hud-mini-lock">過去日閲覧</div>}
                  </div>
                  {dailyEmptyMessage && (
                    <div className="detail-chart-empty">日足: {dailyEmptyMessage}</div>
                  )}
                </div>
              </div>
              <div
                className="detail-row detail-row-bottom"
                style={{ flex: `${1 - DAILY_ROW_RATIO} 1 0%` }}
                ref={bottomRowRef}
              >
                <div className="detail-pane" style={{ flex: `${weeklyRatio} 1 0%` }}>
                  <div className="detail-pane-header">週足</div>
                  <div className="detail-chart">
                    <DetailChart
                      ref={weeklyChartRef}
                      candles={weeklyCandles}
                      volume={weeklyVolume}
                      maLines={weeklyMaLines}
                      showVolume={false}
                      boxes={[]}
                      showBoxes={false}
                      cursorTime={hoverTime == null ? weeklyCursorTime : null}
                      partialTimes={weeklyPartialTimes}
                      onCrosshairMove={handleWeeklyCrosshair}
                    />
                    {weeklyEmptyMessage && (
                      <div className="detail-chart-empty">週足: {weeklyEmptyMessage}</div>
                    )}
                  </div>
                </div>
                <div
                  className="detail-divider detail-divider-vertical"
                  onMouseDown={startResize()}
                  onTouchStart={startResize()}
                />
                <div className="detail-pane" style={{ flex: `${monthlyRatio} 1 0%` }}>
                  <div className="detail-pane-header">月足</div>
                  <div className="detail-chart">
                    <DetailChart
                      ref={monthlyChartRef}
                      candles={monthlyCandles}
                      volume={[]}
                      maLines={monthlyMaLines}
                      showVolume={false}
                      boxes={[]}
                      showBoxes={false}
                      cursorTime={hoverTime == null ? monthlyCursorTime : null}
                      partialTimes={monthlyPartialTimes}
                      onCrosshairMove={handleMonthlyCrosshair}
                    />
                    {monthlyEmptyMessage && (
                      <div className="detail-chart-empty">月足: {monthlyEmptyMessage}</div>
                    )}
                  </div>
                </div>
              </div>
            </div>
          </div>
          <div className={`practice-log ${tradeLogCollapsed ? "is-collapsed" : ""}`}>
            <div className="practice-log-header">
              <div>
                <div className="practice-log-title">建玉履歴</div>
                <div className="practice-log-sub">
                  {visibleTrades.length}件 | 実現損益 {formatNumber(positionSummary.realizedPnL, 0)}
                </div>
              </div>
              <div className="practice-log-actions"></div>
            </div>
            {!tradeLogCollapsed && (
              <>
                <div className="practice-log-table">
                  <div className="practice-log-row practice-log-head">
                    <span>日付</span>
                    <span>種別</span>
                    <span>玉数</span>
                    <span>約定</span>
                    <span>建玉</span>
                    <span>実現損益</span>
                    <span>メモ</span>
                    <span>操作</span>
                  </div>
                  {ledger.entries.length === 0 && (
                    <div className="practice-log-empty">まだ履歴がありません。</div>
                  )}
                  {ledger.entries.map((entry) => {
                    const trade = entry.trade;
                    const label =
                      trade.book === "long"
                        ? trade.action === "open"
                          ? "買い 新規"
                          : "買い 決済"
                        : trade.action === "open"
                          ? "売り 新規"
                          : "売り 決済";
                    const canEdit = !isLocked && cursorTime != null && trade.time === cursorTime;
                    const isEditing = canEdit && editingTradeId === trade.id;
                    return (
                      <div
                        className="practice-log-row"
                        key={trade.id}
                        onClick={() => handleJumpToTrade(trade.time)}
                        role="button"
                        tabIndex={0}
                      >
                        <span>{formatDate(trade.time)}</span>
                        <span>{label}</span>
                        {isEditing ? (
                          <>
                            <span>
                              <input
                                type="number"
                                min={0}
                                value={trade.quantity}
                                onChange={(event) =>
                                  handleEditTrade(trade.id, {
                                    quantity: Number(event.target.value)
                                  })
                                }
                              />
                            </span>
                            <span>
                              <input
                                type="number"
                                step="0.1"
                                min={0}
                                value={trade.price}
                                onChange={(event) =>
                                  handleEditTrade(trade.id, {
                                    price: Number(event.target.value)
                                  })
                                }
                              />
                            </span>
                          </>
                        ) : (
                          <>
                            <span>{trade.quantity}</span>
                            <span>{formatNumber(trade.price, 2)}</span>
                          </>
                        )}
                        <span>{entry.positionText}</span>
                        <span className={entry.realizedDelta >= 0 ? "pnl-up" : "pnl-down"}>
                          {entry.realizedDelta === 0 ? "--" : formatNumber(entry.realizedDelta, 0)}
                        </span>
                        {isEditing ? (
                          <span>
                            <input
                              type="text"
                              value={trade.note ?? ""}
                              onChange={(event) =>
                                handleEditTrade(trade.id, {
                                  note: event.target.value
                                })
                              }
                            />
                          </span>
                        ) : (
                          <span>{trade.note ?? "--"}</span>
                        )}
                        <span className="practice-log-actions">
                          {isEditing ? (
                            <>
                              <button
                                onClick={(event) => {
                                  event.stopPropagation();
                                  setEditingTradeId(null);
                                }}
                                disabled={!canEdit}
                              >
                                保存
                              </button>
                              <button
                                onClick={(event) => {
                                  event.stopPropagation();
                                  setEditingTradeId(null);
                                }}
                                disabled={!canEdit}
                              >
                                キャンセル
                              </button>
                            </>
                          ) : (
                            <>
                              <button
                                onClick={(event) => {
                                  event.stopPropagation();
                                  if (!canEdit) return;
                                  setEditingTradeId(trade.id);
                                }}
                                disabled={!canEdit}
                              >
                                編集
                              </button>
                              <button
                                onClick={(event) => {
                                  event.stopPropagation();
                                  if (!canEdit) return;
                                  handleDeleteTrade(trade.id);
                                }}
                                disabled={!canEdit}
                              >
                                削除
                              </button>
                            </>
                          )}
                        </span>
                      </div>
                    );
                  })}
                </div>
                <div className={`practice-notes ${notesCollapsed ? "is-collapsed" : ""}`}>
                  <div className="practice-notes-header">
                    <div>メモ</div>
                    <div className="practice-notes-actions">
                      <button className="indicator-button" onClick={toggleNotes}>
                        {notesCollapsed ? "メモを表示" : "メモを隠す"}
                      </button>
                      <button className="indicator-button" onClick={handleSaveNotes}>
                        メモを保存
                      </button>
                    </div>
                  </div>
                  {!notesCollapsed && (
                    <textarea
                      value={sessionNotes}
                      onChange={(event) => setSessionNotes(event.target.value)}
                      placeholder="メモ（振り返り）"
                    />
                  )}
                </div>
              </>
            )}
          </div>
        </div>
        <div className="practice-panel">
          <div className="practice-panel-header">
            <div>
              <div className="practice-panel-title">{startDate ? "建玉" : "新規練習"}</div>
              <div className="practice-panel-sub">
                {startDate
                  ? `売${positionSummary.shortLots}-買${positionSummary.longLots}`
                  : "開始日を設定"}
              </div>
            </div>
            <button
              className="practice-panel-close"
              onClick={() => togglePanel(true)}
              aria-label="パネルを閉じる"
            >
              →
            </button>
          </div>
          {!panelCollapsed && (
            <div className="practice-panel-body">
              <div className="practice-guide">{guideText}</div>
              {!startDate ? (
                <div className="practice-session-settings">
                  <div className="practice-session-settings-title">練習の開始日</div>
                  <div className="practice-session-settings-controls">
                    <input
                      ref={startDateInputRef}
                      type="date"
                      value={startDateDraft}
                      onChange={(event) => setStartDateDraft(event.target.value)}
                    />
                    <button className="indicator-button" onClick={handleApplyStartDate}>
                      開始日を確定
                    </button>
                  </div>
                </div>
              ) : (
                <>
                  <div className="practice-hud-net">
                    {netLots === 0
                      ? "ネット0"
                      : netLots > 0
                        ? `ネット買い ${netLots}`
                        : `ネット売り ${Math.abs(netLots)}`}
                  </div>
                  {isLocked && <div className="practice-hud-lock">過去日閲覧（操作不可）</div>}
                  <div className="practice-hud-pnl">
                    <div>実現損益: {formatNumber(positionSummary.realizedPnL, 0)}</div>
                    <div>評価損益: {formatNumber(unrealizedPnL, 0)}</div>
                  </div>
                  <div className="practice-hud-step">
                    <div className="practice-hud-step-label">進める</div>
                    <div className="practice-hud-step-controls">
                      <button onClick={() => handleStep(-1)} disabled={!canStepBack}>
                        前日
                      </button>
                      <button onClick={() => handleStep(1)} disabled={!canStepForward}>
                        翌日
                      </button>
                    </div>
                    <div className="practice-hud-step-meta">{headerMetaLabel}</div>
                  </div>
                  <div className="practice-hud-avg">
                    <div>平均買い: {formatNumber(positionSummary.avgLongPrice, 2)}</div>
                    <div>平均売り: {formatNumber(positionSummary.avgShortPrice, 2)}</div>
                  </div>
                  <div className="practice-hud-lot">
                    <span>株数</span>
                    <input
                      type="number"
                      min={1}
                      value={lotSize}
                      onChange={(event) => {
                        const next =
                          Math.max(1, Number(event.target.value) || DEFAULT_LOT_SIZE);
                        setLotSize(next);
                        persistSession({ lotSize: next });
                      }}
                    />
                    <span>株</span>
                  </div>
                  <div className="practice-hud-note">
                    <input
                      type="text"
                      placeholder="メモ（振り返り）"
                      value={tradeNote}
                      onChange={(event) => setTradeNote(event.target.value)}
                    />
                  </div>
                  <div className="practice-hud-controls v3">
                    <div className="position-control-stack hud-sell-panel">
                      <div className="hud-side-label">売り</div>
                      <div className="control-row">
                        <button onClick={() => handleHudAction("sell", 5)} disabled={isLocked}>
                          +5
                        </button>
                        <button onClick={() => handleHudAction("sell", 1)} disabled={isLocked}>
                          +1
                        </button>
                      </div>
                      <div className="quantity-display">{positionSummary.shortLots}</div>
                      <div className="control-row">
                        <button onClick={() => handleHudAction("sell", -1)} disabled={isLocked}>
                          -1
                        </button>
                        <button onClick={() => handleHudAction("sell", -5)} disabled={isLocked}>
                          -5
                        </button>
                      </div>
                    </div>
                    <PositionDonutChart
                      sell={positionSummary.shortLots}
                      buy={positionSummary.longLots}
                      size={50}
                    />
                    <div className="position-control-stack hud-buy-panel">
                      <div className="hud-side-label">買い</div>
                      <div className="control-row">
                        <button onClick={() => handleHudAction("buy", 5)} disabled={isLocked}>
                          +5
                        </button>
                        <button onClick={() => handleHudAction("buy", 1)} disabled={isLocked}>
                          +1
                        </button>
                      </div>
                      <div className="quantity-display">{positionSummary.longLots}</div>
                      <div className="control-row">
                        <button onClick={() => handleHudAction("buy", -1)} disabled={isLocked}>
                          -1
                        </button>
                        <button onClick={() => handleHudAction("buy", -5)} disabled={isLocked}>
                          -5
                        </button>
                      </div>
                    </div>
                  </div>
                  <div className="practice-hud-actions">
                    <button onClick={handleUndo} disabled={!canUndo}>
                      取り消し
                    </button>
                    <button
                      onClick={handleCloseAllPositions}
                      disabled={
                        isLocked || (positionSummary.longLots === 0 && positionSummary.shortLots === 0)
                      }
                    >
                      全決済
                    </button>
                  </div>
                </>
              )}
            </div>
          )}
        </div>
      </div>
      <Toast
        message={toastMessage}
        onClose={() => { setToastMessage(null); setToastAction(null); }}
        action={toastAction}
        duration={toastAction ? 8000 : 4000}
      />
    </div>
  );
}
