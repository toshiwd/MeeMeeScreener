import * as React from "react";
import type {
  BarsResponse,
  Candle,
  DailyBar,
  OverlayPosition,
  OverlayTradeEvent,
  OverlayTradeMarker,
  PracticeLedgerEntry,
  PracticeTrade,
  VolumePoint
} from "./practiceTypes";

export const PositionDonutChart = ({
  buy,
  sell,
  size = 80,
  strokeWidth = 10
}: {
  buy: number;
  sell: number;
  size?: number;
  strokeWidth?: number;
}) => {
  const gross = buy + sell;
  const r = size / 2 - strokeWidth / 2;
  const circumference = 2 * Math.PI * r;
  const sellPercent = gross > 0 ? sell / gross : 0;
  const sellDash = circumference * sellPercent;
  const buyDash = circumference * (1 - sellPercent);

  if (gross === 0) {
    return React.createElement(
      "div",
      { className: "practice-hud-donut-zero" },
      React.createElement(
        "svg",
        { width: size, height: size, viewBox: `0 0 ${size} ${size}` },
        React.createElement("circle", {
          cx: size / 2,
          cy: size / 2,
          r,
          strokeWidth: 1,
          stroke: "var(--color-fg-4)",
          fill: "none"
        })
      ),
      React.createElement("span", null, "0-0")
    );
  }

  return React.createElement(
    "svg",
    {
      width: size,
      height: size,
      viewBox: `0 0 ${size} ${size}`,
      className: "practice-hud-donut"
    },
    React.createElement("circle", {
      className: "donut-segment-buy",
      cx: size / 2,
      cy: size / 2,
      r,
      strokeWidth,
      strokeDasharray: `${buyDash} ${circumference}`,
      strokeDashoffset: 0
    }),
    React.createElement("circle", {
      className: "donut-segment-sell",
      cx: size / 2,
      cy: size / 2,
      r,
      strokeWidth,
      strokeDasharray: `${sellDash} ${circumference}`,
      strokeDashoffset: -buyDash
    })
  );
};

export const DEFAULT_LIMITS = {
  daily: 2000
};

export const LIMIT_STEP = {
  daily: 1000
};

export const DAILY_ROW_RATIO = 12 / 16;

export const DEFAULT_LOT_SIZE = 100;
export const DEFAULT_RANGE_MONTHS = 6;
export const EXPORT_MA_PERIODS = [7, 20, 60, 100, 200];
export const EXPORT_ATR_PERIOD = 14;
export const EXPORT_VOLUME_PERIOD = 20;
export const EXPORT_SLOPE_LOOKBACK = 3;
export const DEFAULT_WEEKLY_RATIO = 3 / 4;
export const MIN_WEEKLY_RATIO = 0.2;
export const MIN_MONTHLY_RATIO = 0.1;
export const RANGE_PRESETS = [
  { label: "3M", months: 3 },
  { label: "6M", months: 6 },
  { label: "1Y", months: 12 },
  { label: "2Y", months: 24 }
];

export const normalizeDateParts = (year: number, month: number, day: number) => {
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return null;
  if (year < 1900 || month < 1 || month > 12 || day < 1 || day > 31) return null;
  return Math.floor(Date.UTC(year, month - 1, day) / 1000);
};

export const normalizeTime = (value: unknown) => {
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

export const buildDailyBars = (rows: number[][]) => {
  const entries: DailyBar[] = [];
  for (const row of rows) {
    if (!Array.isArray(row) || row.length < 5) continue;
    const time = normalizeTime(row[0]);
    if (time == null) continue;
    const open = Number(row[1]);
    const high = Number(row[2]);
    const low = Number(row[3]);
    const close = Number(row[4]);
    const volume = row.length > 5 ? Number(row[5]) : 0;
    if (![open, high, low, close].every((value) => Number.isFinite(value))) {
      continue;
    }
    entries.push({
      time,
      open,
      high,
      low,
      close,
      volume: Number.isFinite(volume) ? volume : 0
    });
  }
  entries.sort((a, b) => a.time - b.time);
  const deduped: DailyBar[] = [];
  let lastTime = -1;
  for (const item of entries) {
    if (item.time === lastTime) continue;
    deduped.push(item);
    lastTime = item.time;
  }
  return deduped;
};

export const buildCandles = (bars: DailyBar[]) =>
  bars.map((bar) => ({
    time: bar.time,
    open: bar.open,
    high: bar.high,
    low: bar.low,
    close: bar.close,
    isPartial: bar.isPartial
  }));

export const buildVolume = (bars: DailyBar[]): VolumePoint[] =>
  bars.map((bar) => ({ time: bar.time, value: bar.volume }));

export const getWeekStartTime = (time: number) => {
  const date = new Date(time * 1000);
  const day = date.getUTCDay();
  const diff = (day + 6) % 7;
  const weekStart = Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate() - diff);
  return Math.floor(weekStart / 1000);
};

export const getMonthStartTime = (time: number) => {
  const date = new Date(time * 1000);
  const monthStart = Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1);
  return Math.floor(monthStart / 1000);
};

export const isPartialWeek = (time: number | null) => {
  if (time == null) return false;
  const date = new Date(time * 1000);
  const day = date.getUTCDay();
  return day !== 5;
};

export const isPartialMonth = (time: number | null) => {
  if (time == null) return false;
  const date = new Date(time * 1000);
  const end = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 1, 0));
  return date.getUTCDate() !== end.getUTCDate();
};

export const buildAggregatedBars = (
  bars: DailyBar[],
  timeframe: "weekly" | "monthly",
  cursorTime: number | null
) => {
  const groups = new Map<number, DailyBar>();
  const isWeek = timeframe === "weekly";

  for (const bar of bars) {
    const key = isWeek ? getWeekStartTime(bar.time) : getMonthStartTime(bar.time);
    const existing = groups.get(key);
    if (!existing) {
      groups.set(key, {
        time: key,
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
        volume: bar.volume
      });
    } else {
      existing.high = Math.max(existing.high, bar.high);
      existing.low = Math.min(existing.low, bar.low);
      existing.close = bar.close;
      existing.volume += bar.volume;
    }
  }

  const sorted = [...groups.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([time, bar]) => ({
      ...bar,
      time
    }));

  if (sorted.length) {
    const lastIndex = sorted.length - 1;
    const last = sorted[lastIndex];
    const partial = isWeek ? isPartialWeek(cursorTime) : isPartialMonth(cursorTime);
    last.isPartial = partial;
  }

  const candles = sorted.map((bar) => ({
    time: bar.time,
    open: bar.open,
    high: bar.high,
    low: bar.low,
    close: bar.close,
    isPartial: bar.isPartial
  }));
  const volume = sorted.map((bar) => ({ time: bar.time, value: bar.volume }));
  return { candles, volume, bars: sorted };
};

export const computeMA = (candles: Candle[], period: number) => {
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

export const parseDateString = (value: string | null | undefined) => {
  if (!value) return null;
  const match = value.trim().match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return null;
  return Math.floor(Date.UTC(year, month - 1, day) / 1000);
};

export const formatDate = (time: number) => {
  const date = new Date(time * 1000);
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
};

export const formatDateSlash = (time: number) => {
  const date = new Date(time * 1000);
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return `${year}/${month}/${day}`;
};

export const formatNumber = (value: number | null | undefined, digits = 0) => {
  if (value == null || !Number.isFinite(value)) return "--";
  return value.toLocaleString("ja-JP", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits
  });
};

export const subtractMonths = (time: number, months: number) => {
  const date = new Date(time * 1000);
  const next = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
  next.setUTCMonth(next.getUTCMonth() - months);
  return Math.floor(next.getTime() / 1000);
};

export const addDays = (time: number, days: number) => time + days * 86400;

export const clampValue = (value: number, min: number, max: number) =>
  Math.min(max, Math.max(min, value));

export const getNextBusinessDay = (time: number) => {
  let next = addDays(time, 1);
  const day = new Date(next * 1000).getUTCDay();
  if (day === 6) {
    next = addDays(next, 2);
  } else if (day === 0) {
    next = addDays(next, 1);
  }
  return next;
};

export const resolveCursorIndex = (candles: Candle[], targetTime: number) => {
  if (!candles.length) return null;
  const idx = candles.findIndex((candle) => candle.time >= targetTime);
  if (idx >= 0) return idx;
  return candles.length - 1;
};

export const resolveIndexOnOrBefore = (candles: Candle[], targetTime: number) => {
  if (!candles.length) return null;
  let left = 0;
  let right = candles.length - 1;
  while (left <= right) {
    const mid = Math.floor((left + right) / 2);
    const midTime = candles[mid].time;
    if (midTime === targetTime) return mid;
    if (midTime < targetTime) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  return Math.max(0, Math.min(candles.length - 1, right));
};

export const resolveExactIndex = (candles: Candle[], targetTime: number) => {
  if (!candles.length) return null;
  let left = 0;
  let right = candles.length - 1;
  while (left <= right) {
    const mid = Math.floor((left + right) / 2);
    const midTime = candles[mid].time;
    if (midTime === targetTime) return mid;
    if (midTime < targetTime) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  return null;
};

export const buildPracticeLedger = (trades: PracticeTrade[], lotSize: number) => {
  let longLots = 0;
  let shortLots = 0;
  let longShares = 0;
  let shortShares = 0;
  let avgLongPrice = 0;
  let avgShortPrice = 0;
  let realizedPnL = 0;
  const entries: PracticeLedgerEntry[] = [];

  const resolveLotSize = (trade: PracticeTrade) => {
    const value = Number(trade.lotSize ?? lotSize);
    if (!Number.isFinite(value) || value <= 0) return lotSize;
    return value;
  };

  trades.forEach((trade) => {
    if (trade.kind === "DAY_CONFIRM") {
      entries.push({
        trade,
        kind: "DAY_CONFIRM",
        longLots,
        shortLots,
        avgLongPrice,
        avgShortPrice,
        realizedPnL,
        realizedDelta: 0,
        positionText: `${shortLots}-${longLots}`
      });
      return;
    }
    const qty = Math.max(0, Number(trade.quantity) || 0);
    const price = Number(trade.price) || 0;
    const tradeLotSize = resolveLotSize(trade);
    const shares = qty * tradeLotSize;
    let realizedDelta = 0;
    if (trade.book === "long") {
      if (trade.action === "open") {
        const nextShares = longShares + shares;
        avgLongPrice =
          nextShares > 0 ? (avgLongPrice * longShares + price * shares) / nextShares : 0;
        longShares = nextShares;
        longLots += qty;
      } else {
        const closingLots = Math.min(qty, longLots);
        const closingShares = Math.min(shares, longShares);
        realizedDelta = (price - avgLongPrice) * closingShares;
        realizedPnL += realizedDelta;
        longLots = Math.max(0, longLots - closingLots);
        longShares = Math.max(0, longShares - closingShares);
        if (longShares === 0) {
          avgLongPrice = 0;
        }
      }
    } else {
      if (trade.action === "open") {
        const nextShares = shortShares + shares;
        avgShortPrice =
          nextShares > 0 ? (avgShortPrice * shortShares + price * shares) / nextShares : 0;
        shortShares = nextShares;
        shortLots += qty;
      } else {
        const closingLots = Math.min(qty, shortLots);
        const closingShares = Math.min(shares, shortShares);
        realizedDelta = (avgShortPrice - price) * closingShares;
        realizedPnL += realizedDelta;
        shortLots = Math.max(0, shortLots - closingLots);
        shortShares = Math.max(0, shortShares - closingShares);
        if (shortShares === 0) {
          avgShortPrice = 0;
        }
      }
    }
    entries.push({
      trade,
      kind: "TRADE",
      longLots,
      shortLots,
      avgLongPrice,
      avgShortPrice,
      realizedPnL,
      realizedDelta,
      positionText: `${shortLots}-${longLots}`
    });
  });

  return {
    entries,
    summary: {
      longLots,
      shortLots,
      longShares,
      shortShares,
      avgLongPrice,
      avgShortPrice,
      realizedPnL
    }
  };
};

export const buildPracticePositions = (
  bars: DailyBar[],
  trades: PracticeTrade[],
  lotSize: number,
  code?: string,
  name?: string
) => {
  const tradesByTime = new Map<number, PracticeTrade[]>();
  trades.forEach((trade) => {
    const list = tradesByTime.get(trade.time) ?? [];
    list.push(trade);
    tradesByTime.set(trade.time, list);
  });

  const resolveLotSize = (trade: PracticeTrade) => {
    const value = Number(trade.lotSize ?? lotSize);
    if (!Number.isFinite(value) || value <= 0) return lotSize;
    return value;
  };

  let longLots = 0;
  let shortLots = 0;
  let longShares = 0;
  let shortShares = 0;
  let avgLongPrice = 0;
  let avgShortPrice = 0;
  let realizedPnL = 0;

  const dailyPositions: OverlayPosition[] = [];
  const tradeMarkers: OverlayTradeMarker[] = [];

  bars.forEach((bar) => {
    const dayTrades = tradesByTime.get(bar.time) ?? [];
    let buyLots = 0;
    let sellLots = 0;
    const markerTrades: OverlayTradeEvent[] = [];

    dayTrades.forEach((trade) => {
      const qty = Math.max(0, Number(trade.quantity) || 0);
      const price = Number(trade.price) || 0;
      const tradeLotSize = resolveLotSize(trade);
      const shares = qty * tradeLotSize;
      if (trade.side === "buy") {
        buyLots += qty;
      } else {
        sellLots += qty;
      }
      markerTrades.push({
        date: formatDate(trade.time),
        code: code ?? "",
        name: name ?? "",
        side: trade.side,
        action: trade.action,
        units: qty,
        price: trade.price,
        memo: trade.note
      });

      if (trade.book === "long") {
        if (trade.action === "open") {
          const nextShares = longShares + shares;
          avgLongPrice =
            nextShares > 0 ? (avgLongPrice * longShares + price * shares) / nextShares : 0;
          longShares = nextShares;
          longLots += qty;
        } else {
          const closingLots = Math.min(qty, longLots);
          const closingShares = Math.min(shares, longShares);
          realizedPnL += (price - avgLongPrice) * closingShares;
          longLots = Math.max(0, longLots - closingLots);
          longShares = Math.max(0, longShares - closingShares);
          if (longShares === 0) {
            avgLongPrice = 0;
          }
        }
      } else {
        if (trade.action === "open") {
          const nextShares = shortShares + shares;
          avgShortPrice =
            nextShares > 0 ? (avgShortPrice * shortShares + price * shares) / nextShares : 0;
          shortShares = nextShares;
          shortLots += qty;
        } else {
          const closingLots = Math.min(qty, shortLots);
          const closingShares = Math.min(shares, shortShares);
          realizedPnL += (avgShortPrice - price) * closingShares;
          shortLots = Math.max(0, shortLots - closingLots);
          shortShares = Math.max(0, shortShares - closingShares);
          if (shortShares === 0) {
            avgShortPrice = 0;
          }
        }
      }
    });

    const unrealizedLong = longShares > 0 ? (bar.close - avgLongPrice) * longShares : 0;
    const unrealizedShort = shortShares > 0 ? (avgShortPrice - bar.close) * shortShares : 0;
    const unrealizedPnL = unrealizedLong + unrealizedShort;
    const totalPnL = realizedPnL + unrealizedPnL;
    const posText = `${shortLots}-${longLots}`;

    dailyPositions.push({
      time: bar.time,
      date: formatDate(bar.time),
      shortLots,
      longLots,
      posText,
      avgLongPrice,
      avgShortPrice,
      realizedPnL,
      unrealizedPnL,
      totalPnL,
      close: bar.close
    });

    if (markerTrades.length) {
      tradeMarkers.push({
        time: bar.time,
        date: formatDate(bar.time),
        buyLots,
        sellLots,
        trades: markerTrades
      });
    }
  });

  return { dailyPositions, tradeMarkers };
};

export const parseBarsResponse = (payload: BarsResponse | number[][], label: string) => {
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

export const createSessionId = () => {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return crypto.randomUUID();
  }
  return `practice-${Date.now()}-${Math.random().toString(16).slice(2)}`;
};

export const findNearestCandle = (candles: Candle[], time: number) => {
  if (!candles.length) return null;
  let left = 0;
  let right = candles.length - 1;
  while (left <= right) {
    const mid = Math.floor((left + right) / 2);
    const midTime = candles[mid].time;
    if (midTime === time) return candles[mid];
    if (midTime < time) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  const lower = candles[Math.max(0, Math.min(candles.length - 1, right))];
  const upper = candles[Math.max(0, Math.min(candles.length - 1, left))];
  if (!lower) return upper;
  if (!upper) return lower;
  return Math.abs(time - lower.time) <= Math.abs(upper.time - time) ? lower : upper;
};

export const exportFile = (filename: string, contents: string, type: string) => {
  const blob = new Blob([contents], { type });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  anchor.click();
  URL.revokeObjectURL(url);
};
