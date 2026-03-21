import type { MarketMetricKey, MarketPeriodKey, MarketTimelineFrame } from "../features/market/marketHelpers";

export type StoredMarketViewState = {
  stateVersion: number;
  period: MarketPeriodKey;
  metric: MarketMetricKey;
  cursorIndex?: number;
  cursorDate?: string | null;
  userInteracted?: boolean;
  selectedSector: string | null;
};

export type MarketCursorResolution = {
  index: number;
  cursorDate: string | null;
  source: "empty" | "cursorDate" | "cursorIndex" | "latest";
};

export const MARKET_VIEW_STATE_KEY = "marketViewState";
export const MARKET_VIEW_STATE_VERSION = 3;

const clampIndex = (value: unknown, maxIndex: number) => {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return Math.max(0, maxIndex);
  return Math.min(Math.max(Math.floor(numeric), 0), Math.max(0, maxIndex));
};

const normalizeDateKey = (value: unknown): string | null => {
  if (typeof value !== "string") return null;
  const text = value.trim();
  return text || null;
};

const toUtcDateKey = (asof: unknown): string | null => {
  const numeric = Number(asof);
  if (!Number.isFinite(numeric) || numeric <= 0) return null;
  const millis = numeric >= 1_000_000_000_000 ? numeric : numeric * 1000;
  try {
    return new Date(millis).toISOString().slice(0, 10);
  } catch {
    return null;
  }
};

export const getMarketTimelineFrameDateKey = (
  frame: Pick<MarketTimelineFrame, "asof" | "label"> & { date?: string | null }
): string | null => {
  const explicitDate = normalizeDateKey(frame.date);
  if (explicitDate) return explicitDate;
  const derivedDate = toUtcDateKey(frame.asof);
  if (derivedDate) return derivedDate;
  return normalizeDateKey(frame.label);
};

export const resolveInitialMarketCursor = (
  frames: MarketTimelineFrame[],
  stored: Partial<StoredMarketViewState> | null | undefined
): MarketCursorResolution => {
  if (!frames.length) {
    return { index: 0, cursorDate: null, source: "empty" };
  }

  const maxIndex = frames.length - 1;
  const storedDate = normalizeDateKey(stored?.cursorDate);
  if (storedDate) {
    const matchedIndex = frames.findIndex((frame) => getMarketTimelineFrameDateKey(frame) === storedDate);
    if (matchedIndex >= 0) {
      return {
        index: matchedIndex,
        cursorDate: storedDate,
        source: "cursorDate",
      };
    }
  }

  if (stored?.userInteracted === true) {
    const index = clampIndex(stored?.cursorIndex, maxIndex);
    return {
      index,
      cursorDate: getMarketTimelineFrameDateKey(frames[index]) ?? null,
      source: "cursorIndex",
    };
  }

  return {
    index: maxIndex,
    cursorDate: getMarketTimelineFrameDateKey(frames[maxIndex]) ?? null,
    source: "latest",
  };
};

export const buildPersistedMarketViewState = ({
  period,
  metric,
  selectedSector,
  cursorIndex,
  cursorDate,
  cursorUserInteracted,
  previous,
}: {
  period: MarketPeriodKey;
  metric: MarketMetricKey;
  selectedSector: string | null;
  cursorIndex: number;
  cursorDate: string | null;
  cursorUserInteracted: boolean;
  previous: Partial<StoredMarketViewState> | null | undefined;
}): StoredMarketViewState => {
  const payload: StoredMarketViewState = {
    stateVersion: MARKET_VIEW_STATE_VERSION,
    period,
    metric,
    selectedSector,
  };

  if (cursorUserInteracted) {
    payload.cursorIndex = clampIndex(cursorIndex, Number.MAX_SAFE_INTEGER);
    if (cursorDate) {
      payload.cursorDate = cursorDate;
    }
    payload.userInteracted = true;
    return payload;
  }

  if (previous?.userInteracted === true) {
    if (typeof previous.cursorIndex === "number" && Number.isFinite(previous.cursorIndex)) {
      payload.cursorIndex = Math.floor(previous.cursorIndex);
    }
    const previousDate = normalizeDateKey(previous.cursorDate);
    if (previousDate) {
      payload.cursorDate = previousDate;
    }
    payload.userInteracted = true;
  }
  return payload;
};
