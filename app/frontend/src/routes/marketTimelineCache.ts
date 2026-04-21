import type { MarketPeriodKey, MarketTimelineFrame } from "../features/market/marketHelpers";

export type MarketTimelineCacheSource = "timeline" | "snapshot_fallback";

type MarketTimelineCacheEntry = {
  cacheVersion: number;
  period: MarketPeriodKey;
  source: MarketTimelineCacheSource;
  limit: number;
  frames: MarketTimelineFrame[];
};

export type MarketTimelineCachePayload = {
  source: MarketTimelineCacheSource;
  frames: MarketTimelineFrame[];
};

export const MARKET_TIMELINE_CACHE_VERSION = 2;
export const MARKET_TIMELINE_CACHE_PREFIX = "marketTimelineCache";

const CACHE_READ_ORDER: readonly MarketTimelineCacheSource[] = ["timeline", "snapshot_fallback"] as const;

export const buildMarketTimelineCacheKey = (
  period: MarketPeriodKey,
  source: MarketTimelineCacheSource,
  limit: number
) => `${MARKET_TIMELINE_CACHE_PREFIX}:${source}:${period}:${limit}`;

const normalizeCacheEntry = (
  value: unknown,
  period: MarketPeriodKey,
  source: MarketTimelineCacheSource,
  limit: number
): MarketTimelineFrame[] | null => {
  if (!value || typeof value !== "object") return null;
  const parsed = value as Partial<MarketTimelineCacheEntry>;
  if (
    parsed.cacheVersion !== MARKET_TIMELINE_CACHE_VERSION ||
    parsed.period !== period ||
    parsed.source !== source ||
    parsed.limit !== limit ||
    !Array.isArray(parsed.frames)
  ) {
    return null;
  }
  return parsed.frames as MarketTimelineFrame[];
};

export const readMarketTimelineCache = (
  storage: Pick<Storage, "getItem">,
  period: MarketPeriodKey,
  limit: number
): MarketTimelineCachePayload | null => {
  for (const source of CACHE_READ_ORDER) {
    try {
      const raw = storage.getItem(buildMarketTimelineCacheKey(period, source, limit));
      if (!raw) continue;
      const frames = normalizeCacheEntry(JSON.parse(raw), period, source, limit);
      if (frames) {
        return { source, frames };
      }
    } catch {
      // Ignore malformed cache entries.
    }
  }
  return null;
};

export const writeMarketTimelineCache = (
  storage: Pick<Storage, "setItem">,
  period: MarketPeriodKey,
  source: MarketTimelineCacheSource,
  frames: MarketTimelineFrame[],
  limit: number
) => {
  const payload: MarketTimelineCacheEntry = {
    cacheVersion: MARKET_TIMELINE_CACHE_VERSION,
    period,
    source,
    limit,
    frames,
  };
  storage.setItem(buildMarketTimelineCacheKey(period, source, limit), JSON.stringify(payload));
};

export const clearMarketTimelineCache = (
  storage: Pick<Storage, "removeItem">,
  period: MarketPeriodKey,
  source: MarketTimelineCacheSource,
  limit: number
) => {
  storage.removeItem(buildMarketTimelineCacheKey(period, source, limit));
};

export const describeMarketTimelineSource = (source: MarketTimelineCacheSource | null) => {
  if (source !== "snapshot_fallback") return null;
  return "Snapshot fallback shown. Timeline history is currently unavailable.";
};
