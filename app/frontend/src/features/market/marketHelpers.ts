import type { Ticker } from "../../storeTypes";

export type MarketPeriodKey = "1d" | "1w" | "1m";
export type MarketMetricKey = "rate" | "flow" | "both";
export type MarketDisplayMode = "heatmap" | "bubble";
export type MarketDirectionTone = "positive" | "negative" | "neutral";

export const MARKET_NEUTRAL_TONE_THRESHOLD = 0.08;

export const MARKET_SECTOR_MATRIX_ROWS = 5;
export const MARKET_SECTOR_MATRIX_COLS = 7;

type MarketSectorGridPosition = {
  row: number;
  col: number;
};

const MARKET_SECTOR_POSITION_ENTRIES: Array<[string, MarketSectorGridPosition]> = [
  ["50", { row: 0, col: 0 }],
  ["1050", { row: 0, col: 1 }],
  ["2050", { row: 0, col: 2 }],
  ["3050", { row: 0, col: 3 }],
  ["3100", { row: 0, col: 4 }],
  ["3150", { row: 0, col: 5 }],
  ["3200", { row: 0, col: 6 }],
  ["3250", { row: 1, col: 0 }],
  ["3300", { row: 1, col: 1 }],
  ["3350", { row: 1, col: 2 }],
  ["3400", { row: 1, col: 3 }],
  ["3450", { row: 1, col: 4 }],
  ["3500", { row: 1, col: 5 }],
  ["3550", { row: 1, col: 6 }],
  ["3600", { row: 2, col: 0 }],
  ["3650", { row: 2, col: 1 }],
  ["3700", { row: 2, col: 2 }],
  ["3750", { row: 2, col: 3 }],
  ["3800", { row: 2, col: 4 }],
  ["4050", { row: 2, col: 5 }],
  ["5050", { row: 2, col: 6 }],
  ["5100", { row: 3, col: 0 }],
  ["5150", { row: 3, col: 1 }],
  ["5200", { row: 3, col: 2 }],
  ["5250", { row: 3, col: 3 }],
  ["6050", { row: 3, col: 4 }],
  ["6100", { row: 3, col: 5 }],
  ["7050", { row: 3, col: 6 }],
  ["7100", { row: 4, col: 0 }],
  ["7150", { row: 4, col: 1 }],
  ["7200", { row: 4, col: 2 }],
  ["8050", { row: 4, col: 3 }],
  ["9050", { row: 4, col: 4 }]
];

export const MARKET_SECTOR_POSITION_MAP = new Map(MARKET_SECTOR_POSITION_ENTRIES);

export type MarketTimelineItem = {
  name?: string;
  sector33_code?: string | null;
  industryName?: string | null;
  sector33Name?: string | null;
  value?: number | null;
  flow?: number | null;
  weight?: number | null;
  tickerCount?: number | null;
  count?: number | null;
  size?: number | null;
  color?: number | null;
};

export type MarketTimelineFrame = {
  asof: number;
  label: string;
  items: MarketTimelineItem[];
};

export type MarketWatchlistTicker = Pick<Ticker, "code" | "name" | "sector33Code" | "sector33Name">;

export type MarketSectorViewItem = MarketTimelineItem & {
  sector33_code: string;
  label: string;
  rate: number;
  flow: number;
  weight: number;
  tickerCount: number;
  related: boolean;
  watchlistCount: number;
  watchlistTickers: MarketWatchlistTicker[];
  representatives: MarketWatchlistTicker[];
};

const toFinite = (value: unknown) => {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : 0;
};

export const normalizeMarketFrameItems = (items: MarketTimelineItem[]): MarketTimelineItem[] =>
  items.map((item) => ({
    ...item,
    value: toFinite(item.value ?? item.color ?? 0),
    flow: toFinite(item.flow ?? 0),
    weight: toFinite(item.weight ?? item.size ?? 0),
    tickerCount: toFinite(item.tickerCount ?? item.count ?? 0)
  }));

export const buildSectorMemberIndex = (tickers: Ticker[]) => {
  const map = new Map<string, MarketWatchlistTicker[]>();
  tickers.forEach((ticker) => {
    const code = typeof ticker.sector33Code === "string" ? ticker.sector33Code.trim() : "";
    if (!code) return;
    const bucket = map.get(code) ?? [];
    bucket.push({
      code: ticker.code,
      name: ticker.name,
      sector33Code: ticker.sector33Code ?? null,
      sector33Name: ticker.sector33Name ?? null
    });
    map.set(code, bucket);
  });
  map.forEach((bucket) => {
    bucket.sort((a, b) => a.code.localeCompare(b.code, "ja"));
  });
  return map;
};

export const buildWatchlistSectorIndex = (keepList: string[], tickers: Ticker[]) => {
  const keepOrder = new Map(keepList.map((code, index) => [code, index]));
  const sectors = new Map<string, MarketWatchlistTicker[]>();
  const byCode = new Map(tickers.map((ticker) => [ticker.code, ticker]));
  keepList.forEach((code) => {
    const ticker = byCode.get(code);
    const sectorCode = typeof ticker?.sector33Code === "string" ? ticker.sector33Code.trim() : "";
    if (!ticker || !sectorCode) return;
    const bucket = sectors.get(sectorCode) ?? [];
    bucket.push({
      code: ticker.code,
      name: ticker.name,
      sector33Code: ticker.sector33Code ?? null,
      sector33Name: ticker.sector33Name ?? null
    });
    sectors.set(sectorCode, bucket);
  });
  sectors.forEach((bucket) => {
    bucket.sort((a, b) => {
      const orderA = keepOrder.get(a.code) ?? Number.MAX_SAFE_INTEGER;
      const orderB = keepOrder.get(b.code) ?? Number.MAX_SAFE_INTEGER;
      if (orderA !== orderB) return orderA - orderB;
      return a.code.localeCompare(b.code, "ja");
    });
  });
  return sectors;
};

export const isWatchedSector = (item: MarketSectorViewItem) => item.watchlistCount > 0;

export const buildMarketSectorMatrix = (items: MarketSectorViewItem[]) => {
  const matrix: (MarketSectorViewItem | null)[][] = Array.from({ length: MARKET_SECTOR_MATRIX_ROWS }, () =>
    Array.from({ length: MARKET_SECTOR_MATRIX_COLS }, () => null)
  );
  items.forEach((item) => {
    const position = MARKET_SECTOR_POSITION_MAP.get(item.sector33_code);
    if (!position) return;
    matrix[position.row][position.col] = item;
  });
  return matrix;
};

export const enrichMarketItems = (
  items: MarketTimelineItem[],
  sectorMemberIndex: Map<string, MarketWatchlistTicker[]>,
  watchlistIndex: Map<string, MarketWatchlistTicker[]>
): MarketSectorViewItem[] =>
  normalizeMarketFrameItems(items)
    .map((item) => {
      const sectorCode = typeof item.sector33_code === "string" ? item.sector33_code.trim() : "";
      if (!sectorCode) return null;
      const sectorMembers = sectorMemberIndex.get(sectorCode) ?? [];
      const watchlistTickers = watchlistIndex.get(sectorCode) ?? [];
      const representatives =
        watchlistTickers.length > 0
          ? watchlistTickers.slice(0, 2)
          : sectorMembers.slice(0, 2);
      const label =
        item.industryName?.trim() ||
        item.sector33Name?.trim() ||
        item.name?.trim() ||
        sectorCode;
      return {
        ...item,
        sector33_code: sectorCode,
        label,
        rate: toFinite(item.value ?? 0),
        flow: toFinite(item.flow ?? 0),
        weight: toFinite(item.weight ?? 0),
        tickerCount: toFinite(item.tickerCount ?? 0),
        related: watchlistTickers.length > 0,
        watchlistCount: watchlistTickers.length,
        watchlistTickers,
        representatives
      };
    })
    .filter((item): item is MarketSectorViewItem => Boolean(item));

export const formatMarketRate = (value: number) => {
  if (!Number.isFinite(value)) return "--";
  const rounded = Math.round(value * 10) / 10;
  if (rounded > 0) return `+${rounded.toFixed(1)}%`;
  if (rounded < 0) return `${rounded.toFixed(1)}%`;
  return "0.0%";
};

export const formatMarketValue = (value: number) => {
  if (!Number.isFinite(value)) return "--";
  if (value >= 100_000_000) {
    return `${(value / 100_000_000).toFixed(1)}億`;
  }
  if (value >= 1_000_000) {
    return `${(value / 1_000_000).toFixed(1)}百万`;
  }
  return `${Math.round(value).toLocaleString("ja-JP")}`;
};

export const formatMarketFlow = (value: number) => {
  if (!Number.isFinite(value)) return "--";
  const base = formatMarketValue(Math.abs(value));
  if (base === "--") return base;
  if (value > 0) return `+${base}`;
  if (value < 0) return `-${base}`;
  return base;
};

export const getMarketDirectionColor = (value: number, maxAbs: number) => {
  if (!Number.isFinite(value) || maxAbs <= 0) return "var(--theme-text-muted)";
  const normalized = Math.max(-1, Math.min(1, value / maxAbs));
  if (Math.abs(normalized) < MARKET_NEUTRAL_TONE_THRESHOLD) return "var(--theme-text-muted)";
  return normalized > 0 ? "var(--color-pnl-up)" : "var(--color-pnl-down)";
};

export const getMarketDirectionTone = (value: number, maxAbs: number): MarketDirectionTone => {
  if (!Number.isFinite(value) || maxAbs <= 0) return "neutral";
  const normalized = Math.max(-1, Math.min(1, value / maxAbs));
  if (Math.abs(normalized) < MARKET_NEUTRAL_TONE_THRESHOLD) return "neutral";
  return normalized > 0 ? "positive" : "negative";
};

export type MarketTileColors = {
  bodyColor: string;
  bandColor: string | null;
};

export const getMarketTileColors = (
  item: MarketSectorViewItem,
  metric: MarketMetricKey,
  metricDomain: { rateAbs: number; flowAbs: number }
): MarketTileColors => {
  const bodyColor =
    metric === "flow"
      ? getMarketDirectionColor(item.flow, metricDomain.flowAbs)
      : getMarketDirectionColor(item.rate, metricDomain.rateAbs);

  return {
    bodyColor,
    bandColor: metric === "both" ? getMarketDirectionColor(item.flow, metricDomain.flowAbs) : null
  };
};
