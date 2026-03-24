import type { Box, MaSetting, BarsPayload } from "../../storeTypes";
import { drawChart } from "../../components/ThumbnailCanvas";

type AiExplainCandle = {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number | null;
};

type AiExplainChartSource = {
  code: string;
  payload?: BarsPayload | null;
  boxes?: Box[] | null;
  maSettings: MaSetting[];
  rangeBars?: number | null;
  width?: number;
  height?: number;
  maxBars?: number | null;
  showAxes?: boolean;
  showBoxes?: boolean;
  timeframeLabel?: string;
};

const DEFAULT_WIDTH = 1000;
const DEFAULT_HEIGHT = 560;

const createCanvas = (width: number, height: number) => {
  const canvas = document.createElement("canvas");
  const ratio = window.devicePixelRatio || 1;
  canvas.width = Math.floor(width * ratio);
  canvas.height = Math.floor(height * ratio);
  const ctx = canvas.getContext("2d");
  if (ctx) {
    ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
  }
  return canvas;
};

const countBarsInRange = (bars: number[][], rangeBars?: number | null) => {
  if (!rangeBars || rangeBars <= 0) return bars.length;
  return Math.min(rangeBars, bars.length);
};

const resolveMaxBars = (bars: number[][], rangeBars?: number | null, maxBarsLimit?: number | null) => {
  const total = bars.length;
  if (!total) return 0;
  let count = countBarsInRange(bars, rangeBars);
  if (!count) count = total;
  if (maxBarsLimit && maxBarsLimit > 0) {
    count = Math.min(count, maxBarsLimit);
  }
  return Math.max(1, Math.min(count, total));
};

const sanitizeFilenamePart = (value: string) => {
  const trimmed = value.trim().replace(/[^a-zA-Z0-9_-]+/g, "_");
  const normalized = trimmed.replace(/^_+/, "").replace(/_+$/, "");
  return normalized || "chart";
};

export const buildAiExplainBarsPayload = (candles: AiExplainCandle[]): BarsPayload | null => {
  if (!candles.length) return null;
  return {
    bars: candles.map((candle) => [
      candle.time,
      candle.open,
      candle.high,
      candle.low,
      candle.close,
      Number.isFinite(candle.volume ?? NaN) ? Number(candle.volume) : 0
    ]),
    ma: {
      ma7: [],
      ma20: [],
      ma60: []
    }
  };
};

export const buildAiExplainChartImage = (source: AiExplainChartSource): string | null => {
  const payload = source.payload ?? null;
  if (!payload?.bars?.length) return null;
  const width = source.width ?? DEFAULT_WIDTH;
  const height = source.height ?? DEFAULT_HEIGHT;
  const maxBars = resolveMaxBars(payload.bars, source.rangeBars, source.maxBars ?? null);
  if (!maxBars) return null;
  const canvas = createCanvas(width, height);
  drawChart(
    canvas,
    payload,
    source.boxes ?? [],
    source.showBoxes ?? Boolean(source.boxes?.length),
    source.maSettings,
    width,
    height,
    maxBars,
    source.showAxes ?? true,
    "light"
  );
  try {
    return canvas.toDataURL("image/png");
  } catch {
    return null;
  }
};

export const buildAiExplainImages = (sources: AiExplainChartSource[], limit = 3): string[] =>
  sources.slice(0, limit).map(buildAiExplainChartImage).filter((value): value is string => Boolean(value));

export const buildAiExplainChartLabel = (code: string, timeframeLabel?: string) => {
  const safeCode = sanitizeFilenamePart(code || "chart");
  const safeFrame = sanitizeFilenamePart(timeframeLabel || "chart");
  return `${safeCode}_${safeFrame}`;
};
