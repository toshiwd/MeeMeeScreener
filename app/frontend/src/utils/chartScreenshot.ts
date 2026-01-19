import type { BarsPayload, Box, MaSetting } from "../store";
import { drawChart } from "../components/ThumbnailCanvas";

type ScreenshotItem = {
  code: string;
  payload?: BarsPayload | null;
  boxes?: Box[] | null;
  maSettings: MaSetting[];
};

type ScreenshotOptions = {
  rangeMonths?: number | null;
  width?: number;
  height?: number;
  maxBars?: number | null;
  showAxes?: boolean;
  showBoxes?: boolean;
  timeframeLabel?: string;
  stamp?: string;
};

type ScreenshotResult = {
  created: number;
  skipped: number;
};

const DEFAULT_WIDTH = 1280;
const DEFAULT_HEIGHT = 720;

const normalizeDateParts = (year: number, month: number, day: number) => {
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return null;
  if (year < 1900 || month < 1 || month > 12 || day < 1 || day > 31) return null;
  return Math.floor(Date.UTC(year, month - 1, day) / 1000);
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

const getRangeStartTime = (bars: number[][], rangeMonths?: number | null) => {
  if (!rangeMonths || rangeMonths <= 0) return null;
  if (!bars.length) return null;
  const lastTime = normalizeTime(bars[bars.length - 1]?.[0]);
  if (!Number.isFinite(lastTime)) return null;
  const anchor = new Date((lastTime as number) * 1000);
  if (Number.isNaN(anchor.getTime())) return null;
  const start = new Date(
    Date.UTC(anchor.getUTCFullYear(), anchor.getUTCMonth(), anchor.getUTCDate())
  );
  start.setUTCMonth(start.getUTCMonth() - rangeMonths);
  return Math.floor(start.getTime() / 1000);
};

const countBarsInRange = (bars: number[][], rangeMonths?: number | null) => {
  if (!rangeMonths || rangeMonths <= 0) return bars.length;
  const rangeStart = getRangeStartTime(bars, rangeMonths);
  if (rangeStart == null) return bars.length;
  return bars.reduce((count, bar) => {
    const time = normalizeTime(bar?.[0]);
    if (time != null && time >= rangeStart) return count + 1;
    return count;
  }, 0);
};

const resolveMaxBars = (
  bars: number[][],
  rangeMonths?: number | null,
  maxBarsLimit?: number | null
) => {
  const total = bars.length;
  if (!total) return 0;
  let count = countBarsInRange(bars, rangeMonths);
  if (!count) count = total;
  if (maxBarsLimit && maxBarsLimit > 0) {
    count = Math.min(count, maxBarsLimit);
  }
  return Math.max(1, Math.min(count, total));
};

const buildStamp = () => {
  const now = new Date();
  const yyyy = String(now.getFullYear());
  const mm = String(now.getMonth() + 1).padStart(2, "0");
  const dd = String(now.getDate()).padStart(2, "0");
  const hh = String(now.getHours()).padStart(2, "0");
  const min = String(now.getMinutes()).padStart(2, "0");
  return `${yyyy}${mm}${dd}-${hh}${min}`;
};

const sanitizeFilenamePart = (value: string) => {
  const trimmed = value.trim().replace(/[^a-zA-Z0-9_-]+/g, "_");
  const normalized = trimmed.replace(/^_+/, "").replace(/_+$/, "");
  return normalized || "chart";
};

const buildFileName = (code: string, timeframeLabel: string | undefined, stamp: string) => {
  const safeCode = sanitizeFilenamePart(code || "chart");
  const safeFrame = sanitizeFilenamePart(timeframeLabel || "chart");
  return `chart_${safeCode}_${safeFrame}_${stamp}.png`;
};

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

const triggerDownload = (dataUrl: string, filename: string) => {
  const link = document.createElement("a");
  link.href = dataUrl;
  link.download = filename;
  link.rel = "noopener";
  link.style.display = "none";
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
};

export const downloadChartScreenshots = async (
  items: ScreenshotItem[],
  options: ScreenshotOptions = {}
): Promise<ScreenshotResult & { savedDir?: string }> => {
  const width = options.width ?? DEFAULT_WIDTH;
  const height = options.height ?? DEFAULT_HEIGHT;
  const showAxes = options.showAxes ?? true;
  const showBoxes = options.showBoxes ?? false;
  const stamp = options.stamp ?? buildStamp();
  let created = 0;
  let skipped = 0;
  let savedDir: string | undefined;

  for (const item of items) {
    const payload = item.payload ?? null;
    if (!payload?.bars?.length) {
      skipped += 1;
      continue;
    }
    const maxBars = resolveMaxBars(payload.bars, options.rangeMonths, options.maxBars ?? null);
    if (!maxBars) {
      skipped += 1;
      continue;
    }
    const canvas = createCanvas(width, height);
    drawChart(
      canvas,
      payload,
      item.boxes ?? [],
      showBoxes,
      item.maSettings,
      width,
      height,
      maxBars,
      showAxes,
      "light"
    );
    let dataUrl = "";
    try {
      dataUrl = canvas.toDataURL("image/png");
    } catch {
      dataUrl = "";
    }
    if (!dataUrl) {
      skipped += 1;
      continue;
    }
    const fileName = buildFileName(item.code, options.timeframeLabel, stamp);

    if (window.pywebview?.api?.save_screenshot) {
      // Backend save
      const base64Data = dataUrl.split(",")[1];
      try {
        const result = await window.pywebview.api.save_screenshot(base64Data, fileName);
        if (result.success) {
          created += 1;
          if (result.savedDir) {
            savedDir = result.savedDir;
          }
        } else {
          // Backend failed, fallback? 
          // Currently just counting as skipped or let's try fallback.
          triggerDownload(dataUrl, fileName);
          created += 1;
        }
      } catch {
        triggerDownload(dataUrl, fileName);
        created += 1;
      }
    } else {
      // Browser fallback
      triggerDownload(dataUrl, fileName);
      created += 1;
    }
    // Small delay to prevent browser throttling downloads if not using backend
    if (!window.pywebview?.api?.save_screenshot) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }

  return { created, skipped, savedDir };
};
