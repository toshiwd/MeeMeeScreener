import type { BarsPayload, Box, MaSetting } from "../store";
import { drawChart } from "../components/ThumbnailCanvas";

type ScreenshotItem = {
  code: string;
  payload?: BarsPayload | null;
  boxes?: Box[] | null;
  maSettings: MaSetting[];
};

type ScreenshotOptions = {
  rangeBars?: number | null;
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

const countBarsInRange = (bars: number[][], rangeBars?: number | null) => {
  if (!rangeBars || rangeBars <= 0) return bars.length;
  return Math.min(rangeBars, bars.length);
};

const resolveMaxBars = (
  bars: number[][],
  rangeBars?: number | null,
  maxBarsLimit?: number | null
) => {
  const total = bars.length;
  if (!total) return 0;
  let count = countBarsInRange(bars, rangeBars);
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
    const maxBars = resolveMaxBars(payload.bars, options.rangeBars, options.maxBars ?? null);
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
