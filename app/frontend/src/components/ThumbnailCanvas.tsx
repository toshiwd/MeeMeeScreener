import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { MouseEvent as ReactMouseEvent } from "react";
import { BarsPayload, Box, MaSetting } from "../store";
import { getDomTheme } from "../utils/theme";
import { getBodyRangeFromBars, getBoxFill, getBoxStroke } from "../utils/boxes";
import { getThumbnailCache, setThumbnailCache } from "./thumbnailCache";

const COLORS = {
  up: "#ef4444",
  down: "#22c55e"
};

const MIN_HEIGHT = 80;
const DEFAULT_MAX_BARS = 60;
const BOX_FILL = getBoxFill();
const BOX_STROKE = getBoxStroke();

const formatDate = (value: number) => {
  if (!Number.isFinite(value)) return "--";
  const raw = Number(value);
  const formatParts = (year: number, month: number, day: number) => {
    const yy = String(year % 100).padStart(2, "0");
    const mm = String(month).padStart(2, "0");
    const dd = String(day).padStart(2, "0");
    return `${yy}/${mm}/${dd}`;
  };
  if (raw >= 10000000 && raw < 100000000) {
    const year = Math.floor(raw / 10000);
    const month = Math.floor((raw % 10000) / 100);
    const day = raw % 100;
    return formatParts(year, month, day);
  }
  if (raw > 1000000000000) {
    const date = new Date(raw);
    if (Number.isNaN(date.getTime())) return "--";
    return formatParts(date.getUTCFullYear(), date.getUTCMonth() + 1, date.getUTCDate());
  }
  if (raw > 1000000000) {
    const date = new Date(raw * 1000);
    if (Number.isNaN(date.getTime())) return "--";
    return formatParts(date.getUTCFullYear(), date.getUTCMonth() + 1, date.getUTCDate());
  }
  return "--";
};

const formatDateShort = (value: number) => formatDate(value);

const formatPrice = (value: number) => {
  if (!Number.isFinite(value)) return "--";
  return Math.round(value).toLocaleString("ja-JP");
};

function buildMaMap(bars: number[][], period: number) {
  const map = new Map<number, number>();
  if (period <= 1) {
    bars.forEach((bar) => {
      map.set(bar[0], Number(bar[4]));
    });
    return map;
  }
  let sum = 0;
  for (let i = 0; i < bars.length; i += 1) {
    const close = Number(bars[i][4]);
    sum += close;
    if (i >= period) {
      sum -= Number(bars[i - period][4]);
    }
    if (i >= period - 1) {
      map.set(bars[i][0], sum / period);
    }
  }
  return map;
}

export function drawChart(
  canvas: HTMLCanvasElement,
  payload: BarsPayload,
  boxes: Box[],
  showBoxes: boolean,
  maSettings: MaSetting[],
  width: number,
  height: number,
  maxBars: number,
  showAxes: boolean,
  theme: "dark" | "light"
) {
  const isLight = theme === "light";
  const themeColors = {
    grid: isLight ? "rgba(0, 0, 0, 0.08)" : "rgba(148, 163, 184, 0.15)",
    text: isLight ? "rgba(51, 65, 85, 0.85)" : "rgba(148, 163, 184, 0.85)",
    bg: isLight ? "rgba(255, 255, 255, 0.9)" : "rgba(15, 23, 42, 0.9)",
    textHighlight: isLight ? "#334155" : "#e2e8f0"
  };

  const ctx = canvas.getContext("2d");
  if (!ctx) return;

  const allBars = payload.bars;
  const bars = allBars.slice(-maxBars);
  if (!bars.length) {
    ctx.clearRect(0, 0, width, height);
    return;
  }

  const pad = 6;
  const rightPad = showAxes ? 46 : pad;
  const bottomPad = showAxes ? 18 : pad;
  const plotWidth = Math.max(1, width - rightPad);
  const plotHeight = Math.max(1, height - bottomPad);
  const hi = Math.max(...bars.map((b) => b[2]));
  const lo = Math.min(...bars.map((b) => b[3]));
  let min = lo;
  let max = hi;

  const activeMaSettings = maSettings.filter((setting) => setting.visible);
  const maMaps = activeMaSettings.map((setting) => ({
    setting,
    map: buildMaMap(allBars, setting.period)
  }));

  bars.forEach((bar) => {
    const time = bar[0];
    maMaps.forEach(({ map }) => {
      const value = map.get(time);
      if (value == null || Number.isNaN(value)) return;
      min = Math.min(min, value);
      max = Math.max(max, value);
    });
  });

  const range = Math.max(1e-6, max - min);

  const toY = (value: number) => {
    const top = pad;
    const bottom = plotHeight - pad;
    return top + (bottom - top) * (1 - (value - min) / range);
  };

  ctx.clearRect(0, 0, width, height);
  ctx.lineWidth = 1;

  const step = plotWidth / bars.length;
  const candleWidth = Math.max(1, Math.min(6, step * 0.6));

  let timeTickIndexes: number[] = [];
  let priceTicks: number[] = [];
  if (showAxes) {
    const timeTickCount = Math.max(3, Math.min(6, Math.round(plotWidth / 140)));
    timeTickIndexes = Array.from({ length: timeTickCount }, (_, i) =>
      Math.round((bars.length - 1) * (i / Math.max(1, timeTickCount - 1)))
    );
    const priceTickCount = Math.max(3, Math.min(6, Math.round(plotHeight / 80)));
    priceTicks = Array.from({ length: priceTickCount }, (_, i) => {
      const ratio = priceTickCount === 1 ? 0 : i / (priceTickCount - 1);
      return max - range * ratio;
    });

    ctx.save();
    ctx.strokeStyle = themeColors.grid;
    ctx.lineWidth = 1;
    priceTicks.forEach((value) => {
      const y = toY(value);
      ctx.beginPath();
      ctx.moveTo(0, y);
      ctx.lineTo(plotWidth, y);
      ctx.stroke();
    });
    timeTickIndexes.forEach((index) => {
      const x = step * index + step / 2;
      ctx.beginPath();
      ctx.moveTo(x, 0);
      ctx.lineTo(x, plotHeight);
      ctx.stroke();
    });
    ctx.restore();
  }

  bars.forEach((bar, index) => {
    const [t, o, h, l, c] = bar;
    const x = step * index + step / 2;
    const color = c >= o ? COLORS.up : COLORS.down;
    ctx.strokeStyle = color;
    ctx.fillStyle = color;

    const yHigh = toY(h);
    const yLow = toY(l);
    const yOpen = toY(o);
    const yClose = toY(c);

    ctx.beginPath();
    ctx.moveTo(x, yHigh);
    ctx.lineTo(x, yLow);
    ctx.stroke();

    const rectY = Math.min(yOpen, yClose);
    const rectH = Math.max(1, Math.abs(yClose - yOpen));
    ctx.fillRect(x - candleWidth / 2, rectY, candleWidth, rectH);
  });

  if (showBoxes && boxes.length) {
    const times = bars.map((bar) => bar[0]);
    const findStart = (time: number) => {
      let left = 0;
      let right = times.length - 1;
      let result = -1;
      while (left <= right) {
        const mid = Math.floor((left + right) / 2);
        if (times[mid] >= time) {
          result = mid;
          right = mid - 1;
        } else {
          left = mid + 1;
        }
      }
      return result;
    };
    const findEnd = (time: number) => {
      let left = 0;
      let right = times.length - 1;
      let result = -1;
      while (left <= right) {
        const mid = Math.floor((left + right) / 2);
        if (times[mid] <= time) {
          result = mid;
          left = mid + 1;
        } else {
          right = mid - 1;
        }
      }
      return result;
    };

    ctx.save();
    ctx.fillStyle = BOX_FILL;
    ctx.strokeStyle = BOX_STROKE;
    ctx.lineWidth = 1;
    boxes.forEach((box) => {
      const startIdx = findStart(box.startTime);
      const endIdx = findEnd(box.endTime);
      if (startIdx < 0 || endIdx < 0 || endIdx < startIdx) return;
      const bodyRange = getBodyRangeFromBars(bars, box.startTime, box.endTime);
      const upper = bodyRange?.upper ?? box.upper;
      const lower = bodyRange?.lower ?? box.lower;
      const x1 = step * startIdx;
      const x2 = step * (endIdx + 1);
      const y1 = toY(upper);
      const y2 = toY(lower);
      const rectX = x1;
      const rectY = Math.min(y1, y2);
      const rectW = Math.max(1, x2 - x1);
      const rectH = Math.max(1, Math.abs(y2 - y1));
      ctx.fillRect(rectX, rectY, rectW, rectH);
      ctx.strokeRect(rectX, rectY, rectW, rectH);
    });
    ctx.restore();
  }

  const drawMa = (map: Map<number, number>, setting: MaSetting) => {
    ctx.strokeStyle = setting.color;
    ctx.lineWidth = Math.max(1, setting.lineWidth);
    ctx.beginPath();
    let started = false;
    bars.forEach((bar, index) => {
      const t = bar[0];
      const value = map.get(t);
      if (value === undefined) {
        return;
      }
      const x = step * index + step / 2;
      const y = toY(value);
      if (!started) {
        ctx.moveTo(x, y);
        started = true;
      } else {
        ctx.lineTo(x, y);
      }
    });
    if (started) ctx.stroke();
  };

  maMaps.forEach(({ map, setting }) => {
    drawMa(map, setting);
  });

  if (showAxes) {
    ctx.save();
    ctx.font = "11px 'Noto Sans JP', 'IBM Plex Sans', sans-serif";
    ctx.fillStyle = themeColors.text;
    ctx.textAlign = "right";
    ctx.textBaseline = "middle";
    priceTicks.forEach((value) => {
      const y = toY(value);
      ctx.fillText(formatPrice(value), width - 4, y);
    });

    const labelY = height - 4;
    ctx.textAlign = "center";
    ctx.textBaseline = "bottom";
    timeTickIndexes.forEach((index, idx) => {
      const time = bars[index]?.[0];
      if (!time) return;
      const x = step * index + step / 2;
      if (idx === 0) ctx.textAlign = "left";
      else if (idx === timeTickIndexes.length - 1) ctx.textAlign = "right";
      else ctx.textAlign = "center";
      ctx.fillText(formatDateShort(time), x, labelY);
    });

    const lastClose = bars[bars.length - 1]?.[4];
    if (Number.isFinite(lastClose)) {
      const y = toY(lastClose);
      ctx.fillStyle = themeColors.bg;
      ctx.fillRect(plotWidth, y - 8, rightPad - 2, 16);
      ctx.fillStyle = themeColors.textHighlight;
      ctx.textAlign = "right";
      ctx.textBaseline = "middle";
      ctx.fillText(formatPrice(lastClose), width - 4, y);
    }
    ctx.restore();
  }
}

export default function ThumbnailCanvas({
  payload,
  boxes,
  showBoxes,
  maSettings,
  cacheKey,
  maxBars,
  showAxes = false,
  theme
}: {
  payload: BarsPayload;
  boxes: Box[];
  showBoxes: boolean;
  maSettings: MaSetting[];
  cacheKey?: string;
  maxBars?: number;
  showAxes?: boolean;
  theme?: "dark" | "light";
}) {
  const resolvedTheme = theme ?? getDomTheme();
  const containerRef = useRef<HTMLDivElement | null>(null);
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const lastKeyRef = useRef<string>("");
  const rafRef = useRef<number | null>(null);
  const lastSnapshotRef = useRef<string>("");
  const [hoverIndex, setHoverIndex] = useState<number | null>(null);
  const hoverIndexRef = useRef<number | null>(null);
  const hoverRafRef = useRef<number | null>(null);
  const hoverPendingRef = useRef<number | null>(null);
  const [hasDrawn, setHasDrawn] = useState(false);
  const [cachedSnapshot, setCachedSnapshot] = useState<string | null>(
    () => (cacheKey ? getThumbnailCache(cacheKey) ?? null : null)
  );
  const maxBarsValue = Math.max(1, Math.floor(maxBars ?? DEFAULT_MAX_BARS));
  const displayBars = useMemo(() => {
    const allBars = payload.bars ?? [];
    if (allBars.length <= maxBarsValue) return allBars;
    return allBars.slice(-maxBarsValue);
  }, [payload, maxBarsValue]);

  const renderKey = useMemo(() => {
    const bars = displayBars;
    const last = bars[bars.length - 1];
    const firstBox = boxes[0];
    const settingsKey = maSettings
      .map((setting) => `${setting.period}-${setting.visible}-${setting.color}-${setting.lineWidth}`)
      .join("|");
    return `${bars.length}-${bars[0]?.[0]}-${last?.[0]}-${last?.[4]}-${boxes.length}-${firstBox?.startTime ?? "none"}-${showBoxes}-${settingsKey}-${maxBarsValue}-${showAxes}-${resolvedTheme}`;
  }, [displayBars, boxes, showBoxes, maSettings, maxBarsValue, showAxes, resolvedTheme]);

  const draw = useCallback(() => {
    if (!containerRef.current || !canvasRef.current) return false;
    const width = Math.floor(containerRef.current.clientWidth || 0);
    const height = Math.max(MIN_HEIGHT, Math.floor(containerRef.current.clientHeight || 0));
    if (width <= 0 || height <= 0) return false;
    const canvas = canvasRef.current;
    const ratio = window.devicePixelRatio || 1;
    canvas.width = Math.floor(width * ratio);
    canvas.height = Math.floor(height * ratio);
    canvas.style.width = `${width}px`;
    canvas.style.height = `${height}px`;
    const ctx = canvas.getContext("2d");
    if (ctx) {
      ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
    }
    drawChart(
      canvas,
      payload,
      boxes,
      showBoxes,
      maSettings,
      width,
      height,
      maxBarsValue,
      showAxes,
      resolvedTheme
    );
    setHasDrawn(true);
    if (cacheKey) {
      const snapshotKey = `${cacheKey}:${renderKey}:${width}x${height}`;
      if (lastSnapshotRef.current !== snapshotKey) {
        lastSnapshotRef.current = snapshotKey;
        try {
          const dataUrl = canvas.toDataURL("image/png");
          setThumbnailCache(cacheKey, dataUrl);
          setCachedSnapshot(dataUrl);
        } catch {
          // ignore snapshot failures
        }
      }
    }
    return true;
  }, [payload, boxes, showBoxes, maSettings, renderKey, cacheKey, maxBarsValue, showAxes, resolvedTheme]);

  const scheduleDraw = useCallback(() => {
    if (rafRef.current !== null) {
      window.cancelAnimationFrame(rafRef.current);
    }
    rafRef.current = window.requestAnimationFrame(() => {
      rafRef.current = null;
      const ok = draw();
      if (!ok) {
        rafRef.current = window.requestAnimationFrame(() => {
          rafRef.current = null;
          draw();
        });
      }
    });
  }, [draw]);

  useEffect(() => {
    if (!containerRef.current) return;
    const observer = new ResizeObserver(() => scheduleDraw());
    observer.observe(containerRef.current);
    scheduleDraw();
    return () => {
      observer.disconnect();
      if (rafRef.current !== null) {
        window.cancelAnimationFrame(rafRef.current);
        rafRef.current = null;
      }
      if (hoverRafRef.current !== null) {
        window.cancelAnimationFrame(hoverRafRef.current);
        hoverRafRef.current = null;
      }
    };
  }, [scheduleDraw]);

  useEffect(() => {
    if (lastKeyRef.current === renderKey) return;
    lastKeyRef.current = renderKey;
    scheduleDraw();
  }, [renderKey, scheduleDraw]);

  useEffect(() => {
    setHasDrawn(false);
    if (!cacheKey) {
      setCachedSnapshot(null);
      return;
    }
    setCachedSnapshot(getThumbnailCache(cacheKey) ?? null);
  }, [cacheKey, renderKey]);

  const scheduleHoverIndex = (index: number | null) => {
    hoverPendingRef.current = index;
    if (hoverRafRef.current !== null) return;
    hoverRafRef.current = window.requestAnimationFrame(() => {
      hoverRafRef.current = null;
      const next = hoverPendingRef.current;
      if (hoverIndexRef.current === next) return;
      hoverIndexRef.current = next;
      setHoverIndex(next);
    });
  };

  const handleMouseMove = (event: ReactMouseEvent<HTMLDivElement>) => {
    if (!displayBars.length || !containerRef.current) return;
    const rect = containerRef.current.getBoundingClientRect();
    const x = event.clientX - rect.left;
    if (rect.width <= 0) return;
    const ratio = Math.min(0.999, Math.max(0, x / rect.width));
    const nextIndex = Math.min(displayBars.length - 1, Math.floor(ratio * displayBars.length));
    scheduleHoverIndex(nextIndex);
  };

  const handleMouseLeave = () => {
    scheduleHoverIndex(null);
  };

  const safeHoverIndex =
    hoverIndex !== null && displayBars.length
      ? Math.min(hoverIndex, displayBars.length - 1)
      : null;
  const activeIndex =
    safeHoverIndex !== null ? safeHoverIndex : displayBars.length ? displayBars.length - 1 : null;
  const activeBar = activeIndex !== null ? displayBars[activeIndex] : null;
  const crosshairLeft =
    safeHoverIndex !== null && displayBars.length
      ? `${((safeHoverIndex + 0.5) / displayBars.length) * 100}%`
      : null;

  return (
    <div
      ref={containerRef}
      className="thumb-canvas"
      onMouseMove={handleMouseMove}
      onMouseLeave={handleMouseLeave}
    >
      {!hasDrawn && cachedSnapshot && <img className="thumb-canvas-image" src={cachedSnapshot} alt="" />}
      <canvas ref={canvasRef} style={!hasDrawn && cachedSnapshot ? { opacity: 0 } : undefined} />
      {activeBar && (
        <div className="thumb-overlay">
          <div className="thumb-overlay-label">日付</div>
          <div className="thumb-overlay-value">{formatDate(activeBar[0])}</div>
          <div className="thumb-overlay-label">終値</div>
          <div className="thumb-overlay-value">{formatPrice(activeBar[4])}</div>
        </div>
      )}
      {crosshairLeft && <div className="thumb-crosshair" style={{ left: crosshairLeft }} />}
    </div>
  );
}
