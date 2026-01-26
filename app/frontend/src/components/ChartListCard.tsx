import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";
import type { BarsPayload, MaSetting } from "../store";
import type { SignalChip } from "../utils/signals";
import { formatEventBadgeDate } from "../utils/events";
import ChartInfoPanel from "./ChartInfoPanel";
import DetailChart from "./DetailChart";

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

type ActionConfig = {
  label: string;
  ariaLabel: string;
  className?: string;
  onClick: () => void;
};

type ChartListCardProps = {
  code: string;
  name: string;
  payload?: BarsPayload | null;
  fallbackSeries?: number[][] | null;
  status?: "idle" | "loading" | "success" | "empty" | "error";
  maSettings: MaSetting[];
  rangeBars?: number | null;
  eventEarningsDate?: string | null;
  eventRightsDate?: string | null;
  headerLeft?: ReactNode;
  headerRight?: ReactNode;
  tileClassName?: string;
  deferUntilInView?: boolean;
  rootMargin?: string;
  densityKey?: string;
  onOpenDetail: (code: string) => void;
  signals?: SignalChip[];
  action?: ActionConfig | null;
};

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

const buildCandles = (rows: number[][]): Candle[] => {
  const entries: Candle[] = [];
  for (const row of rows) {
    if (!Array.isArray(row) || row.length < 5) continue;
    const time = normalizeTime(row[0]);
    if (time == null) continue;
    const open = Number(row[1]);
    const high = Number(row[2]);
    const low = Number(row[3]);
    const close = Number(row[4]);
    if (![open, high, low, close].every((value) => Number.isFinite(value))) continue;
    entries.push({ time, open, high, low, close });
  }
  entries.sort((a, b) => a.time - b.time);
  return entries;
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
  return entries;
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

const parseRootMarginPx = (value: string): number => {
  const first = value.split(/\s+/)[0] ?? "0px";
  const parsed = Number.parseFloat(first.replace("px", ""));
  return Number.isFinite(parsed) ? parsed : 0;
};

const getScrollParent = (element: HTMLElement): HTMLElement | null => {
  let current: HTMLElement | null = element.parentElement;
  while (current) {
    const style = window.getComputedStyle(current);
    const overflowY = style.overflowY;
    const overflowX = style.overflowX;
    const isScrollableY =
      (overflowY === "auto" || overflowY === "scroll") && current.scrollHeight > current.clientHeight;
    const isScrollableX =
      (overflowX === "auto" || overflowX === "scroll") && current.scrollWidth > current.clientWidth;
    if (isScrollableY || isScrollableX) return current;
    current = current.parentElement;
  }
  return null;
};

const useInView = (enabled: boolean, rootMargin = "220px") => {
  const ref = useRef<HTMLDivElement | null>(null);
  const [inView, setInView] = useState(!enabled);

  useEffect(() => {
    if (!enabled) {
      setInView(true);
      return;
    }
    const element = ref.current;
    if (!element) return;

    if (typeof IntersectionObserver !== "undefined") {
      const observer = new IntersectionObserver(
        ([entry]) => {
          setInView(entry.isIntersecting);
        },
        { rootMargin, threshold: 0.1 }
      );
      observer.observe(element);
      return () => observer.disconnect();
    }

    const margin = parseRootMarginPx(rootMargin);
    const scrollParent = getScrollParent(element);
    let rafId: number | null = null;
    const check = () => {
      rafId = null;
      const rect = element.getBoundingClientRect();
      const intersects =
        rect.bottom >= -margin &&
        rect.right >= 0 &&
        rect.top <= window.innerHeight + margin &&
        rect.left <= window.innerWidth;
      setInView(intersects);
    };
    const scheduleCheck = () => {
      if (rafId !== null) return;
      rafId = window.requestAnimationFrame(check);
    };

    scheduleCheck();
    const options: AddEventListenerOptions = { passive: true };
    window.addEventListener("resize", scheduleCheck, options);
    if (scrollParent) {
      scrollParent.addEventListener("scroll", scheduleCheck, options);
    } else {
      window.addEventListener("scroll", scheduleCheck, options);
    }

    return () => {
      window.removeEventListener("resize", scheduleCheck);
      if (scrollParent) {
        scrollParent.removeEventListener("scroll", scheduleCheck);
      } else {
        window.removeEventListener("scroll", scheduleCheck);
      }
      if (rafId !== null) {
        window.cancelAnimationFrame(rafId);
      }
    };
  }, [enabled, rootMargin]);

  return { ref, inView };
};

const ChartListCard = memo(function ChartListCard({
  code,
  name,
  payload,
  fallbackSeries,
  status,
  maSettings,
  rangeBars,
  eventEarningsDate,
  eventRightsDate,
  headerLeft,
  headerRight,
  tileClassName,
  deferUntilInView = false,
  rootMargin,
  densityKey,
  onOpenDetail,
  signals,
  action
}: ChartListCardProps) {
  const { ref, inView } = useInView(deferUntilInView, rootMargin);
  const [hoverTime, setHoverTime] = useState<number | null>(null);
  const hoverRafRef = useRef<number | null>(null);
  const hoverPendingRef = useRef<number | null>(null);
  const hoverValueRef = useRef<number | null>(null);

  const rows = useMemo(
    () => (payload?.bars?.length ? payload.bars : fallbackSeries ?? []),
    [payload, fallbackSeries]
  );
  const candlesAll = useMemo(() => buildCandles(rows), [rows]);
  const volumeAll = useMemo(() => buildVolume(rows), [rows]);
  const candles = useMemo(() => {
    if (!rangeBars || rangeBars <= 0) return candlesAll;
    return candlesAll.slice(-rangeBars);
  }, [candlesAll, rangeBars]);
  const volume = useMemo(() => {
    if (!rangeBars || rangeBars <= 0) return volumeAll;
    return volumeAll.slice(-rangeBars);
  }, [volumeAll, rangeBars]);
  const maLines = useMemo(
    () =>
      maSettings.map((setting) => ({
        key: setting.key,
        label: setting.label,
        period: setting.period,
        color: setting.color,
        visible: setting.visible,
        lineWidth: setting.lineWidth,
        data: computeMA(candlesAll, setting.period)
      })),
    [candlesAll, maSettings]
  );
  const rangedMaLines = useMemo(() => {
    if (!rangeBars || rangeBars <= 0) return maLines;
    return maLines.map((line) => ({
      ...line,
      data: line.data.slice(-rangeBars)
    }));
  }, [maLines, rangeBars]);
  const barsForInfo = useMemo(
    () => candles.map((bar) => ({ time: bar.time, close: bar.close })),
    [candles]
  );
  const chartKey = `${code}-${rangeBars ?? "all"}-${densityKey ?? "default"}`;

  const scheduleHoverTime = useCallback((time: number | null) => {
    hoverPendingRef.current = time;
    if (hoverRafRef.current !== null) return;
    hoverRafRef.current = window.requestAnimationFrame(() => {
      hoverRafRef.current = null;
      const next = hoverPendingRef.current ?? null;
      if (hoverValueRef.current === next) return;
      hoverValueRef.current = next;
      setHoverTime(next);
    });
  }, []);

  useEffect(
    () => () => {
      if (hoverRafRef.current !== null) {
        window.cancelAnimationFrame(hoverRafRef.current);
        hoverRafRef.current = null;
      }
    },
    []
  );

  const handleOpen = () => onOpenDetail(code);
  const showLoading = rows.length === 0;
  const earningsLabel = formatEventBadgeDate(eventEarningsDate);
  const rightsLabel = formatEventBadgeDate(eventRightsDate);
  const loadingLabel =
    status === "error"
      ? "読み込み失敗"
      : status === "empty"
      ? "データなし"
      : "読み込み中...";

  return (
    <div
      className={`tile rank-tile${tileClassName ? ` ${tileClassName}` : ""}`}
      role="button"
      tabIndex={0}
      onClick={handleOpen}
      ref={deferUntilInView ? ref : undefined}
    >
      <div className="rank-tile-header">
        {headerLeft ? (
          <div className="rank-tile-left">{headerLeft}</div>
        ) : (
          <div className="rank-tile-left">
            <div className="tile-id">
              <span className="tile-code">{code}</span>
              <span className="tile-name">{name}</span>
              {(rightsLabel || earningsLabel) && (
                <span className="event-badges">
                  {rightsLabel && <span className="event-badge event-rights">{"\u6a29\u5229"} {rightsLabel}</span>}
                  {earningsLabel && (
                    <span className="event-badge event-earnings">{"\u6c7a\u7b97"} {earningsLabel}</span>
                  )}
                </span>
              )}
            </div>
          </div>
        )}
        {headerRight ? (
          <div className="rank-tile-right">{headerRight}</div>
        ) : (
          <div className="rank-tile-right">
            {action && (
              <button
                type="button"
                className={action.className ?? "favorite-toggle"}
                aria-label={action.ariaLabel}
                onClick={(event) => {
                  event.stopPropagation();
                  action.onClick();
                }}
              >
                {action.label}
              </button>
            )}
          </div>
        )}
      </div>
      {signals?.length ? (
        <div className="tile-signal-row">
          <div className="signal-chips">
            {signals.slice(0, 4).map((signal) => (
              <span
                key={`${code}-${signal.label}`}
                className={`signal-chip ${signal.kind === "warning" ? "warning" : "achieved"}`}
              >
                {signal.label}
              </span>
            ))}
          </div>
        </div>
      ) : null}
      <div className="tile-chart">
        {deferUntilInView && !inView && <div className="rank-chart-placeholder" />}
        {(!deferUntilInView || inView) && showLoading && (
          <div className="tile-loading">{loadingLabel}</div>
        )}
        {(!deferUntilInView || inView) && !showLoading && (
          <>
            <DetailChart
              key={chartKey}
              candles={candles}
              volume={volume}
              maLines={rangedMaLines}
              showVolume={false}
              boxes={[]}
              showBoxes={false}
              onCrosshairMove={(time) => scheduleHoverTime(time)}
            />
            <ChartInfoPanel bars={barsForInfo} hoverTime={hoverTime} />
          </>
        )}
      </div>
    </div>
  );
});

export default ChartListCard;
