import { memo, useEffect, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";
import type { BarsPayload, MaSetting } from "../store";
import type { SignalChip } from "../utils/signals";
import { formatEventBadgeDate } from "../utils/events";
import ThumbnailCanvas from "./ThumbnailCanvas";

type ActionConfig = {
  label: ReactNode;
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
  maxDate?: string | null;
  phaseBody?: number | null;
  phaseEarly?: number | null;
  phaseLate?: number | null;
  phaseN?: number | null;
};

const normalizeDateParts = (year: number, month: number, day: number) => {
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return null;
  if (year < 1900 || month < 1 || month > 12 || day < 1 || day > 31) return null;
  return Math.floor(Date.UTC(year, month - 1, day) / 1000);
};

const parseMaxDate = (value: string | null | undefined): number | null => {
  if (!value) return null;
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  // Use UTC midnight for comparison
  return Math.floor(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()) / 1000);
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
  action,
  maxDate,
  phaseBody,
  phaseEarly,
  phaseLate,
  phaseN
}: ChartListCardProps) {
  const { ref, inView } = useInView(deferUntilInView, rootMargin);
  const maxTime = useMemo(() => parseMaxDate(maxDate), [maxDate]);
  const basePayload = useMemo(() => {
    if (payload?.bars?.length) return payload;
    if (fallbackSeries?.length) {
      return {
        bars: fallbackSeries,
        ma: { ma7: [], ma20: [], ma60: [] }
      };
    }
    return null;
  }, [payload, fallbackSeries]);
  const barsPayload = useMemo(() => {
    if (!basePayload) return null;
    if (maxTime === null) return basePayload;
    const filteredBars = basePayload.bars.filter((row) => {
      const time = normalizeTime(row[0]);
      return time != null && time <= maxTime;
    });
    return { ...basePayload, bars: filteredBars };
  }, [basePayload, maxTime]);
  const chartKey = `${code}-${rangeBars ?? "all"}-${densityKey ?? "default"}`;

  const handleOpen = () => onOpenDetail(code);
  const showLoading = !barsPayload || barsPayload.bars.length === 0;
  const earningsLabel = formatEventBadgeDate(eventEarningsDate);
  const rightsLabel = formatEventBadgeDate(eventRightsDate);
  const formatScore = (value: number | null | undefined) =>
    Number.isFinite(value)
      ? String(Math.min(10, Math.max(0, Math.round(value! * 10))))
      : "--";
  const formatN = (value: number | null | undefined) =>
    typeof value === "number" ? String(value) : "--";
  const showPhase = false;
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
      {showPhase && (
        <div className="tile-phase">
          <div className="tile-scores">
            <span className="score-chip">B {formatScore(phaseBody)}</span>
            <span className="score-chip">E {formatScore(phaseEarly)}</span>
            <span className="score-chip">L {formatScore(phaseLate)}</span>
            <span className="score-chip">n {formatN(phaseN)}</span>
          </div>
        </div>
      )}
      <div className="tile-chart">
        {deferUntilInView && !inView && <div className="rank-chart-placeholder" />}
        {(!deferUntilInView || inView) && showLoading && (
          <div className="tile-loading">{loadingLabel}</div>
        )}
        {(!deferUntilInView || inView) && !showLoading && barsPayload && (
          <ThumbnailCanvas
            key={chartKey}
            payload={barsPayload}
            boxes={[]}
            showBoxes={false}
            maSettings={maSettings}
            maxBars={rangeBars ?? undefined}
            showAxes
          />
        )}
      </div>
    </div>
  );
});

export default ChartListCard;




