import { useEffect, useMemo, useRef, useState } from "react";
import type { ISeriesApi } from "lightweight-charts";
import type { DailyPosition, TradeEvent, TradeMarker } from "../utils/positions";

type PositionOverlayProps = {
  candleSeries: ISeriesApi<"Candlestick"> | null;
  chart: { timeScale?: () => any } | null;
  dailyPositions: DailyPosition[];
  tradeMarkers: TradeMarker[];
  showOverlay: boolean;
  showPnL: boolean;
  hoverTime: number | null;
  bars: {
    time: number;
    open: number;
    high: number;
    low: number;
    close: number;
  }[];
  volume: {
    time: number;
    value: number;
  }[];
  showMarkers?: boolean;
  markerSuffix?: string;
  maLines?: {
    key: string;
    label?: string;
    period?: number;
    color?: string;
    data: { time: number; value: number }[];
    visible: boolean;
  }[];
  hidePanel?: boolean;
};

const formatNumber = (value: number) => {
  if (!Number.isFinite(value)) return "0";
  return Math.round(value).toLocaleString();
};

const formatSignedNumber = (value: number) => {
  if (!Number.isFinite(value)) return "0";
  const rounded = Math.round(value);
  const prefix = rounded > 0 ? "+" : "";
  return `${prefix}${rounded.toLocaleString()}`;
};

const formatPercent = (value: number) => {
  if (!Number.isFinite(value)) return "0%";
  const rounded = Math.round(value * 100) / 100;
  const prefix = rounded > 0 ? "+" : "";
  return `${prefix}${rounded.toFixed(2)}%`;
};

const formatLots = (value: number) => {
  if (!Number.isFinite(value)) return "0";
  return Number.isInteger(value) ? `${value}` : value.toFixed(1);
};

const formatDate = (time: number) => {
  const date = new Date(time * 1000);
  const year = date.getUTCFullYear();
  const yy = String(year % 100).padStart(2, "0");
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return `${yy}/${month}/${day}`;
};

const findClosestTime = (times: number[], time: number | null) => {
  if (!times.length) return null;
  if (time == null) return null;
  let left = 0;
  let right = times.length - 1;
  while (left <= right) {
    const mid = Math.floor((left + right) / 2);
    const midTime = times[mid];
    if (midTime === time) return midTime;
    if (midTime < time) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  return times[Math.max(0, Math.min(times.length - 1, right))] ?? null;
};

const findClosestValue = (data: { time: number; value: number }[], time: number | null) => {
  if (!data.length || time == null) return null;
  let left = 0;
  let right = data.length - 1;
  while (left <= right) {
    const mid = Math.floor((left + right) / 2);
    const midTime = data[mid].time;
    if (midTime === time) return data[mid].value;
    if (midTime < time) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  const lowerIndex = Math.max(0, Math.min(data.length - 1, right));
  const upperIndex = Math.max(0, Math.min(data.length - 1, left));
  const lower = data[lowerIndex];
  const upper = data[upperIndex];
  if (!lower) return upper?.value ?? null;
  if (!upper) return lower.value;
  return Math.abs(time - lower.time) <= Math.abs(upper.time - time) ? lower.value : upper.value;
};

const findClosestIndex = (bars: PositionOverlayProps["bars"], time: number | null) => {
  if (!bars.length || time == null) return null;
  let left = 0;
  let right = bars.length - 1;
  while (left <= right) {
    const mid = Math.floor((left + right) / 2);
    const midTime = bars[mid].time;
    if (midTime === time) return mid;
    if (midTime < time) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  const lowerIndex = Math.max(0, Math.min(bars.length - 1, right));
  const upperIndex = Math.max(0, Math.min(bars.length - 1, left));
  const lower = bars[lowerIndex];
  const upper = bars[upperIndex];
  if (!lower) return upperIndex ?? null;
  if (!upper) return lowerIndex ?? null;
  return Math.abs(time - lower.time) <= Math.abs(upper.time - time) ? lowerIndex : upperIndex;
};

const resolveMaLabel = (entry: {
  key: string;
  label?: string;
  period?: number;
}) => entry.label ?? (entry.period ? `MA${entry.period}` : entry.key.toUpperCase());

const resolveBrokerMeta = (value: string | undefined) => {
  const raw = (value ?? "").toString().trim();
  const lower = raw.toLowerCase();
  if (lower.includes("sbi")) return { key: "sbi", label: "SBI" };
  if (lower.includes("rakuten")) return { key: "rakuten", label: "RAKUTEN" };
  if (raw) return { key: lower, label: raw.toUpperCase() };
  return { key: "unknown", label: "N/A" };
};

const formatTrade = (trade: TradeEvent) => {
  const action = trade.action === "open" ? "新規" : "決済";
  const side = trade.side === "buy" ? "買い" : "売り";
  const units = Number.isFinite(trade.units) ? formatNumber(trade.units) : "0";
  const price = trade.price ? ` @ ${formatNumber(trade.price)}` : "";
  return `${side} ${action} ${units}${price}`;
};

const brokerOrder = (key: string) => {
  if (key === "rakuten") return 0;
  if (key === "sbi") return 1;
  if (key === "unknown") return 2;
  return 3;
};

const compareBrokerKey = (a?: string, b?: string) => {
  const keyA = a ?? "unknown";
  const keyB = b ?? "unknown";
  const rankA = brokerOrder(keyA);
  const rankB = brokerOrder(keyB);
  if (rankA !== rankB) return rankA - rankB;
  return keyA.localeCompare(keyB);
};

export default function PositionOverlay({
  candleSeries,
  chart,
  dailyPositions,
  tradeMarkers,
  showOverlay,
  showPnL,
  hoverTime,
  bars,
  volume,
  showMarkers = true,
  markerSuffix,
  maLines = [],
  hidePanel = false
}: PositionOverlayProps) {
  const [rangeTick, setRangeTick] = useState(0);
  const rangeRafRef = useRef<number | null>(null);
  const volumeMap = useMemo(
    () => new Map(volume.map((item) => [item.time, item.value])),
    [volume]
  );
  const activeIndex = useMemo(() => {
    if (!bars.length) return null;
    if (hoverTime == null) return bars.length - 1;
    const idx = findClosestIndex(bars, hoverTime);
    return idx ?? bars.length - 1;
  }, [bars, hoverTime]);

  const positionsByTime = useMemo(() => {
    const map = new Map<number, DailyPosition[]>();
    dailyPositions.forEach((pos) => {
      const list = map.get(pos.time) ?? [];
      list.push(pos);
      map.set(pos.time, list);
    });
    map.forEach((list) =>
      list.sort((a, b) => compareBrokerKey(a.brokerKey, b.brokerKey))
    );
    return map;
  }, [dailyPositions]);
  const positionTimes = useMemo(() => {
    const times: number[] = [];
    positionsByTime.forEach((_value, key) => {
      times.push(key);
    });
    times.sort((a, b) => a - b);
    return times;
  }, [positionsByTime]);

  const normalizeTimeValue = (value: unknown) => {
    if (typeof value === "number") return value;
    if (value && typeof value === "object") {
      const data = value as { year?: number; month?: number; day?: number };
      if (data.year && data.month && data.day) {
        return Math.floor(Date.UTC(data.year, data.month - 1, data.day) / 1000);
      }
    }
    return null;
  };

  useEffect(() => {
    if (!chart) return;
    const timeScale = chart.timeScale?.();
    const priceScale = (chart as any)?.priceScale?.("right");

    const schedule = () => {
      if (rangeRafRef.current != null) return;
      rangeRafRef.current = window.requestAnimationFrame(() => {
        rangeRafRef.current = null;
        setRangeTick((prev) => prev + 1);
      });
    };

    const timeHandler = () => schedule();
    const priceHandler = () => schedule();

    timeScale?.subscribeVisibleTimeRangeChange?.(timeHandler);
    timeScale?.subscribeVisibleLogicalRangeChange?.(timeHandler);
    priceScale?.subscribeVisibleLogicalRangeChange?.(priceHandler);

    return () => {
      timeScale?.unsubscribeVisibleTimeRangeChange?.(timeHandler);
      timeScale?.unsubscribeVisibleLogicalRangeChange?.(timeHandler);
      priceScale?.unsubscribeVisibleLogicalRangeChange?.(priceHandler);
      if (rangeRafRef.current != null) {
        window.cancelAnimationFrame(rangeRafRef.current);
        rangeRafRef.current = null;
      }
    };
  }, [chart]);

  const activeBar = activeIndex != null ? bars[activeIndex] : null;
  const showPositionInfo = showOverlay || showPnL;
  const activePositionTime = useMemo(
    () => (showPositionInfo && activeBar ? findClosestTime(positionTimes, activeBar.time) : null),
    [positionTimes, activeBar?.time, showPositionInfo]
  );
  const activeMaValues = useMemo(() => {
    if (!maLines.length || !activeBar) return [];
    return maLines
      .filter((line) => line.visible)
      .map((line) => {
        const value = findClosestValue(line.data, activeBar.time);
        const label = resolveMaLabel(line);
        return {
          key: line.key,
          label,
          value,
          color: line.color
        };
      });
  }, [maLines, activeBar]);
  const maCounts = useMemo(() => {
    if (!bars.length || !maLines.length) return [];
    return maLines
      .filter((line) => line.visible)
      .map((line) => {
        const valueMap = new Map<number, number>();
        line.data.forEach((point) => {
          if (point && Number.isFinite(point.value)) {
            valueMap.set(point.time, point.value);
          }
        });
        const upCounts: Array<number | null> = new Array(bars.length).fill(null);
        const downCounts: Array<number | null> = new Array(bars.length).fill(null);
        let up = 0;
        let down = 0;
        bars.forEach((bar, index) => {
          const maValue = valueMap.get(bar.time);
          if (maValue == null || !Number.isFinite(maValue)) {
            up = 0;
            down = 0;
            upCounts[index] = null;
            downCounts[index] = null;
            return;
          }
          if (bar.close >= maValue) {
            up += 1;
            down = 0;
          } else {
            down += 1;
            up = 0;
          }
          upCounts[index] = up;
          downCounts[index] = down;
        });
        return {
          key: line.key,
          label: resolveMaLabel(line),
          color: line.color,
          upCounts,
          downCounts
        };
      });
  }, [bars, maLines]);
  const activeMaCounts = useMemo(() => {
    if (activeIndex == null) return [];
    return maCounts.map((entry) => ({
      key: entry.key,
      label: entry.label,
      color: entry.color,
      up: entry.upCounts[activeIndex],
      down: entry.downCounts[activeIndex]
    }));
  }, [maCounts, activeIndex]);
  const activePositions = useMemo(() => {
    if (activePositionTime == null) return [];
    const positions = positionsByTime.get(activePositionTime) ?? [];
    if (!positions.length) return [];
    const map = new Map<
      string,
      {
        brokerKey: string;
        brokerLabel: string;
        time: number;
        date: string;
        close: number;
        longLots: number;
        shortLots: number;
        longCost: number;
        shortCost: number;
        realizedPnL: number;
        unrealizedPnL: number;
        totalPnL: number;
      }
    >();
    positions.forEach((pos) => {
      const rawKey = pos.brokerKey ?? pos.brokerLabel ?? "unknown";
      const normalizedKey = rawKey
        .toString()
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9]/g, "");
      const brokerKey = normalizedKey || "unknown";
      const brokerLabel =
        brokerKey === "rakuten"
          ? "RAKUTEN"
          : brokerKey === "sbi"
            ? "SBI"
            : pos.brokerLabel ?? "N/A";
      const entry = map.get(brokerKey);
      if (entry) {
        entry.longLots += pos.longLots;
        entry.shortLots += pos.shortLots;
        entry.longCost += pos.avgLongPrice * pos.longLots;
        entry.shortCost += pos.avgShortPrice * pos.shortLots;
        entry.realizedPnL += pos.realizedPnL;
        entry.unrealizedPnL += pos.unrealizedPnL;
        entry.totalPnL += pos.totalPnL;
      } else {
        map.set(brokerKey, {
          brokerKey,
          brokerLabel,
          time: pos.time,
          date: pos.date,
          close: pos.close,
          longLots: pos.longLots,
          shortLots: pos.shortLots,
          longCost: pos.avgLongPrice * pos.longLots,
          shortCost: pos.avgShortPrice * pos.shortLots,
          realizedPnL: pos.realizedPnL,
          unrealizedPnL: pos.unrealizedPnL,
          totalPnL: pos.totalPnL
        });
      }
    });
    return Array.from(map.values())
      .map((entry) => {
        const avgLongPrice = entry.longLots > 0 ? entry.longCost / entry.longLots : 0;
        const avgShortPrice = entry.shortLots > 0 ? entry.shortCost / entry.shortLots : 0;
        const longText = formatLots(entry.longLots);
        const shortText = formatLots(entry.shortLots);
        return {
          time: entry.time,
          date: entry.date,
          shortLots: entry.shortLots,
          longLots: entry.longLots,
          longText,
          shortText,
          avgLongPrice,
          avgShortPrice,
          realizedPnL: entry.realizedPnL,
          unrealizedPnL: entry.unrealizedPnL,
          totalPnL: entry.totalPnL,
          close: entry.close,
          brokerKey: entry.brokerKey,
          brokerLabel: entry.brokerLabel,
          brokerGroupKey: entry.brokerKey
        };
      })
      .sort((a, b) => compareBrokerKey(a.brokerKey, b.brokerKey));
  }, [positionsByTime, activePositionTime]);
  const activeTradeMarkers = useMemo(() => {
    if (!showOverlay || !showMarkers || activePositionTime == null) return [];
    return tradeMarkers.filter((trade) => trade.time === activePositionTime);
  }, [tradeMarkers, showOverlay, showMarkers, activePositionTime]);

  const labelEntries = useMemo(() => {
    if (!showMarkers || !chart || !candleSeries) return [];
    const timeScale = chart.timeScale?.();
    if (!timeScale || typeof timeScale.timeToCoordinate !== "function") return [];
    if (typeof candleSeries.priceToCoordinate !== "function") return [];

    const barsByTime = new Map(bars.map((bar) => [bar.time, bar]));
    const groups = new Map<string, { brokerKey: string; entries: DailyPosition[] }>();
    dailyPositions.forEach((pos) => {
      const groupKey = pos.brokerGroupKey ?? pos.brokerKey ?? "unknown";
      const brokerKey = pos.brokerKey ?? "unknown";
      const list = groups.get(groupKey);
      if (list) {
        list.entries.push(pos);
      } else {
        groups.set(groupKey, { brokerKey, entries: [pos] });
      }
    });

    const changes: {
      time: number;
      longText: string;
      shortText: string;
      brokerKey: string;
    }[] = [];
    groups.forEach((group) => {
      const sorted = [...group.entries].sort((a, b) => a.time - b.time);
      let prevKey = "";
      sorted.forEach((pos) => {
        const longText = formatLots(pos.longLots);
        const shortText = formatLots(pos.shortLots);
        const nextKey = `${longText}:${shortText}`;
        if (nextKey === "0:0") {
          prevKey = nextKey;
          return;
        }
        if (nextKey !== prevKey) {
          changes.push({ time: pos.time, longText, shortText, brokerKey: group.brokerKey });
          prevKey = nextKey;
        }
      });
    });
    changes.sort((a, b) => a.time - b.time);

    const visibleRange = timeScale.getVisibleRange?.();
    const visibleFrom = normalizeTimeValue(visibleRange?.from ?? null);
    const visibleTo = normalizeTimeValue(visibleRange?.to ?? null);
    const lastBarTime = bars.length ? bars[bars.length - 1].time : null;
    const twoYearsSec = 60 * 60 * 24 * 365 * 2;
    const baseFrom = lastBarTime != null ? lastBarTime - twoYearsSec : null;
    const rangeFrom =
      baseFrom != null && visibleFrom != null ? Math.min(baseFrom, visibleFrom) : baseFrom ?? visibleFrom;
    const rangeTo =
      lastBarTime != null && visibleTo != null ? Math.max(lastBarTime, visibleTo) : lastBarTime ?? visibleTo;
    const limited = changes.filter((entry) => {
      if (rangeFrom != null && entry.time < rangeFrom) return false;
      if (rangeTo != null && entry.time > rangeTo) return false;
      return true;
    });
    const byTime = new Map<
      number,
      { longText: string; shortText: string; brokerKey: string }[]
    >();
    limited.forEach((entry) => {
      const list = byTime.get(entry.time) ?? [];
      list.push({
        longText: entry.longText,
        shortText: entry.shortText,
        brokerKey: entry.brokerKey
      });
      byTime.set(entry.time, list);
    });

    const suffix = markerSuffix ? markerSuffix : null;
    const output: {
      key: string;
      longText: string;
      shortText: string;
      brokerKey: string;
      x: number;
      y: number;
      suffix: string | null;
    }[] = [];
    const placed: { x: number; y: number }[] = [];
    byTime.forEach((list, time) => {
      const bar = barsByTime.get(time);
      if (!bar) return;
      const baseX = timeScale.timeToCoordinate(time as any);
      const yBase = candleSeries.priceToCoordinate(bar.high) ?? candleSeries.priceToCoordinate(bar.close);
      if (baseX == null || yBase == null) return;
      list.sort((a, b) => compareBrokerKey(a.brokerKey, b.brokerKey));
      const hasCollision = (nextX: number, nextY: number) =>
        placed.some((item) => Math.abs(item.x - nextX) < 12 && Math.abs(item.y - nextY) < 10);
      list.forEach((entry, index) => {
        const offsetX = (index - (list.length - 1) / 2) * 14;
        let x = baseX + offsetX;
        let y = yBase - 8 - index * 12;
        let attempts = 0;
        while (hasCollision(x, y) && attempts < 4) {
          y -= 10;
          attempts += 1;
        }
        if (hasCollision(x, y)) return;
        placed.push({ x, y });
        output.push({
          key: `${time}-${entry.brokerKey}`,
          longText: entry.longText,
          shortText: entry.shortText,
          brokerKey: entry.brokerKey,
          x,
          y,
          suffix
        });
      });
    });
    return output;
  }, [showMarkers, markerSuffix, chart, candleSeries, bars, dailyPositions, rangeTick]);

  if (!activeBar) return null;

  const volumeValue = volumeMap.get(activeBar.time);
  const volumeText = Number.isFinite(volumeValue) ? formatNumber(volumeValue ?? 0) : "-";

  const prevBar = activeIndex != null && activeIndex > 0 ? bars[activeIndex - 1] : null;
  const delta = prevBar ? activeBar.close - prevBar.close : null;
  const percent =
    prevBar && prevBar.close !== 0 ? ((activeBar.close - prevBar.close) / prevBar.close) * 100 : null;
  const deltaClass = delta && delta > 0 ? "up" : delta && delta < 0 ? "down" : "";


  return (
    <>
      {labelEntries.length > 0 && (
        <div className="position-marker-layer">
          {labelEntries.map((entry) => (
            <div
              key={entry.key}
              className={`position-marker-label broker-${entry.brokerKey}`}
              data-broker={entry.brokerKey}
              style={{ left: `${entry.x}px`, top: `${entry.y}px` }}
            >
              <span className="position-fraction position-fraction-compact">
                <span className="position-fraction-top">{entry.longText}</span>
                <span className="position-fraction-divider" />
                <span className="position-fraction-bottom">{entry.shortText}</span>
              </span>
              {entry.suffix && (
                <span className="position-marker-suffix">{entry.suffix}</span>
              )}
            </div>
          ))}
        </div>
      )}
      <div className="position-overlay-panel" style={{ display: hidePanel ? 'none' : 'block' }}>
        <div className="position-overlay-header">
          <div className="position-overlay-date">{formatDate(activeBar.time)}</div>
          {delta != null && percent != null && (
            <div className={`position-overlay-change ${deltaClass}`}>
              前日比{formatSignedNumber(delta)} ({formatPercent(percent)})
            </div>
          )}
        </div>
        <div className="position-overlay-grid">
          <div className="position-overlay-label">O</div>
          <div className="position-overlay-value">{formatNumber(activeBar.open)}</div>
          <div className="position-overlay-label">H</div>
          <div className="position-overlay-value">{formatNumber(activeBar.high)}</div>
          <div className="position-overlay-label">L</div>
          <div className="position-overlay-value">{formatNumber(activeBar.low)}</div>
          <div className="position-overlay-label">C</div>
          <div className="position-overlay-value">{formatNumber(activeBar.close)}</div>
          <div className="position-overlay-label">出来高</div>
          <div className="position-overlay-value">{volumeText}</div>
        </div>
        {activeMaValues.length > 0 && (
          <div className="position-overlay-ma">
            {activeMaValues.map((entry) => (
              <div className="position-overlay-ma-row" key={`ma-${entry.key}`}>
                <span className="position-overlay-label" style={{ color: entry.color ?? "#94a3b8" }}>
                  {entry.label}
                </span>
                <span className="position-overlay-value">
                  {entry.value == null ? "--" : formatNumber(entry.value)}
                </span>
              </div>
            ))}
          </div>
        )}
        {activeMaCounts.length > 0 && (
          <div className="position-overlay-ma position-overlay-ma-counts">
            <div className="position-overlay-ma-title">本数</div>
            {activeMaCounts.map((entry) => (
              <div className="position-overlay-ma-row" key={`ma-count-${entry.key}`}>
                <span className="position-overlay-label" style={{ color: entry.color ?? "#94a3b8" }}>
                  {entry.label}
                </span>
                <span className="position-overlay-value">
                  上{entry.up ?? "--"} / 下{entry.down ?? "--"}
                </span>
              </div>
            ))}
          </div>
        )}
        {showPositionInfo && activePositions.length > 0 && (
          <div className="position-overlay-block">
            {activePositions.map((position) => {
              const brokerKey = position.brokerKey ?? "unknown";
              const brokerLabel = position.brokerLabel ?? "N/A";
              return (
                <div
                  key={`${position.time}-${position.brokerGroupKey ?? brokerKey}`}
                  className={`position-overlay-broker broker-${brokerKey}`}
                  data-broker={brokerKey}
                >
                  <div className="position-overlay-row">
                    <span className="position-overlay-label">建玉</span>
                    <span className="position-overlay-value position-overlay-value-fraction">
                      <span className="broker-badge" data-broker={brokerKey}>
                        {brokerLabel}
                      </span>
                      <span className="position-fraction">
                        <span className="position-fraction-top">{position.longText}</span>
                        <span className="position-fraction-divider" />
                        <span className="position-fraction-bottom">{position.shortText}</span>
                      </span>
                    </span>
                  </div>
                  {showPnL && (
                    <>
                      <div className="position-overlay-row">
                        <span className="position-overlay-label">評価損益</span>
                        <span className="position-overlay-value">
                          {formatNumber(position.unrealizedPnL)}
                        </span>
                      </div>
                      <div className="position-overlay-row">
                        <span className="position-overlay-label">実現損益</span>
                        <span className="position-overlay-value">
                          {formatNumber(position.realizedPnL)}
                        </span>
                      </div>
                      <div className="position-overlay-row total">
                        <span className="position-overlay-label">合計</span>
                        <span className="position-overlay-value">
                          {formatNumber(position.totalPnL)}
                        </span>
                      </div>
                    </>
                  )}
                </div>
              );
            })}
          </div>
        )}
        {showOverlay && showMarkers && activeTradeMarkers.length > 0 && (
          <div className="position-overlay-trades">
            {activeTradeMarkers.map((marker) => {
              const brokerKey = marker.brokerKey ?? resolveBrokerMeta(marker.trades[0]?.broker).key;
              const brokerLabel = marker.brokerLabel ?? resolveBrokerMeta(marker.trades[0]?.broker).label;
              return (
                <div
                  key={`${marker.time}-${marker.brokerGroupKey ?? brokerKey}`}
                  className={`broker-${brokerKey}`}
                  data-broker={brokerKey}
                >
                  <div className="position-overlay-trade-header">
                    <span className="broker-badge" data-broker={brokerKey}>
                      {brokerLabel}
                    </span>
                  </div>
                  {marker.trades.map((trade, index) => (
                    <div key={`${trade.date}-${trade.kind ?? "trade"}-${index}`}>
                      {formatTrade(trade)}
                    </div>
                  ))}
                </div>
              );
            })}
          </div>
        )}
      </div>
    </>
  );
}



