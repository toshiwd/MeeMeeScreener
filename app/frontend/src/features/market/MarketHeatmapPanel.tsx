import { useCallback, useLayoutEffect, useMemo, useRef, useState } from "react";
import {
  buildMarketSectorMatrix,
  formatMarketFlow,
  formatMarketRate,
  formatMarketValue,
  getMarketTileColors,
  isWatchedSector,
  type MarketMetricKey,
  type MarketSectorViewItem
} from "./marketHelpers";

type Props = {
  loading: boolean;
  error: string | null;
  items: MarketSectorViewItem[];
  metric: MarketMetricKey;
  selectedSector: string | null;
  onSectorSelect: (item: MarketSectorViewItem) => void;
  onSectorHover: (item: MarketSectorViewItem | null) => void;
};

type TooltipAnchor = {
  top: number;
  right: number;
  bottom: number;
  left: number;
};

type TooltipPlacement = {
  left: number;
  top: number;
};

const TOOLTIP_OFFSET_X = 12;
const TOOLTIP_OFFSET_Y = 12;
const TOOLTIP_EDGE_GAP = 8;
const TOOLTIP_MAX_WIDTH = 220;
const TOOLTIP_APPROX_HEIGHT = 132;

const computeTooltipPlacement = (
  surfaceRect: DOMRect,
  anchorRect: DOMRect,
  tooltipWidth: number,
  tooltipHeight: number
) => {
  const surfaceWidth = surfaceRect.width;
  const surfaceHeight = surfaceRect.height;
  const anchorLeft = anchorRect.left - surfaceRect.left;
  const anchorRight = anchorRect.right - surfaceRect.left;
  const anchorTop = anchorRect.top - surfaceRect.top;
  const anchorBottom = anchorRect.bottom - surfaceRect.top;
  const fitsRight = anchorRight + TOOLTIP_OFFSET_X + tooltipWidth + TOOLTIP_EDGE_GAP <= surfaceWidth;
  const fitsLeft = anchorLeft - TOOLTIP_OFFSET_X - tooltipWidth - TOOLTIP_EDGE_GAP >= 0;
  const fitsTop = anchorTop - TOOLTIP_OFFSET_Y - tooltipHeight - TOOLTIP_EDGE_GAP >= 0;
  const fitsBottom = anchorBottom + TOOLTIP_OFFSET_Y + tooltipHeight + TOOLTIP_EDGE_GAP <= surfaceHeight;

  let left = anchorRight + TOOLTIP_OFFSET_X;
  let top = anchorTop - tooltipHeight - TOOLTIP_OFFSET_Y;

  if (!fitsRight && fitsLeft) {
    left = anchorLeft - tooltipWidth - TOOLTIP_OFFSET_X;
  }

  if (!fitsTop && fitsBottom) {
    top = anchorBottom + TOOLTIP_OFFSET_Y;
  }

  if (!fitsRight && !fitsLeft && fitsBottom && fitsTop) {
    left = anchorLeft - tooltipWidth - TOOLTIP_OFFSET_X;
    top = anchorBottom + TOOLTIP_OFFSET_Y;
  }

  return {
    left: Math.min(
      Math.max(left, TOOLTIP_EDGE_GAP),
      Math.max(TOOLTIP_EDGE_GAP, surfaceWidth - tooltipWidth - TOOLTIP_EDGE_GAP)
    ),
    top: Math.min(
      Math.max(top, TOOLTIP_EDGE_GAP),
      Math.max(TOOLTIP_EDGE_GAP, surfaceHeight - tooltipHeight - TOOLTIP_EDGE_GAP)
    )
  };
};

export default function MarketHeatmapPanel({
  loading,
  error,
  items,
  metric,
  selectedSector,
  onSectorSelect,
  onSectorHover
}: Props) {
  const [hoveredItem, setHoveredItem] = useState<MarketSectorViewItem | null>(null);
  const [hoverAnchor, setHoverAnchor] = useState<TooltipAnchor | null>(null);
  const [tooltipPlacement, setTooltipPlacement] = useState<TooltipPlacement | null>(null);
  const surfaceRef = useRef<HTMLDivElement | null>(null);
  const tooltipRef = useRef<HTMLDivElement | null>(null);
  const matrix = useMemo(() => buildMarketSectorMatrix(items), [items]);

  const metricDomain = useMemo(() => {
    if (!items.length) return { rateAbs: 1, flowAbs: 1 };
    const rateAbs = Math.max(...items.map((item) => Math.abs(item.rate)), 1);
    const flowAbs = Math.max(...items.map((item) => Math.abs(item.flow)), 1);
    return { rateAbs, flowAbs };
  }, [items]);

  const recalculateTooltipPlacement = useCallback(() => {
    if (!hoveredItem || !hoverAnchor || !surfaceRef.current || !tooltipRef.current) return;

    setTooltipPlacement(
      computeTooltipPlacement(
        surfaceRef.current.getBoundingClientRect(),
        hoverAnchor,
        tooltipRef.current.getBoundingClientRect().width,
        tooltipRef.current.getBoundingClientRect().height
      )
    );
  }, [hoverAnchor, hoveredItem]);

  useLayoutEffect(() => {
    recalculateTooltipPlacement();
  }, [recalculateTooltipPlacement, metric, items]);

  useLayoutEffect(() => {
    if (!hoveredItem || !hoverAnchor || !surfaceRef.current || typeof ResizeObserver === "undefined") return;
    const observer = new ResizeObserver(() => {
      recalculateTooltipPlacement();
    });
    observer.observe(surfaceRef.current);
    return () => observer.disconnect();
  }, [hoverAnchor, hoveredItem, recalculateTooltipPlacement]);

  if (loading && !items.length) {
    return (
      <div className="market-empty-state">
        <div className="heatmap-empty-card">
          <div className="heatmap-empty-title">データ取得中...</div>
          <div className="heatmap-empty-sub">市場データを読み込んでいます。</div>
        </div>
      </div>
    );
  }

  if (error && !items.length) {
    return (
      <div className="market-empty-state">
        <div className="heatmap-empty-card">
          <div className="heatmap-empty-title">ヒートマップの取得に失敗しました</div>
          <div className="heatmap-empty-sub">{error}</div>
        </div>
      </div>
    );
  }

  if (!items.length) {
    return (
      <div className="market-empty-state">
        <div className="heatmap-empty-card">
          <div className="heatmap-empty-title">表示対象がありません</div>
          <div className="heatmap-empty-sub">市場データがありません。</div>
        </div>
      </div>
    );
  }

  const tooltipItem = hoveredItem ?? null;

  return (
    <div
      className="market-heatmap-panel"
      onMouseLeave={() => {
        setHoveredItem(null);
        setHoverAnchor(null);
        setTooltipPlacement(null);
        onSectorHover(null);
      }}
    >
      <div className="market-chart-legend">
        <span className="market-legend-item"><i className="is-up" />赤=上昇 / 流入</span>
        <span className="market-legend-item"><i className="is-neutral" />グレー=中立</span>
        <span className="market-legend-item"><i className="is-down" />緑=下落 / 流出</span>
        {metric === "both" ? <span className="market-legend-note">本体=騰落率 / 帯=資金フロー</span> : null}
      </div>
      <div ref={surfaceRef} className="market-chart-surface market-heatmap-surface">
        <div className="market-grid-heatmap">
          {matrix.map((row, rowIndex) =>
            row.map((item, colIndex) => {
              if (!item) {
                return (
                  <div
                    key={`market-empty-${rowIndex}-${colIndex}`}
                    className="market-grid-tile market-grid-tile-empty"
                    aria-hidden="true"
                  />
                );
              }

              const metricLine =
                metric === "flow"
                  ? formatMarketFlow(item.flow)
                  : metric === "both"
                    ? `${formatMarketRate(item.rate)} / ${formatMarketFlow(item.flow)}`
                    : formatMarketRate(item.rate);
              const isSelected = selectedSector === item.sector33_code;
              const watched = isWatchedSector(item);
              const { bodyColor, bandColor } = getMarketTileColors(item, metric, metricDomain);
              return (
                <button
                  key={item.sector33_code}
                  type="button"
                  className={`market-grid-tile${isSelected ? " is-selected" : ""}${watched ? " is-related" : ""}${metric === "both" ? " is-both" : ""}`}
                  style={{
                    background: bodyColor,
                    boxShadow: isSelected ? "0 0 0 2px rgba(59, 130, 246, 0.95) inset" : undefined
                  }}
                  onMouseEnter={(event) => {
                    const nextAnchor = event.currentTarget.getBoundingClientRect();
                    setHoveredItem(item);
                    setHoverAnchor(nextAnchor);
                    if (surfaceRef.current) {
                      setTooltipPlacement(
                        computeTooltipPlacement(
                          surfaceRef.current.getBoundingClientRect(),
                          nextAnchor,
                          TOOLTIP_MAX_WIDTH,
                          TOOLTIP_APPROX_HEIGHT
                        )
                      );
                    }
                    onSectorHover(item);
                  }}
                  onFocus={(event) => {
                    const nextAnchor = event.currentTarget.getBoundingClientRect();
                    setHoveredItem(item);
                    setHoverAnchor(nextAnchor);
                    if (surfaceRef.current) {
                      setTooltipPlacement(
                        computeTooltipPlacement(
                          surfaceRef.current.getBoundingClientRect(),
                          nextAnchor,
                          TOOLTIP_MAX_WIDTH,
                          TOOLTIP_APPROX_HEIGHT
                        )
                      );
                    }
                    onSectorHover(item);
                  }}
                  onBlur={() => {
                    setHoveredItem(null);
                    setHoverAnchor(null);
                    setTooltipPlacement(null);
                    onSectorHover(null);
                  }}
                  onClick={() => onSectorSelect(item)}
                >
                  {bandColor ? (
                    <span className="market-grid-tile-band" style={{ background: bandColor }} aria-hidden="true" />
                  ) : null}
                  {watched ? <span className="market-grid-tile-badge">監視あり</span> : null}
                  <div className="market-grid-tile-header">
                    <div className="market-grid-tile-label">{item.label}</div>
                    <div className="market-grid-tile-code">{item.sector33_code}</div>
                  </div>
                  <div className="market-grid-tile-value">{metricLine}</div>
                  <div className="market-grid-tile-meta">
                    <span>監視 {item.watchlistCount}件</span>
                    <span>{formatMarketValue(item.weight)}</span>
                  </div>
                  <div className="market-grid-tile-reps">
                    {item.representatives.length > 0
                      ? `代表 ${item.representatives[0].code} ${item.representatives[0].name}`
                      : "代表 --"}
                  </div>
                </button>
              );
            })
          )}
        </div>
        {tooltipItem ? (
          <div
            ref={tooltipRef}
            className="market-float-tooltip"
            style={{
              left: tooltipPlacement?.left ?? TOOLTIP_EDGE_GAP,
              top: tooltipPlacement?.top ?? TOOLTIP_EDGE_GAP,
              maxWidth: TOOLTIP_MAX_WIDTH,
              visibility: tooltipPlacement ? "visible" : "hidden"
            }}
          >
            <div className="market-tooltip-title">{tooltipItem.label}</div>
            <div className="market-tooltip-row">
              <span>騰落率</span>
              <span>{formatMarketRate(tooltipItem.rate)}</span>
            </div>
            <div className="market-tooltip-row">
              <span>資金フロー</span>
              <span>{formatMarketFlow(tooltipItem.flow)}</span>
            </div>
            <div className="market-tooltip-row">
              <span>監視銘柄</span>
              <span>{tooltipItem.watchlistCount}件</span>
            </div>
          </div>
        ) : null}
      </div>
    </div>
  );
}
