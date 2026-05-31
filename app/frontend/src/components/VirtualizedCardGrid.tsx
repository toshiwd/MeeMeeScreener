import { useCallback, useEffect, useMemo, useRef, useState, type CSSProperties, type ReactNode } from "react";
import { FixedSizeGrid as Grid, type GridOnItemsRenderedProps } from "react-window";

type ItemKeyFn<T> = (item: T, index: number) => string;

type Props<T> = {
  items: T[];
  columns: number;
  itemKey: ItemKeyFn<T>;
  renderItem: (item: T, index: number) => ReactNode;
  className?: string;
  overscanRowCount?: number;
  onVisibleItemsChange?: (items: T[], indexes: number[]) => void;
};

const GRID_GAP_PX = 8;
const FALLBACK_CARD_HEIGHT = 280;
const FALLBACK_WIDTH = 1024;
const FALLBACK_HEIGHT = 768;

const parseCssPixelValue = (value: string | null | undefined) => {
  if (!value) return null;
  const parsed = Number.parseFloat(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const parseCssIntegerValue = (value: string | null | undefined) => {
  if (!value) return null;
  const parsed = Number.parseInt(value.trim(), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
};

export default function VirtualizedCardGrid<T>({
  items,
  columns,
  itemKey,
  renderItem,
  className,
  overscanRowCount = 2,
  onVisibleItemsChange,
}: Props<T>) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [viewport, setViewport] = useState({ width: 0, height: 0, rowHeight: FALLBACK_CARD_HEIGHT });

  useEffect(() => {
    const element = containerRef.current;
    if (!element) return;

    const measure = () => {
      const style = window.getComputedStyle(element);
      const measuredWidth = Math.floor(element.clientWidth);
      const measuredHeight = Math.floor(element.clientHeight);
      const fallbackWidth = Math.max(1, Math.floor(window.innerWidth || FALLBACK_WIDTH));
      const fallbackHeight = Math.max(1, Math.floor(window.innerHeight || FALLBACK_HEIGHT));
      const requestedRows = parseCssIntegerValue(style.getPropertyValue("--list-rows")) ?? 1;
      const rowHeightFromViewport =
        measuredHeight > 0
          ? Math.floor((measuredHeight - GRID_GAP_PX * Math.max(0, requestedRows - 1)) / requestedRows)
          : null;
      const rowHeight =
        rowHeightFromViewport && rowHeightFromViewport > 0
          ? rowHeightFromViewport
          : parseCssPixelValue(style.getPropertyValue("--list-card-height")) ?? FALLBACK_CARD_HEIGHT;
      setViewport({
        width: Math.max(1, measuredWidth || fallbackWidth),
        height: Math.max(1, measuredHeight || fallbackHeight),
        rowHeight: Math.max(120, Math.floor(rowHeight)),
      });
    };

    measure();
    if (typeof ResizeObserver === "undefined") {
      window.addEventListener("resize", measure);
      return () => {
        window.removeEventListener("resize", measure);
      };
    }

    const observer = new ResizeObserver(() => {
      measure();
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  const columnCount = Math.max(1, Math.floor(columns || 1));
  const rowCount = Math.max(1, Math.ceil(items.length / columnCount));
  const columnWidth = useMemo(() => {
    if (viewport.width <= 0) return 1;
    return Math.max(
      1,
      Math.floor((viewport.width - GRID_GAP_PX * Math.max(0, columnCount - 1)) / columnCount)
    );
  }, [columnCount, viewport.width]);

  const handleItemsRendered = useCallback(
    ({ visibleRowStartIndex, visibleRowStopIndex, visibleColumnStartIndex, visibleColumnStopIndex }: GridOnItemsRenderedProps) => {
      if (!onVisibleItemsChange) return;
      const nextItems: T[] = [];
      const nextIndexes: number[] = [];
      for (let rowIndex = visibleRowStartIndex; rowIndex <= visibleRowStopIndex; rowIndex += 1) {
        for (let columnIndex = visibleColumnStartIndex; columnIndex <= visibleColumnStopIndex; columnIndex += 1) {
          const itemIndex = rowIndex * columnCount + columnIndex;
          const item = items[itemIndex];
          if (item == null) continue;
          nextItems.push(item);
          nextIndexes.push(itemIndex);
        }
      }
      onVisibleItemsChange(nextItems, nextIndexes);
    },
    [columnCount, items, onVisibleItemsChange]
  );

  const cellRenderer = useCallback(
    ({
      columnIndex,
      rowIndex,
      style,
    }: {
      columnIndex: number;
      rowIndex: number;
      style: CSSProperties;
    }) => {
      const itemIndex = rowIndex * columnCount + columnIndex;
      const item = items[itemIndex];
      if (item == null) return null;
      return (
        <div
          className="virtualized-card-grid-cell"
          style={{
            ...style,
            left: Number(style.left ?? 0) + (columnIndex > 0 ? GRID_GAP_PX : 0),
            top: Number(style.top ?? 0) + (rowIndex > 0 ? GRID_GAP_PX : 0),
            width: Math.max(1, Number(style.width ?? columnWidth) - (columnIndex > 0 ? GRID_GAP_PX : 0)),
            height: Math.max(1, Number(style.height ?? viewport.rowHeight) - GRID_GAP_PX),
          }}
        >
          {renderItem(item, itemIndex)}
        </div>
      );
    },
    [columnCount, columnWidth, items, renderItem, viewport.rowHeight]
  );

  return (
    <div className={`virtualized-card-grid${className ? ` ${className}` : ""}`} ref={containerRef}>
      {viewport.width > 0 && viewport.height > 0 && items.length > 0 ? (
        <Grid
          className="virtualized-card-grid-scroller"
          columnCount={columnCount}
          columnWidth={columnWidth}
          height={viewport.height}
          overscanRowCount={overscanRowCount}
          rowCount={rowCount}
          rowHeight={viewport.rowHeight + GRID_GAP_PX}
          width={viewport.width}
          itemKey={({ columnIndex, rowIndex }) => {
            const itemIndex = rowIndex * columnCount + columnIndex;
            const item = items[itemIndex];
            return item == null ? `empty-${rowIndex}-${columnIndex}` : itemKey(item, itemIndex);
          }}
          onItemsRendered={handleItemsRendered}
        >
          {cellRenderer}
        </Grid>
      ) : null}
    </div>
  );
}
