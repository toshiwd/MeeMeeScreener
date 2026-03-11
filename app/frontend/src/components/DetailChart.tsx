import { forwardRef, useEffect, useImperativeHandle, useLayoutEffect, useRef, useState } from "react";
import { IconTrash } from "@tabler/icons-react";
import { CrosshairMode, createChart, type Time } from "lightweight-charts";
import type { Box } from "../store";
import type { CurrentPosition, DailyPosition, TradeMarker } from "../utils/positions";
import { getBodyRangeFromCandles, getBoxFill, getBoxStroke } from "../utils/boxes";
import { computeGapBands, type GapBand } from "../utils/gapBands";
import {
  buildDrawBoxShape,
  buildPriceBandShape,
  buildTimeZoneShape,
  getHitKindsForTool
} from "../utils/drawingInteraction";
import { getDomTheme, type Theme } from "../utils/theme";
import PositionOverlay from "./PositionOverlay";

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

type MaLine = {
  key: string;
  label?: string;
  period?: number;
  color: string;
  data: { time: number; value: number }[];
  visible: boolean;
  lineWidth: number;
};

type EventMarker = {
  time: number;
  label?: string;
  kind?:
    | "earnings"
    | "decision-buy"
    | "decision-sell"
    | "decision-neutral"
    | "tdnet-positive"
    | "tdnet-negative"
    | "tdnet-neutral";
};

type ChartWithCrosshairApi = ReturnType<typeof createChart> & {
  setCrosshairPosition?: (price: number, time: Time, series: unknown) => void;
  clearCrosshairPosition?: () => void;
};

export type TimeZone = {
  side: "buy" | "sell";
  startTime: number;
  endTime: number;
  color?: string;
};

export type PriceBand = {
  topPrice: number;
  bottomPrice: number;
  opacity: number;
  lineWidth?: number;
};

export type DrawBox = {
  startTime: number;
  endTime: number;
  topPrice: number;
  bottomPrice: number;
  color?: string;
  opacity?: number;
  lineWidth?: number;
};

export type HorizontalLine = {
  price: number;
  color?: string;
  opacity?: number;
  lineWidth?: number;
};

export type DrawTool = "timeZone" | "priceBand" | "drawBox" | "horizontalLine";

export type SelectedDrawingInfo =
  | { kind: "timeZone"; startTime: number; endTime: number }
  | { kind: "priceBand"; topPrice: number; bottomPrice: number }
  | { kind: "drawBox"; startTime: number; endTime: number; topPrice: number; bottomPrice: number }
  | { kind: "horizontalLine"; price: number };

type SelectedShape =
  | { kind: "timeZone"; index: number }
  | { kind: "priceBand"; index: number }
  | { kind: "drawBox"; index: number }
  | { kind: "horizontalLine"; index: number };

type DragState = {
  kind: SelectedShape["kind"];
  index: number;
  handle: "start" | "end" | "top" | "bottom" | "tl" | "tr" | "bl" | "br" | "move";
  startTime?: number;
  endTime?: number;
  startPrice?: number;
  endPrice?: number;
  anchorTime?: number;
  anchorPrice?: number;
};

type ContextBarState = {
  open: boolean;
  x: number;
  y: number;
};

export type DetailChartHandle = {
  setVisibleRange: (range: { from: number; to: number } | null) => void;
  fitContent: () => void;
  setCrosshair: (time: number | null, point?: { x: number; y: number } | null) => void;
  clearCrosshair: () => void;
  deleteSelectedShape: () => void;
};

type DetailChartProps = {
  candles: Candle[];
  volume: VolumePoint[];
  maLines: MaLine[];
  showVolume: boolean;
  boxes: Box[];
  showBoxes: boolean;
  visibleRange?: { from: number; to: number } | null;
  positionOverlay?: {
    dailyPositions: DailyPosition[];
    tradeMarkers: TradeMarker[];
    currentPositions?: CurrentPosition[];
    latestTradeTime?: number | null;
    showOverlay: boolean;
    showPnL: boolean;
    hoverTime: number | null;
    showMarkers?: boolean;
    markerSuffix?: string;
    maLines?: MaLine[];
    hidePanel?: boolean;
  };
  eventMarkers?: EventMarker[];
  cursorTime?: number | null;
  partialTimes?: number[];
  timeZones?: TimeZone[];
  priceBands?: PriceBand[];
  drawBoxes?: DrawBox[];
  horizontalLines?: HorizontalLine[];
  showPriceBands?: boolean;
  gapBands?: GapBand[];
  drawingEnabled?: boolean;
  activeTool?: DrawTool | null;
  activeDrawColor?: string;
  activeLineOpacity?: number;
  activeLineWidth?: number;
  onSelectShape?: (info: SelectedDrawingInfo | null) => void;
  onAddTimeZone?: (zone: TimeZone) => void;
  onAddPriceBand?: (band: PriceBand) => void;
  onAddDrawBox?: (box: DrawBox) => void;
  onAddHorizontalLine?: (line: HorizontalLine) => void;
  onUpdateTimeZone?: (index: number, zone: TimeZone) => void;
  onUpdatePriceBand?: (index: number, band: PriceBand) => void;
  onUpdateDrawBox?: (index: number, box: DrawBox) => void;
  onUpdateHorizontalLine?: (index: number, line: HorizontalLine) => void;
  onDeleteTimeZone?: (index: number) => void;
  onDeletePriceBand?: (index: number) => void;
  onDeleteDrawBox?: (index: number) => void;
  onDeleteHorizontalLine?: (index: number) => void;
  onCrosshairMove?: (time: number | null, point?: { x: number; y: number } | null) => void;
  onVisibleRangeChange?: (range: { from: number; to: number } | null) => void;
  onChartClick?: (time: number | null) => void;
  theme?: "dark" | "light";
};

const DetailChart = forwardRef<DetailChartHandle, DetailChartProps>(function DetailChart(
  {
    candles,
    volume,
    maLines,
    showVolume,
    boxes,
    showBoxes,
    visibleRange,
    positionOverlay,
    eventMarkers,
    cursorTime,
    partialTimes,
    timeZones,
    priceBands,
    drawBoxes,
    horizontalLines,
    showPriceBands,
    gapBands,
    drawingEnabled,
    activeTool,
    activeDrawColor,
    activeLineOpacity,
    activeLineWidth,
    onSelectShape,
    onAddTimeZone,
    onAddPriceBand,
    onAddDrawBox,
    onAddHorizontalLine,
    onUpdateTimeZone,
    onUpdatePriceBand,
    onUpdateDrawBox,
    onUpdateHorizontalLine,
    onDeleteTimeZone,
    onDeletePriceBand,
    onDeleteDrawBox,
    onDeleteHorizontalLine,
    onCrosshairMove,
    onVisibleRangeChange,
    onChartClick,
    theme
  },
  ref
) {
  const [observedTheme, setObservedTheme] = useState<Theme>(() => getDomTheme());
  const resolvedTheme = theme ?? observedTheme;
  const isDrawingEnabled = drawingEnabled !== false;
  const containerRef = useRef<HTMLDivElement | null>(null);
  const wrapperRef = useRef<HTMLDivElement | null>(null);
  const overlayRef = useRef<HTMLCanvasElement | null>(null);
  const chartRef = useRef<ReturnType<typeof createChart> | null>(null);
  const candleSeriesRef = useRef<any>(null);
  const volumeSeriesRef = useRef<any>(null);
  const lineSeriesRef = useRef<any[]>([]);
  const [overlayTargets, setOverlayTargets] = useState<{
    candleSeries: any;
    chart: ReturnType<typeof createChart> | null;
  }>({ candleSeries: null, chart: null });
  const [selectedShapeState, setSelectedShapeState] = useState<SelectedShape | null>(null);
  const [contextBarState, setContextBarState] = useState<ContextBarState>({
    open: false,
    x: 0,
    y: 0
  });
  const dataRef = useRef({
    candles,
    volume,
    maLines,
    showVolume,
    boxes,
    showBoxes,
    cursorTime
  });
  const visibleRangeRef = useRef<DetailChartProps["visibleRange"]>(visibleRange);
  const hasAppliedVisibleRangeRef = useRef(false);
  const candlesRef = useRef<Candle[]>(candles);
  const boxesRef = useRef<Box[]>(boxes);
  const showBoxesRef = useRef(showBoxes);
  const cursorTimeRef = useRef<number | null>(cursorTime ?? null);
  const partialTimesRef = useRef<number[]>(partialTimes ?? []);
  const eventMarkersRef = useRef<DetailChartProps["eventMarkers"]>(eventMarkers ?? []);
  const timeZonesRef = useRef<TimeZone[]>(timeZones ?? []);
  const priceBandsRef = useRef<PriceBand[]>(priceBands ?? []);
  const drawBoxesRef = useRef<DrawBox[]>(drawBoxes ?? []);
  const horizontalLinesRef = useRef<HorizontalLine[]>(horizontalLines ?? []);
  const showPriceBandsRef = useRef(showPriceBands ?? false);
  const gapBandsRef = useRef<GapBand[]>(gapBands ?? []);
  const gapBandsPropRef = useRef<GapBand[] | undefined>(gapBands);
  const drawingEnabledRef = useRef<boolean>(drawingEnabled !== false);
  const draftTimeZoneRef = useRef<TimeZone | null>(null);
  const draftPriceBandRef = useRef<PriceBand | null>(null);
  const draftDrawBoxRef = useRef<DrawBox | null>(null);
  const drawModeRef = useRef<"timeZone" | "priceBand" | "drawBox" | null>(null);
  const drawStartRef = useRef<{ time?: number | null; price?: number | null }>({});
  const gapAsOfRef = useRef<number | null>(null);
  const gapSourceCandlesRef = useRef<Candle[] | null>(null);
  const activeToolRef = useRef<DrawTool | null>(activeTool ?? null);
  const prevActiveToolRef = useRef<DrawTool | null>(activeTool ?? null);
  const selectedShapeRef = useRef<SelectedShape | null>(null);
  const dragStateRef = useRef<DragState | null>(null);
  const lastDragAtRef = useRef<number>(0);
  const activeDrawColorRef = useRef<string | null>(activeDrawColor ?? null);
  const activeLineOpacityRef = useRef<number | null>(activeLineOpacity ?? null);
  const activeLineWidthRef = useRef<number | null>(activeLineWidth ?? null);
  const onSelectShapeRef = useRef<DetailChartProps["onSelectShape"]>(onSelectShape);
  const onAddTimeZoneRef = useRef<DetailChartProps["onAddTimeZone"]>(onAddTimeZone);
  const onAddPriceBandRef = useRef<DetailChartProps["onAddPriceBand"]>(onAddPriceBand);
  const onAddDrawBoxRef = useRef<DetailChartProps["onAddDrawBox"]>(onAddDrawBox);
  const onAddHorizontalLineRef = useRef<DetailChartProps["onAddHorizontalLine"]>(
    onAddHorizontalLine
  );
  const onUpdateTimeZoneRef = useRef<DetailChartProps["onUpdateTimeZone"]>(onUpdateTimeZone);
  const onUpdatePriceBandRef = useRef<DetailChartProps["onUpdatePriceBand"]>(onUpdatePriceBand);
  const onUpdateDrawBoxRef = useRef<DetailChartProps["onUpdateDrawBox"]>(onUpdateDrawBox);
  const onUpdateHorizontalLineRef = useRef<DetailChartProps["onUpdateHorizontalLine"]>(
    onUpdateHorizontalLine
  );
  const onDeleteTimeZoneRef = useRef<DetailChartProps["onDeleteTimeZone"]>(onDeleteTimeZone);
  const onDeletePriceBandRef = useRef<DetailChartProps["onDeletePriceBand"]>(onDeletePriceBand);
  const onDeleteDrawBoxRef = useRef<DetailChartProps["onDeleteDrawBox"]>(onDeleteDrawBox);
  const onDeleteHorizontalLineRef = useRef<DetailChartProps["onDeleteHorizontalLine"]>(
    onDeleteHorizontalLine
  );
  const suppressCrosshairRef = useRef(false);
  const suppressVisibleRangeUntilRef = useRef(0);
  const onCrosshairMoveRef = useRef<DetailChartProps["onCrosshairMove"]>(onCrosshairMove);
  const onVisibleRangeChangeRef = useRef<DetailChartProps["onVisibleRangeChange"]>(
    onVisibleRangeChange
  );
  const lastCrosshairTimeRef = useRef<number | null>(null);
  const resizeRafRef = useRef<number | null>(null);
  const pendingResizeRef = useRef<{ width: number; height: number; fit: boolean } | null>(null);

  const readChartColors = () => {
    const styles = getComputedStyle(document.documentElement);
    const pick = (name: string, fallback: string) => {
      const value = styles.getPropertyValue(name).trim();
      return value || fallback;
    };
    return {
      bg: pick("--theme-chart-bg", resolvedTheme === "light" ? "#ffffff" : "#0f1628"),
      text: pick("--theme-chart-text", resolvedTheme === "light" ? "#334155" : "#cbd5f5"),
      grid: pick(
        "--theme-chart-grid",
        resolvedTheme === "light" ? "#f1f5f9" : "rgba(255,255,255,0.06)"
      ),
      muted: pick("--theme-text-muted", resolvedTheme === "light" ? "#94a3b8" : "#64748b"),
      earnings: pick("--theme-event-earnings", "#ef4444"),
      decisionBuy: pick("--color-pnl-up", "#ef4444"),
      decisionSell: pick("--color-pnl-down", "#22c55e"),
      decisionNeutral: pick("--theme-text-muted", resolvedTheme === "light" ? "#64748b" : "#94a3b8"),
      gapBandFill: pick(
        "--theme-gap-band-fill",
        resolvedTheme === "light" ? "rgba(100, 116, 139, 0.10)" : "rgba(148, 163, 184, 0.12)"
      )
    };
  };

  useEffect(() => {
    const root = document.documentElement;
    const update = () => {
      const next = getDomTheme();
      setObservedTheme((prev) => (prev === next ? prev : next));
    };
    update();
    const observer = new MutationObserver(update);
    observer.observe(root, { attributes: true, attributeFilter: ["data-theme"] });
    return () => observer.disconnect();
  }, []);

  const BOX_FILL = getBoxFill();
  const BOX_STROKE = getBoxStroke();
  const CURSOR_STROKE = "rgba(148, 163, 184, 0.7)";
  const PARTIAL_STROKE = "rgba(125, 211, 252, 0.3)";
  const PARTIAL_LABEL = "";
  const BUY_ZONE_COLOR = "rgba(255, 99, 132, 0.08)";
  const SELL_ZONE_COLOR = "rgba(50, 205, 50, 0.08)";
  const PRICE_BAND_COLOR = "rgba(128, 128, 128, 0.08)";
  const PRICE_BAND_STROKE = "rgba(100, 116, 139, 0.6)";
  const DRAW_BOX_COLOR = "rgba(100, 116, 139, 0.5)";
  const DRAW_BOX_FILL = "rgba(100, 116, 139, 0.08)";
  const HORIZONTAL_LINE_COLOR = "rgba(51, 65, 85, 0.8)";
  const CONTEXT_COLOR_PALETTE = ["#ef4444", "#22c55e", "#0ea5e9", "#f59e0b", "#64748b"];
  const GAP_BAND_MAX_VISIBLE = 2;

  const buildVolumeSeriesData = (bars: Candle[], points: VolumePoint[]) => {
    if (!points.length) return [];
    const candleMap = new Map<number, Candle>();
    bars.forEach((bar) => {
      if (Number.isFinite(bar.time)) {
        candleMap.set(bar.time, bar);
      }
    });
    return points.map((point) => {
      const bar = candleMap.get(point.time);
      const isUp = bar ? bar.close >= bar.open : true;
      return {
        time: point.time,
        value: point.value,
        color: isUp ? "#ef4444" : "#22c55e"
      };
    });
  };

  const suppressVisibleRangeEvents = (ms = 80) => {
    const until = Date.now() + ms;
    if (until > suppressVisibleRangeUntilRef.current) {
      suppressVisibleRangeUntilRef.current = until;
    }
  };

  const isValidVisibleRange = (range: { from: number; to: number } | null | undefined) => {
    if (!range) return false;
    if (!Number.isFinite(range.from) || !Number.isFinite(range.to)) return false;
    return range.from <= range.to;
  };

  const normalizeRangeTime = (value: unknown) => {
    if (typeof value === "number") return value;
    if (value && typeof value === "object") {
      const data = value as { year?: number; month?: number; day?: number };
      if (data.year && data.month && data.day) {
        return Math.floor(Date.UTC(data.year, data.month - 1, data.day) / 1000);
      }
    }
    return null;
  };

  const applyAlpha = (color: string, alpha: number) => {
    if (!color) return color;
    const hexMatch = color.trim().match(/^#?([0-9a-fA-F]{6})$/);
    if (hexMatch) {
      const raw = hexMatch[1];
      const r = parseInt(raw.slice(0, 2), 16);
      const g = parseInt(raw.slice(2, 4), 16);
      const b = parseInt(raw.slice(4, 6), 16);
      return `rgba(${r}, ${g}, ${b}, ${alpha})`;
    }
    const rgbMatch = color.match(/rgba?\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)(?:\s*,\s*[\d.]+)?\s*\)/);
    if (!rgbMatch) return color;
    const r = rgbMatch[1];
    const g = rgbMatch[2];
    const b = rgbMatch[3];
    return `rgba(${r}, ${g}, ${b}, ${alpha})`;
  };

  const normalizeCoordinatePrice = (value: unknown) => {
    if (typeof value === "number" && Number.isFinite(value)) return value;
    return null;
  };

  const HANDLE_RADIUS = 8;
  const isNear = (a: number, b: number, radius = HANDLE_RADIUS) => Math.abs(a - b) <= radius;

  const resolveGapAsOf = (asOf: number | null) => {
    if (asOf == null) return null;
    const candles = candlesRef.current ?? [];
    if (!candles.length) return asOf;
    for (let i = candles.length - 1; i >= 0; i -= 1) {
      const time = candles[i]?.time;
      if (!Number.isFinite(time)) continue;
      if (time <= asOf) return time;
    }
    return Number.isFinite(candles[0]?.time) ? candles[0].time : asOf;
  };

  const updateGapBands = (asOf: number | null) => {
    if (gapBandsPropRef.current) return;
    const candleSource = candlesRef.current;
    if (!candleSource.length) {
      gapBandsRef.current = [];
      gapAsOfRef.current = null;
      gapSourceCandlesRef.current = candleSource;
      return;
    }
    const anchorAsOf = asOf ?? resolveGapAsOfFromLatestCandle();
    const resolvedAsOf = resolveGapAsOf(anchorAsOf);
    if (resolvedAsOf == null) return;
    if (gapAsOfRef.current === resolvedAsOf && gapSourceCandlesRef.current === candleSource) return;
    gapSourceCandlesRef.current = candleSource;
    gapAsOfRef.current = resolvedAsOf;
    const poolCount = Math.max(GAP_BAND_MAX_VISIBLE, candleSource.length);
    gapBandsRef.current = computeGapBands(candleSource, resolvedAsOf, poolCount);
  };

  const resolveGapAsOfFromLatestCandle = () =>
    candlesRef.current.length ? candlesRef.current[candlesRef.current.length - 1].time : null;

  const buildSelectionInfo = (selected: SelectedShape | null): SelectedDrawingInfo | null => {
    if (!selected) return null;
    if (selected.kind === "timeZone") {
      const zone = timeZonesRef.current[selected.index];
      if (!zone) return null;
      return { kind: "timeZone", startTime: zone.startTime, endTime: zone.endTime };
    }
    if (selected.kind === "priceBand") {
      const band = priceBandsRef.current[selected.index];
      if (!band) return null;
      return { kind: "priceBand", topPrice: band.topPrice, bottomPrice: band.bottomPrice };
    }
    if (selected.kind === "drawBox") {
      const box = drawBoxesRef.current[selected.index];
      if (!box) return null;
      return {
        kind: "drawBox",
        startTime: box.startTime,
        endTime: box.endTime,
        topPrice: box.topPrice,
        bottomPrice: box.bottomPrice
      };
    }
    if (selected.kind === "horizontalLine") {
      const line = horizontalLinesRef.current[selected.index];
      if (!line) return null;
      return { kind: "horizontalLine", price: line.price };
    }
    return null;
  };

  const clampContextBarPos = (x: number, y: number) => {
    const wrapper = wrapperRef.current;
    if (!wrapper) {
      return { x, y };
    }
    const minX = 8;
    const minY = 8;
    const maxX = Math.max(minX, wrapper.clientWidth - 180);
    const maxY = Math.max(minY, wrapper.clientHeight - 56);
    return {
      x: Math.min(Math.max(x, minX), maxX),
      y: Math.min(Math.max(y, minY), maxY)
    };
  };

  const closeContextBar = () => {
    setContextBarState((prev) => (prev.open ? { ...prev, open: false } : prev));
  };

  const openContextBar = (x: number, y: number) => {
    const point = clampContextBarPos(x, y);
    setContextBarState({ open: true, x: point.x, y: point.y });
  };

  const emitSelection = (
    selected: SelectedShape | null,
    options?: { contextPoint?: { x: number; y: number }; preserveContextBar?: boolean }
  ) => {
    onSelectShapeRef.current?.(buildSelectionInfo(selected));
    setSelectedShapeState(selected ? { ...selected } : null);
    if (!selected) {
      closeContextBar();
      return;
    }
    if (options?.contextPoint) {
      openContextBar(options.contextPoint.x, options.contextPoint.y);
      return;
    }
    if (!options?.preserveContextBar) {
      closeContextBar();
    }
  };

  const updateSelectedDrawColor = (color: string) => {
    const selected = selectedShapeRef.current;
    if (!selected) return;
    if (selected.kind === "drawBox") {
      const box = drawBoxesRef.current[selected.index];
      if (!box) return;
      updateDrawBoxAt(selected.index, { ...box, color });
      drawOverlay();
      return;
    }
    if (selected.kind === "timeZone") {
      const zone = timeZonesRef.current[selected.index];
      if (!zone) return;
      updateTimeZoneAt(selected.index, { ...zone, color });
      drawOverlay();
      return;
    }
    if (selected.kind === "horizontalLine") {
      const line = horizontalLinesRef.current[selected.index];
      if (!line) return;
      updateHorizontalLineAt(selected.index, { ...line, color });
      drawOverlay();
    }
  };

  const updateTimeZoneAt = (index: number, zone: TimeZone) => {
    const next = [...(timeZonesRef.current ?? [])];
    if (!next[index]) return;
    next[index] = zone;
    timeZonesRef.current = next;
    onUpdateTimeZoneRef.current?.(index, zone);
    if (selectedShapeRef.current?.kind === "timeZone" && selectedShapeRef.current.index === index) {
      emitSelection(selectedShapeRef.current);
    }
  };

  const updatePriceBandAt = (index: number, band: PriceBand) => {
    const next = [...(priceBandsRef.current ?? [])];
    if (!next[index]) return;
    next[index] = band;
    priceBandsRef.current = next;
    onUpdatePriceBandRef.current?.(index, band);
    if (selectedShapeRef.current?.kind === "priceBand" && selectedShapeRef.current.index === index) {
      emitSelection(selectedShapeRef.current);
    }
  };

  const updateDrawBoxAt = (index: number, box: DrawBox) => {
    const next = [...(drawBoxesRef.current ?? [])];
    if (!next[index]) return;
    next[index] = box;
    drawBoxesRef.current = next;
    onUpdateDrawBoxRef.current?.(index, box);
    if (selectedShapeRef.current?.kind === "drawBox" && selectedShapeRef.current.index === index) {
      emitSelection(selectedShapeRef.current);
    }
  };

  const updateHorizontalLineAt = (index: number, line: HorizontalLine) => {
    const next = [...(horizontalLinesRef.current ?? [])];
    if (!next[index]) return;
    next[index] = line;
    horizontalLinesRef.current = next;
    onUpdateHorizontalLineRef.current?.(index, line);
    if (
      selectedShapeRef.current?.kind === "horizontalLine" &&
      selectedShapeRef.current.index === index
    ) {
      emitSelection(selectedShapeRef.current);
    }
  };

  const clearDraftState = () => {
    drawModeRef.current = null;
    drawStartRef.current = {};
    draftTimeZoneRef.current = null;
    draftPriceBandRef.current = null;
    draftDrawBoxRef.current = null;
  };

  const applyDraftForTool = (
    tool: "timeZone" | "priceBand" | "drawBox",
    time: number | null,
    price: number | null
  ) => {
    if (tool === "timeZone") {
      const startTime = drawStartRef.current.time ?? null;
      const zone = buildTimeZoneShape(startTime, time, "buy");
      draftTimeZoneRef.current = zone;
      return;
    }
    if (tool === "priceBand") {
      const startPrice = drawStartRef.current.price ?? null;
      const band = buildPriceBandShape(startPrice, price, 0.12);
      draftPriceBandRef.current = band;
      return;
    }
    const startTime = drawStartRef.current.time ?? null;
    const startPrice = drawStartRef.current.price ?? null;
    const box = buildDrawBoxShape(startTime, time, startPrice, price, {
      opacity: 0.08,
      color: activeDrawColorRef.current ?? undefined
    });
    draftDrawBoxRef.current = box;
  };

  const commitDraftForTool = (
    tool: "timeZone" | "priceBand" | "drawBox",
    time: number | null,
    price: number | null
  ) => {
    if (tool === "timeZone") {
      const zone = buildTimeZoneShape(
        drawStartRef.current.time ?? null,
        time,
        "buy",
        activeDrawColorRef.current ?? undefined
      );
      if (zone) onAddTimeZoneRef.current?.(zone);
      return;
    }
    if (tool === "priceBand") {
      const band = buildPriceBandShape(drawStartRef.current.price ?? null, price, 0.12);
      if (band) onAddPriceBandRef.current?.(band);
      return;
    }
    const box = buildDrawBoxShape(
      drawStartRef.current.time ?? null,
      time,
      drawStartRef.current.price ?? null,
      price,
      {
        opacity: 0.08,
        color: activeDrawColorRef.current ?? undefined
      }
    );
    if (box) onAddDrawBoxRef.current?.(box);
  };

  const formatChartDate = (value: any) => {
    if (!value) return "";
    if (typeof value === "number") {
      const date = new Date(value * 1000);
      if (Number.isNaN(date.getTime())) return "";
      const yy = String(date.getUTCFullYear() % 100).padStart(2, "0");
      const mm = String(date.getUTCMonth() + 1).padStart(2, "0");
      const dd = String(date.getUTCDate()).padStart(2, "0");
      return `${yy}/${mm}/${dd}`;
    }
    if (typeof value === "object") {
      const data = value as { year?: number; month?: number; day?: number };
      if (data.year && data.month && data.day) {
        const yy = String(data.year % 100).padStart(2, "0");
        const mm = String(data.month).padStart(2, "0");
        const dd = String(data.day).padStart(2, "0");
        return `${yy}/${mm}/${dd}`;
      }
    }
    return "";
  };

  const findNearestCandle = (time: number) => {
    const items = candlesRef.current;
    if (!items.length) return null;
    let left = 0;
    let right = items.length - 1;
    while (left <= right) {
      const mid = Math.floor((left + right) / 2);
      const midTime = items[mid].time;
      if (midTime === time) return items[mid];
      if (midTime < time) {
        left = mid + 1;
      } else {
        right = mid - 1;
      }
    }
    const lower = items[Math.max(0, Math.min(items.length - 1, right))];
    const upper = items[Math.max(0, Math.min(items.length - 1, left))];
    if (!lower) return upper;
    if (!upper) return lower;
    return Math.abs(time - lower.time) <= Math.abs(upper.time - time) ? lower : upper;
  };

  const hitTestShape = (
    x: number,
    y: number,
    time: number | null,
    price: number | null,
    effectiveTool: DrawTool | null
  ): DragState | null => {
    const chart = chartRef.current;
    const series = candleSeriesRef.current;
    if (!chart || !series) return null;
    const timeScale = chart.timeScale();
    if (
      typeof timeScale.timeToCoordinate !== "function" ||
      typeof series.priceToCoordinate !== "function"
    ) {
      return null;
    }

    const hitHorizontal = () => {
      const lines = horizontalLinesRef.current ?? [];
      for (let index = lines.length - 1; index >= 0; index -= 1) {
        const line = lines[index];
        const lineY = series.priceToCoordinate(line.price);
        if (lineY == null) continue;
        if (isNear(y, lineY)) {
          return {
            kind: "horizontalLine" as const,
            index,
            handle: "move" as const,
            startPrice: line.price,
            anchorPrice: price ?? line.price
          };
        }
      }
      return null;
    };

    const hitDrawBox = () => {
      const boxes = drawBoxesRef.current ?? [];
      for (let index = boxes.length - 1; index >= 0; index -= 1) {
        const box = boxes[index];
        const x1 = timeScale.timeToCoordinate(box.startTime as Time);
        const x2 = timeScale.timeToCoordinate(box.endTime as Time);
        const y1 = series.priceToCoordinate(box.topPrice);
        const y2 = series.priceToCoordinate(box.bottomPrice);
        if (x1 == null || x2 == null || y1 == null || y2 == null) continue;
        const left = Math.min(x1, x2);
        const right = Math.max(x1, x2);
        const top = Math.min(y1, y2);
        const bottom = Math.max(y1, y2);
        if (isNear(x, left) && isNear(y, top)) {
          return {
            kind: "drawBox" as const,
            index,
            handle: "tl" as const,
            startTime: box.startTime,
            endTime: box.endTime,
            startPrice: box.topPrice,
            endPrice: box.bottomPrice,
            anchorTime: time ?? box.startTime,
            anchorPrice: price ?? box.topPrice
          };
        }
        if (isNear(x, right) && isNear(y, top)) {
          return {
            kind: "drawBox" as const,
            index,
            handle: "tr" as const,
            startTime: box.startTime,
            endTime: box.endTime,
            startPrice: box.topPrice,
            endPrice: box.bottomPrice,
            anchorTime: time ?? box.endTime,
            anchorPrice: price ?? box.topPrice
          };
        }
        if (isNear(x, left) && isNear(y, bottom)) {
          return {
            kind: "drawBox" as const,
            index,
            handle: "bl" as const,
            startTime: box.startTime,
            endTime: box.endTime,
            startPrice: box.topPrice,
            endPrice: box.bottomPrice,
            anchorTime: time ?? box.startTime,
            anchorPrice: price ?? box.bottomPrice
          };
        }
        if (isNear(x, right) && isNear(y, bottom)) {
          return {
            kind: "drawBox" as const,
            index,
            handle: "br" as const,
            startTime: box.startTime,
            endTime: box.endTime,
            startPrice: box.topPrice,
            endPrice: box.bottomPrice,
            anchorTime: time ?? box.endTime,
            anchorPrice: price ?? box.bottomPrice
          };
        }
        if (x >= left && x <= right && y >= top && y <= bottom) {
          return {
            kind: "drawBox" as const,
            index,
            handle: "move" as const,
            startTime: box.startTime,
            endTime: box.endTime,
            startPrice: box.topPrice,
            endPrice: box.bottomPrice,
            anchorTime: time ?? box.startTime,
            anchorPrice: price ?? box.topPrice
          };
        }
      }
      return null;
    };

    const hitPriceBand = () => {
      const bands = priceBandsRef.current ?? [];
      for (let index = bands.length - 1; index >= 0; index -= 1) {
        const band = bands[index];
        const y1 = series.priceToCoordinate(band.topPrice);
        const y2 = series.priceToCoordinate(band.bottomPrice);
        if (y1 == null || y2 == null) continue;
        const top = Math.min(y1, y2);
        const bottom = Math.max(y1, y2);
        if (isNear(y, top)) {
          return {
            kind: "priceBand" as const,
            index,
            handle: "top" as const,
            startPrice: band.topPrice,
            endPrice: band.bottomPrice,
            anchorPrice: price ?? band.topPrice
          };
        }
        if (isNear(y, bottom)) {
          return {
            kind: "priceBand" as const,
            index,
            handle: "bottom" as const,
            startPrice: band.topPrice,
            endPrice: band.bottomPrice,
            anchorPrice: price ?? band.bottomPrice
          };
        }
        if (y >= top && y <= bottom) {
          return {
            kind: "priceBand" as const,
            index,
            handle: "move" as const,
            startPrice: band.topPrice,
            endPrice: band.bottomPrice,
            anchorPrice: price ?? band.topPrice
          };
        }
      }
      return null;
    };

    const hitTimeZone = () => {
      const zones = timeZonesRef.current ?? [];
      for (let index = zones.length - 1; index >= 0; index -= 1) {
        const zone = zones[index];
        const x1 = timeScale.timeToCoordinate(zone.startTime as Time);
        const x2 = timeScale.timeToCoordinate(zone.endTime as Time);
        if (x1 == null || x2 == null) continue;
        const left = Math.min(x1, x2);
        const right = Math.max(x1, x2);
        if (isNear(x, left)) {
          return {
            kind: "timeZone" as const,
            index,
            handle: "start" as const,
            startTime: zone.startTime,
            endTime: zone.endTime,
            anchorTime: time ?? zone.startTime
          };
        }
        if (isNear(x, right)) {
          return {
            kind: "timeZone" as const,
            index,
            handle: "end" as const,
            startTime: zone.startTime,
            endTime: zone.endTime,
            anchorTime: time ?? zone.endTime
          };
        }
        if (x >= left && x <= right) {
          return {
            kind: "timeZone" as const,
            index,
            handle: "move" as const,
            startTime: zone.startTime,
            endTime: zone.endTime,
            anchorTime: time ?? zone.startTime
          };
        }
      }
      return null;
    };

    const allowedKinds = getHitKindsForTool(effectiveTool);
    for (const kind of allowedKinds) {
      if (kind === "horizontalLine") {
        const hit = hitHorizontal();
        if (hit) return hit;
      }
      if (kind === "drawBox") {
        const hit = hitDrawBox();
        if (hit) return hit;
      }
      if (kind === "priceBand") {
        const hit = hitPriceBand();
        if (hit) return hit;
      }
      if (kind === "timeZone") {
        const hit = hitTimeZone();
        if (hit) return hit;
      }
    }
    return null;
  };

  const drawOverlay = () => {
    const canvas = overlayRef.current;
    const chart = chartRef.current;
    if (!canvas || !chart) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const width = canvas.clientWidth;
    const height = canvas.clientHeight;
    ctx.clearRect(0, 0, width, height);

    if (showBoxesRef.current) {
      const boxesToDraw = boxesRef.current;
      if (boxesToDraw.length) {
        const timeScale = chart.timeScale();
        const series = candleSeriesRef.current;
        if (
          typeof timeScale.timeToCoordinate === "function" &&
          typeof series?.priceToCoordinate === "function"
        ) {
          ctx.fillStyle = BOX_FILL;
          ctx.strokeStyle = BOX_STROKE;
          ctx.lineWidth = 1;

          boxesToDraw.forEach((box) => {
            const x1 = timeScale.timeToCoordinate(box.startTime as Time);
            const x2 = timeScale.timeToCoordinate(box.endTime as Time);
            const bodyRange = getBodyRangeFromCandles(candlesRef.current, box.startTime, box.endTime);
            const upper = bodyRange?.upper ?? box.upper;
            const lower = bodyRange?.lower ?? box.lower;
            if (!Number.isFinite(upper) || !Number.isFinite(lower)) return;
            const y1 = series.priceToCoordinate(upper);
            const y2 = series.priceToCoordinate(lower);
            if (x1 == null || x2 == null || y1 == null || y2 == null) return;
            const rectX = Math.min(x1, x2);
            const rectY = Math.min(y1, y2);
            const rectW = Math.max(1, Math.abs(x2 - x1));
            const rectH = Math.max(1, Math.abs(y2 - y1));
            ctx.fillRect(rectX, rectY, rectW, rectH);
            ctx.strokeRect(rectX, rectY, rectW, rectH);
          });
        }
      }
    }

    const timeScale = chart.timeScale();
    const series = candleSeriesRef.current;

    const timeZonesToDraw = timeZonesRef.current ?? [];
    if (timeZonesToDraw.length && typeof timeScale.timeToCoordinate === "function") {
      timeZonesToDraw.forEach((zone) => {
        if (!Number.isFinite(zone.startTime) || !Number.isFinite(zone.endTime)) return;
        const x1 = timeScale.timeToCoordinate(zone.startTime as Time);
        const x2 = timeScale.timeToCoordinate(zone.endTime as Time);
        if (x1 == null || x2 == null) return;
        const rectX = Math.min(x1, x2);
        const rectW = Math.max(1, Math.abs(x2 - x1));
        const baseColor = zone.side === "sell" ? SELL_ZONE_COLOR : BUY_ZONE_COLOR;
        ctx.fillStyle = zone.color ? applyAlpha(zone.color, 0.2) : baseColor;
        ctx.fillRect(rectX, 0, rectW, height);

        // Draw Candle Count
        const start = Math.min(zone.startTime, zone.endTime);
        const end = Math.max(zone.startTime, zone.endTime);
        const candleData = candlesRef.current;
        let count = 0;

        // Find indices using binary search or simple bounds check if sorted
        if (candleData.length > 0) {
          // Providing a rough count based on time range might be inaccurate due to gaps.
          // Better to find exact visible candles within range.
          // Since candles are sorted by time:
          let startIndex = -1;
          let endIndex = -1;

          // Simple binary search for start
          let l = 0, r = candleData.length - 1;
          while (l <= r) {
            const m = (l + r) >>> 1;
            if (candleData[m].time >= start) {
              startIndex = m;
              r = m - 1;
            } else {
              l = m + 1;
            }
          }

          // Simple binary search for end
          l = 0; r = candleData.length - 1;
          while (l <= r) {
            const m = (l + r) >>> 1;
            if (candleData[m].time <= end) {
              endIndex = m;
              l = m + 1;
            } else {
              r = m - 1;
            }
          }

          if (startIndex !== -1 && endIndex !== -1 && startIndex <= endIndex) {
            count = endIndex - startIndex + 1;
          }
        }

        if (count > 0) {
          ctx.save();
          ctx.font = "bold 12px sans-serif";
          ctx.fillStyle = resolvedTheme === "light" ? "#334155" : "#cbd5f5";
          ctx.textAlign = "center";
          ctx.textBaseline = "top";
          ctx.shadowColor = resolvedTheme === "light" ? "white" : "black";
          ctx.shadowBlur = 4;
          ctx.fillText(`${count}本`, rectX + rectW / 2, 8);
          ctx.restore();
        }
      });
    }

    if (showPriceBandsRef.current && typeof series?.priceToCoordinate === "function") {
      const bandsToDraw = priceBandsRef.current ?? [];
      if (bandsToDraw.length) {
        ctx.strokeStyle = PRICE_BAND_STROKE;
        bandsToDraw.forEach((band) => {
          if (!Number.isFinite(band.topPrice) || !Number.isFinite(band.bottomPrice)) return;
          const y1 = series.priceToCoordinate(band.topPrice);
          const y2 = series.priceToCoordinate(band.bottomPrice);
          if (y1 == null || y2 == null) return;
          const rectY = Math.min(y1, y2);
          const rectH = Math.max(1, Math.abs(y2 - y1));
          const opacity = Number.isFinite(band.opacity) ? band.opacity : 0.12;
          const lineWidth = Number.isFinite(band.lineWidth) ? band.lineWidth : 1;
          ctx.lineWidth = lineWidth;
          ctx.strokeStyle = PRICE_BAND_STROKE;
          ctx.fillStyle = applyAlpha(PRICE_BAND_COLOR, opacity);
          ctx.fillRect(0, rectY, width, rectH);
          ctx.strokeRect(0, rectY, width, rectH);
        });
      }
    }

    const gapsToDraw = gapBandsRef.current ?? [];
    if (gapsToDraw.length && typeof series?.priceToCoordinate === "function") {
      const colors = readChartColors();
      const visibleRange =
        typeof timeScale.getVisibleRange === "function" ? timeScale.getVisibleRange() : null;
      const visibleFrom = visibleRange ? normalizeRangeTime(visibleRange.from) : null;
      const visibleTo = visibleRange ? normalizeRangeTime(visibleRange.to) : null;
      const sortedByRecent = [...gapsToDraw].sort((a, b) => b.createdAt - a.createdAt);
      const inViewport =
        visibleTo != null
          ? sortedByRecent.filter((gap) => Number.isFinite(gap.createdAt) && gap.createdAt <= visibleTo)
          : sortedByRecent;
      const limited = (inViewport.length ? inViewport : sortedByRecent).slice(
        0,
        GAP_BAND_MAX_VISIBLE
      );
      ctx.save();
      ctx.fillStyle = colors.gapBandFill;
      limited.forEach((gap) => {
        if (!Number.isFinite(gap.topPrice) || !Number.isFinite(gap.bottomPrice)) return;
        const y1 = series.priceToCoordinate(gap.topPrice);
        const y2 = series.priceToCoordinate(gap.bottomPrice);
        if (y1 == null || y2 == null) return;
        if (visibleTo != null && gap.createdAt > visibleTo) return;
        let xStart = timeScale.timeToCoordinate(gap.createdAt as Time);
        if (xStart == null || !Number.isFinite(xStart)) {
          if (visibleFrom != null && gap.createdAt <= visibleFrom) {
            xStart = 0;
          } else {
            return;
          }
        }
        const rectY = Math.min(y1, y2);
        const rectH = Math.max(1, Math.abs(y2 - y1));
        const rectX = Math.max(0, Math.min(width, xStart));
        const rectRight = width;
        if (rectRight <= rectX) return;
        const rectW = Math.max(1, rectRight - rectX);
        ctx.fillRect(rectX, rectY, rectW, rectH);
      });
      ctx.restore();
    }

    const drawBoxesToDraw = drawBoxesRef.current ?? [];
    if (
      drawBoxesToDraw.length &&
      typeof timeScale.timeToCoordinate === "function" &&
      typeof series?.priceToCoordinate === "function"
    ) {
      drawBoxesToDraw.forEach((box) => {
        if (
          !Number.isFinite(box.startTime) ||
          !Number.isFinite(box.endTime) ||
          !Number.isFinite(box.topPrice) ||
          !Number.isFinite(box.bottomPrice)
        ) {
          return;
        }
        const x1 = timeScale.timeToCoordinate(box.startTime as Time);
        const x2 = timeScale.timeToCoordinate(box.endTime as Time);
        const y1 = series.priceToCoordinate(box.topPrice);
        const y2 = series.priceToCoordinate(box.bottomPrice);
        if (x1 == null || x2 == null || y1 == null || y2 == null) return;
        const rectX = Math.min(x1, x2);
        const rectY = Math.min(y1, y2);
        const rectW = Math.max(1, Math.abs(x2 - x1));
        const rectH = Math.max(1, Math.abs(y2 - y1));
        const lineWidth = Number.isFinite(box.lineWidth) ? box.lineWidth! : 1;
        const opacity = Number.isFinite(box.opacity) ? box.opacity! : 0.08;
        const baseColor = box.color ?? DRAW_BOX_COLOR;
        ctx.save();
        ctx.lineWidth = lineWidth;
        ctx.strokeStyle = applyAlpha(baseColor, Math.min(0.7, opacity + 0.4));
        ctx.fillStyle = box.color ? applyAlpha(baseColor, opacity) : DRAW_BOX_FILL;
        ctx.fillRect(rectX, rectY, rectW, rectH);
        ctx.strokeRect(rectX, rectY, rectW, rectH);
        ctx.restore();
      });
    }

    const horizontalLinesToDraw = horizontalLinesRef.current ?? [];
    if (horizontalLinesToDraw.length && typeof series?.priceToCoordinate === "function") {
      horizontalLinesToDraw.forEach((line) => {
        if (!Number.isFinite(line.price)) return;
        const y = series.priceToCoordinate(line.price);
        if (y == null) return;
        const lineWidth = Number.isFinite(line.lineWidth) ? line.lineWidth! : 2;
        const opacity = Number.isFinite(line.opacity) ? line.opacity! : 0.8;
        const baseColor = line.color ?? HORIZONTAL_LINE_COLOR;
        ctx.save();
        ctx.strokeStyle = applyAlpha(baseColor, opacity);
        ctx.lineWidth = lineWidth;
        ctx.beginPath();
        ctx.moveTo(0, y);
        ctx.lineTo(width, y);
        ctx.stroke();
        ctx.restore();
      });
    }

    const draftTimeZone = draftTimeZoneRef.current;
    if (draftTimeZone && typeof timeScale.timeToCoordinate === "function") {
      if (Number.isFinite(draftTimeZone.startTime) && Number.isFinite(draftTimeZone.endTime)) {
        const x1 = timeScale.timeToCoordinate(draftTimeZone.startTime as Time);
        const x2 = timeScale.timeToCoordinate(draftTimeZone.endTime as Time);
        if (x1 != null && x2 != null) {
          const rectX = Math.min(x1, x2);
          const rectW = Math.max(1, Math.abs(x2 - x1));
          ctx.save();
          ctx.globalAlpha = 0.6;
          ctx.fillStyle =
            draftTimeZone.side === "sell" ? SELL_ZONE_COLOR : BUY_ZONE_COLOR;
          ctx.fillRect(rectX, 0, rectW, height);
          ctx.restore();
        }
      }
    }

    const draftPriceBand = draftPriceBandRef.current;
    if (draftPriceBand && typeof series?.priceToCoordinate === "function") {
      if (Number.isFinite(draftPriceBand.topPrice) && Number.isFinite(draftPriceBand.bottomPrice)) {
        const y1 = series.priceToCoordinate(draftPriceBand.topPrice);
        const y2 = series.priceToCoordinate(draftPriceBand.bottomPrice);
        if (y1 != null && y2 != null) {
          const rectY = Math.min(y1, y2);
          const rectH = Math.max(1, Math.abs(y2 - y1));
          ctx.save();
          ctx.globalAlpha = 0.6;
          ctx.strokeStyle = applyAlpha(PRICE_BAND_COLOR, 0.6);
          ctx.lineWidth = 1;
          ctx.fillStyle = applyAlpha(PRICE_BAND_COLOR, 0.1);
          ctx.fillRect(0, rectY, width, rectH);
          ctx.strokeRect(0, rectY, width, rectH);
          ctx.restore();
        }
      }
    }

    const draftDrawBox = draftDrawBoxRef.current;
    if (
      draftDrawBox &&
      typeof timeScale.timeToCoordinate === "function" &&
      typeof series?.priceToCoordinate === "function"
    ) {
      const x1 = timeScale.timeToCoordinate(draftDrawBox.startTime as Time);
      const x2 = timeScale.timeToCoordinate(draftDrawBox.endTime as Time);
      const y1 = series.priceToCoordinate(draftDrawBox.topPrice);
      const y2 = series.priceToCoordinate(draftDrawBox.bottomPrice);
      if (x1 != null && x2 != null && y1 != null && y2 != null) {
        const rectX = Math.min(x1, x2);
        const rectY = Math.min(y1, y2);
        const rectW = Math.max(1, Math.abs(x2 - x1));
        const rectH = Math.max(1, Math.abs(y2 - y1));
        ctx.save();
        ctx.globalAlpha = 0.6;
        ctx.strokeStyle = DRAW_BOX_COLOR;
        ctx.lineWidth = 1;
        ctx.fillStyle = DRAW_BOX_FILL;
        ctx.fillRect(rectX, rectY, rectW, rectH);
        ctx.strokeRect(rectX, rectY, rectW, rectH);
        ctx.restore();
      }
    }

    const selected = selectedShapeRef.current;
    if (selected && typeof timeScale.timeToCoordinate === "function" && typeof series?.priceToCoordinate === "function") {
      const drawHandle = (x: number, y: number) => {
        const size = 4;
        ctx.save();
        ctx.fillStyle = "#ffffff";
        ctx.strokeStyle = "rgba(100, 116, 139, 0.8)";
        ctx.lineWidth = 1;
        ctx.fillRect(x - size, y - size, size * 2, size * 2);
        ctx.strokeRect(x - size, y - size, size * 2, size * 2);
        ctx.restore();
      };
      if (selected.kind === "timeZone") {
        const zone = timeZonesRef.current[selected.index];
        if (zone) {
          const x1 = timeScale.timeToCoordinate(zone.startTime as Time);
          const x2 = timeScale.timeToCoordinate(zone.endTime as Time);
          if (x1 != null && x2 != null) {
            drawHandle(x1, 8);
            drawHandle(x2, 8);
          }
        }
      }
      if (selected.kind === "priceBand") {
        const band = priceBandsRef.current[selected.index];
        if (band) {
          const y1 = series.priceToCoordinate(band.topPrice);
          const y2 = series.priceToCoordinate(band.bottomPrice);
          if (y1 != null && y2 != null) {
            drawHandle(width - 8, y1);
            drawHandle(width - 8, y2);
          }
        }
      }
      if (selected.kind === "drawBox") {
        const box = drawBoxesRef.current[selected.index];
        if (box) {
          const x1 = timeScale.timeToCoordinate(box.startTime as Time);
          const x2 = timeScale.timeToCoordinate(box.endTime as Time);
          const y1 = series.priceToCoordinate(box.topPrice);
          const y2 = series.priceToCoordinate(box.bottomPrice);
          if (x1 != null && x2 != null && y1 != null && y2 != null) {
            drawHandle(x1, y1);
            drawHandle(x1, y2);
            drawHandle(x2, y1);
            drawHandle(x2, y2);
          }
        }
      }
      if (selected.kind === "horizontalLine") {
        const line = horizontalLinesRef.current[selected.index];
        if (line) {
          const y = series.priceToCoordinate(line.price);
          if (y != null) {
            drawHandle(width - 8, y);
          }
        }
      }
    }

    const eventMarkers = eventMarkersRef.current ?? [];
    if (eventMarkers.length) {
      const timeScale = chart.timeScale();
      if (typeof timeScale.timeToCoordinate === "function") {
        const colors = readChartColors();
        const markerRadius = 3;
        const decisionMarkerRadius = 3;
        const markerBaseY = Math.max(10, height - 14);
        const earningsStackIndex = new Map<number, number>();
        const decisionStackIndex = new Map<number, number>();
        const candleMap = new Map<number, Candle>();
        (candlesRef.current ?? []).forEach((candle) => {
          if (Number.isFinite(candle.time)) {
            candleMap.set(candle.time, candle);
          }
        });
        ctx.save();
        ctx.font = "9px sans-serif";
        ctx.textBaseline = "middle";
        ctx.textAlign = "left";
        eventMarkers.forEach((marker) => {
          const x = timeScale.timeToCoordinate(marker.time as Time);
          if (x == null || !Number.isFinite(x)) return;
          const markerColor =
            marker.kind === "decision-buy"
              ? colors.decisionBuy
              : marker.kind === "decision-sell"
                ? colors.decisionSell
                : marker.kind === "decision-neutral"
                  ? colors.decisionNeutral
                  : marker.kind === "tdnet-positive"
                    ? "#22c55e"
                    : marker.kind === "tdnet-negative"
                      ? "#ef4444"
                      : marker.kind === "tdnet-neutral"
                        ? "#f59e0b"
                        : colors.earnings;
          const isDecisionMarker =
            marker.kind === "decision-buy" ||
            marker.kind === "decision-sell" ||
            marker.kind === "decision-neutral";
          let markerY = markerBaseY;
          if (isDecisionMarker && typeof series?.priceToCoordinate === "function") {
            const candle = candleMap.get(marker.time);
            const highY = candle ? series.priceToCoordinate(candle.high) : null;
            if (highY != null && Number.isFinite(highY)) {
              const stackCount = decisionStackIndex.get(marker.time) ?? 0;
              decisionStackIndex.set(marker.time, stackCount + 1);
              markerY = Math.max(
                8,
                highY - (decisionMarkerRadius + 6) - stackCount * (decisionMarkerRadius * 2 + 3)
              );
            } else {
              const stackCount = earningsStackIndex.get(marker.time) ?? 0;
              earningsStackIndex.set(marker.time, stackCount + 1);
              markerY = Math.max(8, markerBaseY - stackCount * 8);
            }
          } else {
            const stackCount = earningsStackIndex.get(marker.time) ?? 0;
            earningsStackIndex.set(marker.time, stackCount + 1);
            markerY = Math.max(8, markerBaseY - stackCount * 8);
          }
          const radius = isDecisionMarker ? decisionMarkerRadius : markerRadius;
          ctx.globalAlpha = 0.45;
          ctx.fillStyle = markerColor;
          ctx.strokeStyle = markerColor;
          ctx.lineWidth = 1;
          ctx.beginPath();
          ctx.arc(x, markerY, radius, 0, Math.PI * 2);
          ctx.fill();
          ctx.globalAlpha = 0.65;
          ctx.stroke();
          if (!isDecisionMarker) {
            ctx.globalAlpha = 0.6;
            ctx.fillStyle = colors.muted;
            ctx.fillText(marker.label ?? "E", x + 6, markerY);
          }
        });
        ctx.restore();
      }
    }

    const cursorValue = cursorTimeRef.current;
    if (cursorValue != null) {
      const timeScale = chart.timeScale();
      const x = typeof timeScale.timeToCoordinate === "function"
        ? timeScale.timeToCoordinate(cursorValue as Time)
        : null;
      if (x != null) {
        ctx.strokeStyle = CURSOR_STROKE;
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(x, 0);
        ctx.lineTo(x, height);
        ctx.stroke();
      }
    }

    const partialTimes = partialTimesRef.current;
    if (partialTimes.length) {
      const timeScale = chart.timeScale();
      if (typeof timeScale.timeToCoordinate === "function") {
        ctx.save();
        ctx.strokeStyle = PARTIAL_STROKE;
        ctx.setLineDash([3, 5]);
        ctx.lineWidth = 0.8;
        ctx.font = "11px sans-serif";
        partialTimes.forEach((time) => {
          const x = timeScale.timeToCoordinate(time as Time);
          if (x == null) return;
          ctx.beginPath();
          ctx.moveTo(x, 0);
          ctx.lineTo(x, height);
          ctx.stroke();
          if (PARTIAL_LABEL) {
            const labelWidth = ctx.measureText(PARTIAL_LABEL).width;
            const labelX = Math.max(4, x - labelWidth / 2 - 4);
            const labelY = 6;
            ctx.fillStyle = "rgba(15, 23, 42, 0.8)";
            ctx.fillRect(labelX, labelY, labelWidth + 8, 14);
            ctx.fillStyle = PARTIAL_STROKE;
            ctx.fillText(PARTIAL_LABEL, labelX + 4, labelY + 11);
          }
        });
        ctx.restore();
      }
    }
  };

  const resizeOverlay = () => {
    const wrapper = wrapperRef.current;
    const canvas = overlayRef.current;
    if (!wrapper || !canvas) return;
    const width = Math.floor(wrapper.clientWidth);
    const height = Math.floor(wrapper.clientHeight);
    const ratio = window.devicePixelRatio || 1;
    canvas.width = Math.floor(width * ratio);
    canvas.height = Math.floor(height * ratio);
    canvas.style.width = `${width}px`;
    canvas.style.height = `${height}px`;
    const ctx = canvas.getContext("2d");
    if (ctx) {
      ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
    }
    drawOverlay();
  };

  const scheduleResizeAndFit = (width: number, height: number, fit: boolean) => {
    if (width <= 0 || height <= 0) return;
    const pending = pendingResizeRef.current;
    pendingResizeRef.current = { width, height, fit: pending?.fit || fit };
    if (resizeRafRef.current !== null) return;
    resizeRafRef.current = window.requestAnimationFrame(() => {
      resizeRafRef.current = null;
      const next = pendingResizeRef.current;
      pendingResizeRef.current = null;
      const chart = chartRef.current;
      if (!chart || !next) return;
      if (next.width <= 0 || next.height <= 0) return;
      chart.applyOptions({ width: next.width, height: next.height });
      resizeOverlay();
      if (next.fit && !visibleRangeRef.current) {
        suppressVisibleRangeEvents();
        chart.timeScale().fitContent();
      }
    });
  };

  const syncLineSeries = (nextLines: MaLine[]) => {
    const chart = chartRef.current;
    if (!chart) return;
    const current = lineSeriesRef.current;
    if (current.length > nextLines.length) {
      for (let index = nextLines.length; index < current.length; index += 1) {
        chart.removeSeries(current[index]);
      }
      current.length = nextLines.length;
    }
    if (current.length < nextLines.length) {
      for (let index = current.length; index < nextLines.length; index += 1) {
        const line = nextLines[index];
        current.push(
          chart.addLineSeries({
            color: line.color,
            lineWidth: line.lineWidth,
            priceLineVisible: false,
            crosshairMarkerVisible: false
          })
        );
      }
    }
  };

  const applyData = (next: typeof dataRef.current) => {
    const chart = chartRef.current;
    if (chart) {
      chart.applyOptions({
        rightPriceScale: {
          scaleMargins: { top: 0.08, bottom: next.showVolume ? 0.25 : 0.12 }
        }
      });
      chart.priceScale("volume").applyOptions({
        scaleMargins: { top: next.showVolume ? 0.82 : 1, bottom: 0 }
      });
      syncLineSeries(next.maLines);
    }
    if (candleSeriesRef.current) {
      candleSeriesRef.current.setData(next.candles);
    }
    if (volumeSeriesRef.current) {
      const volumeData = next.showVolume ? buildVolumeSeriesData(next.candles, next.volume) : [];
      volumeSeriesRef.current.setData(volumeData);
      volumeSeriesRef.current.applyOptions({ visible: next.showVolume });
    }
    next.maLines.forEach((line, index) => {
      const series = lineSeriesRef.current[index];
      if (!series) return;
      series.applyOptions({
        color: line.color,
        visible: line.visible,
        lineWidth: line.lineWidth,
        crosshairMarkerVisible: false
      });
      series.setData(line.data);
    });
    if (chart && next.candles.length) {
      const wrapper = wrapperRef.current;
      if (wrapper) {
        scheduleResizeAndFit(
          Math.floor(wrapper.clientWidth),
          Math.floor(wrapper.clientHeight),
          !visibleRangeRef.current
        );

      }
    }
    drawOverlay();
  };

  const deleteSelectedShape = () => {
    const selected = selectedShapeRef.current;
    if (!selected) return;
    if (selected.kind === "timeZone") {
      onDeleteTimeZoneRef.current?.(selected.index);
    }
    if (selected.kind === "priceBand") {
      onDeletePriceBandRef.current?.(selected.index);
    }
    if (selected.kind === "drawBox") {
      onDeleteDrawBoxRef.current?.(selected.index);
    }
    if (selected.kind === "horizontalLine") {
      onDeleteHorizontalLineRef.current?.(selected.index);
    }
    selectedShapeRef.current = null;
    dragStateRef.current = null;
    emitSelection(null);
    drawOverlay();
  };

  useImperativeHandle(ref, () => ({
    setVisibleRange: (range) => {
      const chart = chartRef.current;
      if (!chart) return;
      if (!candlesRef.current.length) return;
      if (!isValidVisibleRange(range)) {
        suppressVisibleRangeEvents();
        chart.timeScale().fitContent();
        return;
      }
      suppressVisibleRangeEvents();
      chart.timeScale().setVisibleRange(range);
    },
    fitContent: () => {
      chartRef.current?.timeScale().fitContent();
    },
    setCrosshair: (time, point) => {
      const chart = chartRef.current as ChartWithCrosshairApi | null;
      const series = candleSeriesRef.current;
      if (!chart || !series) return;
      const clearCrosshair = chart.clearCrosshairPosition;
      if (time == null) {
        lastCrosshairTimeRef.current = null;
        if (typeof clearCrosshair === "function") {
          suppressCrosshairRef.current = true;
          clearCrosshair.call(chart);
        }
        return;
      }
      const setCrosshairPosition = chart.setCrosshairPosition;
      if (typeof setCrosshairPosition === "function") {
        if (!point || !Number.isFinite(point.y)) return;
        const priceScale = chart.priceScale?.("right");
        const height = wrapperRef.current?.clientHeight ?? null;
        let y = point.y;
        if (height != null) {
          y = Math.max(0, Math.min(height, y));
        }
        if (!priceScale || typeof priceScale.coordinateToPrice !== "function") return;
        const mapped = priceScale.coordinateToPrice(y);
        if (mapped == null || !Number.isFinite(mapped)) return;
        lastCrosshairTimeRef.current = time;
        suppressCrosshairRef.current = true;
        setCrosshairPosition.call(chart, mapped, time, series);
      }
    },
    clearCrosshair: () => {
      const chart = chartRef.current as ChartWithCrosshairApi | null;
      if (!chart) return;
      lastCrosshairTimeRef.current = null;
      if (typeof chart.clearCrosshairPosition === "function") {
        suppressCrosshairRef.current = true;
        chart.clearCrosshairPosition();
      }
    },
    deleteSelectedShape
  }));

  useEffect(() => {
    dataRef.current = { candles, volume, maLines, showVolume, boxes, showBoxes, cursorTime };
    candlesRef.current = candles;
    boxesRef.current = boxes;
    showBoxesRef.current = showBoxes;
    cursorTimeRef.current = cursorTime ?? null;
    partialTimesRef.current = partialTimes ?? [];
    eventMarkersRef.current = eventMarkers ?? [];
    if (!gapBandsPropRef.current) {
      updateGapBands(resolveGapAsOfFromLatestCandle());
    }
    applyData(dataRef.current);
  }, [
    candles,
    volume,
    maLines,
    showVolume,
    boxes,
    showBoxes,
    cursorTime,
    partialTimes,
    eventMarkers
  ]);

  useEffect(() => {
    timeZonesRef.current = timeZones ?? [];
    priceBandsRef.current = priceBands ?? [];
    drawBoxesRef.current = drawBoxes ?? [];
    horizontalLinesRef.current = horizontalLines ?? [];
    showPriceBandsRef.current = showPriceBands ?? false;
    onAddTimeZoneRef.current = onAddTimeZone;
    onAddPriceBandRef.current = onAddPriceBand;
    onAddDrawBoxRef.current = onAddDrawBox;
    onAddHorizontalLineRef.current = onAddHorizontalLine;
    onUpdateTimeZoneRef.current = onUpdateTimeZone;
    onUpdatePriceBandRef.current = onUpdatePriceBand;
    onUpdateDrawBoxRef.current = onUpdateDrawBox;
    onUpdateHorizontalLineRef.current = onUpdateHorizontalLine;
    onDeleteTimeZoneRef.current = onDeleteTimeZone;
    onDeletePriceBandRef.current = onDeletePriceBand;
    onDeleteDrawBoxRef.current = onDeleteDrawBox;
    onDeleteHorizontalLineRef.current = onDeleteHorizontalLine;
    onSelectShapeRef.current = onSelectShape;
    gapBandsPropRef.current = gapBands;
    const nextTool = activeTool ?? null;
    activeToolRef.current = nextTool;
    const nextDrawingEnabled = drawingEnabled !== false;
    drawingEnabledRef.current = nextDrawingEnabled;
    activeDrawColorRef.current = activeDrawColor ?? null;
    activeLineOpacityRef.current =
      typeof activeLineOpacity === "number" ? activeLineOpacity : null;
    activeLineWidthRef.current =
      typeof activeLineWidth === "number" ? activeLineWidth : null;
    if (prevActiveToolRef.current !== nextTool) {
      prevActiveToolRef.current = nextTool;
      clearDraftState();
      selectedShapeRef.current = null;
      dragStateRef.current = null;
      emitSelection(null);
    }
    if (gapBands) {
      gapBandsRef.current = gapBands;
    } else {
      updateGapBands(resolveGapAsOfFromLatestCandle());
    }
    drawOverlay();
  }, [
    timeZones,
    priceBands,
    drawBoxes,
    horizontalLines,
    showPriceBands,
    gapBands,
    drawingEnabled,
    activeTool,
    activeDrawColor,
    activeLineOpacity,
    activeLineWidth,
    onAddTimeZone,
    onAddPriceBand,
    onAddDrawBox,
    onAddHorizontalLine,
    onUpdateTimeZone,
    onUpdatePriceBand,
    onUpdateDrawBox,
    onUpdateHorizontalLine,
    onDeleteTimeZone,
    onDeletePriceBand,
    onDeleteDrawBox,
    onDeleteHorizontalLine
  ]);

  useEffect(() => {
    visibleRangeRef.current = visibleRange ?? null;
    const chart = chartRef.current;
    if (!chart) return;
    if (!candlesRef.current.length) return;
    if (!isValidVisibleRange(visibleRange)) {
      if (hasAppliedVisibleRangeRef.current) {
        return;
      }
      suppressVisibleRangeEvents();
      chart.timeScale().fitContent();
      return;
    }
    hasAppliedVisibleRangeRef.current = true;
    suppressVisibleRangeEvents();
    chart.timeScale().setVisibleRange(visibleRange);
  }, [visibleRange]);

  useEffect(() => {
    if (gapBands) return;
    updateGapBands(resolveGapAsOfFromLatestCandle());
    drawOverlay();
  }, [candles, gapBands]);

  useEffect(() => {
    onCrosshairMoveRef.current = onCrosshairMove;
  }, [onCrosshairMove]);

  useEffect(() => {
    onVisibleRangeChangeRef.current = onVisibleRangeChange;
  }, [onVisibleRangeChange]);

  useEffect(() => {
    activeDrawColorRef.current = activeDrawColor ?? null;
    activeLineOpacityRef.current =
      typeof activeLineOpacity === "number" ? activeLineOpacity : null;
    activeLineWidthRef.current =
      typeof activeLineWidth === "number" ? activeLineWidth : null;
    const selected = selectedShapeRef.current;
    if (!selected) return;
    if (selected.kind === "horizontalLine") {
      const line = horizontalLinesRef.current[selected.index];
      if (!line) return;
      const nextOpacity =
        typeof activeLineOpacity === "number" ? activeLineOpacity : line.opacity;
      const nextWidth =
        typeof activeLineWidth === "number" ? activeLineWidth : line.lineWidth;
      const nextColor = activeDrawColor ?? line.color;
      updateHorizontalLineAt(selected.index, {
        ...line,
        opacity: nextOpacity,
        lineWidth: nextWidth,
        color: nextColor
      });
      drawOverlay();
      return;
    }
    if (!activeDrawColor) return;
    if (selected.kind === "drawBox") {
      const box = drawBoxesRef.current[selected.index];
      if (!box) return;
      updateDrawBoxAt(selected.index, { ...box, color: activeDrawColor });
      drawOverlay();
    }
  }, [activeDrawColor, activeLineOpacity, activeLineWidth]);

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement | null;
      if (target && ["INPUT", "TEXTAREA", "SELECT"].includes(target.tagName)) {
        return;
      }
      if (event.key === "Escape") {
        clearDraftState();
        closeContextBar();
        drawOverlay();
        return;
      }
      if (event.key !== "Delete" && event.key !== "Backspace") return;
      deleteSelectedShape();
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, []);

  useEffect(() => {
    if (!contextBarState.open) return;
    const handleDocumentMouseDown = (event: MouseEvent) => {
      const wrapper = wrapperRef.current;
      if (!wrapper) return;
      const target = event.target as Node | null;
      if (target && wrapper.contains(target)) return;
      closeContextBar();
    };
    document.addEventListener("mousedown", handleDocumentMouseDown);
    return () => document.removeEventListener("mousedown", handleDocumentMouseDown);
  }, [contextBarState.open]);

  useLayoutEffect(() => {
    const chart = chartRef.current;
    if (!chart) return;
    const colors = readChartColors();
    chart.applyOptions({
      layout: {
        background: { color: colors.bg },
        textColor: colors.text
      },
      grid: {
        vertLines: { color: colors.grid },
        horzLines: { color: colors.grid }
      }
    });
  }, [resolvedTheme]);

  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) return;
    chart.applyOptions({
      handleScroll: !isDrawingEnabled,
      handleScale: !isDrawingEnabled
    });
  }, [isDrawingEnabled]);

  useLayoutEffect(() => {
    if (!containerRef.current || chartRef.current) return;
    const element = containerRef.current;
    let resizeObserver: ResizeObserver | null = null;

    const baseColors = readChartColors();

    let teardown: (() => void) | null = null;

    const init = (width: number, height: number) => {
      if (chartRef.current) return;
      console.debug("[DetailChart] init", { width, height });

      const chart = createChart(element, {
        height,
        width,
        layout: {
          background: { color: baseColors.bg },
          textColor: baseColors.text,
          attributionLogo: false
        },
        localization: {
          locale: "ja-JP",
          timeFormatter: formatChartDate
        },
        grid: {
          vertLines: { color: baseColors.grid },
          horzLines: { color: baseColors.grid }
        },
        crosshair: {
          mode: CrosshairMode.Normal
        },
        handleScroll: !drawingEnabledRef.current,
        handleScale: !drawingEnabledRef.current,
        rightPriceScale: {
          visible: true,
          borderVisible: false,
          scaleMargins: { top: 0.08, bottom: 0.25 }
        },
        timeScale: {
          borderVisible: false,
          tickMarkFormatter: formatChartDate
        }
      });

      const candleSeries = chart.addCandlestickSeries({
        upColor: "#ef4444",
        downColor: "#22c55e",
        borderVisible: false,
        wickUpColor: "#ef4444",
        wickDownColor: "#22c55e"
      });

      const volumeSeries = chart.addHistogramSeries({
        priceScaleId: "volume",
        color: "rgba(148, 163, 184, 0.45)",
        priceFormat: { type: "volume" },
        lastValueVisible: false
      });

      chart.priceScale("volume").applyOptions({
        scaleMargins: { top: 0.82, bottom: 0 },
        visible: false,
        borderVisible: false
      });

      const lineSeries = maLines.map((line) =>
        chart.addLineSeries({
          color: line.color,
          lineWidth: line.lineWidth,
          priceLineVisible: false,
          crosshairMarkerVisible: false
        })
      );

      const crosshairHandler = (param: any) => {
        if (suppressCrosshairRef.current) {
          suppressCrosshairRef.current = false;
          return;
        }
        if (!onCrosshairMoveRef.current) return;
        const point =
          param && param.point && Number.isFinite(param.point.x) && Number.isFinite(param.point.y)
            ? { x: param.point.x, y: param.point.y }
            : null;
        if (!param || !param.point) {
          lastCrosshairTimeRef.current = null;
          onCrosshairMoveRef.current(null, point);
          return;
        }
        const normalizedTime = normalizeRangeTime(param.time);
        if (normalizedTime == null) {
          const fallbackTime = lastCrosshairTimeRef.current;
          if (fallbackTime != null) {
            onCrosshairMoveRef.current(fallbackTime, point);
          } else {
            onCrosshairMoveRef.current(null, point);
          }
          return;
        }
        lastCrosshairTimeRef.current = normalizedTime;
        onCrosshairMoveRef.current(normalizedTime, point);
      };

      chart.subscribeCrosshairMove(crosshairHandler);
      const timeScale = chart.timeScale();
      const priceScale = chart.priceScale("right");
      const rangeHandler = () => {
        if (!gapBandsPropRef.current) {
          updateGapBands(resolveGapAsOfFromLatestCandle());
        }
        drawOverlay();
        if (Date.now() < suppressVisibleRangeUntilRef.current) return;
        const handler = onVisibleRangeChangeRef.current;
        if (!handler || typeof timeScale.getVisibleRange !== "function") return;
        const range = timeScale.getVisibleRange();
        if (!range) {
          handler(null);
          return;
        }
        const from = normalizeRangeTime(range.from);
        const to = normalizeRangeTime(range.to);
        if (from == null || to == null) return;
        handler({ from, to });
      };
      if (timeScale?.subscribeVisibleLogicalRangeChange) {
        timeScale.subscribeVisibleLogicalRangeChange(rangeHandler);
      }
      if (timeScale?.subscribeVisibleTimeRangeChange) {
        timeScale.subscribeVisibleTimeRangeChange(rangeHandler);
      }
      if (priceScale?.subscribeVisibleLogicalRangeChange) {
        priceScale.subscribeVisibleLogicalRangeChange(rangeHandler);
      }

      chartRef.current = chart;
      candleSeriesRef.current = candleSeries;
      volumeSeriesRef.current = volumeSeries;
      lineSeriesRef.current = lineSeries;
      setOverlayTargets({ candleSeries, chart });

      applyData(dataRef.current);
      resizeOverlay();

      teardown = () => {
        chart.unsubscribeCrosshairMove(crosshairHandler);
        if (timeScale?.unsubscribeVisibleLogicalRangeChange) {
          timeScale.unsubscribeVisibleLogicalRangeChange(rangeHandler);
        }
        if (timeScale?.unsubscribeVisibleTimeRangeChange) {
          timeScale.unsubscribeVisibleTimeRangeChange(rangeHandler);
        }
        if (priceScale?.unsubscribeVisibleLogicalRangeChange) {
          priceScale.unsubscribeVisibleLogicalRangeChange(rangeHandler);
        }
      };
    };

    const handleSize = (width: number, height: number) => {
      if (width <= 0 || height <= 0) return;
      if (!chartRef.current) {
        init(width, height);
        return;
      }
      scheduleResizeAndFit(width, height, true);
    };

    resizeObserver = new ResizeObserver((entries) => {
      for (const entry of entries) {
        const width = Math.floor(entry.contentRect.width);
        const height = Math.floor(entry.contentRect.height);
        handleSize(width, height);
      }
    });
    resizeObserver.observe(element);

    const initialWidth = Math.floor(element.clientWidth);
    const initialHeight = Math.floor(element.clientHeight);
    if (initialWidth > 0 && initialHeight > 0) {
      handleSize(initialWidth, initialHeight);
    }

    return () => {
      if (teardown) teardown();
      if (resizeRafRef.current !== null) {
        window.cancelAnimationFrame(resizeRafRef.current);
        resizeRafRef.current = null;
      }
      if (resizeObserver) resizeObserver.disconnect();
      if (chartRef.current) {
        chartRef.current.remove();
      }
      chartRef.current = null;
      candleSeriesRef.current = null;
      volumeSeriesRef.current = null;
      lineSeriesRef.current = [];
      setOverlayTargets({ candleSeries: null, chart: null });
    };
  }, []);

  const selectedContextShape = selectedShapeState;
  const selectedColor =
    selectedContextShape?.kind === "drawBox"
      ? drawBoxesRef.current[selectedContextShape.index]?.color ?? "#64748b"
      : selectedContextShape?.kind === "horizontalLine"
      ? horizontalLinesRef.current[selectedContextShape.index]?.color ?? "#334155"
      : selectedContextShape?.kind === "timeZone"
        ? timeZonesRef.current[selectedContextShape.index]?.color ?? BUY_ZONE_COLOR
        : null;

  return (
    <div
      className={`detail-chart-wrapper ${isDrawingEnabled ? "is-drawing" : ""}`}
      ref={wrapperRef}
      onMouseDown={(e) => {
        const chart = chartRef.current;
        if (!chart) return;
        const rect = wrapperRef.current?.getBoundingClientRect();
        if (!rect) return;
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;
        const timeScale = chart.timeScale();
        const priceScale = chart.priceScale("right");
        const tool = activeToolRef.current;
        const effectiveTool = tool ?? (e.shiftKey ? "timeZone" : e.altKey ? "priceBand" : null);

        const series = candleSeriesRef.current;
        if (
          typeof timeScale.timeToCoordinate !== "function" ||
          typeof series?.priceToCoordinate !== "function"
        ) {
          return;
        }
        const time = typeof timeScale.coordinateToTime === "function"
          ? normalizeRangeTime(timeScale.coordinateToTime(x))
          : null;
        // Try priceScale first, fallback to series.coordinateToPrice if available
        let price: number | null = null;
        if (priceScale && typeof priceScale.coordinateToPrice === "function") {
          price = normalizeCoordinatePrice(priceScale.coordinateToPrice(y));
        }
        if (price == null && series && typeof series.coordinateToPrice === "function") {
          price = normalizeCoordinatePrice(series.coordinateToPrice(y));
        }
        const hit = hitTestShape(x, y, time, price, effectiveTool);
        const isRightClick = e.button === 2;
        const currentSelected = selectedShapeRef.current;
        const isSameAsSelected =
          !!currentSelected &&
          currentSelected.kind === hit?.kind &&
          currentSelected.index === hit?.index;
        if (hit) {
          // Restrict interaction in Chart Mode (null tool) to Right-Click or selected shape
          const isChartMode = !effectiveTool;
          const isSelected = selectedShapeRef.current?.kind === hit.kind && selectedShapeRef.current.index === hit.index;

          if (isChartMode && !isRightClick && !isSelected) {
            // In Chart Mode, if we Left-Click something that isn't already selected,
            // we ignore the hit so the user can interact with the chart (pan/scroll).
          } else {
            e.preventDefault();
            e.stopPropagation();
            if (isSameAsSelected) {
              if (isRightClick) {
                // Right click on same selection -> allow context menu (fallthrough to re-emit selection)
              } else {
                selectedShapeRef.current = null;
                dragStateRef.current = null;
                emitSelection(null);
                drawOverlay();
                return;
              }
            }
            selectedShapeRef.current = { kind: hit.kind, index: hit.index };
            dragStateRef.current = isRightClick ? null : hit;
            emitSelection(
              selectedShapeRef.current,
              isRightClick ? { contextPoint: { x, y } } : { preserveContextBar: false }
            );
            drawOverlay();
            return;
          }
        }

        if (isRightClick) {
          e.preventDefault();
          e.stopPropagation();
          selectedShapeRef.current = null;
          dragStateRef.current = null;
          emitSelection(null);
          drawOverlay();
          return;
        }

        selectedShapeRef.current = null;
        dragStateRef.current = null;
        emitSelection(null);
        drawOverlay();

        if (!drawingEnabledRef.current || e.button !== 0) {
          return;
        }

        e.preventDefault();
        e.stopPropagation();
        if (!effectiveTool || effectiveTool === "horizontalLine") {
          return;
        }
        if (effectiveTool === "timeZone") {
          if (time == null) return;
          clearDraftState();
          drawModeRef.current = "timeZone";
          drawStartRef.current = { time };
          applyDraftForTool("timeZone", time, price);
          drawOverlay();
          return;
        }
        if (effectiveTool === "priceBand") {
          if (price == null) return;
          clearDraftState();
          drawModeRef.current = "priceBand";
          drawStartRef.current = { price };
          applyDraftForTool("priceBand", time, price);
          drawOverlay();
          return;
        }
        if (time == null || price == null) return;
        clearDraftState();
        drawModeRef.current = "drawBox";
        drawStartRef.current = { time, price };
        applyDraftForTool("drawBox", time, price);
        drawOverlay();
      }}
      onMouseMove={(e) => {
        const chart = chartRef.current;
        if (!chart) return;
        const rect = wrapperRef.current?.getBoundingClientRect();
        if (!rect) return;
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;
        const timeScale = chart.timeScale();
        const priceScale = chart.priceScale("right");
        const series = candleSeriesRef.current;

        const drag = dragStateRef.current;
        if (!drag && !drawModeRef.current) return;
        e.preventDefault();
        e.stopPropagation();
        if (drag) {
          if ((e.buttons ?? 0) === 0) {
            dragStateRef.current = null;
            return;
          }
          if (!series || typeof series.priceToCoordinate !== "function") return;
          const currentTime =
            typeof timeScale.coordinateToTime === "function"
              ? normalizeRangeTime(timeScale.coordinateToTime(x))
              : null;
          // Fallback to series.coordinateToPrice if priceScale fails
          let currentPrice: number | null = null;
          if (priceScale && typeof priceScale.coordinateToPrice === "function") {
            currentPrice = normalizeCoordinatePrice(priceScale.coordinateToPrice(y));
          }
          if (currentPrice == null && series && typeof series.coordinateToPrice === "function") {
            currentPrice = normalizeCoordinatePrice(series.coordinateToPrice(y));
          }
          if (drag.kind === "timeZone") {
            if (currentTime == null) return;
            const zone = timeZonesRef.current[drag.index];
            if (!zone) return;
            if (drag.handle === "move") {
              const delta = currentTime - (drag.anchorTime ?? currentTime);
              updateTimeZoneAt(drag.index, {
                ...zone,
                startTime: (drag.startTime ?? zone.startTime) + delta,
                endTime: (drag.endTime ?? zone.endTime) + delta
              });
            } else {
              const otherTime =
                drag.handle === "start" ? drag.endTime ?? zone.endTime : drag.startTime ?? zone.startTime;
              const startTime = drag.handle === "start" ? currentTime : otherTime;
              const endTime = drag.handle === "end" ? currentTime : otherTime;
              updateTimeZoneAt(drag.index, {
                ...zone,
                startTime: Math.min(startTime, endTime),
                endTime: Math.max(startTime, endTime)
              });
            }
            drawOverlay();
            return;
          }
          if (drag.kind === "priceBand") {
            if (currentPrice == null) return;
            const band = priceBandsRef.current[drag.index];
            if (!band) return;
            if (drag.handle === "move") {
              const delta = currentPrice - (drag.anchorPrice ?? currentPrice);
              updatePriceBandAt(drag.index, {
                ...band,
                topPrice: (drag.startPrice ?? band.topPrice) + delta,
                bottomPrice: (drag.endPrice ?? band.bottomPrice) + delta
              });
            } else {
              const other =
                drag.handle === "top" ? drag.endPrice ?? band.bottomPrice : drag.startPrice ?? band.topPrice;
              const topPrice = drag.handle === "top" ? currentPrice : other;
              const bottomPrice = drag.handle === "bottom" ? currentPrice : other;
              updatePriceBandAt(drag.index, {
                ...band,
                topPrice: Math.max(topPrice, bottomPrice),
                bottomPrice: Math.min(topPrice, bottomPrice)
              });
            }
            drawOverlay();
            return;
          }
          if (drag.kind === "drawBox") {
            if (currentTime == null || currentPrice == null) return;
            const box = drawBoxesRef.current[drag.index];
            if (!box) return;
            if (drag.handle === "move") {
              const deltaTime = currentTime - (drag.anchorTime ?? currentTime);
              const deltaPrice = currentPrice - (drag.anchorPrice ?? currentPrice);
              updateDrawBoxAt(drag.index, {
                ...box,
                startTime: (drag.startTime ?? box.startTime) + deltaTime,
                endTime: (drag.endTime ?? box.endTime) + deltaTime,
                topPrice: (drag.startPrice ?? box.topPrice) + deltaPrice,
                bottomPrice: (drag.endPrice ?? box.bottomPrice) + deltaPrice
              });
            } else {
              const baseStartTime = drag.startTime ?? box.startTime;
              const baseEndTime = drag.endTime ?? box.endTime;
              const baseTop = drag.startPrice ?? box.topPrice;
              const baseBottom = drag.endPrice ?? box.bottomPrice;
              const nextStartTime = drag.handle === "tl" || drag.handle === "bl" ? currentTime : baseStartTime;
              const nextEndTime = drag.handle === "tr" || drag.handle === "br" ? currentTime : baseEndTime;
              const nextTop = drag.handle === "tl" || drag.handle === "tr" ? currentPrice : baseTop;
              const nextBottom = drag.handle === "bl" || drag.handle === "br" ? currentPrice : baseBottom;
              updateDrawBoxAt(drag.index, {
                ...box,
                startTime: Math.min(nextStartTime, nextEndTime),
                endTime: Math.max(nextStartTime, nextEndTime),
                topPrice: Math.max(nextTop, nextBottom),
                bottomPrice: Math.min(nextTop, nextBottom)
              });
            }
            drawOverlay();
            return;
          }
          if (drag.kind === "horizontalLine") {
            if (currentPrice == null) return;
            const line = horizontalLinesRef.current[drag.index];
            if (!line) return;
            updateHorizontalLineAt(drag.index, { ...line, price: currentPrice });
            drawOverlay();
            return;
          }
        }

        if (!drawModeRef.current) return;
        const normalizedTime =
          typeof timeScale.coordinateToTime === "function"
            ? normalizeRangeTime(timeScale.coordinateToTime(x))
            : null;
        // Fallback to series.coordinateToPrice if priceScale fails
        let normalizedPrice: number | null = null;
        if (priceScale && typeof priceScale.coordinateToPrice === "function") {
          normalizedPrice = normalizeCoordinatePrice(priceScale.coordinateToPrice(y));
        }
        if (normalizedPrice == null && series && typeof series.coordinateToPrice === "function") {
          normalizedPrice = normalizeCoordinatePrice(series.coordinateToPrice(y));
        }
        applyDraftForTool(drawModeRef.current, normalizedTime, normalizedPrice);
        drawOverlay();
      }}
      onMouseUp={(e) => {
        if (dragStateRef.current || drawModeRef.current) {
          e.preventDefault();
          e.stopPropagation();
        }
        if (dragStateRef.current) {
          dragStateRef.current = null;
          lastDragAtRef.current = Date.now();
          return;
        }
        const tool = drawModeRef.current;
        if (!tool) return;
        const chart = chartRef.current;
        if (!chart) return;
        const rect = wrapperRef.current?.getBoundingClientRect();
        if (!rect) return;
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;
        const timeScale = chart.timeScale();
        const priceScale = chart.priceScale("right");
        const series = candleSeriesRef.current;
        const normalizedTime =
          typeof timeScale.coordinateToTime === "function"
            ? normalizeRangeTime(timeScale.coordinateToTime(x))
            : null;
        // Fallback to series.coordinateToPrice if priceScale fails
        let normalizedPrice: number | null = null;
        if (priceScale && typeof priceScale.coordinateToPrice === "function") {
          normalizedPrice = normalizeCoordinatePrice(priceScale.coordinateToPrice(y));
        }
        if (normalizedPrice == null && series && typeof series.coordinateToPrice === "function") {
          normalizedPrice = normalizeCoordinatePrice(series.coordinateToPrice(y));
        }
        commitDraftForTool(tool, normalizedTime, normalizedPrice);
        clearDraftState();
        drawOverlay();
        lastDragAtRef.current = Date.now();
      }}
      onMouseLeave={(e) => {
        if (dragStateRef.current) {
          dragStateRef.current = null;
          lastDragAtRef.current = Date.now();
          return;
        }
        if (drawModeRef.current) {
          clearDraftState();
          drawOverlay();
          lastDragAtRef.current = Date.now();
        }
      }}
      onClick={(e) => {
        const chart = chartRef.current;
        if (!chart) return;
        if (Date.now() - lastDragAtRef.current < 150) return;

        const rect = wrapperRef.current?.getBoundingClientRect();
        if (!rect) return;

        const x = e.clientX - rect.left;
        const timeScale = chart.timeScale();
        const priceScale = chart.priceScale("right");
        const series = candleSeriesRef.current;
        const tool = activeToolRef.current;
        const effectiveTool = tool ?? (e.shiftKey ? "timeZone" : e.altKey ? "priceBand" : null);
        if (drawingEnabledRef.current && effectiveTool === "horizontalLine") {
          e.preventDefault();
          e.stopPropagation();
          // Fallback to series.coordinateToPrice if priceScale fails
          let normalizedPrice: number | null = null;
          const y = e.clientY - rect.top;
          if (priceScale && typeof priceScale.coordinateToPrice === "function") {
            normalizedPrice = normalizeCoordinatePrice(priceScale.coordinateToPrice(y));
          }
          if (normalizedPrice == null && series && typeof series.coordinateToPrice === "function") {
            normalizedPrice = normalizeCoordinatePrice(series.coordinateToPrice(y));
          }
          if (normalizedPrice == null) return;
          onAddHorizontalLineRef.current?.({
            price: normalizedPrice,
            opacity: typeof activeLineOpacityRef.current === "number" ? activeLineOpacityRef.current : 0.6,
            lineWidth: typeof activeLineWidthRef.current === "number" ? activeLineWidthRef.current : 1,
            color: activeDrawColorRef.current ?? undefined
          });
          drawOverlay();
          return;
        }

        if (drawingEnabledRef.current) {
          return;
        }
        if (!onChartClick) return;
        if (typeof timeScale.coordinateToTime === "function") {
          const time = timeScale.coordinateToTime(x);
          if (time != null) {
            const normalizedTime = normalizeRangeTime(time);
            if (normalizedTime != null) {
              onChartClick(normalizedTime);
            }
          }
        }
      }}
      onContextMenu={(e) => {
        e.preventDefault();
        e.stopPropagation();
      }}
    >
      <div className="detail-chart-inner" ref={containerRef} />
      <canvas className="detail-chart-overlay" ref={overlayRef} />
      {contextBarState.open && selectedContextShape && (
        <div
          className="detail-chart-context-anchor"
          style={{ top: contextBarState.y, left: contextBarState.x }}
          onMouseDown={(e) => e.stopPropagation()}
          onClick={(e) => e.stopPropagation()}
          onContextMenu={(e) => {
            e.preventDefault();
            e.stopPropagation();
          }}
        >
          <div className="detail-chart-context-bar">
            {(selectedContextShape.kind === "drawBox" ||
              selectedContextShape.kind === "horizontalLine" ||
              selectedContextShape.kind === "timeZone") &&
              CONTEXT_COLOR_PALETTE.map((color) => (
                <button
                  key={color}
                  type="button"
                  className={`detail-chart-context-swatch ${selectedColor === color ? "is-active" : ""
                    }`}
                  style={{ backgroundColor: color }}
                  aria-label={`濶ｲ ${color}`}
                  onClick={() => updateSelectedDrawColor(color)}
                />
              ))}
            <button
              type="button"
              className="detail-chart-context-btn is-danger"
              aria-label="驕ｸ謚樔ｸｭ縺ｮ謠冗判繧貞炎髯､"
              onClick={deleteSelectedShape}
            >
              <IconTrash size={15} />
            </button>
          </div>
        </div>
      )}
      {positionOverlay && (
        <PositionOverlay
          candleSeries={overlayTargets.candleSeries}
          chart={overlayTargets.chart}
          dailyPositions={positionOverlay.dailyPositions}
          tradeMarkers={positionOverlay.tradeMarkers}
          currentPositions={positionOverlay.currentPositions}
          latestTradeTime={positionOverlay.latestTradeTime}
          showOverlay={positionOverlay.showOverlay}
          showPnL={positionOverlay.showPnL}
          hoverTime={positionOverlay.hoverTime}
          showMarkers={positionOverlay.showMarkers}
          markerSuffix={positionOverlay.markerSuffix}
          bars={candles}
          volume={volume}
          maLines={positionOverlay.maLines ?? maLines}
          hidePanel={positionOverlay.hidePanel}
        />
      )}
    </div>
  );
});

export default DetailChart;

