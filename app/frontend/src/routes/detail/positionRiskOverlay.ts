import type { HorizontalLine, PriceBand } from "../../components/DetailChart";
import type { CurrentPosition } from "../../utils/positions";
import type { Candle } from "./detailTypes";
import type { DetailMaLine } from "./detailChartFrameAdapter";

type PositionRiskOverlay = {
  priceBands: PriceBand[];
  horizontalLines: HorizontalLine[];
  exitLinePrice: number | null;
  exitLineSide: "long" | "short" | null;
};

const EXIT_LINE_COLOR = "#475569";

const isUsablePrice = (value: unknown): value is number =>
  typeof value === "number" && Number.isFinite(value) && value > 0;

const latestVisibleMaValue = (maLines: DetailMaLine[], period: number) => {
  const line = maLines.find((item) => item.period === period);
  if (!line) return null;
  for (let index = line.data.length - 1; index >= 0; index -= 1) {
    const value = line.data[index]?.value;
    if (isUsablePrice(value)) return value;
  }
  return null;
};

const recentLow = (candles: Candle[], count: number) => {
  const rows = candles.slice(Math.max(0, candles.length - count));
  const lows = rows.map((bar) => bar.low).filter(isUsablePrice);
  if (!lows.length) return null;
  return Math.min(...lows);
};

const recentHigh = (candles: Candle[], count: number) => {
  const rows = candles.slice(Math.max(0, candles.length - count));
  const highs = rows.map((bar) => bar.high).filter(isUsablePrice);
  if (!highs.length) return null;
  return Math.max(...highs);
};

const addLine = (
  lines: HorizontalLine[],
  price: number | null,
  color: string,
  lineWidth: number,
  opacity = 0.46
) => {
  if (!isUsablePrice(price)) return;
  lines.push({ price, color, lineWidth, opacity, lineDash: [6, 5] });
};

export const buildPositionRiskOverlayDrawings = ({
  candles,
  maLines,
  currentPositions,
}: {
  candles: Candle[];
  maLines: DetailMaLine[];
  currentPositions: CurrentPosition[] | null | undefined;
}): PositionRiskOverlay => {
  if (!candles.length || !currentPositions?.length) {
    return { priceBands: [], horizontalLines: [], exitLinePrice: null, exitLineSide: null };
  }

  const ma20 = latestVisibleMaValue(maLines, 20);
  const low20 = recentLow(candles, 20);
  const high20 = recentHigh(candles, 20);
  const priceBands: PriceBand[] = [];
  const horizontalLines: HorizontalLine[] = [];
  const totalLongLots = currentPositions.reduce((sum, position) => sum + Math.max(0, position.longLots || 0), 0);
  const totalShortLots = currentPositions.reduce((sum, position) => sum + Math.max(0, position.shortLots || 0), 0);

  if (totalLongLots <= 0 && totalShortLots <= 0) {
    return { priceBands, horizontalLines, exitLinePrice: null, exitLineSide: null };
  }

  if (totalLongLots >= totalShortLots) {
    const maExit = isUsablePrice(ma20) ? ma20 * 0.98 : null;
    const structureExit = isUsablePrice(low20) ? low20 * 0.99 : null;
    const exitLine =
      maExit != null && structureExit != null
        ? Math.max(maExit, structureExit)
        : maExit ?? structureExit;
    addLine(horizontalLines, exitLine, EXIT_LINE_COLOR, 1, 0.46);
    return {
      priceBands,
      horizontalLines,
      exitLinePrice: isUsablePrice(exitLine) ? exitLine : null,
      exitLineSide: isUsablePrice(exitLine) ? "long" : null,
    };
  } else {
    const maExit = isUsablePrice(ma20) ? ma20 * 1.02 : null;
    const structureExit = isUsablePrice(high20) ? high20 * 1.01 : null;
    const exitLine =
      maExit != null && structureExit != null
        ? Math.min(maExit, structureExit)
        : maExit ?? structureExit;
    addLine(horizontalLines, exitLine, EXIT_LINE_COLOR, 1, 0.46);
    return {
      priceBands,
      horizontalLines,
      exitLinePrice: isUsablePrice(exitLine) ? exitLine : null,
      exitLineSide: isUsablePrice(exitLine) ? "short" : null,
    };
  }
};
