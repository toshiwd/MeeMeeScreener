import type { MaSetting } from "../../storeTypes";
import { buildCandlesWithStats, buildVolume, computeMA } from "./detailHelpers";
import type { Candle, FetchState, ParseStats, Timeframe, VolumePoint } from "./detailTypes";

export type DetailMaLine = {
  key: string;
  label: string;
  period: number;
  color: string;
  visible: boolean;
  lineWidth: number;
  data: Array<{ time: number; value: number }>;
  chartData: Array<{ time: number; value: number }>;
};

export type DetailChartPanelInput = {
  fetch: FetchState;
  errors: string[];
  candleCount: number;
  parseStats: ParseStats;
  loading: boolean;
  pendingSwap: boolean;
};

export type DetailChartFrameModel = {
  timeframe: Timeframe;
  rows: number[][];
  candles: Candle[];
  volume: VolumePoint[];
  parseStats: ParseStats;
  maLines: DetailMaLine[];
  chartMaLines: Array<Omit<DetailMaLine, "chartData">>;
  shouldRenderChart: boolean;
};

export const getDetailFrameRows = (frame: { rows?: number[][] | null } | null | undefined) =>
  Array.isArray(frame?.rows) ? frame.rows : [];

export const buildDetailParsedFrame = (rows: number[][]) => buildCandlesWithStats(rows);

export const buildDetailVolume = (rows: number[][]) => buildVolume(rows);

export const buildDetailMaLines = (candles: Candle[], settings: MaSetting[]): DetailMaLine[] =>
  settings.map((setting) => {
    const data = computeMA(candles, setting.period);
    return {
      key: setting.key,
      label: setting.label,
      period: setting.period,
      color: setting.color,
      visible: setting.visible,
      lineWidth: setting.lineWidth,
      data,
      chartData: setting.visible ? data : [],
    };
  });

export const toDetailChartMaLines = (lines: DetailMaLine[]) =>
  lines.map(({ chartData, data, ...line }) => ({
    ...line,
    data: chartData ?? data,
  }));

export const buildDetailChartFrameModel = ({
  timeframe,
  rows,
  maSettings,
}: {
  timeframe: Timeframe;
  rows: number[][];
  maSettings: MaSetting[];
}): DetailChartFrameModel => {
  const parsed = buildDetailParsedFrame(rows);
  const maLines = buildDetailMaLines(parsed.candles, maSettings);
  return {
    timeframe,
    rows,
    candles: parsed.candles,
    volume: buildDetailVolume(rows),
    parseStats: parsed.stats,
    maLines,
    chartMaLines: toDetailChartMaLines(maLines),
    shouldRenderChart: parsed.candles.length > 0,
  };
};

export const buildDetailChartPanelInput = ({
  fetch,
  errors,
  candleCount,
  parseStats,
  loading,
  pendingSwap,
}: DetailChartPanelInput): DetailChartPanelInput => ({
  fetch,
  errors,
  candleCount,
  parseStats,
  loading,
  pendingSwap,
});

