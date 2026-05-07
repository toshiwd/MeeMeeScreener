import { describe, expect, it } from "vitest";
import {
  buildDetailChartFrameModel,
  buildDetailChartPanelInput,
  buildDetailMaLines,
  getDetailFrameRows,
  toDetailChartMaLines,
} from "./detailChartFrameAdapter";

const maSettings = [
  { key: "ma7", label: "7MA", period: 2, color: "#fff", visible: true, lineWidth: 1 },
  { key: "ma20", label: "20MA", period: 3, color: "#aaa", visible: false, lineWidth: 2 },
];

const rows = [
  [20260501, 10, 12, 9, 11, 100],
  [20260502, 11, 13, 10, 12, 150],
  [20260503, 12, 14, 11, 13, 200],
];

describe("detailChartFrameAdapter", () => {
  it("normalizes frame rows without inventing data", () => {
    expect(getDetailFrameRows({ rows })).toBe(rows);
    expect(getDetailFrameRows(null)).toEqual([]);
    expect(getDetailFrameRows({ rows: null })).toEqual([]);
  });

  it("builds MA lines and chart-visible MA lines with existing hidden-line semantics", () => {
    const model = buildDetailChartFrameModel({ timeframe: "daily", rows, maSettings });

    expect(model.candles).toHaveLength(3);
    expect(model.volume).toHaveLength(3);
    expect(model.parseStats.parsed).toBe(3);
    expect(model.shouldRenderChart).toBe(true);
    expect(model.maLines[0].data).toHaveLength(2);
    expect(model.maLines[1].data).toHaveLength(1);
    expect(model.maLines[1].chartData).toEqual([]);
    expect(model.chartMaLines[0].data).toHaveLength(2);
    expect(model.chartMaLines[1].data).toEqual([]);
  });

  it("keeps raw MA data available for memo/position calculations", () => {
    const lines = buildDetailMaLines(buildDetailChartFrameModel({ timeframe: "daily", rows, maSettings }).candles, maSettings);
    const chartLines = toDetailChartMaLines(lines);

    expect(lines[1].data).toHaveLength(1);
    expect(chartLines[1].data).toEqual([]);
  });

  it("passes lifecycle input through as a stable normalized shape", () => {
    const input = buildDetailChartPanelInput({
      fetch: { status: "success", responseCount: 3, errorMessage: null },
      errors: [],
      candleCount: 3,
      parseStats: { total: 3, parsed: 3, invalidRow: 0, invalidTime: 0, invalidValue: 0 },
      loading: false,
      pendingSwap: false,
    });

    expect(input.candleCount).toBe(3);
    expect(input.fetch.status).toBe("success");
  });
});

