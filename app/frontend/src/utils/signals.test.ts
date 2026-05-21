import { describe, expect, it } from "vitest";
import { computeSignalMetrics } from "./signals";

const buildFailedRetestBars = () => {
  const bars: number[][] = [];
  for (let i = 0; i < 100; i += 1) {
    const close = 100 + i * 0.3;
    bars.push([i + 1, close - 1, close + 2, close - 2, close]);
  }
  bars[70] = [71, 145, 150, 144, 148];
  for (let i = 100; i < 112; i += 1) {
    const close = 119 - (i - 100) * 1.8;
    bars.push([i + 1, close + 1, close + 3, close - 2, close]);
  }
  bars[103] = [104, 136, 140, 134, 137];
  bars.push([113, 96, 97, 93, 94]);
  return bars;
};

describe("computeSignalMetrics", () => {
  it("surfaces a sell warning when a prior-high retest fails and rolls below short MAs", () => {
    const metrics = computeSignalMetrics(buildFailedRetestBars(), 5);

    expect(metrics.signals.some((signal) => signal.label === "売り:高値未達失速")).toBe(true);
  });

  it("does not surface the prior-high warning while the retest is still holding", () => {
    const bars = buildFailedRetestBars();
    bars[bars.length - 1] = [113, 116, 118, 114, 117];

    const metrics = computeSignalMetrics(bars, 5);

    expect(metrics.signals.some((signal) => signal.label === "売り:高値未達失速")).toBe(false);
  });
});
