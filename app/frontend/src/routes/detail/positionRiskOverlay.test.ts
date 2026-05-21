import { describe, expect, it } from "vitest";
import { buildPositionRiskOverlayDrawings } from "./positionRiskOverlay";

const candles = Array.from({ length: 20 }, (_, index) => ({
  time: 1_700_000_000 + index * 86_400,
  open: 1000 + index,
  high: 1010 + index,
  low: 960 + index,
  close: 990 + index,
}));
const latestTime = candles[candles.length - 1]?.time ?? 0;

describe("buildPositionRiskOverlayDrawings", () => {
  it("builds only the long position exit line", () => {
    const result = buildPositionRiskOverlayDrawings({
      candles,
      maLines: [
        {
          key: "ma20",
          label: "MA20",
          period: 20,
          color: "#22c55e",
          visible: true,
          lineWidth: 1,
          data: [{ time: latestTime, value: 980 }],
          chartData: [{ time: latestTime, value: 980 }],
        },
      ],
      currentPositions: [
        {
          brokerKey: "rakuten",
          brokerLabel: "RAKUTEN",
          longLots: 1,
          shortLots: 0,
          avgLongPrice: 1000,
          avgShortPrice: 0,
          realizedPnL: 0,
        },
      ],
    });

    expect(result.priceBands).toEqual([]);
    expect(result.horizontalLines).toHaveLength(1);
    expect(result.horizontalLines[0]?.price).toBeCloseTo(960.4);
    expect(result.horizontalLines[0]).toMatchObject({
      color: "#475569",
      lineWidth: 1,
      opacity: 0.46,
      lineDash: [6, 5],
    });
    expect(result.exitLinePrice).toBeCloseTo(960.4);
    expect(result.exitLineSide).toBe("long");
  });

  it("emits one dominant-side exit line when both long and short holdings exist", () => {
    const result = buildPositionRiskOverlayDrawings({
      candles,
      maLines: [],
      currentPositions: [
        {
          brokerKey: "rakuten",
          brokerLabel: "RAKUTEN",
          longLots: 2,
          shortLots: 0,
          avgLongPrice: 1000,
          avgShortPrice: 0,
          realizedPnL: 0,
        },
        {
          brokerKey: "sbi",
          brokerLabel: "SBI",
          longLots: 0,
          shortLots: 1,
          avgLongPrice: 0,
          avgShortPrice: 1000,
          realizedPnL: 0,
        },
      ],
    });

    expect(result.priceBands).toEqual([]);
    expect(result.horizontalLines).toHaveLength(1);
    expect(result.exitLineSide).toBe("long");
  });

  it("does not emit drawings without active holdings", () => {
    const result = buildPositionRiskOverlayDrawings({
      candles,
      maLines: [],
      currentPositions: [],
    });

    expect(result.priceBands).toEqual([]);
    expect(result.horizontalLines).toEqual([]);
    expect(result.exitLinePrice).toBeNull();
    expect(result.exitLineSide).toBeNull();
  });
});
