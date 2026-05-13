import { describe, expect, it } from "vitest";

import {
  buildMarketSectorMatrix,
  MARKET_CANDLE_DOWN_COLOR,
  MARKET_CANDLE_UP_COLOR,
  getMarketTileColors,
  type MarketSectorViewItem
} from "./marketHelpers";

const makeItem = (rate: number, flow = 0, sectorCode = "50"): MarketSectorViewItem => ({
  sector33_code: sectorCode,
  label: "水産・農林業",
  rate,
  flow,
  weight: 100,
  tickerCount: 2,
  related: false,
  watchlistCount: 0,
  watchlistTickers: [],
  representatives: [],
});

describe("market tile colors", () => {
  it("uses the same red and green base colors as the candlestick chart", () => {
    const positive = getMarketTileColors(makeItem(5), "rate", { rateAbs: 5, flowAbs: 1 });
    const negative = getMarketTileColors(makeItem(-5), "rate", { rateAbs: 5, flowAbs: 1 });
    const neutral = getMarketTileColors(makeItem(0), "rate", { rateAbs: 5, flowAbs: 1 });

    expect(positive.bodyColor).toContain("color-mix");
    expect(positive.bodyColor).toContain(MARKET_CANDLE_UP_COLOR);
    expect(negative.bodyColor).toContain(MARKET_CANDLE_DOWN_COLOR);
    expect(neutral.bodyColor).toBe("var(--market-tile-neutral)");
  });

  it("packs available sectors without rendering empty grid holes", () => {
    const rows = buildMarketSectorMatrix([
      makeItem(1, 0, "50"),
      makeItem(1, 0, "2050"),
      makeItem(1, 0, "3400"),
      makeItem(1, 0, "6100"),
      makeItem(1, 0, "9999")
    ]);

    expect(rows).toHaveLength(1);
    expect(rows.flat().map((item) => item.sector33_code)).toEqual(["50", "2050", "3400", "6100", "9999"]);
  });
});
