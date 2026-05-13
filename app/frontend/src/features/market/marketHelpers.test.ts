import { describe, expect, it } from "vitest";

import { getMarketTileColors, type MarketSectorViewItem } from "./marketHelpers";

const makeItem = (rate: number, flow = 0): MarketSectorViewItem => ({
  sector33_code: "50",
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
  it("uses light mixed colors instead of raw red and green backgrounds", () => {
    const positive = getMarketTileColors(makeItem(5), "rate", { rateAbs: 5, flowAbs: 1 });
    const negative = getMarketTileColors(makeItem(-5), "rate", { rateAbs: 5, flowAbs: 1 });
    const neutral = getMarketTileColors(makeItem(0), "rate", { rateAbs: 5, flowAbs: 1 });

    expect(positive.bodyColor).toContain("color-mix");
    expect(positive.bodyColor).toContain("var(--color-pnl-up)");
    expect(negative.bodyColor).toContain("var(--color-pnl-down)");
    expect(neutral.bodyColor).toContain("var(--theme-text-muted)");
  });
});
