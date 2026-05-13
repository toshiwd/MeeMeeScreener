import { describe, expect, it } from "vitest";

import {
  buildSectorMemberIndex,
  buildMarketSectorMatrix,
  buildWatchlistSectorIndex,
  MARKET_CANDLE_DOWN_COLOR,
  MARKET_CANDLE_UP_COLOR,
  getMarketTileColors,
  resolveMarketTickerRate,
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

describe("market sector ticker rates", () => {
  const tickers = [
    {
      code: "7203",
      name: "Toyota",
      sector33Code: "3700",
      sector33Name: "輸送用機器",
      stage: "",
      score: null,
      reason: "",
      chg1D: 1.2,
      chg1W: -2.3,
      chg1M: 4.5
    },
    {
      code: "6758",
      name: "Sony",
      sector33Code: "3650",
      sector33Name: "電気機器",
      stage: "",
      score: null,
      reason: "",
      chg1D: null,
      chg1W: 0,
      chg1M: -1.1
    }
  ];

  it("keeps period change fields on sector members", () => {
    const members = buildSectorMemberIndex(tickers).get("3700") ?? [];

    expect(members[0]).toMatchObject({
      code: "7203",
      chg1D: 1.2,
      chg1W: -2.3,
      chg1M: 4.5
    });
  });

  it("keeps period change fields on watchlist sector members", () => {
    const members = buildWatchlistSectorIndex(["6758"], tickers).get("3650") ?? [];

    expect(members[0]).toMatchObject({
      code: "6758",
      chg1D: null,
      chg1W: 0,
      chg1M: -1.1
    });
  });

  it("resolves the ticker rate for the selected market period", () => {
    const member = buildSectorMemberIndex(tickers).get("3700")?.[0];

    expect(member ? resolveMarketTickerRate(member, "1d") : null).toBe(1.2);
    expect(member ? resolveMarketTickerRate(member, "1w") : null).toBe(-2.3);
    expect(member ? resolveMarketTickerRate(member, "1m") : null).toBe(4.5);
  });
});
