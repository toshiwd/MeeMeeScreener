import { describe, expect, it } from "vitest";
import { buildTradexListSummaryWarmItems } from "./tradexSummary";

describe("buildTradexListSummaryWarmItems", () => {
  it("dedupes by symbol/asof and caps visible warm items", () => {
    const items = Array.from({ length: 60 }).map((_, index) => ({
      code: String(index + 1).padStart(4, "0"),
      asof: null,
    }));
    items.push({ code: "0001", asof: null });

    const warmItems = buildTradexListSummaryWarmItems(items, "grid-visible");

    expect(warmItems).toHaveLength(48);
    expect(new Set(warmItems.map((item) => `${item.code}:${item.asof ?? "latest"}`)).size).toBe(48);
  });

  it("uses the smaller favorites cap", () => {
    const items = Array.from({ length: 40 }).map((_, index) => ({
      code: String(index + 1).padStart(4, "0"),
      asof: null,
    }));

    const warmItems = buildTradexListSummaryWarmItems(items, "favorites-visible");

    expect(warmItems).toHaveLength(24);
  });
});
