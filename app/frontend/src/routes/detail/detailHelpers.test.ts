import { describe, expect, it } from "vitest";

import { resolveAnalysisBaseAsOfTime, resolveLatestResolvedMetaDate } from "./detailHelpers";

describe("resolveLatestResolvedMetaDate", () => {
  it("prefers the latest resolved meta date", () => {
    expect(
      resolveLatestResolvedMetaDate(
        { latestResolvedDate: 20260310 },
        { latestResolvedDate: 20260311 }
      )
    ).toBe(1773187200);
  });
});

describe("resolveAnalysisBaseAsOfTime", () => {
  it("uses latestResolvedMetaDate before latestDailyAsOfTime", () => {
    expect(
      resolveAnalysisBaseAsOfTime({
        mainAsOfTime: null,
        resolvedCursorAsOfTime: null,
        analysisBaseAsOfTime: null,
        latestResolvedMetaDate: 1773187200,
        latestDailyAsOfTime: 1773100800,
      })
    ).toBe(1773187200);
  });

  it("keeps existing precedence for cursor and mainAsOf", () => {
    expect(
      resolveAnalysisBaseAsOfTime({
        mainAsOfTime: 1773014400,
        resolvedCursorAsOfTime: 1772928000,
        analysisBaseAsOfTime: 1773187200,
        latestResolvedMetaDate: 1773273600,
        latestDailyAsOfTime: 1773100800,
      })
    ).toBe(1772928000);

    expect(
      resolveAnalysisBaseAsOfTime({
        mainAsOfTime: 1773014400,
        resolvedCursorAsOfTime: null,
        analysisBaseAsOfTime: 1773187200,
        latestResolvedMetaDate: 1773273600,
        latestDailyAsOfTime: 1773100800,
      })
    ).toBe(1773014400);
  });
});
