import { describe, expect, it } from "vitest";

import {
  MAX_DAILY_BATCH_BARS_LIMIT,
  MAX_MONTHLY_BATCH_BARS_LIMIT,
  incrementBarLimit,
  resolveAnalysisBaseAsOfTime,
  resolveAutoAnalysisBackfillRequest,
  resolveLatestAnalysisAvailableAsOfTime,
  resolveLatestResolvedMetaDate,
} from "./detailHelpers";

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
  it("uses the latest available date between resolved meta and daily bars", () => {
    expect(
      resolveAnalysisBaseAsOfTime({
        mainAsOfTime: null,
        resolvedCursorAsOfTime: null,
        analysisBaseAsOfTime: null,
        latestResolvedMetaDate: 1773187200,
        latestDailyAsOfTime: 1773100800,
      })
    ).toBe(1773187200);

    expect(
      resolveAnalysisBaseAsOfTime({
        mainAsOfTime: null,
        resolvedCursorAsOfTime: null,
        analysisBaseAsOfTime: null,
        latestResolvedMetaDate: 1773187200,
        latestDailyAsOfTime: 1773273600,
      })
    ).toBe(1773273600);
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

describe("resolveLatestAnalysisAvailableAsOfTime", () => {
  it("prefers the newer daily date when yahoo provisional data is ahead of resolved meta", () => {
    expect(
      resolveLatestAnalysisAvailableAsOfTime({
        latestResolvedMetaDate: 1773187200,
        latestDailyAsOfTime: 1773273600,
      })
    ).toBe(1773273600);
  });
});

describe("resolveAutoAnalysisBackfillRequest", () => {
  it("targets only the current analysis date when panel data is stale", () => {
    expect(
      resolveAutoAnalysisBackfillRequest({
        code: "2413",
        analysisAsOfTime: 1773273600,
        analysisMissingDataVisible: true,
      })
    ).toEqual({
      requestKey: "current:2413:20260312",
      queuedMessage: "最新の解析判定を準備しています。",
      params: {
        start_dt: 20260312,
        end_dt: 20260312,
        include_sell: false,
        include_phase: false,
        force_recompute: false,
      },
    });
  });

  it("returns null when panel data is already available", () => {
    expect(
      resolveAutoAnalysisBackfillRequest({
        code: "2413",
        analysisAsOfTime: 1773273600,
        analysisMissingDataVisible: false,
      })
    ).toBeNull();
  });
});

describe("incrementBarLimit", () => {
  it("clamps to the provided batch API limit", () => {
    expect(incrementBarLimit(1000, 500, MAX_DAILY_BATCH_BARS_LIMIT)).toBe(1500);
    expect(incrementBarLimit(MAX_DAILY_BATCH_BARS_LIMIT, 1000, MAX_DAILY_BATCH_BARS_LIMIT)).toBe(
      MAX_DAILY_BATCH_BARS_LIMIT
    );
    expect(incrementBarLimit(1900, 500, MAX_MONTHLY_BATCH_BARS_LIMIT)).toBe(MAX_MONTHLY_BATCH_BARS_LIMIT);
  });
});
