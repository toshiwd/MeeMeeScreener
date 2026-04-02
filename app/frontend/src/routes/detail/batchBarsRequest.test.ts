import { describe, expect, it } from "vitest";
import { buildDetailBatchBarsRequestPayload, buildSingleBatchBarsRequestPayload } from "./batchBarsRequest";

describe("buildSingleBatchBarsRequestPayload", () => {
  it("includes compare asof when provided", () => {
    expect(
      buildSingleBatchBarsRequestPayload({
        code: "7203",
        timeframe: "daily",
        limit: 240,
        asof: "2026-03-19",
      })
    ).toEqual({
      codes: ["7203"],
      timeframes: ["daily"],
      limit: 240,
      includeProvisional: true,
      asof: "2026-03-19",
    });
  });

  it("omits empty asof and preserves includeBoxes", () => {
    expect(
      buildSingleBatchBarsRequestPayload({
        code: "7203",
        timeframe: "monthly",
        limit: 120,
        includeBoxes: false,
        asof: "   ",
      })
    ).toEqual({
      codes: ["7203"],
      timeframes: ["monthly"],
      limit: 120,
      includeProvisional: true,
      includeBoxes: false,
    });
  });
});

describe("buildDetailBatchBarsRequestPayload", () => {
  it("builds a combined daily/weekly/monthly payload with timeframe limits", () => {
    expect(
      buildDetailBatchBarsRequestPayload({
        code: "7203",
        dailyLimit: 240,
        weeklyLimit: 120,
        monthlyLimit: 120,
        asof: "2026-03-19",
      })
    ).toEqual({
      codes: ["7203"],
      timeframes: ["daily", "weekly", "monthly"],
      limit: 240,
      timeframeLimits: {
        daily: 240,
        weekly: 120,
        monthly: 120,
      },
      includeProvisional: true,
      includeBoxes: true,
      asof: "2026-03-19",
    });
  });
});
