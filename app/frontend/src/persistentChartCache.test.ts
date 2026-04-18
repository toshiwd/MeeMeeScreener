// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  __resetPersistentChartCacheForTests,
  applyChartDataVersion,
  buildPersistentChartCacheBaseKey,
  getPersistentChartFrame,
  setPersistentChartFrame,
} from "./persistentChartCache";

describe("persistentChartCache", () => {
  beforeEach(async () => {
    vi.restoreAllMocks();
    await __resetPersistentChartCacheForTests();
  });

  afterEach(async () => {
    await __resetPersistentChartCacheForTests();
  });

  it("builds a stable cache key without dataVersion", () => {
    expect(
      buildPersistentChartCacheBaseKey({
        code: "7203",
        timeframe: "daily",
        limit: 2000,
        asof: "2026-04-04",
        includeBoxes: true,
      })
    ).toBe("7203|daily|2000|2026-04-04|boxes:1");
  });

  it("invalidates old-version entries after dataVersion changes", async () => {
    await setPersistentChartFrame(
      {
        code: "7203",
        timeframe: "daily",
        limit: 2000,
        includeBoxes: true,
        dataVersion: "v1",
      },
      {
        bars: [[1, 2, 3, 4, 5, 6]],
        boxes: [],
        fetchedAt: 1,
        dataVersion: "v1",
        provenance: null,
      }
    );

    const cachedV1 = await getPersistentChartFrame({
      code: "7203",
      timeframe: "daily",
      limit: 2000,
      includeBoxes: true,
    });
    expect(cachedV1?.dataVersion).toBe("v1");

    await applyChartDataVersion("v2");

    const cachedAfterSwitch = await getPersistentChartFrame({
      code: "7203",
      timeframe: "daily",
      limit: 2000,
      includeBoxes: true,
    });
    expect(cachedAfterSwitch).toBeNull();
  });

  it("returns the latest saved frame when no active dataVersion is known yet", async () => {
    await setPersistentChartFrame(
      {
        code: "6758",
        timeframe: "monthly",
        limit: 240,
        includeBoxes: true,
        dataVersion: "v1",
      },
      {
        bars: [[10, 11, 12, 13, 14, 15]],
        boxes: [],
        fetchedAt: 10,
        dataVersion: "v1",
        provenance: null,
      }
    );

    await __resetPersistentChartCacheForTests();
    await setPersistentChartFrame(
      {
        code: "6758",
        timeframe: "monthly",
        limit: 240,
        includeBoxes: true,
        dataVersion: "v1",
      },
      {
        bars: [[10, 11, 12, 13, 14, 15]],
        boxes: [],
        fetchedAt: 10,
        dataVersion: "v1",
        provenance: null,
      }
    );

    const cached = await getPersistentChartFrame({
      code: "6758",
      timeframe: "monthly",
      limit: 240,
      includeBoxes: true,
    });
    expect(cached?.bars).toEqual([[10, 11, 12, 13, 14, 15]]);
  });

  it("round-trips provenance and labels indexeddb reads", async () => {
    await setPersistentChartFrame(
      {
        code: "9984",
        timeframe: "daily",
        limit: 120,
        includeBoxes: false,
        dataVersion: "v2",
      },
      {
        bars: [[1, 2, 3, 4, 5, 6]],
        boxes: [],
        fetchedAt: 20,
        dataVersion: "v2",
        provenance: {
          chart_source_provider: "runtime_stock_db.daily_bars",
          chart_source_type: "confirmed",
          chart_source_path_or_identifier: "C:/tmp/stocks.duckdb#daily_bars",
          chart_requested_date: 20260416,
          chart_last_confirmed_date: 20260403,
          chart_last_provisional_date: null,
          chart_date_match_status: "lagged_provisional",
          chart_source_freshness_status: "lagged",
          chart_data_classification: "mixed",
          chart_aggregation_source: "direct",
        },
      }
    );

    const cached = await getPersistentChartFrame({
      code: "9984",
      timeframe: "daily",
      limit: 120,
      includeBoxes: false,
      dataVersion: "v2",
    });
    expect(cached?.cacheSource).toBe("memory");
    expect(cached?.provenance?.chart_source_provider).toBe("runtime_stock_db.daily_bars");
  });
});
