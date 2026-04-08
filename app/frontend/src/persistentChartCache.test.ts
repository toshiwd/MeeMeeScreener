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
});
