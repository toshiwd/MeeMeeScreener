import { describe, expect, it } from "vitest";
import {
  buildAvailableSectorOptions,
  buildVisibleRequestSignature,
  TERMINAL_JOB_STATUS,
  gridPresetOptions,
  mergeHealthStatus,
  normalizeHealthStatus,
  isGridBarsDependentSortKey,
  resolveGridPrimaryChangeValue,
  resolveGridFastSortValue,
  resolveGridChartPrefetchWindow,
  resolveGridRefineWindow,
  resolveGridSignalSortScore,
  resolveGridRangeBars,
  resolveGridVolumeSurgeRatio
} from "./gridHelpers";

describe("normalizeHealthStatus", () => {
  it("preserves unknown txt_count as null", () => {
    expect(normalizeHealthStatus({ txt_count: undefined, code_txt_missing: undefined }).txt_count).toBeNull();
  });
});

describe("mergeHealthStatus", () => {
  it("keeps previous txt metadata when light health omits it", () => {
    const prev = {
      txt_count: 698,
      code_count: 4200,
      last_updated: "2026-03-11T04:00:00Z",
      code_txt_missing: false,
      pan_out_txt_dir: "C:/txt"
    };

    expect(
      mergeHealthStatus(prev, {
        ok: true,
        ready: true,
        txt_count: undefined,
        last_updated: null,
        code_txt_missing: undefined
      })
    ).toEqual(prev);
  });

  it("applies explicit txt_count values from deep health", () => {
    const prev = {
      txt_count: 698,
      code_count: 4200,
      last_updated: "2026-03-11T04:00:00Z",
      code_txt_missing: false,
      pan_out_txt_dir: "C:/txt"
    };

    expect(mergeHealthStatus(prev, { txt_count: 0, code_txt_missing: false }).txt_count).toBe(0);
  });
});

describe("resolveGridRangeBars", () => {
  it("maps square grid density to the expected bar count", () => {
    expect(resolveGridRangeBars(1, 1, 120)).toBe(180);
    expect(resolveGridRangeBars(2, 2, 120)).toBe(90);
    expect(resolveGridRangeBars(3, 3, 120)).toBe(60);
    expect(resolveGridRangeBars(4, 4, 120)).toBe(45);
  });

  it("falls back for non-square layouts", () => {
    expect(resolveGridRangeBars(3, 4, 120)).toBe(120);
  });
});

describe("gridPresetOptions", () => {
  it("exposes only square presets", () => {
    expect(gridPresetOptions.map((item) => item.label)).toEqual(["1x1", "2x2", "3x3", "4x4"]);
    expect(gridPresetOptions.map((item) => item.bars)).toEqual([180, 90, 60, 45]);
  });
});

describe("buildAvailableSectorOptions", () => {
  it("returns distinct sector options sorted by display name", () => {
    expect(
      buildAvailableSectorOptions([
        { sector33Code: null, sector33Name: null },
        { sector33Code: "30", sector33Name: "情報・通信業" },
        { sector33Code: "10", sector33Name: "食料品" },
        { sector33Code: "30", sector33Name: "情報・通信業" },
        { sector33Code: "99", sector33Name: "" }
      ])
    ).toEqual([
      { code: "99", name: "99" },
      { code: "30", name: "情報・通信業" },
      { code: "10", name: "食料品" }
    ]);
  });

  it("returns an empty list when there is no usable sector data", () => {
    expect(buildAvailableSectorOptions([{ sector33Code: null, sector33Name: "UNCLASSIFIED" }])).toEqual([]);
  });
});

describe("resolveGridVolumeSurgeRatio", () => {
  it("returns the latest volume ratio against the trailing average", () => {
    const bars = [
      [1, 1, 1, 1, 10, 100],
      [2, 1, 1, 1, 10, 100],
      [3, 1, 1, 1, 10, 200]
    ];
    expect(resolveGridVolumeSurgeRatio(bars)).toBeCloseTo(1.5);
  });

  it("returns null when there is no usable volume", () => {
    expect(resolveGridVolumeSurgeRatio([[1, 1, 1, 1, 10, 0]])).toBeNull();
  });
});

describe("resolveGridPrimaryChangeValue", () => {
  const ticker = {
    chg1D: 0.01,
    chg1W: 0.05,
    chg1M: 0.12
  };

  it("uses the daily change on daily view", () => {
    expect(resolveGridPrimaryChangeValue(ticker, "daily")).toBe(0.01);
  });

  it("uses the weekly change on weekly view", () => {
    expect(resolveGridPrimaryChangeValue(ticker, "weekly")).toBe(0.05);
  });

  it("uses the monthly change on monthly view", () => {
    expect(resolveGridPrimaryChangeValue(ticker, "monthly")).toBe(0.12);
  });

  it("prefers the change computed from loaded bars when available", () => {
    const bars = [
      [20260421, 1, 1, 1, 100, 10],
      [20260422, 1, 1, 1, 110, 12]
    ];
    expect(resolveGridPrimaryChangeValue(ticker, "daily", bars)).toBeCloseTo(0.1);
  });
});

describe("isGridBarsDependentSortKey", () => {
  it("identifies bars-dependent sort keys", () => {
    expect(isGridBarsDependentSortKey("ma20Dev")).toBe(true);
    expect(isGridBarsDependentSortKey("volumeSurge")).toBe(true);
    expect(isGridBarsDependentSortKey("code")).toBe(false);
  });
});

describe("buildVisibleRequestSignature", () => {
  it("normalizes visible codes independently of order and duplicates", () => {
    const a = buildVisibleRequestSignature("daily", 60, "sig", ["7203", "9984", "7203"]);
    const b = buildVisibleRequestSignature("daily", 60, "sig", ["9984", "7203"]);
    expect(a).toBe(b);
  });
});

describe("resolveGridFastSortValue", () => {
  const ticker = {
    code: "7203",
    name: "トヨタ",
    chg1D: 0.01,
    chg1W: 0.02,
    chg1M: 0.03,
    chg1Q: 0.04,
    chg1Y: 0.05,
    prevWeekChg: 0.06,
    prevMonthChg: 0.07,
    prevQuarterChg: 0.08,
    prevYearChg: 0.09,
    scores: {
      upScore: 1.5,
      downScore: 2.5,
      overheatUp: 3.5,
      overheatDown: 4.5
    },
    swingScore: 5.5,
    swingLongScore: null,
    swingShortScore: null,
    mlEv20Net: 6.5,
    mlPUpShort: 7.5,
    mlPDownShort: 8.5,
    boxState: "IN_BOX",
    shortScore: 9.5,
    aScore: 10.5,
    bScore: 11.5,
    shortPriorityScore: 12.5,
    entryPriorityScore: 13.5,
    buyStateScore: 14.5,
    sector33Code: "30"
  } as any;

  it("uses snapshot-only fields for fast sort values", () => {
    expect(resolveGridFastSortValue(ticker, "code", "1M")).toBe("7203");
    expect(resolveGridFastSortValue(ticker, "buySignalLatest", "1M")).toBe(14.5);
    expect(resolveGridFastSortValue(ticker, "sellSignalLatest", "1M")).toBe(12.5);
    expect(resolveGridFastSortValue(ticker, "ma20Dev", "1M")).toBeNull();
    expect(resolveGridFastSortValue(ticker, "performance", "1Q")).toBe(0.04);
  });
});

describe("resolveGridRefineWindow", () => {
  it("expands the visible window by one viewport", () => {
    expect(resolveGridRefineWindow(10, 20, 100, 3, 3)).toEqual({ start: 1, stop: 29 });
  });
});

describe("resolveGridChartPrefetchWindow", () => {
  it("loads the visible rows plus nearby rows so chart tiles are ready before scrolling into view", () => {
    expect(resolveGridChartPrefetchWindow(5, 7, 180, 3, 3)).toEqual({ start: 6, stop: 50 });
  });

  it("clamps the prefetch window to the available list", () => {
    expect(resolveGridChartPrefetchWindow(0, 2, 20, 3, 3)).toEqual({ start: 0, stop: 19 });
  });

  it("returns null when the list is empty", () => {
    expect(resolveGridChartPrefetchWindow(0, 2, 0, 3, 3)).toBeNull();
  });
});

describe("resolveGridSignalSortScore", () => {
  const metrics = {
    counts: {
      7: { upCount: 7, downCount: 0, pendingSide: null },
      20: { upCount: 16, downCount: 0, pendingSide: null },
      60: { upCount: 0, downCount: 0, pendingSide: null },
      100: { upCount: 0, downCount: 0, pendingSide: null }
    },
    signals: [{ label: "20上:16", kind: "warning", priority: 200 }],
    trendStrength: 11,
    exhaustionRisk: 0
  };

  it("rewards direction-matched buy signals", () => {
    expect(resolveGridSignalSortScore(metrics, 50_000_000, "up")).toBeGreaterThan(
      resolveGridSignalSortScore(metrics, 50_000_000, "down")
    );
  });
});

describe("TERMINAL_JOB_STATUS", () => {
  it("treats skipped as terminal", () => {
    expect(TERMINAL_JOB_STATUS.has("skipped")).toBe(true);
  });
});
