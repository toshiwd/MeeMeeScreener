import { describe, expect, it } from "vitest";
import {
  buildAvailableSectorOptions,
  gridPresetOptions,
  mergeHealthStatus,
  normalizeHealthStatus,
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
    expect(resolveGridRangeBars(5, 5, 120)).toBe(30);
  });

  it("falls back for non-square layouts", () => {
    expect(resolveGridRangeBars(3, 4, 120)).toBe(120);
  });
});

describe("gridPresetOptions", () => {
  it("exposes only square presets", () => {
    expect(gridPresetOptions.map((item) => item.label)).toEqual(["1×1", "2×2", "3×3", "4×4", "5×5"]);
    expect(gridPresetOptions.map((item) => item.bars)).toEqual([180, 90, 60, 45, 30]);
  });
});

describe("buildAvailableSectorOptions", () => {
  it("returns distinct sector options sorted by display name", () => {
    expect(
      buildAvailableSectorOptions([
        { sector33Code: null, sector33Name: null },
        { sector33Code: "30", sector33Name: "輸送用機器" },
        { sector33Code: "10", sector33Name: "銀行業" },
        { sector33Code: "30", sector33Name: "輸送用機器" },
        { sector33Code: "99", sector33Name: "" }
      ])
    ).toEqual([
      { code: "99", name: "99" },
      { code: "10", name: "銀行業" },
      { code: "30", name: "輸送用機器" }
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
