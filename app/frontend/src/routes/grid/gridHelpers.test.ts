import { describe, expect, it } from "vitest";
import { mergeHealthStatus, normalizeHealthStatus, resolveGridRangeBars } from "./gridHelpers";

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
