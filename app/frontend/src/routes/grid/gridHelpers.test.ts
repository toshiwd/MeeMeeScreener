import { describe, expect, it } from "vitest";
import { mergeHealthStatus, normalizeHealthStatus } from "./gridHelpers";

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
