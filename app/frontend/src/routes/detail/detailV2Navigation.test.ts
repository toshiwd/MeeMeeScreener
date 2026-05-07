import { describe, expect, it } from "vitest";
import { shouldUseDetailV2Navigation } from "./detailV2Navigation";

describe("shouldUseDetailV2Navigation", () => {
  it("uses v2 detail navigation by default", () => {
    expect(shouldUseDetailV2Navigation("", undefined)).toBe(true);
  });

  it("keeps old detail navigation when explicitly opted out", () => {
    expect(shouldUseDetailV2Navigation("?detailV2=0", undefined)).toBe(false);
    expect(shouldUseDetailV2Navigation("?detail_v2=false", "true")).toBe(false);
    expect(shouldUseDetailV2Navigation("", "off")).toBe(false);
  });

  it("keeps v2 navigation from an explicit ranking query opt-in", () => {
    expect(shouldUseDetailV2Navigation("?detailV2=1", undefined)).toBe(true);
    expect(shouldUseDetailV2Navigation("?foo=bar&detail_v2=true", undefined)).toBe(true);
  });

  it("keeps v2 navigation from the build-time flag", () => {
    expect(shouldUseDetailV2Navigation("", "on")).toBe(true);
    expect(shouldUseDetailV2Navigation("", "true")).toBe(true);
  });
});
