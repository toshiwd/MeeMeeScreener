import { describe, expect, it } from "vitest";

import { normalizeSearchInput } from "./searchInput";

describe("normalizeSearchInput", () => {
  it("converts full-width digits to half-width digits", () => {
    expect(normalizeSearchInput("７２０３")).toBe("7203");
  });

  it("keeps stock names and other characters unchanged", () => {
    expect(normalizeSearchInput("トヨタ１２Ａ")).toBe("トヨタ12Ａ");
  });
});
