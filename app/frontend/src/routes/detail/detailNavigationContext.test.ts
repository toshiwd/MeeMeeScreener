// @vitest-environment jsdom
import { afterEach, describe, expect, it } from "vitest";
import {
  normalizeDetailBackPath,
  normalizeDetailListCodes,
  readDetailListBackPath,
  readDetailListCodes,
  saveDetailListContext,
} from "./detailNavigationContext";

describe("detailNavigationContext", () => {
  afterEach(() => {
    sessionStorage.clear();
  });

  it("allows known product return paths with query strings", () => {
    expect(normalizeDetailBackPath("/ranking?dir=up")).toBe("/ranking?dir=up");
    expect(normalizeDetailBackPath("/positions?tab=held#row")).toBe("/positions?tab=held#row");
    expect(normalizeDetailBackPath("/market")).toBe("/market");
  });

  it("rejects unsafe or unknown return paths", () => {
    expect(normalizeDetailBackPath("https://example.com/ranking")).toBe("/");
    expect(normalizeDetailBackPath("//example.com/ranking")).toBe("/");
    expect(normalizeDetailBackPath("/admin")).toBe("/");
    expect(normalizeDetailBackPath("/ranking-legacy")).toBe("/");
  });

  it("normalizes list codes before storage or navigation", () => {
    expect(normalizeDetailListCodes([" 7203 ", "", "6758", "7203", 123])).toEqual(["7203", "6758"]);
  });

  it("saves and reads normalized detail list context", () => {
    saveDetailListContext("/positions?tab=held", [" 9101 ", "9102", "9101"]);

    expect(sessionStorage.getItem("detailListBack")).toBe("/positions?tab=held");
    expect(sessionStorage.getItem("detailListCodes")).toBe(JSON.stringify(["9101", "9102"]));
    expect(readDetailListBackPath(null)).toBe("/positions?tab=held");
    expect(readDetailListCodes()).toEqual(["9101", "9102"]);
  });

  it("prefers safe route state over stored back path", () => {
    sessionStorage.setItem("detailListBack", "/favorites");

    expect(readDetailListBackPath({ from: "/ranking/tracking?view=ranking" })).toBe(
      "/ranking/tracking?view=ranking"
    );
    expect(readDetailListBackPath({ from: "/admin" })).toBe("/");
  });
});
