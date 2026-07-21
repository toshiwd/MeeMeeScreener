import { describe, expect, it } from "vitest";
import { getDesktopLayoutViewportHeight, getDesktopUiScale } from "./desktopUiScale";

describe("getDesktopUiScale", () => {
  it.each([
    [1, 1],
    [1.25, 1],
    [1.5, 1],
    [2, 1]
  ])("leaves the OS/WebView DPI scale unchanged at devicePixelRatio %s", (ratio, expected) => {
    expect(getDesktopUiScale(ratio)).toBeCloseTo(expected, 8);
  });

  it.each([0, -1, Number.NaN, Number.POSITIVE_INFINITY])(
    "remains unscaled for invalid ratio %s",
    (ratio) => {
      expect(getDesktopUiScale(ratio)).toBe(1);
    }
  );
});

describe("getDesktopLayoutViewportHeight", () => {
  it.each([
    [1000, 1, 1000],
    [800, 0.8, 1000],
    [672, 2 / 3, 1008],
    [500, 0.5, 1000]
  ])("expands an inner height of %s at scale %s to %s", (height, scale, expected) => {
    expect(getDesktopLayoutViewportHeight(height, scale)).toBeCloseTo(expected, 8);
  });

  it("falls back to the reported inner height for an invalid scale", () => {
    expect(getDesktopLayoutViewportHeight(1000, 0)).toBe(1000);
  });
});
