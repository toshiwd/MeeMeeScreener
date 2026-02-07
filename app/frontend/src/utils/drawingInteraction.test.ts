import { describe, expect, it } from "vitest";
import {
  buildDrawBoxShape,
  buildPriceBandShape,
  buildTimeZoneShape,
  getHitKindsForTool
} from "./drawingInteraction";

describe("getHitKindsForTool", () => {
  it("returns all kinds when no tool is active", () => {
    expect(getHitKindsForTool(null)).toEqual(["horizontalLine", "drawBox", "priceBand", "timeZone"]);
  });

  it("limits hit targets to active tool only", () => {
    expect(getHitKindsForTool("timeZone")).toEqual(["timeZone"]);
    expect(getHitKindsForTool("priceBand")).toEqual(["priceBand"]);
    expect(getHitKindsForTool("drawBox")).toEqual(["drawBox"]);
    expect(getHitKindsForTool("horizontalLine")).toEqual(["horizontalLine"]);
  });
});

describe("drawing shape builders", () => {
  it("normalizes time zone edges", () => {
    expect(buildTimeZoneShape(200, 100, "sell")).toEqual({
      side: "sell",
      startTime: 100,
      endTime: 200,
      color: undefined
    });
  });

  it("normalizes price band bounds", () => {
    expect(buildPriceBandShape(120, 95, 0.2)).toEqual({
      topPrice: 120,
      bottomPrice: 95,
      opacity: 0.2,
      lineWidth: undefined
    });
  });

  it("normalizes draw box bounds", () => {
    expect(buildDrawBoxShape(200, 100, 95, 120, { opacity: 0.08, color: "#123456" })).toEqual({
      startTime: 100,
      endTime: 200,
      topPrice: 120,
      bottomPrice: 95,
      opacity: 0.08,
      color: "#123456",
      lineWidth: undefined
    });
  });

  it("returns null when required coordinates are missing", () => {
    expect(buildTimeZoneShape(null, 100)).toBeNull();
    expect(buildPriceBandShape(120, null)).toBeNull();
    expect(buildDrawBoxShape(100, 200, 90, null)).toBeNull();
  });
});
