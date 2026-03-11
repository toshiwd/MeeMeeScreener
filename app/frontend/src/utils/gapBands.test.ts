import { describe, expect, it } from "vitest";
import { computeGapBands } from "./gapBands";

type Candle = {
  time: number;
  high: number;
  low: number;
  close?: number;
};

const make = (rows: Array<[number, number, number, number?]>): Candle[] =>
  rows.map(([time, high, low, close]) => ({ time, high, low, close }));

describe("computeGapBands", () => {
  it("ignores gaps smaller than 0.4% by default", () => {
    const candles = make([
      [1, 100, 99, 99.8],
      [2, 100.2, 100.1, 100.15]
    ]);
    const result = computeGapBands(candles, 2, 2);
    expect(result).toEqual([]);
  });

  it("keeps unfilled gap that is 0.4% or larger", () => {
    const candles = make([
      [1, 100, 95, 98],
      [2, 104, 101, 103]
    ]);
    const result = computeGapBands(candles, 2, 2);
    expect(result).toEqual([
      { direction: "up", topPrice: 101, bottomPrice: 100, createdAt: 2, filledAt: null }
    ]);
  });

  it("drops gap once filled by wick touch", () => {
    const candles = make([
      [1, 100, 95, 98],
      [2, 104, 101, 103],
      [3, 106, 99, 102]
    ]);
    const result = computeGapBands(candles, 3, 2);
    expect(result).toEqual([]);
  });

  it("returns newest pending gaps first and limits by global count", () => {
    const candles = make([
      [1, 100, 90, 95],
      [2, 110, 105, 108],
      [3, 120, 115, 118],
      [4, 130, 125, 128]
    ]);
    const result = computeGapBands(candles, 4, 2);
    expect(result).toEqual([
      { direction: "up", topPrice: 125, bottomPrice: 120, createdAt: 4, filledAt: null },
      { direction: "up", topPrice: 115, bottomPrice: 110, createdAt: 3, filledAt: null }
    ]);
  });

  it("uses 0.004 as default minGapRatio when omitted", () => {
    const candles = make([
      [1, 100, 99, 100],
      [2, 101, 100.4, 100.8]
    ]);
    const result = computeGapBands(candles, 2, 2);
    expect(result).toEqual([
      { direction: "up", topPrice: 100.4, bottomPrice: 100, createdAt: 2, filledAt: null }
    ]);
  });
});
