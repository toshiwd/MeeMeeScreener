import { describe, expect, it } from "vitest";
import {
  buildPersistedMarketViewState,
  getMarketTimelineFrameDateKey,
  resolveInitialMarketCursor,
  type StoredMarketViewState,
} from "./marketViewState";
import type { MarketTimelineFrame } from "../features/market/marketHelpers";

const makeFrame = (ymd: string, overrides: Partial<MarketTimelineFrame> = {}): MarketTimelineFrame => {
  const asof = Date.parse(`${ymd}T00:00:00Z`) / 1000;
  return {
    asof,
    label: ymd,
    items: [],
    ...overrides,
  };
};

describe("getMarketTimelineFrameDateKey", () => {
  it("prefers explicit date over asof and label", () => {
    expect(
      getMarketTimelineFrameDateKey({
        asof: Date.parse("2026-03-19T00:00:00Z") / 1000,
        label: "legacy-label",
        date: "2026-03-18",
      })
    ).toBe("2026-03-18");
  });
});

describe("resolveInitialMarketCursor", () => {
  const frames = [
    makeFrame("2026-03-19"),
    makeFrame("2026-03-20"),
    makeFrame("2026-03-21"),
  ];

  it("defaults to the latest frame when there is no stored cursor state", () => {
    expect(resolveInitialMarketCursor(frames, null)).toEqual({
      index: 2,
      cursorDate: "2026-03-21",
      source: "latest",
    });
  });

  it("ignores legacy index-only state and still starts from the latest frame", () => {
    const legacyState: Partial<StoredMarketViewState> = {
      cursorIndex: 0,
    };
    expect(resolveInitialMarketCursor(frames, legacyState)).toEqual({
      index: 2,
      cursorDate: "2026-03-21",
      source: "latest",
    });
  });

  it("restores by cursorDate before falling back to index", () => {
    const stored: Partial<StoredMarketViewState> = {
      cursorIndex: 0,
      cursorDate: "2026-03-20",
      userInteracted: true,
    };
    expect(resolveInitialMarketCursor(frames, stored)).toEqual({
      index: 1,
      cursorDate: "2026-03-20",
      source: "cursorDate",
    });
  });

  it("falls back to a clamped cursorIndex when the stored cursorDate is missing", () => {
    const stored: Partial<StoredMarketViewState> = {
      cursorIndex: 99,
      cursorDate: "2026-01-01",
      userInteracted: true,
    };
    expect(resolveInitialMarketCursor(frames, stored)).toEqual({
      index: 2,
      cursorDate: "2026-03-21",
      source: "cursorIndex",
    });
  });
});

describe("buildPersistedMarketViewState", () => {
  it("preserves existing cursor state until the user interacts", () => {
    const previous: Partial<StoredMarketViewState> = {
      cursorIndex: 1,
      cursorDate: "2026-03-20",
      userInteracted: true,
    };

    expect(
      buildPersistedMarketViewState({
        period: "1w",
        metric: "flow",
        selectedSector: "33",
        cursorIndex: 2,
        cursorDate: "2026-03-21",
        cursorUserInteracted: false,
        previous,
      })
    ).toEqual({
      stateVersion: 3,
      period: "1w",
      metric: "flow",
      selectedSector: "33",
      cursorIndex: 1,
      cursorDate: "2026-03-20",
      userInteracted: true,
    });
  });

  it("writes the current cursor only after an actual cursor interaction", () => {
    expect(
      buildPersistedMarketViewState({
        period: "1d",
        metric: "rate",
        selectedSector: null,
        cursorIndex: 2,
        cursorDate: "2026-03-21",
        cursorUserInteracted: true,
        previous: null,
      })
    ).toEqual({
      stateVersion: 3,
      period: "1d",
      metric: "rate",
      selectedSector: null,
      cursorIndex: 2,
      cursorDate: "2026-03-21",
      userInteracted: true,
    });
  });

  it("does not persist cursor state before the first user interaction", () => {
    expect(
      buildPersistedMarketViewState({
        period: "1d",
        metric: "rate",
        selectedSector: null,
        cursorIndex: 0,
        cursorDate: "2026-03-19",
        cursorUserInteracted: false,
        previous: null,
      })
    ).toEqual({
      stateVersion: 3,
      period: "1d",
      metric: "rate",
      selectedSector: null,
    });
  });
});
