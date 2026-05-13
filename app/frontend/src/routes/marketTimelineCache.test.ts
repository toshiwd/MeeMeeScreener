import { describe, expect, it } from "vitest";

import type { MarketTimelineFrame } from "../features/market/marketHelpers";
import {
  buildMarketTimelineCacheKey,
  clearMarketTimelineCache,
  describeMarketTimelineSource,
  readMarketTimelineCache,
  writeMarketTimelineCache,
} from "./marketTimelineCache";

class MemoryStorage {
  private readonly entries = new Map<string, string>();

  getItem(key: string) {
    return this.entries.get(key) ?? null;
  }

  setItem(key: string, value: string) {
    this.entries.set(key, value);
  }

  removeItem(key: string) {
    this.entries.delete(key);
  }
}

const makeFrame = (ymd: string): MarketTimelineFrame => ({
  asof: Date.parse(`${ymd}T00:00:00Z`) / 1000,
  label: ymd,
  items: [],
});

describe("market timeline cache", () => {
  it("keeps timeline and snapshot fallback caches separate", () => {
    const storage = new MemoryStorage();
    const timelineFrames = [makeFrame("2026-03-20"), makeFrame("2026-03-21")];
    const fallbackFrames = [makeFrame("2026-03-22")];

    writeMarketTimelineCache(storage, "1d", "snapshot_fallback", fallbackFrames, 180);
    writeMarketTimelineCache(storage, "1d", "timeline", timelineFrames, 180);

    expect(buildMarketTimelineCacheKey("1d", "timeline", 180)).not.toBe(
      buildMarketTimelineCacheKey("1d", "snapshot_fallback", 180)
    );
    expect(readMarketTimelineCache(storage, "1d", 180)).toEqual({
      source: "timeline",
      frames: timelineFrames,
    });
  });

  it("returns snapshot fallback only when timeline cache is unavailable", () => {
    const storage = new MemoryStorage();
    const fallbackFrames = [makeFrame("2026-03-22")];

    writeMarketTimelineCache(storage, "1w", "snapshot_fallback", fallbackFrames, 180);

    expect(readMarketTimelineCache(storage, "1w", 180)).toEqual({
      source: "snapshot_fallback",
      frames: fallbackFrames,
    });
  });

  it("clears fallback cache independently", () => {
    const storage = new MemoryStorage();
    const fallbackFrames = [makeFrame("2026-03-22")];

    writeMarketTimelineCache(storage, "1m", "snapshot_fallback", fallbackFrames, 180);
    clearMarketTimelineCache(storage, "1m", "snapshot_fallback", 180);

    expect(readMarketTimelineCache(storage, "1m", 180)).toBeNull();
  });
});

describe("describeMarketTimelineSource", () => {
  it("only emits a note for snapshot fallback", () => {
    expect(describeMarketTimelineSource("timeline")).toBeNull();
    expect(describeMarketTimelineSource("snapshot_fallback")).toContain("直近の市場状態");
  });
});
