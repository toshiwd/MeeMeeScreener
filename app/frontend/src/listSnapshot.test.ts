import { describe, expect, it } from "vitest";

import { normalizeScreenerListResponse } from "./listSnapshot";

describe("normalizeScreenerListResponse", () => {
  it("preserves stale metadata from the screener snapshot object response", () => {
    const normalized = normalizeScreenerListResponse({
      items: [{ code: "1001", name: "Nikkei", stage: "WATCH", score: 10, reason: "" }],
      stale: true,
      asOf: "2026-03-13",
      updatedAt: "2026-03-13T03:00:00Z",
      generation: "g1",
      lastError: "refresh failed"
    });

    expect(normalized.items).toHaveLength(1);
    expect(normalized.meta).toEqual({
      stale: true,
      asOf: "2026-03-13",
      updatedAt: "2026-03-13T03:00:00Z",
      generation: "g1",
      lastError: "refresh failed"
    });
  });

  it("keeps legacy array payloads readable without inventing stale errors", () => {
    const normalized = normalizeScreenerListResponse([
      { code: "1306", name: "ETF", stage: "WATCH", score: 8, reason: "" }
    ]);

    expect(normalized.items).toHaveLength(1);
    expect(normalized.meta).toEqual({
      stale: false,
      asOf: null,
      updatedAt: null,
      generation: null,
      lastError: null
    });
  });
});
