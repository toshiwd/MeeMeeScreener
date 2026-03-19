// @vitest-environment jsdom
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";
import type { TradexListSummaryRequestItem } from "./tradexSummary";

const useTradexListSummaryMock = vi.fn();
let tradexListSummaryFlagEnabled = true;

vi.mock("../../hooks/useTradexListSummary", () => ({
  useTradexListSummary: (...args: unknown[]) => useTradexListSummaryMock(...args),
}));

vi.mock("./tradexSummary", async () => {
  const actual = await vi.importActual<typeof import("./tradexSummary")>("./tradexSummary");
  return {
    ...actual,
    shouldShowTradexListSummary: () => tradexListSummaryFlagEnabled,
  };
});

import { TradexListSummaryMount } from "./TradexListSummaryMount";

describe("TradexListSummaryMount", () => {
  it("caps warm items for favorites and forwards the hook state", () => {
    tradexListSummaryFlagEnabled = true;
    useTradexListSummaryMock.mockReturnValue({
      loading: false,
      reason: null,
      itemsByKey: {
        "0001:latest": {
          code: "0001",
          asof: null,
          available: true,
          reason: null,
          dominantTone: "buy",
          confidence: 0.8,
          publishReadiness: null,
          reasons: [],
        },
      },
    });

    const items: TradexListSummaryRequestItem[] = Array.from({ length: 40 }).map((_, index) => ({
      code: String(index + 1).padStart(4, "0"),
      asof: null,
    }));
    const markup = renderToStaticMarkup(
      <TradexListSummaryMount backendReady enabled scope="favorites-visible" items={items}>
        {(state) => <div data-testid="summary-state">{Object.keys(state.itemsByKey).length}</div>}
      </TradexListSummaryMount>
    );

    expect(markup).toContain("1");
    expect(useTradexListSummaryMock).toHaveBeenCalledWith(
      expect.objectContaining({
        backendReady: true,
        enabled: true,
        scope: "favorites-visible",
      })
    );
    const firstCallItems = useTradexListSummaryMock.mock.calls[0]?.[0]?.items as
      | TradexListSummaryRequestItem[]
      | undefined;
    expect(firstCallItems).toHaveLength(24);
  });

  it("falls back to nothing when the feature flag is disabled", () => {
    tradexListSummaryFlagEnabled = false;
    useTradexListSummaryMock.mockReset();
    useTradexListSummaryMock.mockReturnValue({
      loading: false,
      reason: "feature flag disabled",
      itemsByKey: {},
    });

    const markup = renderToStaticMarkup(
      <TradexListSummaryMount backendReady enabled scope="grid-visible" items={[]}>
        {(state) => <div data-testid="summary-state">{Object.keys(state.itemsByKey).length}</div>}
      </TradexListSummaryMount>
    );

    expect(markup).toContain("0");
    expect(useTradexListSummaryMock).toHaveBeenCalledWith(
      expect.objectContaining({
        enabled: false,
      })
    );
  });
});
