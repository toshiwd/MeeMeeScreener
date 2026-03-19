// @vitest-environment jsdom
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

const useTradexDetailAnalysisMock = vi.fn();
let tradexDetailFlagEnabled = true;

vi.mock("./tradexAnalysis", () => ({
  shouldShowTradexDetailAnalysis: () => tradexDetailFlagEnabled,
  buildTradexDetailAnalysisWarmRequest: (code: string | null, asof: number | null) =>
    code ? { code, asof } : null,
}));

vi.mock("./useTradexDetailAnalysis", () => ({
  useTradexDetailAnalysis: (...args: unknown[]) => useTradexDetailAnalysisMock(...args),
}));

vi.mock("./TradexAnalysisPanel", () => ({
  TradexAnalysisPanel: ({ state }: { state: { available: boolean; analysis: { symbol: string } | null } }) => (
    <div data-testid="tradex-panel">{state.available ? state.analysis?.symbol ?? "unknown" : "unavailable"}</div>
  ),
}));

import { TradexAnalysisMount } from "./TradexAnalysisMount";

describe("TradexAnalysisMount", () => {
  it("keeps the detail mount localized and passes warm request context", () => {
    tradexDetailFlagEnabled = true;
    useTradexDetailAnalysisMock.mockReturnValue({
      available: true,
      reason: null,
      analysis: { symbol: "7203" },
      loading: false,
    });

    const markup = renderToStaticMarkup(
      <TradexAnalysisMount
        backendReady
        readyToFetch
        analysisFetchEnabled
        code="7203"
        asof={20260319}
        formatPercentLabel={(value) => `${Math.round((value ?? 0) * 100)}%`}
        formatSignedPercentLabel={(value) => `${Math.round((value ?? 0) * 100)}%`}
        formatNumber={(value) => String(value ?? "--")}
      />
    );

    expect(markup).toContain("7203");
    expect(useTradexDetailAnalysisMock).toHaveBeenCalledWith(
      expect.objectContaining({
        backendReady: true,
        readyToFetch: true,
        enabled: true,
        code: "7203",
        asof: 20260319,
      })
    );
  });

  it("falls back to nothing when the feature flag is disabled", () => {
    tradexDetailFlagEnabled = false;
    useTradexDetailAnalysisMock.mockReset();
    useTradexDetailAnalysisMock.mockReturnValue({
      available: false,
      reason: "analysis unavailable",
      analysis: null,
      loading: false,
    });

    const markup = renderToStaticMarkup(
      <TradexAnalysisMount
        backendReady
        readyToFetch
        analysisFetchEnabled
        code="7203"
        asof={20260319}
        formatPercentLabel={(value) => `${Math.round((value ?? 0) * 100)}%`}
        formatSignedPercentLabel={(value) => `${Math.round((value ?? 0) * 100)}%`}
        formatNumber={(value) => String(value ?? "--")}
      />
    );

    expect(markup).toBe("");
    expect(useTradexDetailAnalysisMock).toHaveBeenCalledWith(
      expect.objectContaining({
        enabled: false,
        code: "7203",
        asof: 20260319,
      })
    );
  });
});
