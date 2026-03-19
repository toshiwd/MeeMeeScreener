// @vitest-environment jsdom
import { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, describe, expect, it, vi } from "vitest";
import { useTradexDetailAnalysis } from "./useTradexDetailAnalysis";

const { getMock } = vi.hoisted(() => ({
  getMock: vi.fn(),
}));

vi.mock("../../api", () => ({
  api: {
    get: getMock,
  },
}));

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

const makeResponse = (symbol: string) => ({
  available: true,
  reason: null,
  analysis: {
    symbol,
    asof: "2026-03-19",
    side_ratios: { buy: 0.61, neutral: 0.24, sell: 0.15 },
    confidence: 0.77,
    reasons: [`symbol=${symbol}`],
    candidate_comparisons: [],
    publish_readiness: { ready: true, status: "ready", reasons: [] },
    override_state: { present: false, source: null, logic_key: null, logic_version: null, reason: null },
    source: "tradex_analysis",
    schema_version: "tradex_analysis_output_v1",
  },
});

function Probe({ code }: { code: string }) {
  const state = useTradexDetailAnalysis({
    backendReady: true,
    readyToFetch: true,
    enabled: true,
    code,
    asof: null,
  });

  return <div data-testid="tradex-state">{state.loading ? "loading" : state.analysis?.symbol ?? state.reason ?? "none"}</div>;
}

afterEach(() => {
  getMock.mockReset();
});

describe("useTradexDetailAnalysis", () => {
  it("ignores stale responses when the symbol changes", async () => {
    let resolveFirst: ((value: { data: unknown }) => void) | null = null;
    let resolveSecond: ((value: { data: unknown }) => void) | null = null;

    getMock
      .mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            resolveFirst = resolve;
          })
      )
      .mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            resolveSecond = resolve;
          })
      );

    const container = document.createElement("div");
    document.body.appendChild(container);
    const root = createRoot(container);

    await act(async () => {
      root.render(<Probe code="7203" />);
    });
    expect(container.textContent).toContain("loading");

    await act(async () => {
      root.render(<Probe code="6758" />);
    });
    expect(container.textContent).toContain("loading");

    await act(async () => {
      resolveFirst?.({ data: makeResponse("7203") });
      await Promise.resolve();
    });
    expect(container.textContent).toContain("loading");
    expect(container.textContent).not.toContain("7203");

    await act(async () => {
      resolveSecond?.({ data: makeResponse("6758") });
      await Promise.resolve();
    });

    expect(container.textContent).toContain("6758");
    expect(container.textContent).not.toContain("7203");

    act(() => {
      root.unmount();
    });
    container.remove();
  });
});
