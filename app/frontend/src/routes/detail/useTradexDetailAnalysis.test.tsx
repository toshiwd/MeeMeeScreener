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
    promotion_review: {
      as_of_date: "2026-03-19",
      champion_version: "champion-v1",
      challenger_version: "challenger-v1",
      sample_count: 25,
      expectancy_delta: 0.018,
      improved_expectancy: true,
      mae_non_worse: true,
      adverse_move_non_worse: true,
      stable_window: true,
      alignment_ok: true,
      readiness_pass: true,
      reason_codes: ["readiness_pass"],
      approval_decision: {
        decision_id: "pub:approved:1",
        decision: "approved",
        note: "ready_for_authoritative_cutover",
        actor: "codex",
        created_at: "2026-03-19T09:00:00Z",
      },
    },
    source: "tradex_analysis",
    schema_version: "tradex_analysis_output_v1",
  },
  forecast_surface: {
    code: symbol,
    asof: "2026-03-19",
    rows: [
      {
        code: symbol,
        side: "long",
        action_state: "enter",
        direction_prob: 0.71,
        expected_upside: 0.084,
        expected_downside: -0.031,
        invalidation_price: 1234.5,
        reason_codes: ["signal_buy_qualified"],
        setup_tags: ["breakout"],
        opportunity_score: 0.63,
        freshness_state: "fresh",
      },
    ],
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

  const forecast = state.forecastSurface?.rows[0];

  return (
    <div data-testid="tradex-state">
      {state.loading
        ? "loading"
        : `${state.analysis?.symbol ?? state.reason ?? "none"}:${forecast?.actionState ?? "no-forecast"}:${forecast?.directionProb ?? "na"}:${state.analysis?.promotionReview?.approvalDecision?.decision ?? "no-decision"}`}
    </div>
  );
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

  it("normalizes forecast surface rows when present", async () => {
    getMock.mockResolvedValueOnce({ data: makeResponse("9984") });

    const container = document.createElement("div");
    document.body.appendChild(container);
    const root = createRoot(container);

    await act(async () => {
      root.render(<Probe code="9984" />);
      await Promise.resolve();
    });

    expect(container.textContent).toContain("9984");
    expect(container.textContent).toContain("enter");
    expect(container.textContent).toContain("0.71");
    expect(container.textContent).toContain("approved");

    act(() => {
      root.unmount();
    });
    container.remove();
  });
});
