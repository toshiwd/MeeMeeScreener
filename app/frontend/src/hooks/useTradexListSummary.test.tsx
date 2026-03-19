// @vitest-environment jsdom
import { act, useMemo } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "../api";

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

let useTradexListSummary: typeof import("./useTradexListSummary").useTradexListSummary;
const originalPost = api.post;
let postMock: ReturnType<typeof vi.fn> | null = null;

type ProbeProps = {
  codes: string[];
  asof?: string | null;
};

function Probe({ codes, asof = null }: ProbeProps) {
  const items = useMemo(
    () => codes.map((code) => ({ code, asof })),
    [codes, asof]
  );
  const state = useTradexListSummary({
    backendReady: true,
    enabled: true,
    scope: "grid-visible",
    items,
  });

  const firstKey = codes[0] ? `${codes[0]}:${asof ?? "latest"}` : "none";
  const first = state.itemsByKey[firstKey];

  return (
    <div data-testid="summary-state">
      {state.loading ? "loading" : "idle"}|{state.reason ?? "none"}|{first?.dominantTone ?? "none"}
    </div>
  );
}

const makeResponse = (code: string) => ({
  available: true,
  reason: null,
  scope: "grid-visible",
  items: [
    {
      code,
      asof: "2026-03-19",
      available: true,
      reason: null,
      dominant_tone: "buy",
      confidence: 0.84,
      publish_readiness: {
        ready: true,
        status: "ready",
        reasons: ["validation_pass"],
        candidate_key: `candidate:${code}`,
        approved: true,
      },
      reasons: ["tone=up", "pattern=breakout"],
    },
  ],
});

beforeEach(() => {
  vi.useFakeTimers();
});

beforeEach(async () => {
  if (!useTradexListSummary) {
    ({ useTradexListSummary } = await import("./useTradexListSummary"));
  }
  postMock = vi.fn();
  api.post = postMock as typeof api.post;
});

afterEach(() => {
  postMock?.mockReset();
  api.post = originalPost;
  vi.runOnlyPendingTimers();
  vi.useRealTimers();
});

describe("useTradexListSummary", () => {
  it("dedupes repeated reads through the short-lived cache", async () => {
    postMock?.mockImplementation(() => Promise.resolve({ data: makeResponse("7203") }));

    const container = document.createElement("div");
    document.body.appendChild(container);
    const root = createRoot(container);

    await act(async () => {
      root.render(<Probe codes={["7203"]} asof="2026-03-19" />);
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(150);
    });

    expect(container.textContent).toContain("idle");
    expect(postMock).toHaveBeenCalledTimes(1);

    act(() => {
      root.unmount();
    });

    const nextContainer = document.createElement("div");
    document.body.appendChild(nextContainer);
    const nextRoot = createRoot(nextContainer);

    await act(async () => {
      nextRoot.render(<Probe codes={["7203"]} asof="2026-03-19" />);
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(10);
    });

    expect(nextContainer.textContent).toContain("buy");
    expect(postMock).toHaveBeenCalledTimes(1);

    act(() => {
      nextRoot.unmount();
    });
    container.remove();
    nextContainer.remove();
  });

  it("ignores stale responses when the symbol changes", async () => {
    let resolveFirst: ((value: { data: unknown }) => void) | null = null;

    postMock
      .mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            resolveFirst = resolve;
          })
      );

    const container = document.createElement("div");
    document.body.appendChild(container);
    const root = createRoot(container);

    await act(async () => {
      root.render(<Probe codes={["7203"]} asof="2026-03-19" />);
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(150);
    });

    await act(async () => {
      root.render(<Probe codes={["6758"]} asof="2026-03-19" />);
    });

    await act(async () => {
      resolveFirst?.({ data: makeResponse("7203") });
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(container.textContent).not.toContain("7203");

    act(() => {
      root.unmount();
    });
    container.remove();
  });
});
