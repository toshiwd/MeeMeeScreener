import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  get: vi.fn(),
}));

vi.mock("../../api", () => ({
  api: {
    get: mocks.get,
    put: vi.fn(),
    post: vi.fn(),
    defaults: { baseURL: "" },
  },
}));

import { loadAiExplainSettings, streamAiExplain } from "./aiExplainApi";

describe("aiExplainApi", () => {
  beforeEach(() => {
    mocks.get.mockReset();
    vi.unstubAllGlobals();
  });

  it("normalizes malformed settings without crashing the UI", async () => {
    mocks.get.mockResolvedValueOnce({ data: { ok: true } });

    const state = await loadAiExplainSettings();

    expect(state.canShowUi).toBe(false);
    expect(state.canUse).toBe(false);
    expect(state.settings.compareEnabled).toBe(true);
    expect(state.settings.sendImages).toBe(true);
  });

  it("parses a final SSE frame even when the stream has no trailing separator", async () => {
    const encoder = new TextEncoder();
    const read = vi
      .fn()
      .mockResolvedValueOnce({
        done: false,
        value: encoder.encode(
          'event: done\ndata: {"answer":"ok","cached":false,"provider":"sakura","model":"m","latencyMs":12,"error":null}'
        ),
      })
      .mockResolvedValueOnce({ done: true, value: undefined });
    const releaseLock = vi.fn();
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        body: {
          getReader: () => ({ read, releaseLock }),
        },
      })
    );

    const result = await streamAiExplain({
      mode: "explain",
      screenType: "ranking",
      userQuestion: "why",
      snapshot: {},
    });

    expect(result.answer).toBe("ok");
    expect(result.model).toBe("m");
    expect(releaseLock).toHaveBeenCalled();
  });
});
