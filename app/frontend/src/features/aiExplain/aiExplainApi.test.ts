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

import { loadAiExplainSettings } from "./aiExplainApi";

describe("aiExplainApi", () => {
  beforeEach(() => {
    mocks.get.mockReset();
  });

  it("normalizes malformed settings without crashing the UI", async () => {
    mocks.get.mockResolvedValueOnce({ data: { ok: true } });

    const state = await loadAiExplainSettings();

    expect(state.canShowUi).toBe(false);
    expect(state.canUse).toBe(false);
    expect(state.settings.compareEnabled).toBe(true);
    expect(state.settings.sendImages).toBe(true);
  });
});
