import { describe, expect, it } from "vitest";

import { shouldEnableAiExplainProvider } from "./App";

describe("shouldEnableAiExplainProvider", () => {
  it("enables AI settings only on routes that expose AI UI", () => {
    expect(shouldEnableAiExplainProvider("/")).toBe(true);
    expect(shouldEnableAiExplainProvider("/ranking")).toBe(true);
    expect(shouldEnableAiExplainProvider("/detail/8053")).toBe(true);
  });

  it("initializes AI provider state for detail v2 because it uses the full detail surface", () => {
    expect(shouldEnableAiExplainProvider("/detail-v2/8053")).toBe(true);
  });

  it("does not initialize AI provider state for non-detail routes", () => {
    expect(shouldEnableAiExplainProvider("/favorites")).toBe(false);
    expect(shouldEnableAiExplainProvider("/market")).toBe(false);
  });
});
