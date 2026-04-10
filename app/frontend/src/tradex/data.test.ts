import { beforeEach, describe, expect, it, vi } from "vitest";

const { tradexFetchJson, tradexFetchJsonWithRetry } = vi.hoisted(() => ({
  tradexFetchJson: vi.fn(),
  tradexFetchJsonWithRetry: vi.fn()
}));

vi.mock("./http", () => ({
  tradexFetchJson,
  tradexFetchJsonWithRetry
}));

import { loadTradexBootstrapFromLegacySources } from "./data";
import { TRADEX_RESEARCH_ENDPOINTS, tradexResearchRoute } from "./researchRoutes";

describe("loadTradexBootstrapFromLegacySources", () => {
  beforeEach(() => {
    tradexFetchJson.mockReset();
    tradexFetchJsonWithRetry.mockReset();

    tradexFetchJson.mockImplementation(async (url: string) => {
      if (url === "/analysis-bridge/status") {
        return {
          publish: {
            publish_id: "pub_2026-03-12_01",
            as_of_date: "2026-03-12",
            published_at: "2026-03-12T15:00:00Z",
            freshness_state: "fresh"
          }
        };
      }
      if (url === tradexResearchRoute(TRADEX_RESEARCH_ENDPOINTS.replayProgress)) {
        return {
          running: false,
          current_run: null,
          recent_runs: []
        };
      }
      if (url === tradexResearchRoute(TRADEX_RESEARCH_ENDPOINTS.stateEvalActionQueue)) {
        return {
          actions: []
        };
      }
      throw new Error(`unexpected tradexFetchJson url: ${url}`);
    });

    tradexFetchJsonWithRetry.mockImplementation(async (url: string) => {
      if (url === "/system/runtime-selection") {
        return {
          selected_logic_id: "family_a",
          selected_logic_version: "v1",
          logic_key: "family_a:v1",
          source_of_truth: "external_analysis"
        };
      }
      if (url === "/system/publish/state") {
        return {
          champion_logic_key: "family_a:v1",
          default_logic_pointer: "family_a:v1",
          source_of_truth: "external_analysis"
        };
      }
      if (url === "/system/publish/queue") {
        return {};
      }
      if (url === "/system/publish/candidates") {
        return {
          items: []
        };
      }
      throw new Error(`unexpected tradexFetchJsonWithRetry url: ${url}`);
    });
  });

  it("uses tradex research endpoints for replay progress and action queue fallback calls", async () => {
    const payload = await loadTradexBootstrapFromLegacySources();

    expect(tradexFetchJson).toHaveBeenCalledWith("/analysis-bridge/status");
    expect(tradexFetchJson).toHaveBeenCalledWith(tradexResearchRoute(TRADEX_RESEARCH_ENDPOINTS.replayProgress));
    expect(tradexFetchJson).toHaveBeenCalledWith(tradexResearchRoute(TRADEX_RESEARCH_ENDPOINTS.stateEvalActionQueue));
    expect(tradexFetchJsonWithRetry).toHaveBeenCalledWith("/system/runtime-selection");
    expect(tradexFetchJsonWithRetry).toHaveBeenCalledWith("/system/publish/state");
    expect(tradexFetchJsonWithRetry).toHaveBeenCalledWith("/system/publish/queue");
    expect(tradexFetchJsonWithRetry).toHaveBeenCalledWith("/system/publish/candidates");
    expect(payload.baseline.publish_id).toBe("pub_2026-03-12_01");
    expect(payload.summary.attention_count).toBe(0);
    expect(payload.summary.publish_id).toBe("pub_2026-03-12_01");
  });
});
