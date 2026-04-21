import { beforeEach, describe, expect, it, vi } from "vitest";

const { tradexFetchJson, tradexFetchJsonWithRetry } = vi.hoisted(() => ({
  tradexFetchJson: vi.fn(),
  tradexFetchJsonWithRetry: vi.fn()
}));

vi.mock("./http", () => ({
  tradexFetchJson,
  tradexFetchJsonWithRetry
}));

import { loadTradexBootstrap, loadTradexBootstrapFromLegacySources } from "./data";
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
          },
          authoritative_state: {
            status: "authoritative",
            decision: "approved",
            readiness_pass: true,
            sample_count: 25,
            reason_codes: ["readiness_pass"],
            publish_id: "pub_2026-03-12_01",
            as_of_date: "2026-03-12",
            note: "ready_for_authoritative_cutover"
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
      if (url === tradexResearchRoute(TRADEX_RESEARCH_ENDPOINTS.forecastSurfaceProjection)) {
        return {
          projection: {
            summary: {
              as_of_date: "2026-03-12",
              model_version: "v1",
              universe_code_count: 1,
              expected_row_count: 2,
              actual_row_count: 2,
              missing_row_count: 0,
              coverage_ratio: 1,
              feature_frame_version: "ff-1",
              market_opportunity_score_enabled: true,
              personal_fit_score_enabled: false,
              side_counts: { long: 1, short: 1 },
              action_counts: { enter: 1, wait: 1, skip: 0 },
              source_context_presence: {},
              alerts: [],
              created_at: "2026-03-12T09:00:00Z"
            },
            long_rank: [
              {
                as_of_date: "2026-03-12",
                code: "1301",
                side: "long",
                action_state: "enter",
                direction_prob: 0.82,
                expected_ret_20: 0.12,
                expected_upside: 0.18,
                expected_downside: 0.04,
                invalidation_price: 100,
                setup_tags: ["trend"],
                reason_codes: ["gap_ok"],
                opportunity_score: 0.91,
                freshness_state: "fresh"
              }
            ],
            short_rank: [],
            high_risk_avoid: [],
            watchlist_promotions: []
          }
        };
      }
      throw new Error(`unexpected tradexFetchJson url: ${url}`);
    });

    tradexFetchJsonWithRetry.mockImplementation(async (url: string) => {
      if (url === "/system/runtime-selection?surface=tradex") {
        return {
          selected_logic_id: "family_a",
          selected_logic_version: "v1",
          logic_key: "family_a:v1",
          source_of_truth: "external_analysis"
        };
      }
      if (url === "/system/publish/state?surface=tradex") {
        return {
          champion_logic_key: "family_a:v1",
          default_logic_pointer: "family_a:v1",
          source_of_truth: "external_analysis"
        };
      }
      if (url === "/system/publish/queue?surface=tradex") {
        return {};
      }
      if (url === "/system/publish/candidates?surface=tradex") {
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
    expect(tradexFetchJson).toHaveBeenCalledWith(tradexResearchRoute(TRADEX_RESEARCH_ENDPOINTS.forecastSurfaceProjection));
    expect(tradexFetchJsonWithRetry).toHaveBeenCalledWith("/system/runtime-selection?surface=tradex");
    expect(tradexFetchJsonWithRetry).toHaveBeenCalledWith("/system/publish/state?surface=tradex");
    expect(tradexFetchJsonWithRetry).toHaveBeenCalledWith("/system/publish/queue?surface=tradex");
    expect(tradexFetchJsonWithRetry).toHaveBeenCalledWith("/system/publish/candidates?surface=tradex");
    expect(payload.baseline.publish_id).toBe("pub_2026-03-12_01");
    expect(payload.summary.attention_count).toBe(0);
    expect(payload.summary.publish_id).toBe("pub_2026-03-12_01");
    expect(payload.summary.authoritative_state).toBe("authoritative");
    expect(payload.summary.authoritative_decision).toBe("approved");
    expect(payload.summary.authoritative_ready).toBe(true);
    expect(payload.forecast_surface_projection?.projection?.summary.coverage_ratio).toBe(1);
    expect(payload.forecast_surface_projection?.projection?.long_rank[0]?.code).toBe("1301");
  });
});

describe("loadTradexBootstrap", () => {
  it("preserves forecast surface projection from the backend bootstrap response", async () => {
    tradexFetchJson.mockReset();
    tradexFetchJsonWithRetry.mockReset();
    tradexFetchJson.mockImplementation(async (url: string) => {
      if (url === "/tradex/bootstrap") {
        return {
          baseline: {
            logic_id: "family_a",
            version: "v1",
            published_at: "2026-03-12T15:00:00Z",
            publish_id: "pub_2026-03-12_01"
          },
          summary: {
            as_of_date: "2026-03-12",
            freshness_state: "fresh",
            replay_status: "running",
            replay_phase: "review",
            attention_count: 0,
            candidate_count: 0,
            champion_logic_key: "family_a:v1",
            publish_id: "pub_2026-03-12_01",
            authoritative_state: "authoritative",
            authoritative_decision: "approved",
            authoritative_ready: true
          },
          candidates: [],
          live_strategy_judgement: {
            status: "available",
            reason: null,
            experiment_id: "exp_live_001",
            hypothesis_id: "live-trader-foundation-1301-20260403-numeric_baseline_v1",
            generated_at: "2026-04-12T11:33:51Z",
            target: {
              code: "1301",
              as_of_date: "20260403",
              side: "long",
              judgement_type: "close_based_daily_buy_v1"
            },
            primary_adapter_id: "numeric_baseline_v1",
            machine_action_state: "enter",
            human_readable_judgement: "buy",
            buy_score: 0.81,
            environment_score: 0.76,
            trend_score: 0.72,
            trigger_score: 0.69,
            risk_score: 0.58,
            reason_codes: ["support_confirmed"],
            authoritative_decision: "hold",
            authoritative_decision_path: "G:/Tradex/keep/research_os/experiments/exp_live_001/authoritative_decision.json",
            strategy_judgement_path: "G:/Tradex/keep/research_os/experiments/exp_live_001/strategy_judgement.json",
            experiment_manifest_path: "G:/Tradex/keep/research_os/experiments/exp_live_001/experiment_manifest.json",
            is_buy_signal: true
          },
          forecast_surface_projection: {
            projection: {
              summary: {
                as_of_date: "2026-03-12",
                model_version: "v1",
                universe_code_count: 1,
                expected_row_count: 2,
                actual_row_count: 2,
                missing_row_count: 0,
                coverage_ratio: 1,
                feature_frame_version: "ff-1",
                market_opportunity_score_enabled: true,
                personal_fit_score_enabled: false,
                side_counts: { long: 1, short: 1 },
                action_counts: { enter: 1, wait: 1, skip: 0 },
                source_context_presence: {},
                alerts: [],
                created_at: "2026-03-12T09:00:00Z"
              },
              long_rank: [],
              short_rank: [],
              high_risk_avoid: [],
              watchlist_promotions: []
            }
          },
          raw: {
            analysis_status: {},
            runtime_selection: {},
            publish_state: {},
            publish_queue: {},
            replay_progress: {},
            action_queue: {},
            forecast_surface_projection: {}
          }
        };
      }
      throw new Error(`unexpected tradexFetchJson url: ${url}`);
    });

    const payload = await loadTradexBootstrap();

    expect(payload.forecast_surface_projection?.projection?.summary.coverage_ratio).toBe(1);
    expect(payload.live_strategy_judgement?.human_readable_judgement).toBe("buy");
  });
});
