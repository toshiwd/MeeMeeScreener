import { expect, test } from "@playwright/test";

type AnyRecord = Record<string, unknown>;

type JsonResponse = {
  status: number;
  contentType: string;
  body: string;
};

const responseJson = (status: number, payload: unknown): JsonResponse => ({
  status,
  contentType: "application/json",
  body: JSON.stringify(payload)
});

const buildTagRow = (overrides: Partial<AnyRecord> = {}) => ({
  publish_id: "pub_2026-03-12_20260312T233000Z_01",
  as_of_date: "2026-03-12",
  side: "long",
  holding_band: "buy_21_60",
  strategy_tag: "box_breakout",
  observation_count: 12,
  labeled_count: 12,
  enter_count: 7,
  wait_count: 3,
  skip_count: 2,
  expectancy_mean: 0.051,
  adverse_mean: 0.032,
  large_loss_rate: 0.08,
  win_rate: 0.58,
  teacher_alignment_mean: 0.67,
  failure_count: 3,
  readiness_hint: "needs_samples",
  latest_failure_examples: '[{"code":"1301","side":"long","holding_band":"buy_21_60","strategy_tag":"box_breakout"}]',
  worst_failure_examples: '[{"code":"1302","side":"long","holding_band":"buy_21_60","strategy_tag":"box_breakout"}]',
  summary_json: '{"teacher_signal_mean":0.66,"similarity_signal_mean":0.62}',
  ...overrides
});

test.describe("tradex legacy tags smoke", () => {
  test("uses tradex research APIs and export links", async ({ page }) => {
    const calls: string[] = [];
    const tagRow = buildTagRow();
    const candleRow = buildTagRow({
      strategy_tag: "bullish_engulfing",
      holding_band: "buy_5_20",
      readiness_hint: "promotable"
    });
    const comboRow = buildTagRow({
      strategy_tag: "bullish_engulfing_after_inside",
      holding_band: "buy_5_20",
      readiness_hint: "promotable"
    });
    const promotionReview = {
      as_of_date: "2026-03-12",
      champion_version: "state_eval_baseline_v2",
      challenger_version: "state_eval_challenger_v2",
      sample_count: 64,
      expectancy_delta: 0.021,
      improved_expectancy: true,
      mae_non_worse: true,
      adverse_move_non_worse: true,
      stable_window: true,
      alignment_ok: true,
      readiness_pass: true,
      reason_codes: [],
      summary: { champion_selected: 20, challenger_selected: 24 },
      approval_decision: null,
      by_side: []
    };
    const replayRun = {
      replay_id: "replay_20260312_close",
      status: "running",
      start_as_of_date: "2026-03-10",
      end_as_of_date: "2026-03-12",
      total_days: 3,
      completed_days: 2,
      processed_days: 2,
      remaining_days: 1,
      success_days: 2,
      failed_days: 0,
      skipped_days: 0,
      running_days: 1,
      progress_pct: 66.7,
      started_at: "2026-03-12T14:00:00Z",
      finished_at: null,
      last_completed_as_of_date: "2026-03-11",
      last_heartbeat_at: "2026-03-12T14:05:00Z",
      current_phase: "candidate",
      current_publish_id: "pub_2026-03-12_20260312T233000Z_01",
      eta_seconds: 1800,
      eta_at: "2026-03-12T14:35:00Z",
      error_class: null,
      current_day: {
        as_of_date: "2026-03-12",
        publish_id: "pub_2026-03-12_20260312T233000Z_01",
        started_at: "2026-03-12T14:00:00Z"
      }
    };

    await page.route("**/api/**", async (route) => {
      const url = new URL(route.request().url());
      const method = route.request().method();
      const path = url.pathname;
      calls.push(`${method} ${path}`);

      if (path === "/api/health" && method === "GET") {
        return route.fulfill(responseJson(200, { ready: true, phase: "ready", message: "ok", status: "healthy" }));
      }
      if (path === "/api/health/live" && method === "GET") {
        return route.fulfill(responseJson(200, { ready: true, phase: "live", message: "ok", status: "healthy" }));
      }
      if (path === "/api/tradex/bootstrap" && method === "GET") {
        return route.fulfill(
          responseJson(200, {
            baseline: {
              logic_id: "family_a",
              version: "v1",
              published_at: "2026-03-12T15:00:00Z",
              publish_id: "pub_2026-03-12_20260312T233000Z_01"
            },
            summary: {
              as_of_date: "2026-03-12",
              freshness_state: "fresh",
              replay_status: "running / candidate",
              replay_phase: "candidate",
              attention_count: 1,
              candidate_count: 0,
              champion_logic_key: "family_a:v1",
              publish_id: "pub_2026-03-12_20260312T233000Z_01"
            },
            candidates: [],
            raw: {}
          })
        );
      }

      if (path === "/api/tradex/research/state-eval-tags/summary" && method === "GET") {
        return route.fulfill(
          responseJson(200, {
            degraded: false,
            publish_id: tagRow.publish_id,
            as_of_date: tagRow.as_of_date,
            freshness_state: "fresh",
            summary: {
              top_expectancy: [tagRow],
              risk_heavy: [],
              needs_samples: [tagRow]
            }
          })
        );
      }
      if (path === "/api/tradex/research/state-eval-tags" && method === "GET") {
        return route.fulfill(
          responseJson(200, {
            degraded: false,
            publish_id: tagRow.publish_id,
            as_of_date: tagRow.as_of_date,
            freshness_state: "fresh",
            rows: [tagRow]
          })
        );
      }
      if (path === "/api/tradex/research/state-eval-promotion-review" && method === "GET") {
        return route.fulfill(
          responseJson(200, {
            degraded: false,
            publish_id: tagRow.publish_id,
            as_of_date: tagRow.as_of_date,
            freshness_state: "fresh",
            review: promotionReview
          })
        );
      }
      if (path === "/api/tradex/research/state-eval-candles/summary" && method === "GET") {
        return route.fulfill(
          responseJson(200, {
            degraded: false,
            publish_id: candleRow.publish_id,
            as_of_date: candleRow.as_of_date,
            freshness_state: "fresh",
            summary: {
              top_expectancy: [candleRow],
              risk_heavy: [],
              needs_samples: []
            }
          })
        );
      }
      if (path === "/api/tradex/research/state-eval-candle-combos/summary" && method === "GET") {
        return route.fulfill(
          responseJson(200, {
            degraded: false,
            publish_id: comboRow.publish_id,
            as_of_date: comboRow.as_of_date,
            freshness_state: "fresh",
            summary: {
              top_expectancy: [comboRow],
              risk_heavy: [],
              needs_samples: []
            }
          })
        );
      }
      if (path === "/api/tradex/research/state-eval-daily-summary" && method === "GET") {
        return route.fulfill(
          responseJson(200, {
            degraded: false,
            publish_id: tagRow.publish_id,
            as_of_date: tagRow.as_of_date,
            freshness_state: "fresh",
            daily_summary: {
              promotion: promotionReview,
              top_strategy: tagRow,
              top_strategy_reason: "expectancy leader",
              top_candle: candleRow,
              top_candle_reason: "candle leader",
              risk_watch: null,
              risk_watch_reason: null,
              sample_watch: tagRow,
              sample_watch_reason: "needs samples"
            }
          })
        );
      }
      if (path === "/api/tradex/research/state-eval-daily-summary/history" && method === "GET") {
        return route.fulfill(
          responseJson(200, {
            degraded: false,
            publish_id: tagRow.publish_id,
            as_of_date: tagRow.as_of_date,
            freshness_state: "fresh",
            rows: [
              {
                publish_id: tagRow.publish_id,
                as_of_date: tagRow.as_of_date,
                side_scope: "all",
                top_strategy_tag: "box_breakout",
                top_strategy_expectancy: 0.051,
                top_candle_tag: "bullish_engulfing",
                top_candle_expectancy: 0.044,
                risk_watch_tag: null,
                risk_watch_loss_rate: null,
                sample_watch_tag: "box_breakout",
                sample_watch_labeled_count: 12,
                promotion_ready: true,
                promotion_sample_count: 64,
                summary_json: "{}"
              }
            ]
          })
        );
      }
      if (path === "/api/tradex/research/state-eval-trends" && method === "GET") {
        return route.fulfill(
          responseJson(200, {
            degraded: false,
            publish_id: tagRow.publish_id,
            as_of_date: tagRow.as_of_date,
            freshness_state: "fresh",
            trends: {
              improving: [
                {
                  side: "long",
                  holding_band: "buy_21_60",
                  strategy_tag: "box_breakout",
                  recent_expectancy: 0.051,
                  prior_expectancy: 0.031,
                  expectancy_delta: 0.02,
                  recent_risk: 0.08,
                  prior_risk: 0.1,
                  risk_delta: -0.02,
                  recent_labeled_count: 12,
                  teacher_signal_mean: 0.66,
                  similarity_signal_mean: 0.62,
                  last_as_of_date: "2026-03-12"
                }
              ],
              weakening: [],
              persistent_risk: []
            }
          })
        );
      }
      if (path === "/api/tradex/research/state-eval-candle-combo-trends" && method === "GET") {
        return route.fulfill(
          responseJson(200, {
            degraded: false,
            publish_id: comboRow.publish_id,
            as_of_date: comboRow.as_of_date,
            freshness_state: "fresh",
            trends: {
              improving: [
                {
                  side: "long",
                  holding_band: "buy_5_20",
                  strategy_tag: "bullish_engulfing_after_inside",
                  recent_expectancy: 0.06,
                  prior_expectancy: 0.03,
                  expectancy_delta: 0.03,
                  recent_risk: 0.08,
                  prior_risk: 0.1,
                  risk_delta: -0.02,
                  recent_labeled_count: 16,
                  teacher_signal_mean: 0.68,
                  similarity_signal_mean: 0.64,
                  last_as_of_date: "2026-03-12"
                }
              ],
              weakening: [],
              persistent_risk: []
            }
          })
        );
      }
      if (path === "/api/tradex/research/state-eval-action-queue" && method === "GET") {
        return route.fulfill(
          responseJson(200, {
            degraded: false,
            publish_id: tagRow.publish_id,
            as_of_date: tagRow.as_of_date,
            freshness_state: "fresh",
            actions: [
              {
                kind: "promote_tag",
                priority: 1,
                title: "Promote tag",
                label: "Tag",
                side: "long",
                strategy_tag: "box_breakout",
                holding_band: "buy_21_60",
                metric_label: "Exp",
                metric_value: 0.051,
                note: "ready for review"
              }
            ]
          })
        );
      }
      if (path === "/api/tradex/research/replay-progress" && method === "GET") {
        return route.fulfill(
          responseJson(200, {
            running: true,
            current_run: replayRun,
            recent_runs: [replayRun]
          })
        );
      }

      return route.fulfill(responseJson(200, { ok: true }));
    });

    await page.goto("/tradex/legacy/tags");
    await expect(page.locator(".tradex-tag-main")).toBeVisible();
    await expect(page.locator(".tradex-replay-panel")).toBeVisible();

    const exports = page.locator("a.tradex-tag-export");
    await expect(exports).toHaveCount(2);
    await expect(exports.nth(0)).toHaveAttribute(
      "href",
      "/api/tradex/research/state-eval-tags.csv?limit=200"
    );
    await expect(exports.nth(1)).toHaveAttribute(
      "href",
      "/api/tradex/research/state-eval-daily-summary.csv?limit=60"
    );

    expect(calls).toContain("GET /api/tradex/bootstrap");
    expect(calls).toContain("GET /api/tradex/research/state-eval-tags/summary");
    expect(calls).toContain("GET /api/tradex/research/state-eval-tags");
    expect(calls).toContain("GET /api/tradex/research/state-eval-promotion-review");
    expect(calls).toContain("GET /api/tradex/research/state-eval-action-queue");
    expect(calls).toContain("GET /api/tradex/research/replay-progress");
    expect(calls.some((call) => call.includes("/api/analysis-bridge/internal/"))).toBe(false);

    await page.unrouteAll({ behavior: "ignoreErrors" });
  });
});
