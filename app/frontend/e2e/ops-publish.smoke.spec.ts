import { expect, test } from "@playwright/test";

type AnyRecord = Record<string, unknown>;

type CandidateState = {
  candidate_id: string;
  logic_key: string;
  logic_id: string;
  logic_version: string;
  logic_family: string;
  status: string;
  validation_state: string;
  created_at: string;
  updated_at: string;
  published_logic_manifest: AnyRecord;
  validation_summary: AnyRecord;
  published_logic_artifact: AnyRecord;
  published_ranking_snapshot?: AnyRecord | null;
};

function buildState() {
  const now = "2026-03-19T09:00:00+09:00";
  const candidates: CandidateState[] = [
    {
      candidate_id: "cand-001",
      logic_key: "family_a:v1",
      logic_id: "family_a",
      logic_version: "v1",
      logic_family: "family_a",
      status: "approved",
      validation_state: "approved",
      created_at: now,
      updated_at: now,
      published_logic_manifest: {
        logic_key: "family_a:v1",
        logic_id: "family_a",
        logic_version: "v1",
        logic_family: "family_a",
        artifact_uri: "artifact://family_a/v1",
        checksum: "sha256:aaa",
      },
      validation_summary: {
        readiness_pass: true,
        sample_count: 120,
        expectancy_delta: 0.38,
        improved_expectancy: true,
        mae_non_worse: true,
        adverse_move_non_worse: true,
        stable_window: true,
        alignment_ok: true,
      },
      published_logic_artifact: {
        artifact_version: 1,
        logic_key: "family_a:v1",
        logic_id: "family_a",
        logic_version: "v1",
        logic_family: "family_a",
        feature_spec_version: "1",
        required_inputs: ["confirmed_market_bars"],
        scorer_type: "declarative-score",
        params: {},
        thresholds: {},
        weights: {},
        output_spec: { ranking: "score" },
        checksum: "sha256:aaa",
      },
      published_ranking_snapshot: {
        rows: [{ code: "1301", rank: 1, score: 0.81 }],
      },
    },
    {
      candidate_id: "cand-002",
      logic_key: "family_b:v2",
      logic_id: "family_b",
      logic_version: "v2",
      logic_family: "family_b",
      status: "candidate",
      validation_state: "candidate",
      created_at: now,
      updated_at: now,
      published_logic_manifest: {
        logic_key: "family_b:v2",
        logic_id: "family_b",
        logic_version: "v2",
        logic_family: "family_b",
        artifact_uri: "artifact://family_b/v2",
        checksum: "sha256:bbb",
      },
      validation_summary: {
        readiness_pass: true,
        sample_count: 88,
        expectancy_delta: 0.24,
        improved_expectancy: true,
        mae_non_worse: true,
        adverse_move_non_worse: true,
        stable_window: true,
        alignment_ok: true,
      },
      published_logic_artifact: {
        artifact_version: 1,
        logic_key: "family_b:v2",
        logic_id: "family_b",
        logic_version: "v2",
        logic_family: "family_b",
        feature_spec_version: "1",
        required_inputs: ["confirmed_market_bars"],
        scorer_type: "declarative-score",
        params: {},
        thresholds: {},
        weights: {},
        output_spec: { ranking: "score" },
        checksum: "sha256:bbb",
      },
      published_ranking_snapshot: null,
    },
  ];

  return {
    runtimeSelection: {
      resolved_source: "external_analysis",
      selected_logic_id: "family_a",
      selected_logic_version: "v1",
      logic_key: "family_a:v1",
      artifact_uri: "artifact://family_a/v1",
      source_of_truth: "external_analysis",
      degraded: false,
      bootstrap_rule: "explicit bootstrap_champion flag",
      selected_logic_override: null,
      last_known_good: null,
      last_known_good_present: false,
      override_present: false,
      last_sync_time: now,
      registry_sync_state: "in_sync",
      maintenance_state: {
        candidate_backfill_last_run: { started_at: now, ended_at: now, dry_run: true },
        snapshot_sweep_last_run: { started_at: now, ended_at: now, dry_run: true },
        non_promotable_legacy_count: 1,
        maintenance_degraded: false,
      },
      candidate_backfill_last_run: { started_at: now, ended_at: now, dry_run: true },
      snapshot_sweep_last_run: { started_at: now, ended_at: now, dry_run: true },
      non_promotable_legacy_count: 1,
      maintenance_degraded: false,
    },
    publishState: {
      source_of_truth: "external_analysis",
      registry_sync_state: "in_sync",
      degraded: false,
      last_sync_time: now,
      bootstrap_rule: "explicit bootstrap_champion flag",
      default_logic_pointer: "family_a:v1",
      champion: {
        logic_key: "family_a:v1",
        logic_id: "family_a",
        logic_version: "v1",
      },
      challengers: [
        {
          logic_key: "family_b:v2",
          logic_id: "family_b",
          logic_version: "v2",
        },
      ],
      champion_logic_key: "family_a:v1",
      challenger_logic_keys: ["family_b:v2"],
      previous_stable_champion_logic_key: "family_a:v1",
      external_registry_version: "2026.03.19",
      local_mirror_version: "2026.03.19",
      mirror_schema_version: "7",
      mirror_normalized: true,
      candidate_backfill_last_run: { started_at: now, ended_at: now, dry_run: true },
      snapshot_sweep_last_run: { started_at: now, ended_at: now, dry_run: true },
      non_promotable_legacy_count: 1,
      maintenance_degraded: false,
      maintenance_state: {
        candidate_backfill_last_run: { started_at: now, ended_at: now, dry_run: true },
        snapshot_sweep_last_run: { started_at: now, ended_at: now, dry_run: true },
        non_promotable_legacy_count: 1,
        maintenance_degraded: false,
      },
    },
    candidates,
  };
}

function responseJson(status: number, payload: unknown) {
  return {
    status,
    contentType: "application/json",
    body: JSON.stringify(payload),
  };
}

test.describe("operator console smoke", () => {
  test("opens /ops/publish and supports safe operator actions", async ({ page }) => {
    const state = buildState();
    const calls: string[] = [];

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

      if (path === "/api/system/runtime-selection" && method === "GET") {
        return route.fulfill(responseJson(200, state.runtimeSelection));
      }

      if (path === "/api/system/publish/state" && method === "GET") {
        return route.fulfill(responseJson(200, state.publishState));
      }

      if (path === "/api/system/publish/candidates" && method === "GET") {
        return route.fulfill(responseJson(200, { items: state.candidates }));
      }

      if (path.startsWith("/api/system/publish/candidates/") && method === "GET") {
        const logicKey = decodeURIComponent(path.split("/").pop() ?? "");
        const candidate = state.candidates.find((item) => item.logic_key === logicKey) ?? null;
        if (candidate) {
          await new Promise((resolve) => setTimeout(resolve, 80));
        }
        return route.fulfill(responseJson(candidate ? 200 : 404, candidate ? { candidate } : { detail: "not found" }));
      }

      if (path.endsWith("/approve") && method === "POST") {
        const logicKey = decodeURIComponent(path.split("/")[5] ?? "");
        const candidate = state.candidates.find((item) => item.logic_key === logicKey);
        if (candidate) {
          candidate.status = "approved";
          candidate.validation_state = "approved";
          candidate.updated_at = "2026-03-19T09:01:00+09:00";
        }
        return route.fulfill(responseJson(200, { ok: true }));
      }

      if (path.endsWith("/reject") && method === "POST") {
        const logicKey = decodeURIComponent(path.split("/")[5] ?? "");
        const candidate = state.candidates.find((item) => item.logic_key === logicKey);
        if (candidate) {
          candidate.status = "rejected";
          candidate.validation_state = "rejected";
          candidate.updated_at = "2026-03-19T09:01:00+09:00";
        }
        return route.fulfill(responseJson(200, { ok: true }));
      }

      if (path === "/api/system/publish/promote" && method === "POST") {
        const body = JSON.parse(route.request().postData() ?? "{}") as { logicKey?: string };
        const logicKey = body.logicKey ?? "";
        const candidate = state.candidates.find((item) => item.logic_key === logicKey);
        if (candidate) {
          state.publishState.champion_logic_key = candidate.logic_key;
          state.publishState.default_logic_pointer = candidate.logic_key;
          state.publishState.champion = {
            logic_key: candidate.logic_key,
            logic_id: candidate.logic_id,
            logic_version: candidate.logic_version,
          };
          state.publishState.challenger_logic_keys = state.candidates
            .filter((item) => item.logic_key !== candidate.logic_key && item.status !== "rejected")
            .map((item) => item.logic_key);
          state.runtimeSelection.selected_logic_id = candidate.logic_id;
          state.runtimeSelection.selected_logic_version = candidate.logic_version;
          state.runtimeSelection.logic_key = candidate.logic_key;
          state.runtimeSelection.artifact_uri = candidate.published_logic_manifest.artifact_uri as string;
          candidate.status = "promoted";
          candidate.validation_state = "promoted";
          candidate.updated_at = "2026-03-19T09:02:00+09:00";
        }
        return route.fulfill(responseJson(200, { ok: true }));
      }

      if (path.endsWith("/demote") && method === "POST") {
        return route.fulfill(responseJson(200, { ok: true }));
      }

      if (path.endsWith("/rollback") && method === "POST") {
        return route.fulfill(responseJson(200, { ok: true }));
      }

      if (path.endsWith("/maintenance/backfill") && method === "POST") {
        return route.fulfill(responseJson(200, { scanned: 2, updated: 0, skipped: 2, failed: 0, dry_run: true }));
      }

      if (path.endsWith("/maintenance/snapshot-sweep") && method === "POST") {
        return route.fulfill(responseJson(200, { scanned: 2, deleted: 0, pruned_snapshot_count: 0, dry_run: true }));
      }

      if (path.endsWith("/maintenance/cleanup") && method === "POST") {
        return route.fulfill(responseJson(200, { scanned: 2, deleted: 0, pruned_snapshot_count: 0, dry_run: true }));
      }

      if (path.endsWith("/mirror/normalize") && method === "POST") {
        return route.fulfill(responseJson(200, { ok: true, dry_run: false }));
      }

      if (path.endsWith("/mirror/resync") && method === "POST") {
        return route.fulfill(responseJson(200, { ok: true, dry_run: false }));
      }

      return route.fulfill(responseJson(200, { ok: true }));
    });

    await page.goto("/ops/publish");

    await expect(page.getByText("Operator Console")).toBeVisible();
    await expect(page.getByText("Runtime selection", { exact: true })).toBeVisible();
    await expect(page.getByText("Publish registry", { exact: true })).toBeVisible();
    await expect(page.getByText("Maintenance", { exact: true })).toBeVisible();
    await expect(page.getByText("Candidate bundles", { exact: true })).toBeVisible();
    await expect(page.locator("tbody tr").filter({ hasText: "family_a:v1" })).toBeVisible();
    await expect(page.locator("tbody tr").filter({ hasText: "family_b:v2" })).toBeVisible();

    await page.locator("tbody tr").filter({ hasText: "family_b:v2" }).getByRole("button", { name: "Detail" }).click();
    await expect(page.getByText("Loading candidate detail...")).toBeVisible();
    await expect(page.getByText("Loading candidate detail...")).toBeHidden();
    await expect(page.getByText("Selected candidate detail")).toBeVisible();
    await expect(page.getByText("candidate_id")).toBeVisible();
    await expect(
      page.locator(".ops-detail-card").filter({ hasText: "Selected candidate detail" }).getByText("validation_summary", { exact: true })
    ).toBeVisible();
    await expect(
      page.locator(".ops-detail-card").filter({ hasText: "Selected candidate detail" }).getByText("ranking snapshot", { exact: true })
    ).toBeVisible();
    await expect(page.getByText("absent")).toBeVisible();

    page.once("dialog", async (dialog) => {
      expect(dialog.message()).toContain("Approve candidate family_b:v2?");
      await dialog.accept();
    });
    await page.locator("tbody tr").filter({ hasText: "family_b:v2" }).getByRole("button", { name: "Approve" }).click();
    await expect(page.getByText("Approve family_b:v2 finished")).toBeVisible();

    page.once("dialog", async (dialog) => {
      expect(dialog.message()).toContain("Promote candidate family_b:v2?");
      await dialog.accept();
    });
    await page.getByRole("button", { name: "Promote selected" }).click();
    await expect(page.getByText("Promote family_b:v2 finished")).toBeVisible();

    await expect(page.locator(".ops-card").filter({ hasText: "Publish registry" }).getByText("champion", { exact: true })).toBeVisible();

    await page.getByRole("button", { name: "Backfill dry-run" }).click();
    await expect(page.getByText("backfill finished")).toBeVisible();
    page.once("dialog", async (dialog) => {
      expect(dialog.message()).toContain("Run mirror normalize?");
      await dialog.accept();
    });
    await page.getByRole("button", { name: "Mirror normalize" }).click();
    await expect(page.getByText("mirror normalize finished")).toBeVisible();

    expect(calls).toContain("GET /api/system/runtime-selection");
    expect(calls).toContain("GET /api/system/publish/state");
    expect(calls).toContain("GET /api/system/publish/candidates");
    expect(calls.some((call) => call.includes("/api/system/publish/candidates/family_b%3Av2"))).toBe(true);
    expect(calls.some((call) => call.includes("/approve"))).toBe(true);
    expect(calls.some((call) => call.includes("/promote"))).toBe(true);
    expect(calls.some((call) => call.includes("/maintenance/backfill"))).toBe(true);

    await page.unrouteAll({ behavior: "ignoreErrors" });
  });
});
