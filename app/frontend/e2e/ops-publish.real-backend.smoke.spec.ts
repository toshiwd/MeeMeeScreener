import { expect, test } from "@playwright/test";
import { spawn } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = resolve(fileURLToPath(new URL("../../..", import.meta.url)));
const seedScript = resolve(repoRoot, "app", "backend", "tools", "seed_publish_ops_e2e.py");
const backendHost = "127.0.0.1";
const backendPort = 8000;
const backendBaseUrl = `http://${backendHost}:${backendPort}`;

function bumpReasonCount(target: Map<string, number>, reason: string) {
  const key = reason.trim() || "unknown";
  target.set(key, (target.get(key) || 0) + 1);
}

function spawnProcess(command: string, args: string[], env: NodeJS.ProcessEnv) {
  const child = spawn(command, args, {
    cwd: repoRoot,
    env: { ...process.env, ...env },
    stdio: "inherit",
    shell: false,
  });
  return child;
}

async function waitForHealth() {
  await expect
    .poll(async () => {
      try {
        const response = await fetch(`${backendBaseUrl}/api/health`);
        if (!response.ok) return false;
        const payload = (await response.json()) as { ready?: boolean; status?: string };
        return Boolean(payload.ready || payload.status === "healthy");
      } catch {
        return false;
      }
    }, { timeout: 120_000, intervals: [1000, 2000, 3000] })
    .toBe(true);
}

test.describe("operator console real backend smoke", () => {
  test.setTimeout(180_000);

  test("opens /ops/publish against a live backend and can run safe actions", async ({ page }) => {
    const tempRoot = mkdtempSync(join(tmpdir(), "meemee-ops-publish-real-"));
    const dataDir = resolve(tempRoot, "data");
    const resultDb = resolve(tempRoot, "result.duckdb");
    const stockDb = resolve(dataDir, "stocks.duckdb");
    const opsDb = resolve(tempRoot, "ops.duckdb");

    const seed = spawn("python", [seedScript, "--data-dir", dataDir, "--result-db", resultDb, "--ops-db", opsDb], {
      cwd: repoRoot,
      env: { ...process.env },
      stdio: "inherit",
      shell: false,
    });
    const seedExit = await new Promise<number>((resolveSeed, rejectSeed) => {
      seed.once("error", rejectSeed);
      seed.once("exit", (code) => resolveSeed(code ?? 1));
    });
    expect(seedExit).toBe(0);

    const backend = spawnProcess(
      "python",
      ["-m", "uvicorn", "app.main:app", "--host", backendHost, "--port", String(backendPort)],
      {
        MEEMEE_DATA_DIR: dataDir,
        MEEMEE_RESULT_DB_PATH: resultDb,
        STOCKS_DB_PATH: stockDb,
        MEEMEE_OPS_DB_PATH: opsDb,
        MEEMEE_OPERATOR_CONSOLE_GATE_MODE: "header",
        MEEMEE_PROCESS_LOCK_ENABLED: "0",
      }
    );

    try {
      const transientReasonCounts = new Map<string, number>();
      page.on("response", async (response) => {
        if (response.status() !== 503) return;
        const url = response.url();
        if (!url.includes("/api/system/")) return;
        try {
          const payload = (await response.json()) as { reason?: string; detail?: { reason?: string } };
          const reason = payload?.detail?.reason ?? payload?.reason ?? "503";
          bumpReasonCount(transientReasonCounts, String(reason));
        } catch {
          bumpReasonCount(transientReasonCounts, "503_unparsed");
        }
      });

      await waitForHealth();

      const stateResponse = await fetch(`${backendBaseUrl}/api/system/publish/state`, {
        headers: { "X-MeeMee-Operator-Mode": "operator" },
      });
      expect(stateResponse.ok).toBe(true);
      const state = (await stateResponse.json()) as { champion_logic_key?: string; challenger_logic_keys?: string[] };
      expect(state.champion_logic_key).toBe("logic_family_a:v1");
      expect(state.challenger_logic_keys ?? []).toContain("logic_family_a:v2");

      await page.goto("/ops/publish");
      await expect(page.getByText("Operator Console")).toBeVisible();
      await expect(page.getByText("Runtime selection", { exact: true })).toBeVisible();
      await expect(page.getByText("Publish registry", { exact: true })).toBeVisible();
      await expect(page.getByText("Maintenance", { exact: true })).toBeVisible();
      await expect(page.getByText("Candidate bundles", { exact: true })).toBeVisible();
      await expect(page.locator(".ops-table-card tbody tr")).toHaveCount(4);

      await expect(page.locator("tbody tr").filter({ hasText: "logic_family_a:v1" })).toBeVisible();
      await expect(page.locator("tbody tr").filter({ hasText: "logic_family_a:v2" })).toBeVisible();
      await expect(page.locator("tbody tr").filter({ hasText: "logic_family_a:v3" })).toBeVisible();
      await expect(page.locator("tbody tr").filter({ hasText: "logic_family_a:v4" })).toBeVisible();

      await page.getByPlaceholder("logic_key contains...").fill("v4");
      await expect(page.locator(".ops-table-card tbody tr")).toHaveCount(1);
      await expect(page.locator("tbody tr").filter({ hasText: "logic_family_a:v4" })).toBeVisible();
      await page.locator("tbody tr").filter({ hasText: "logic_family_a:v4" }).getByRole("button", { name: "Detail" }).click();
      await expect(page.getByText("Loading candidate detail...")).toBeVisible();
      await expect(page.getByText("Loading candidate detail...")).toBeHidden();
      await expect(page.getByText("Selected candidate detail")).toBeVisible();
      await expect(page.locator(".ops-detail-card").filter({ hasText: "Selected candidate detail" }).getByText("published_logic_manifest", { exact: true })).toBeVisible();
      await page.getByRole("button", { name: "Clear filters" }).click();
      await expect(page.locator(".ops-table-card tbody tr")).toHaveCount(4);

      await page.locator("tbody tr").filter({ hasText: "logic_family_a:v2" }).getByRole("button", { name: "Detail" }).click();
      await expect(page.getByText("Loading candidate detail...")).toBeVisible();
      await expect(page.getByText("Loading candidate detail...")).toBeHidden();
      await expect(page.getByText("Selected candidate detail")).toBeVisible();
      const detailPane = page.locator(".ops-detail-card").filter({ hasText: "Selected candidate detail" });
      await expect(detailPane.getByText("published_logic_manifest", { exact: true })).toBeVisible();
      await expect(detailPane.getByText("validation_summary", { exact: true })).toBeVisible();
      await expect(detailPane.getByText("published_ranking_snapshot", { exact: true })).toBeVisible();

      await page.getByRole("button", { name: "Backfill dry-run" }).click();
      await expect(page.getByText("backfill finished")).toBeVisible();

      page.once("dialog", async (dialog) => {
        expect(dialog.message()).toContain("Approve candidate logic_family_a:v2?");
        await dialog.accept();
      });
      await page.locator("tbody tr").filter({ hasText: "logic_family_a:v2" }).getByRole("button", { name: "Approve" }).click();
      await expect(page.getByText("Approve logic_family_a:v2 finished")).toBeVisible();

      page.once("dialog", async (dialog) => {
        expect(dialog.message()).toContain("Promote candidate logic_family_a:v2?");
        await dialog.accept();
      });
      await page.locator(".ops-detail-card").filter({ hasText: "Selected candidate detail" }).getByRole("button", { name: "Promote" }).click();
      await expect
        .poll(async () => {
          const response = await fetch(`${backendBaseUrl}/api/system/publish/state`, {
            headers: { "X-MeeMee-Operator-Mode": "operator" },
          });
          if (!response.ok) return null;
          const payload = (await response.json()) as { champion_logic_key?: string };
          return payload.champion_logic_key;
        }, { timeout: 30_000, intervals: [1000, 2000, 3000] })
        .toBe("logic_family_a:v2");

      page.once("dialog", async (dialog) => {
        expect(dialog.message()).toContain("Rollback to logic_family_a:");
        await dialog.accept();
      });
      await page.locator(".ops-card").filter({ hasText: "Publish registry" }).getByRole("button", { name: "Rollback" }).click();
      await expect
        .poll(async () => {
          const response = await fetch(`${backendBaseUrl}/api/system/publish/state`, {
            headers: { "X-MeeMee-Operator-Mode": "operator" },
          });
          if (!response.ok) return null;
          const payload = (await response.json()) as { champion_logic_key?: string };
          return payload.champion_logic_key;
        }, { timeout: 30_000, intervals: [1000, 2000, 3000] })
        .toBe("logic_family_a:v1");

      page.once("dialog", async (dialog) => {
        expect(dialog.message()).toContain("Promote candidate logic_family_a:v2?");
        await dialog.accept();
      });
      await page.locator(".ops-detail-card").filter({ hasText: "Selected candidate detail" }).getByRole("button", { name: "Promote" }).click();
      await expect
        .poll(async () => {
          const response = await fetch(`${backendBaseUrl}/api/system/publish/state`, {
            headers: { "X-MeeMee-Operator-Mode": "operator" },
          });
          if (!response.ok) return null;
          const payload = (await response.json()) as { champion_logic_key?: string };
          return payload.champion_logic_key;
        }, { timeout: 30_000, intervals: [1000, 2000, 3000] })
        .toBe("logic_family_a:v2");

      page.once("dialog", async (dialog) => {
        expect(dialog.message()).toContain("Rollback to logic_family_a:");
        await dialog.accept();
      });
      await page.locator(".ops-card").filter({ hasText: "Publish registry" }).getByRole("button", { name: "Rollback" }).click();
      await expect
        .poll(async () => {
          const response = await fetch(`${backendBaseUrl}/api/system/publish/state`, {
            headers: { "X-MeeMee-Operator-Mode": "operator" },
          });
          if (!response.ok) return null;
          const payload = (await response.json()) as { champion_logic_key?: string };
          return payload.champion_logic_key;
        }, { timeout: 30_000, intervals: [1000, 2000, 3000] })
        .toBe("logic_family_a:v1");

      if (process.env.MEEMEE_SMOKE_OBSERVABILITY === "1") {
        const finalStateResponse = await fetch(`${backendBaseUrl}/api/system/publish/state`, {
          headers: { "X-MeeMee-Operator-Mode": "operator" },
        });
        const finalRuntimeResponse = await fetch(`${backendBaseUrl}/api/system/runtime-selection`, {
          headers: { "X-MeeMee-Operator-Mode": "operator" },
        });
        const finalState = (await finalStateResponse.json()) as {
          champion_logic_key?: string;
          registry_sync_state?: string | null;
          operator_mutation_observability?: Record<string, unknown> | null;
        };
        const finalRuntime = (await finalRuntimeResponse.json()) as {
          operator_mutation_observability?: Record<string, unknown> | null;
        };
        console.log(
          JSON.stringify({
            smoke: "ops_publish_real_backend",
            champion_logic_key: finalState.champion_logic_key,
            registry_sync_state: finalState.registry_sync_state ?? null,
            operator_mutation_observability:
              finalState.operator_mutation_observability ?? finalRuntime.operator_mutation_observability ?? null,
            transient_reason_counts: Object.fromEntries(transientReasonCounts.entries()),
          })
        );
      }
    } finally {
      backend.kill("SIGTERM");
      await new Promise<void>((resolveStop) => {
        backend.once("exit", () => resolveStop());
        setTimeout(() => {
          if (!backend.killed) backend.kill("SIGKILL");
          resolveStop();
        }, 5000);
      });
    }
  });
});
