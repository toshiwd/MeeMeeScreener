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
        MEEMEE_SCREENER_SNAPSHOT_ENABLED: "0",
        MEEMEE_OPS_DB_PATH: opsDb,
        MEEMEE_OPERATOR_CONSOLE_GATE_MODE: "header",
        MEEMEE_PROCESS_LOCK_ENABLED: "0",
      }
    );

    try {
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

      await expect(page.locator("tbody tr").filter({ hasText: "logic_family_a:v1" })).toBeVisible();
      await expect(page.locator("tbody tr").filter({ hasText: "logic_family_a:v2" })).toBeVisible();

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

      await expect(page.locator(".ops-card").filter({ hasText: "Publish registry" }).getByText("champion", { exact: true })).toBeVisible();
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
