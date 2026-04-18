import { expect, test, type Page, type TestInfo } from "@playwright/test";
import { spawn } from "node:child_process";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = resolve(fileURLToPath(new URL("../../..", import.meta.url)));
const backendHost = "127.0.0.1";
const backendPort = 8001;
const backendBaseUrl = `http://${backendHost}:${backendPort}`;
const detailLiveValidationPath = "/detail/7203?overwriteLiveValidation=1";
const liveValidationArtifact = resolve(
  repoRoot,
  "artifacts",
  "research_inventory",
  "chart_gallery_overwrite_live_validation.json"
);

type OverwriteObservability = {
  timeframe: "daily" | "weekly" | "monthly";
  mainAsOf: string | null;
  daily: {
    cacheSource: "memory" | "indexeddb" | null;
    dataVersion: string | null;
    chartSourceProvider: string | null;
    displayBasisClassification: "confirmed" | "provisional" | "mixed" | null;
    judgmentBasisClassification: "confirmed" | "provisional" | "dual" | null;
    overwriteStatus: "authoritative_confirmed" | "provisional_only" | "provisional_replaced_by_confirmed" | null;
    confirmedChartSourceProvider: string | null;
    provisionalChartSourceProvider: string | null;
    confirmedJudgmentBasis: string | null;
    provisionalJudgmentBasis: string | null;
    confirmedJudgmentAvailable: boolean | null;
    provisionalJudgmentAvailable: boolean | null;
    confirmedLastAvailableDate: number | null;
    provisionalLastAvailableDate: number | null;
  } | null;
  weekly: OverwriteObservability["daily"];
  monthly: OverwriteObservability["daily"];
};

type BatchBarsProvenanceSummary = {
  chart_source_provider?: string | null;
  display_basis_classification?: string | null;
  judgment_basis_classification?: string | null;
  overwrite_status?: string | null;
  confirmed_chart_source_provider?: string | null;
  provisional_chart_source_provider?: string | null;
  confirmed_judgment_basis?: string | null;
  provisional_judgment_basis?: string | null;
  confirmed_judgment_available?: boolean | null;
  provisional_judgment_available?: boolean | null;
};

function spawnProcess(command: string, args: string[], env: NodeJS.ProcessEnv) {
  return spawn(command, args, {
    cwd: repoRoot,
    env: { ...process.env, ...env },
    stdio: "inherit",
    shell: false,
  });
}

async function stopProcess(child: ReturnType<typeof spawn>) {
  child.kill("SIGTERM");
  await new Promise<void>((resolveStop) => {
    child.once("exit", () => resolveStop());
    setTimeout(() => {
      if (!child.killed) child.kill("SIGKILL");
      resolveStop();
    }, 5000);
  });
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

async function waitForDetailBatchBarsReady(code: string) {
  await expect
    .poll(async () => {
      try {
        const response = await fetch(`${backendBaseUrl}/api/batch_bars_v3`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            codes: [code],
            timeframes: ["daily", "weekly", "monthly"],
            limit: 24,
            includeProvisional: true,
            includeBoxes: true,
          }),
        });
        if (!response.ok) return false;
        const payload = (await response.json()) as {
          items?: Record<
            string,
            {
              daily?: { bars?: unknown[] } | null;
              weekly?: { bars?: unknown[] } | null;
              monthly?: { bars?: unknown[] } | null;
            }
          >;
        };
        const item = payload.items?.[code];
        return Boolean(
          item?.daily?.bars?.length &&
            item?.weekly?.bars?.length &&
            item?.monthly?.bars?.length
        );
      } catch {
        return false;
      }
    }, { timeout: 120_000, intervals: [1000, 2000, 3000] })
    .toBe(true);
}

function buildSeedDbScript() {
  return `
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

import duckdb

db_path = Path(sys.argv[1])
db_path.parent.mkdir(parents=True, exist_ok=True)
conn = duckdb.connect(str(db_path))
try:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_bars (
            code TEXT,
            date BIGINT,
            o DOUBLE,
            h DOUBLE,
            l DOUBLE,
            c DOUBLE,
            v DOUBLE,
            source TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS monthly_bars (
            code TEXT,
            month BIGINT,
            o DOUBLE,
            h DOUBLE,
            l DOUBLE,
            c DOUBLE,
            v DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS market_regime_daily (
            dt INTEGER,
            regime_id TEXT,
            breadth_above_ma20 DOUBLE,
            breadth_above_ma60 DOUBLE,
            advancers_ratio DOUBLE,
            index_close_vs_ma20 DOUBLE,
            index_close_vs_ma60 DOUBLE,
            market_atr_pct DOUBLE,
            sector_dispersion DOUBLE,
            regime_score DOUBLE,
            label_version TEXT,
            created_at TIMESTAMP
        )
        """
    )
    conn.execute("DELETE FROM daily_bars")
    conn.execute("DELETE FROM monthly_bars")
    conn.execute("DELETE FROM market_regime_daily")
    start = datetime(2026, 3, 28)
    rows = []
    for index in range(20):
        day = start + timedelta(days=index)
        ymd = int(day.strftime("%Y%m%d"))
        open_price = 3240.0 + index * 6.0
        close_price = open_price + (4.0 if index % 2 == 0 else -3.0)
        high_price = max(open_price, close_price) + 12.0
        low_price = min(open_price, close_price) - 9.0
        volume = 12000.0 + index * 180.0
        rows.append(("7203", ymd, open_price, high_price, low_price, close_price, volume, "pan"))
    conn.executemany(
        "INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.executemany(
        "INSERT INTO monthly_bars VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ("7203", 202603, 3210.0, 3370.0, 3190.0, 3340.0, 210000.0),
            ("7203", 202604, 3340.0, 3410.0, 3290.0, 3385.0, 285000.0),
        ],
    )
    conn.executemany(
        "INSERT INTO market_regime_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (20260413, "flat", 0.51, 0.48, 0.50, 1.00, 1.00, 0.9, 0.3, 0.50, "v1", datetime.now(timezone.utc)),
        ],
    )
finally:
    conn.close()
`;
}

function buildImportDbScript() {
  return `
from datetime import datetime, timezone
from pathlib import Path
import os
import sys

import duckdb

db_path = Path(sys.argv[1])
now = float(sys.argv[2])
conn = duckdb.connect(str(db_path))
try:
    conn.execute(
        "INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("7203", 20260417, 3400.0, 3400.0, 3343.0, 3343.0, 14252.0, "chart_gallery_confirmed"),
    )
finally:
    conn.close()
os.utime(db_path, (now, now))
`;
}

function buildEmptyDbScript() {
  return `
from pathlib import Path
import duckdb
import sys

db_path = Path(sys.argv[1])
db_path.parent.mkdir(parents=True, exist_ok=True)
duckdb.connect(str(db_path)).close()
`;
}

function summarizeFrame(frame: BatchBarsProvenanceSummary | undefined | null) {
  if (!frame) return null;
  return {
    chartSourceProvider: frame.chart_source_provider ?? null,
    displayBasisClassification: frame.display_basis_classification ?? null,
    judgmentBasisClassification: frame.judgment_basis_classification ?? null,
    overwriteStatus: frame.overwrite_status ?? null,
    confirmedChartSourceProvider: frame.confirmed_chart_source_provider ?? null,
    provisionalChartSourceProvider: frame.provisional_chart_source_provider ?? null,
    confirmedJudgmentBasis: frame.confirmed_judgment_basis ?? null,
    provisionalJudgmentBasis: frame.provisional_judgment_basis ?? null,
    confirmedJudgmentAvailable: frame.confirmed_judgment_available ?? null,
    provisionalJudgmentAvailable: frame.provisional_judgment_available ?? null,
  };
}

async function readActiveChartCacheVersion(page: Page) {
  return await page.evaluate(() => window.localStorage.getItem("meemee.chart-cache.active-version"));
}

async function fetchBatchBarsSummary(page: Page, code: string) {
  return await page.evaluate(async (symbol) => {
    const response = await fetch("/api/batch_bars_v3", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        codes: [symbol],
        timeframes: ["daily", "weekly", "monthly"],
        limit: 24,
        includeProvisional: true,
        includeBoxes: true,
      }),
    });
    if (!response.ok) {
      throw new Error(`batch_bars_v3 failed: ${response.status}`);
    }
    const payload = (await response.json()) as {
      items?: Record<
        string,
        {
          daily?: { provenance?: BatchBarsProvenanceSummary | null } | null;
          weekly?: { provenance?: BatchBarsProvenanceSummary | null } | null;
          monthly?: { provenance?: BatchBarsProvenanceSummary | null } | null;
        }
      >;
      meta?: { data_version?: string | null } | null;
    };
    const item = payload.items?.[symbol] ?? {};
    return {
      dataVersion: payload.meta?.data_version ?? null,
      daily: item.daily?.provenance ?? null,
      weekly: item.weekly?.provenance ?? null,
      monthly: item.monthly?.provenance ?? null,
    };
  }, code);
}

test.describe("chart gallery overwrite live validation", () => {
  test.setTimeout(180_000);

  test("records a live before/after overwrite switch in detail view", async ({ page }, testInfo: TestInfo) => {
    const tempRoot = mkdtempSync(join(tmpdir(), "meemee-chart-overwrite-live-"));
    const dataDir = resolve(tempRoot, "data");
    const resultDb = resolve(tempRoot, "result.duckdb");
    const opsDb = resolve(tempRoot, "ops.duckdb");
    const stockDb = resolve(dataDir, "stocks.duckdb");
    const seedScript = buildSeedDbScript();
    const importScript = buildImportDbScript();
    const emptyDbScript = buildEmptyDbScript();

    const seed = spawn("python", ["-c", seedScript, stockDb], {
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

    for (const dbPath of [resultDb, opsDb]) {
      const initDb = spawn("python", ["-c", emptyDbScript, dbPath], {
        cwd: repoRoot,
        env: { ...process.env },
        stdio: "inherit",
        shell: false,
      });
      const initExit = await new Promise<number>((resolveInit, rejectInit) => {
        initDb.once("error", rejectInit);
        initDb.once("exit", (code) => resolveInit(code ?? 1));
      });
      expect(initExit).toBe(0);
    }

    let backend = spawnProcess(
      "python",
      ["-m", "uvicorn", "app.main:app", "--host", backendHost, "--port", String(backendPort)],
      {
        MEEMEE_DATA_DIR: dataDir,
        MEEMEE_RESULT_DB_PATH: resultDb,
        MEEMEE_OPS_DB_PATH: opsDb,
        STOCKS_DB_PATH: stockDb,
        MEEMEE_PROCESS_LOCK_ENABLED: "0",
        MEEMEE_RANKINGS_WARMUP_ENABLED: "0",
        MEEMEE_SCREENER_SNAPSHOT_ENABLED: "0",
        MEEMEE_YF_DAILY_INGEST_ENABLED: "0",
        MEEMEE_EDINET_AUTO_START_ENABLED: "0",
        MEEMEE_ANALYSIS_PREWARM_ENABLED: "0",
        MEEMEE_PUBLISH_CANDIDATE_MAINTENANCE_ENABLED: "0",
        MEEMEE_AUTO_REPAIR_MIN_MISSING: "999999",
        MEEMEE_AUTO_REPAIR_MIN_RATIO: "1.1",
        MEEMEE_YF_PROVISIONAL_ENABLED: "1",
      }
    );

    try {
      await page.context().addInitScript((apiBase) => {
        const globalWindow = window as Window & {
          MEEMEE_API_BASE?: string;
          MEEMEE_DISABLE_WATCHLIST_AUTO_REPAIR?: boolean;
        };
        globalWindow.MEEMEE_API_BASE = apiBase;
        globalWindow.MEEMEE_DISABLE_WATCHLIST_AUTO_REPAIR = true;
      }, `${backendBaseUrl}/api`);
      await waitForHealth();
      await waitForDetailBatchBarsReady("7203");

      await page.goto(detailLiveValidationPath, { waitUntil: "domcontentloaded" });
      const beforeActiveVersion = await readActiveChartCacheVersion(page);
      const beforeBatch = await fetchBatchBarsSummary(page, "7203");
      const beforeObservability: OverwriteObservability = {
        timeframe: "daily",
        mainAsOf: null,
        daily: summarizeFrame(beforeBatch.daily),
        weekly: summarizeFrame(beforeBatch.weekly),
        monthly: summarizeFrame(beforeBatch.monthly),
      };

      await stopProcess(backend);

      const confirmImportTimestamp = Date.now() / 1000 + 120;
      const importer = spawn("python", ["-c", importScript, stockDb, String(confirmImportTimestamp)], {
        cwd: repoRoot,
        env: { ...process.env },
        stdio: "inherit",
        shell: false,
      });
      const importExit = await new Promise<number>((resolveImport, rejectImport) => {
        importer.once("error", rejectImport);
        importer.once("exit", (code) => resolveImport(code ?? 1));
      });
      expect(importExit).toBe(0);

      backend = spawnProcess(
        "python",
        ["-m", "uvicorn", "app.main:app", "--host", backendHost, "--port", String(backendPort)],
        {
          MEEMEE_DATA_DIR: dataDir,
          MEEMEE_RESULT_DB_PATH: resultDb,
          MEEMEE_OPS_DB_PATH: opsDb,
          STOCKS_DB_PATH: stockDb,
          MEEMEE_PROCESS_LOCK_ENABLED: "0",
          MEEMEE_RANKINGS_WARMUP_ENABLED: "0",
          MEEMEE_SCREENER_SNAPSHOT_ENABLED: "0",
          MEEMEE_YF_DAILY_INGEST_ENABLED: "0",
          MEEMEE_EDINET_AUTO_START_ENABLED: "0",
          MEEMEE_ANALYSIS_PREWARM_ENABLED: "0",
          MEEMEE_PUBLISH_CANDIDATE_MAINTENANCE_ENABLED: "0",
          MEEMEE_AUTO_REPAIR_MIN_MISSING: "999999",
          MEEMEE_AUTO_REPAIR_MIN_RATIO: "1.1",
          MEEMEE_YF_PROVISIONAL_ENABLED: "1",
        }
      );
      await waitForHealth();
      await waitForDetailBatchBarsReady("7203");

      const afterPage = await page.context().newPage();
      await afterPage.addInitScript((apiBase) => {
        const globalWindow = window as Window & {
          MEEMEE_API_BASE?: string;
          MEEMEE_DISABLE_WATCHLIST_AUTO_REPAIR?: boolean;
        };
        globalWindow.MEEMEE_API_BASE = apiBase;
        globalWindow.MEEMEE_DISABLE_WATCHLIST_AUTO_REPAIR = true;
      }, `${backendBaseUrl}/api`);
      await afterPage.goto(detailLiveValidationPath, { waitUntil: "domcontentloaded" });
      const afterActiveVersion = await readActiveChartCacheVersion(afterPage);
      const afterBatch = await fetchBatchBarsSummary(afterPage, "7203");
      const afterObservability: OverwriteObservability = {
        timeframe: "daily",
        mainAsOf: null,
        daily: summarizeFrame(afterBatch.daily),
        weekly: summarizeFrame(afterBatch.weekly),
        monthly: summarizeFrame(afterBatch.monthly),
      };

      const preImportDaily = beforeObservability.daily;
      const postImportDaily = afterObservability.daily;
      expect(preImportDaily).not.toBeNull();
      expect(postImportDaily).not.toBeNull();
      expect(preImportDaily?.displayBasisClassification).toMatch(/^(provisional|mixed)$/);
      expect(preImportDaily?.judgmentBasisClassification).toMatch(/^(provisional|dual)$/);
      expect(postImportDaily?.displayBasisClassification).toBe("confirmed");
      expect(postImportDaily?.judgmentBasisClassification).toBe("confirmed");
      expect(preImportDaily?.overwriteStatus).toMatch(/^(provisional_only|provisional_replaced_by_confirmed)$/);
      expect(postImportDaily?.overwriteStatus).toBe("provisional_replaced_by_confirmed");
      expect(beforeActiveVersion).not.toBe(afterActiveVersion);
      expect(beforeBatch.dataVersion).not.toBe(afterBatch.dataVersion);
      expect(summarizeFrame(beforeBatch.daily)?.displayBasisClassification).toMatch(/^(provisional|mixed)$/);
      expect(summarizeFrame(afterBatch.daily)?.displayBasisClassification).toBe("confirmed");
      expect(summarizeFrame(afterBatch.daily)?.judgmentBasisClassification).toBe("confirmed");
      expect(summarizeFrame(afterBatch.daily)?.overwriteStatus).toBe("provisional_replaced_by_confirmed");

      const artifact = {
        schema_version: "chart_gallery_overwrite_live_validation_v1",
        generated_at: new Date().toISOString(),
        scenario_id: "7203_live_detail_overwrite_import",
        symbol: "7203",
        pre_import_state: {
          browser_active_chart_cache_version: beforeActiveVersion,
          detail_overwrite_observability: beforeObservability,
          batch_bars_v3: beforeBatch,
        },
        post_import_state: {
          browser_active_chart_cache_version: afterActiveVersion,
          detail_overwrite_observability: afterObservability,
          batch_bars_v3: afterBatch,
        },
        confirmed_basis_before: preImportDaily?.confirmedChartSourceProvider ?? null,
        confirmed_basis_after: postImportDaily?.confirmedChartSourceProvider ?? null,
        provisional_basis_before: preImportDaily?.provisionalChartSourceProvider ?? null,
        provisional_basis_after: postImportDaily?.provisionalChartSourceProvider ?? null,
        overwrite_status_before: preImportDaily?.overwriteStatus ?? null,
        overwrite_status_after: postImportDaily?.overwriteStatus ?? null,
        cache_invalidation_observed:
          beforeActiveVersion !== afterActiveVersion ||
          beforeBatch.dataVersion !== afterBatch.dataVersion ||
          preImportDaily?.displayBasisClassification !== postImportDaily?.displayBasisClassification,
        judgment_basis_before: preImportDaily?.judgmentBasisClassification ?? null,
        judgment_basis_after: postImportDaily?.judgmentBasisClassification ?? null,
        validation_status: "passed",
        browser_cache_validation_status: "verified",
        live_validation_status: "verified",
      };

      writeFileSync(liveValidationArtifact, `${JSON.stringify(artifact, null, 2)}\n`, "utf-8");
      await testInfo.attach("chart-gallery-overwrite-live-validation.json", {
        body: JSON.stringify(artifact, null, 2),
        contentType: "application/json",
      });
      console.log(JSON.stringify(artifact, null, 2));
    } finally {
      await stopProcess(backend);
    }
  });
});
