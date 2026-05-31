import { expect, test, type Page } from "@playwright/test";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = resolve(fileURLToPath(new URL("../../..", import.meta.url)));
const artifactDir = resolve(
  repoRoot,
  process.env.DETAIL_JA_UI_ARTIFACT_DIR ?? "artifacts/detail_japanese_ui_and_mojibake_smoke_playwright"
);
const screenshotDir = resolve(artifactDir, "screenshots");
const backendPort = Number(process.env.DETAIL_JA_UI_BACKEND_PORT ?? "8000");
const backendBaseUrl = `http://127.0.0.1:${backendPort}`;
const code = "1001";
const asOfDate = "2026-03-31";

let backendProcess: ChildProcess | null = null;

function writeArtifact(name: string, payload: unknown) {
  mkdirSync(artifactDir, { recursive: true });
  writeFileSync(resolve(artifactDir, name), JSON.stringify(payload, null, 2), "utf-8");
}

function spawnBackend() {
  return spawn("python", ["-m", "uvicorn", "app.backend.main:app", "--host", "127.0.0.1", "--port", String(backendPort)], {
    cwd: repoRoot,
    env: {
      ...process.env,
      MEEMEE_DEV: "1",
      MEEMEE_DEV_MODE: "1",
      MEEMEE_DATA_DIR: "C:\\Users\\enish\\AppData\\Local\\MeeMeeScreener-dev\\data",
      STOCKS_DB_PATH: "C:\\Users\\enish\\AppData\\Local\\MeeMeeScreener-dev\\data\\stocks.duckdb",
      MEEMEE_YF_DAILY_INGEST_ENABLED: "0",
      MEEMEE_YF_DAILY_INGEST_INTRADAY_ENABLED: "0",
      MEEMEE_EDINET_AUTO_START_ENABLED: "0",
      MEEMEE_SCREENER_SNAPSHOT_ENABLED: "0",
      MEEMEE_RANKINGS_WARMUP_ENABLED: "0",
      MEEMEE_ANALYSIS_PREWARM_ENABLED: "0",
    },
    stdio: "inherit",
    shell: false,
  });
}

async function waitForBackend() {
  await expect
    .poll(async () => {
      try {
        const response = await fetch(`${backendBaseUrl}/api/chart-reading/bundle?code=${code}&as_of_date=${asOfDate}`);
        return response.ok;
      } catch {
        return false;
      }
    }, { timeout: 120_000, intervals: [1000, 2000, 3000] })
    .toBe(true);
}

async function pageText(page: Page) {
  return (await page.locator("body").innerText()).replace(/\s+/g, " ");
}

test.beforeAll(async () => {
  mkdirSync(screenshotDir, { recursive: true });
  backendProcess = spawnBackend();
  await waitForBackend();
});

test.afterAll(async () => {
  backendProcess?.kill();
});

test("detail japanese labels and annotation/note panels render without mojibake", async ({ page }) => {
  await page.goto(`/detail/${code}?mainAsOf=${asOfDate}`);
  await expect(page.getByRole("button", { name: "日付選択" })).toBeVisible({ timeout: 30_000 });
  await page.screenshot({ path: resolve(screenshotDir, "detail_normal.png"), fullPage: true });

  await page.getByRole("button", { name: "日付選択" }).click();
  await expect(page.getByRole("button", { name: "日付選択 ON" })).toBeVisible();
  await expect(page.getByText("日足情報")).toBeVisible();
  await expect(page.getByText("日付メモ (100文字以内)")).toBeVisible();
  await page.screenshot({ path: resolve(screenshotDir, "detail_date_select.png"), fullPage: true });

  await page.getByRole("button", { name: "注釈編集モード" }).click();
  await expect(page.getByRole("button", { name: "注釈編集モード" })).toBeVisible();
  await expect(page.getByRole("button", { name: "日付選択 ON" })).toHaveCount(0);
  await expect(page.getByText("チャート読解", { exact: true })).toBeVisible();
  await expect(page.locator("body")).toContainText("ローソク足");
  await expect(page.locator("body")).toContainText("ボックス/範囲");
  await expect(page.getByRole("button", { name: "注釈 水平ライン" })).toBeVisible();
  await expect(page.getByRole("button", { name: "注釈 引き出しコメント" })).toBeVisible();
  await page.screenshot({ path: resolve(screenshotDir, "detail_annotation_mode.png"), fullPage: true });

  await page.getByRole("button", { name: "チャートノート" }).click();
  await expect(page.getByTestId("chart-note-panel").getByText("チャートノート", { exact: true })).toBeVisible();
  await expect(page.getByText("段落 1")).toBeVisible();
  await expect(page.getByText("本文")).toBeVisible();
  await expect(page.getByText("理由タグ")).toBeVisible();
  await page.getByTestId("chart-note-paragraph-text").first().fill("日本語 paragraph 表示確認");
  await expect(page.getByText("日本語 paragraph 表示確認")).toBeVisible();
  await page.screenshot({ path: resolve(screenshotDir, "detail_chart_note.png"), fullPage: true });

  const text = await pageText(page);
  const mojibakePattern = /�|Ã|Â|ã|æ|縺|譁|莨|繧|繝|譛|蠢/;
  expect(text).not.toMatch(mojibakePattern);
  expect(text).not.toContain("カーソルON");
  expect(text).not.toContain("Annotation ON");

  const bundleResponse = await fetch(`${backendBaseUrl}/api/chart-reading/bundle?code=${code}&as_of_date=${asOfDate}`);
  writeArtifact("detail_japanese_ui_smoke.json", {
    ok: true,
    bundle_status: bundleResponse.status,
    screenshots: ["detail_normal.png", "detail_date_select.png", "detail_annotation_mode.png", "detail_chart_note.png"],
    checked_labels: [
      "日付選択 ON",
      "注釈編集 ON",
      "ローソク足",
      "ボックス/範囲",
      "水平ライン",
      "引き出しコメント",
      "チャート読解",
      "チャートノート",
      "段落",
      "本文",
      "理由タグ",
    ],
  });
});
