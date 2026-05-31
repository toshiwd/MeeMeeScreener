import { expect, test, type Page } from "@playwright/test";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = resolve(fileURLToPath(new URL("../../..", import.meta.url)));
const artifactDir = resolve(repoRoot, "artifacts", process.env.CALLOUT_VISUAL_ARTIFACT_DIR ?? "callout_visual_v1_playwright");
const backendPort = Number(process.env.CALLOUT_E2E_BACKEND_PORT ?? "8784");
const backendBaseUrl = `http://127.0.0.1:${backendPort}`;
const code = "1001";
const asOfDate = "2026-03-31";
const e2eTag = "e2e_callout_visual_v1";

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

async function bundle() {
  let lastStatus = 0;
  for (let attempt = 0; attempt < 8; attempt += 1) {
    const response = await fetch(`${backendBaseUrl}/api/chart-reading/bundle?code=${code}&as_of_date=${asOfDate}`);
    lastStatus = response.status;
    if (response.ok) {
      return response.json() as Promise<{ annotations?: Array<Record<string, any>> }>;
    }
    await new Promise((resolve) => setTimeout(resolve, 500 + attempt * 350));
  }
  throw new Error(`bundle request failed: ${lastStatus}`);
}

async function e2eCallouts() {
  return (await e2eAnnotations()).filter((annotation) => annotation.object_type === "callout");
}

async function e2eAnnotations() {
  const payload = await bundle();
  return (payload.annotations ?? []).filter((annotation) => {
    const tags = Array.isArray(annotation.tags) ? annotation.tags : [];
    const payloadTags = Array.isArray(annotation.payload?.tags) ? annotation.payload.tags : [];
    const freeText = String(annotation.payload?.free_text ?? "");
    return tags.includes(e2eTag) || payloadTags.includes(e2eTag) || freeText.startsWith("e2e ");
  });
}

async function cleanupE2eAnnotations() {
  for (const annotation of await e2eAnnotations()) {
    if (typeof annotation.id === "string") {
      await fetch(`${backendBaseUrl}/api/chart-annotations/${encodeURIComponent(annotation.id)}`, { method: "DELETE" });
    }
  }
}

async function waitForE2eAnnotationId(objectType: string) {
  await expect
    .poll(async () => (await e2eAnnotations()).some((item) => item.object_type === objectType), { timeout: 30_000 })
    .toBe(true);
  const item = (await e2eAnnotations()).find((annotation) => annotation.object_type === objectType);
  if (typeof item?.id !== "string") throw new Error(`missing ${objectType} annotation id`);
  return item.id;
}

async function openAnnotationMode(page: Page) {
  await page.goto(`/detail/${code}?mainAsOf=${asOfDate}`);
  await expect(page.getByRole("button", { name: "注釈編集モード" })).toBeVisible({ timeout: 60_000 });
  await page.getByRole("button", { name: "注釈編集モード" }).click();
  await expect(page.getByTestId("chart-reading-panel")).toBeVisible();
}

async function chartBox(page: Page) {
  const box = await page.locator(".detail-row-top .detail-chart-wrapper").first().boundingBox();
  expect(box).not.toBeNull();
  return box!;
}

async function overlayPixels(page: Page) {
  return page.locator(".detail-row-top .detail-chart-overlay").first().evaluate((canvas: HTMLCanvasElement) => {
    const ctx = canvas.getContext("2d");
    if (!ctx) return 0;
    const { width, height } = canvas;
    const data = ctx.getImageData(0, 0, width, height).data;
    let count = 0;
    for (let index = 3; index < data.length; index += 4) {
      if (data[index] > 0) count += 1;
    }
    return count;
  });
}

test.beforeAll(async () => {
  mkdirSync(artifactDir, { recursive: true });
  backendProcess = spawnBackend();
  await waitForBackend();
  await cleanupE2eAnnotations();
});
test.afterAll(async () => {
  try {
    await cleanupE2eAnnotations();
    writeArtifact("cleanup_result.json", {
      ok: true,
      remaining_e2e_annotations: (await e2eAnnotations()).length,
    });
  } finally {
    backendProcess?.kill("SIGTERM");
  }
});

test("creates, renders, reloads, filters, and bundles a candle callout", async ({ page }) => {
  await openAnnotationMode(page);
  await page.waitForTimeout(6000);
  const chart = await chartBox(page);

  await page.getByTestId("chart-reading-note-text").fill("e2e candle callout");
  await page.getByTestId("chart-reading-tags").fill(e2eTag);
  await page.getByTestId("chart-reading-comment-type").selectOption("review");
  await page.getByRole("button", { name: "注釈 引き出しコメント" }).click();

  const overlayBefore = await overlayPixels(page);
  const dailyChart = page.locator(".detail-row-top .detail-chart-wrapper").first();
  await dailyChart.click({ position: { x: chart.width * 0.82, y: chart.height * 0.34 } });
  await dailyChart.click({ position: { x: chart.width * 0.68, y: chart.height * 0.18 } });

  await expect.poll(async () => (await e2eCallouts()).length, { timeout: 30_000 }).toBeGreaterThanOrEqual(1);
  await page.getByTestId("chart-reading-note-text").fill("e2e ma20 callout");
  await page.getByTestId("chart-reading-tags").fill(`${e2eTag}, ma20`);
  await page.getByTestId("chart-reading-target-type").selectOption("indicator");
  for (const yRatio of [0.30, 0.34, 0.38, 0.42, 0.46]) {
    await dailyChart.click({ position: { x: chart.width * 0.82, y: chart.height * yRatio } });
    await dailyChart.click({ position: { x: chart.width * 0.61, y: chart.height * 0.16 } });
    if (
      (await e2eCallouts()).some(
        (item) => item.payload?.anchor_type === "indicator" && item.payload?.anchor_target === "ma20"
      )
    ) {
      break;
    }
  }
  await expect
    .poll(async () => (await e2eCallouts()).some((item) => item.payload?.anchor_type === "indicator" && item.payload?.anchor_target === "ma20"), {
      timeout: 30_000,
    })
    .toBe(true);

  await page.getByLabel("注釈表示フィルター").selectOption("callout");
  await expect.poll(() => overlayPixels(page)).toBeGreaterThan(overlayBefore);
  const calloutPixels = await overlayPixels(page);

  await page.getByLabel("注釈表示フィルター").selectOption("hidden");
  await expect.poll(() => overlayPixels(page)).toBeLessThan(calloutPixels);
  const hiddenPixels = await overlayPixels(page);

  await page.reload();
  await page.getByRole("button", { name: "注釈編集モード" }).click();
  await page.getByLabel("注釈表示フィルター").selectOption("callout");
  await expect.poll(() => overlayPixels(page)).toBeGreaterThan(hiddenPixels);
  const reloadPixels = await overlayPixels(page);

  const items = await e2eCallouts();
  const createdIds = items.map((item) => item.id).sort();
  expect(items.some((item) => item.payload?.free_text === "e2e candle callout")).toBe(true);
  expect(items.some((item) => item.payload?.anchor_type === "indicator" && item.payload?.anchor_target === "ma20")).toBe(true);
  writeArtifact("playwright_callout_visual_result.json", {
    ok: true,
    code,
    as_of_date: asOfDate,
    created_annotation_ids: createdIds,
    anchor_types: [...new Set(items.map((item) => item.payload?.anchor_type))].sort(),
    anchor_targets: [...new Set(items.map((item) => item.payload?.anchor_target))].sort(),
    callout_pixels: calloutPixels,
    hidden_pixels: hiddenPixels,
    reload_pixels: reloadPixels,
    bundle_checked: true,
    reload_persistence_checked: true,
  });
});
test("creates region and line callouts through browser selection", async ({ page }) => {
  await openAnnotationMode(page);
  await page.waitForTimeout(6000);
  const chart = await chartBox(page);
  const dailyChart = page.locator(".detail-row-top .detail-chart-wrapper").first();

  await page.getByTestId("chart-reading-tags").fill(e2eTag);
  await page.getByRole("button", { name: "注釈 ボックス範囲" }).click();
  await page.mouse.move(chart.x + chart.width * 0.36, chart.y + chart.height * 0.34);
  await page.mouse.down();
  await page.mouse.move(chart.x + chart.width * 0.61, chart.y + chart.height * 0.54);
  await page.mouse.up();
  await expect(page.getByTestId("annotation-region-type")).toBeVisible();
  await page.getByTestId("annotation-free-text").fill("e2e selectable region");
  await page.getByTestId("annotation-tags").fill(e2eTag);
  const regionId = await waitForE2eAnnotationId("region");

  await page.getByRole("button", { name: "注釈 対象選択" }).click();
  await dailyChart.click({ button: "right", position: { x: chart.width * 0.48, y: chart.height * 0.44 } });
  await expect(page.getByTestId("annotation-region-type")).toBeVisible();
  await page.getByTestId("chart-reading-target-type").selectOption("region");
  await page.getByTestId("chart-reading-note-text").fill("e2e region browser callout");
  await page.getByTestId("chart-reading-tags").fill(e2eTag);
  await page.getByRole("button", { name: "注釈 引き出しコメント" }).click();
  await dailyChart.click({ position: { x: chart.width * 0.48, y: chart.height * 0.44 } });
  await dailyChart.click({ position: { x: chart.width * 0.66, y: chart.height * 0.22 } });
  await expect
    .poll(
      async () =>
        (await e2eCallouts()).some(
          (item) =>
            item.payload?.anchor_type === "region" &&
            item.payload?.anchor_object_id === regionId &&
            item.payload?.resolution === "annotation_id"
        ),
      { timeout: 30_000 }
    )
    .toBe(true);

  await page.getByRole("button", { name: "注釈 水平ライン" }).click();
  await page.mouse.click(chart.x + chart.width * 0.52, chart.y + chart.height * 0.49);
  await expect(page.getByTestId("annotation-line-type")).toBeVisible();
  await page.getByTestId("annotation-line-type").fill("invalidation_line");
  await page.getByTestId("annotation-free-text").fill("e2e selectable line");
  await page.getByTestId("annotation-tags").fill(e2eTag);
  const lineId = await waitForE2eAnnotationId("line");

  await page.getByRole("button", { name: "注釈 対象選択" }).click();
  await dailyChart.click({ button: "right", position: { x: chart.width * 0.50, y: chart.height * 0.49 } });
  await expect(page.getByTestId("annotation-line-type")).toBeVisible();
  await page.getByTestId("chart-reading-target-type").selectOption("line");
  await page.getByTestId("chart-reading-note-text").fill("e2e line browser callout");
  await page.getByTestId("chart-reading-tags").fill(e2eTag);
  await page.getByRole("button", { name: "注釈 引き出しコメント" }).click();
  await dailyChart.click({ position: { x: chart.width * 0.50, y: chart.height * 0.49 } });
  await dailyChart.click({ position: { x: chart.width * 0.72, y: chart.height * 0.25 } });
  await expect
    .poll(
      async () =>
        (await e2eCallouts()).some(
          (item) =>
            item.payload?.anchor_type === "line" &&
            item.payload?.anchor_object_id === lineId &&
            item.payload?.resolution === "annotation_id"
        ),
      { timeout: 30_000 }
    )
    .toBe(true);

  await page.getByLabel("注釈表示フィルター").selectOption("callout");
  const calloutPixels = await overlayPixels(page);
  expect(calloutPixels).toBeGreaterThan(0);
  await page.reload();
  await page.getByRole("button", { name: "注釈編集モード" }).click();
  await page.getByLabel("注釈表示フィルター").selectOption("callout");
  await expect.poll(() => overlayPixels(page)).toBeGreaterThan(0);

  const items = await e2eCallouts();
  writeArtifact("playwright_region_line_callout_selection_result.json", {
    ok: true,
    code,
    as_of_date: asOfDate,
    region_id: regionId,
    line_id: lineId,
    callout_anchor_types: [...new Set(items.map((item) => item.payload?.anchor_type))].sort(),
    region_callout_has_anchor_object_id: items.some((item) => item.payload?.anchor_type === "region" && item.payload?.anchor_object_id === regionId),
    line_callout_has_anchor_object_id: items.some((item) => item.payload?.anchor_type === "line" && item.payload?.anchor_object_id === lineId),
    resolution_values: [...new Set(items.map((item) => item.payload?.resolution).filter(Boolean))].sort(),
    reload_persistence_checked: true,
  });
});
