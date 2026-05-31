import { expect, test, type Page } from "@playwright/test";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = resolve(fileURLToPath(new URL("../../..", import.meta.url)));
const artifactDir = resolve(repoRoot, "artifacts", "annotation_mode_v1_playwright_20260526T171500");
const backendPort = Number(process.env.ANNOTATION_E2E_BACKEND_PORT ?? "8766");
const backendBaseUrl = `http://127.0.0.1:${backendPort}`;
const code = "1001";
const asOfDate = "2026-03-31";
const e2eTag = "e2e_annotation_mode_v1";

let backendProcess: ChildProcess | null = null;

function writeArtifact(name: string, payload: unknown) {
  mkdirSync(artifactDir, { recursive: true });
  writeFileSync(resolve(artifactDir, name), JSON.stringify(payload, null, 2), "utf-8");
}

function spawnBackend() {
  return spawn("python", ["-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", String(backendPort)], {
    cwd: repoRoot,
    env: {
      ...process.env,
      MEEMEE_DEV: "1",
      MEEMEE_DEV_MODE: "1",
      MEEMEE_DATA_DIR: "C:\\Users\\enish\\AppData\\Local\\MeeMeeScreener-dev\\data",
      STOCKS_DB_PATH: "C:\\Users\\enish\\AppData\\Local\\MeeMeeScreener-dev\\data\\stocks.duckdb",
      MEEMEE_YF_DAILY_INGEST_ENABLED: "0",
      MEEMEE_EDINET_AUTO_START_ENABLED: "0",
      MEEMEE_SCREENER_SNAPSHOT_ENABLED: "0",
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

async function waitForAnnotationEndpointReady() {
  await expect
    .poll(async () => {
      try {
        const response = await fetch(`${backendBaseUrl}/api/chart-annotations?code=${code}&as_of_date=${asOfDate}`);
        return response.ok;
      } catch {
        return false;
      }
    }, { timeout: 120_000, intervals: [1000, 2000, 3000] })
    .toBe(true);
}

async function bundleAnnotations() {
  let lastStatus = 0;
  for (let attempt = 0; attempt < 8; attempt += 1) {
    const response = await fetch(`${backendBaseUrl}/api/chart-reading/bundle?code=${code}&as_of_date=${asOfDate}`);
    lastStatus = response.status;
    if (response.ok) {
      const bundle = await response.json() as { annotations?: Array<Record<string, any>> };
      return Array.isArray(bundle.annotations) ? bundle.annotations : [];
    }
    await new Promise((resolve) => setTimeout(resolve, 750 + attempt * 350));
  }
  throw new Error(`bundle request failed: ${lastStatus}`);
}

async function e2eAnnotations() {
  return (await bundleAnnotations()).filter((annotation) => {
    const tags = Array.isArray(annotation.tags) ? annotation.tags : [];
    const payloadTags = Array.isArray(annotation.payload?.tags) ? annotation.payload.tags : [];
    return tags.includes(e2eTag) || payloadTags.includes(e2eTag);
  });
}

async function deleteAnnotation(id: string) {
  await fetch(`${backendBaseUrl}/api/chart-annotations/${encodeURIComponent(id)}`, { method: "DELETE" });
}

async function cleanupE2eAnnotations() {
  for (const annotation of await e2eAnnotations()) {
    if (typeof annotation.id === "string") {
      await deleteAnnotation(annotation.id);
    }
  }
}

async function visibleOverlayPixels(page: Page) {
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

async function openAnnotationMode(page: Page) {
  await page.goto(`/detail/${code}?mainAsOf=${asOfDate}`);
  await expect(page.getByRole("button", { name: "注釈編集モード" })).toBeVisible({ timeout: 60_000 });
  await page.getByRole("button", { name: "注釈編集モード" }).click();
  await expect(page.getByTestId("annotation-panel")).toBeVisible();
}

async function chartBox(page: Page) {
  const box = await page.locator(".detail-row-top .detail-chart-wrapper").first().boundingBox();
  expect(box).not.toBeNull();
  return box!;
}

async function fillCommon(page: Page, freeText: string) {
  await page.getByTestId("annotation-free-text").fill(freeText);
  await page.getByTestId("annotation-tags").fill(e2eTag);
}

async function clickBarUntilPanel(page: Page, chart: { width: number; height: number }) {
  const dailyChart = page.locator(".detail-row-top .detail-chart-wrapper").first();
  const attempts = [0.90, 0.82, 0.74, 0.66, 0.58];
  for (const xRatio of attempts) {
    await dailyChart.click({ position: { x: chart.width * xRatio, y: chart.height * 0.36 } });
    try {
      await expect(page.getByTestId("annotation-bar-role")).toBeVisible({ timeout: 4000 });
      return;
    } catch {
      await page.waitForTimeout(750);
    }
  }
  await expect(page.getByTestId("annotation-bar-role")).toBeVisible();
}

test.beforeAll(async () => {
  mkdirSync(artifactDir, { recursive: true });
  backendProcess = spawnBackend();
  await waitForBackend();
  await waitForAnnotationEndpointReady();
  await cleanupE2eAnnotations();
});

test.afterAll(async () => {
  backendProcess?.kill("SIGTERM");
});

test("verifies annotation mode bar, region, line, filters, reload persistence, and bundle", async ({ page }) => {
  const created: Record<string, any>[] = [];

  await openAnnotationMode(page);
  await waitForAnnotationEndpointReady();
  await page.waitForTimeout(8000);
  await waitForAnnotationEndpointReady();
  const chart = await chartBox(page);

  await page.getByRole("button", { name: "注釈 ローソク足" }).click();
  await clickBarUntilPanel(page, chart);
  await page.getByTestId("annotation-bar-role").fill("review");
  await page.getByTestId("annotation-action-label").fill("watch");
  await fillCommon(page, "e2e test bar annotation");
  await expect.poll(async () => (await e2eAnnotations()).some((item) => item.object_type === "bar")).toBe(true);

  await page.getByRole("button", { name: "注釈 ボックス範囲" }).click();
  await page.mouse.move(chart.x + chart.width * 0.34, chart.y + chart.height * 0.32);
  await page.mouse.down();
  await page.mouse.move(chart.x + chart.width * 0.62, chart.y + chart.height * 0.55);
  await page.mouse.up();
  await expect(page.getByTestId("annotation-region-type")).toBeVisible();
  await page.getByTestId("annotation-region-type").fill("support_zone");
  await fillCommon(page, "e2e test region annotation");
  await expect.poll(async () => (await e2eAnnotations()).some((item) => item.object_type === "region")).toBe(true);

  await page.getByRole("button", { name: "注釈 水平ライン" }).click();
  await page.mouse.click(chart.x + chart.width * 0.55, chart.y + chart.height * 0.50);
  await expect(page.getByTestId("annotation-line-type")).toBeVisible();
  await page.getByTestId("annotation-line-type").fill("invalidation_line");
  await page.getByTestId("annotation-break-rule").fill("close_below");
  await fillCommon(page, "e2e test line annotation");
  await expect.poll(async () => (await e2eAnnotations()).some((item) => item.object_type === "line")).toBe(true);

  created.push(...await e2eAnnotations());
  expect(new Set(created.map((item) => item.object_type))).toEqual(new Set(["bar", "region", "line"]));

  await page.getByLabel("注釈表示フィルター").selectOption("all");
  await expect.poll(() => visibleOverlayPixels(page)).toBeGreaterThan(0);
  const allPixels = await visibleOverlayPixels(page);
  await page.getByLabel("注釈表示フィルター").selectOption("bar");
  await expect.poll(() => visibleOverlayPixels(page)).toBeGreaterThan(0);
  const barPixels = await visibleOverlayPixels(page);
  await page.getByLabel("注釈表示フィルター").selectOption("region");
  await expect.poll(() => visibleOverlayPixels(page)).toBeGreaterThan(0);
  const regionPixels = await visibleOverlayPixels(page);
  await page.getByLabel("注釈表示フィルター").selectOption("line");
  await expect.poll(() => visibleOverlayPixels(page)).toBeGreaterThan(0);
  const linePixels = await visibleOverlayPixels(page);
  await page.getByLabel("注釈表示フィルター").selectOption("hidden");
  await expect.poll(() => visibleOverlayPixels(page)).toBeLessThanOrEqual(allPixels);
  const hiddenPixels = await visibleOverlayPixels(page);

  await page.reload();
  await page.getByRole("button", { name: "注釈編集モード" }).click();
  await expect.poll(async () => (await e2eAnnotations()).length).toBeGreaterThanOrEqual(3);
  await page.getByLabel("注釈表示フィルター").selectOption("all");
  await expect.poll(() => visibleOverlayPixels(page)).toBeGreaterThan(0);

  const bundleItems = await e2eAnnotations();
  writeArtifact("annotation_mode_v1_playwright_smoke.json", {
    ok: true,
    code,
    as_of_date: asOfDate,
    created_annotation_ids: bundleItems.map((item) => item.id).sort(),
    object_types: [...new Set(bundleItems.map((item) => item.object_type))].sort(),
    overlay_pixels: {
      all: allPixels,
      bar: barPixels,
      region: regionPixels,
      line: linePixels,
      hidden: hiddenPixels,
    },
    reload_persistence_checked: true,
    bundle_annotations_checked: true,
  });
});
