import { expect, test, type Page } from "@playwright/test";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = resolve(fileURLToPath(new URL("../../..", import.meta.url)));
const artifactDir = resolve(repoRoot, "artifacts", process.env.CHART_NOTE_ARTIFACT_DIR ?? "chart_note_mode_v1_playwright");
const backendPort = Number(process.env.CHART_NOTE_E2E_BACKEND_PORT ?? "8000");
const backendBaseUrl = `http://127.0.0.1:${backendPort}`;
const code = "1001";
const asOfDate = "2026-03-31";
const e2eTag = "e2e_chart_note_mode_v1";
const paragraphText = "e2e 20under20 previous box lower support";

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
  const response = await fetch(`${backendBaseUrl}/api/chart-reading/bundle?code=${code}&as_of_date=${asOfDate}`);
  if (!response.ok) throw new Error(`bundle request failed: ${response.status}`);
  return response.json() as Promise<{ notes?: Array<Record<string, any>>; annotations?: Array<Record<string, any>> }>;
}

async function e2eAnnotations() {
  const payload = await bundle();
  return (payload.annotations ?? []).filter((annotation) => {
    const tags = Array.isArray(annotation.tags) ? annotation.tags : [];
    const payloadTags = Array.isArray(annotation.payload?.tags) ? annotation.payload.tags : [];
    const freeText = String(annotation.payload?.free_text ?? "");
    return tags.includes(e2eTag) || payloadTags.includes(e2eTag) || freeText.startsWith("e2e chart note");
  });
}

async function cleanupE2eData() {
  for (const annotation of await e2eAnnotations()) {
    if (typeof annotation.id === "string") {
      await fetch(`${backendBaseUrl}/api/chart-annotations/${encodeURIComponent(annotation.id)}`, { method: "DELETE" });
    }
  }
  await fetch(`${backendBaseUrl}/api/chart-notes`, {
    method: "PUT",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      code,
      as_of_date: asOfDate,
      timeframe: "mixed",
      note_text: "",
      paragraphs: [],
      tags: [],
      no_lookahead: true,
    }),
  });
}

async function openDetail(page: Page) {
  await page.goto(`/detail/${code}?mainAsOf=${asOfDate}`);
  await expect(page.getByRole("button", { name: "注釈編集モード" })).toBeVisible({ timeout: 60_000 });
}

async function chartBox(page: Page) {
  const box = await page.locator(".detail-row-top .detail-chart-wrapper").first().boundingBox();
  expect(box).not.toBeNull();
  return box!;
}

test.beforeAll(async () => {
  mkdirSync(artifactDir, { recursive: true });
  backendProcess = spawnBackend();
  await waitForBackend();
  await cleanupE2eData();
});

test.afterAll(async () => {
  try {
    await cleanupE2eData();
    writeArtifact("cleanup_result.json", {
      ok: true,
      remaining_e2e_annotations: (await e2eAnnotations()).length,
      remaining_e2e_notes: (await bundle()).notes?.filter((note) => note.title === "e2e chart note").length ?? 0,
    });
  } finally {
    backendProcess?.kill("SIGTERM");
  }
});

test("creates paragraph linked_objects in チャートノート", async ({ page }) => {
  await openDetail(page);
  await page.getByRole("button", { name: "注釈編集モード" }).click();
  await page.waitForTimeout(6000);
  const chart = await chartBox(page);
  const dailyChart = page.locator(".detail-row-top .detail-chart-wrapper").first();

  await page.getByRole("button", { name: "注釈 ボックス範囲" }).click();
  await page.mouse.move(chart.x + chart.width * 0.36, chart.y + chart.height * 0.34);
  await page.mouse.down();
  await page.mouse.move(chart.x + chart.width * 0.61, chart.y + chart.height * 0.54);
  await page.mouse.up();
  await expect(page.getByTestId("annotation-region-type")).toBeVisible();
  await page.getByTestId("annotation-free-text").fill("e2e chart note selectable region");
  await page.getByTestId("annotation-tags").fill(e2eTag);
  await expect.poll(async () => (await e2eAnnotations()).some((item) => item.object_type === "region"), { timeout: 30_000 }).toBe(true);
  const regionId = (await e2eAnnotations()).find((item) => item.object_type === "region")?.id as string;

  await page.getByRole("button", { name: "注釈 対象選択" }).click();
  await dailyChart.click({ button: "right", position: { x: chart.width * 0.48, y: chart.height * 0.44 } });
  await expect(page.getByTestId("annotation-region-type")).toBeVisible();

  await page.getByRole("button", { name: "チャートノート" }).click();
  await expect(page.getByTestId("chart-note-panel")).toBeVisible();
  await page.waitForTimeout(1000);
  await page.getByTestId("chart-note-title").fill("e2e chart note");
  await page.getByTestId("chart-note-paragraph-text").first().fill(paragraphText);
  await page.getByTestId("chart-note-paragraph-comment-type").first().selectOption("region_reading");
  await page.getByTestId("chart-note-paragraph-tags").first().fill("20荳・0, 蜑阪・繝・け繧ｹ荳矩剞");
  await page.getByTestId("chart-note-link-selected").first().click();
  await page.getByTestId("chart-note-link-ma20").first().click();
  await expect(page.getByTestId("chart-note-linked-objects").first()).toContainText("ボックス");
  await expect(page.getByTestId("chart-note-linked-objects").first()).toContainText("移動平均:ma20");
  await page.getByTestId("chart-note-save").click();

  await expect
    .poll(async () => {
      const currentBundle = await bundle();
      const note = currentBundle.notes?.find((item) =>
        item.paragraphs?.some((paragraph: Record<string, any>) => paragraph.text === paragraphText)
      );
      writeArtifact("chart_note_mode_v1_smoke.json", {
        ok: Boolean(note),
        observed_note: note ?? null,
        note_summaries: (currentBundle.notes ?? []).map((item) => ({
          as_of_date: item.as_of_date,
          timeframe: item.timeframe,
          title: item.title,
          note_text: item.note_text,
          paragraphs: item.paragraphs,
        })),
      });
      return Boolean(
        note?.paragraphs?.some(
          (paragraph: Record<string, any>) =>
            paragraph.text === paragraphText &&
            paragraph.linked_objects?.some((link: Record<string, any>) => link.annotation_id === regionId) &&
            paragraph.linked_objects?.some((link: Record<string, any>) => link.anchor_target === "ma20")
        )
      );
    }, { timeout: 30_000 })
    .toBe(true);

  await page.reload();
  await page.getByRole("button", { name: "チャートノート" }).click();
  await expect(page.getByTestId("chart-note-paragraph-text").first()).toHaveValue(paragraphText);
  await expect(page.getByTestId("chart-note-linked-objects").first()).toContainText("移動平均:ma20");

  const sample = await bundle();
  const note = sample.notes?.find((item) =>
    item.paragraphs?.some((paragraph: Record<string, any>) => paragraph.text === paragraphText)
  );
  writeArtifact("playwright_chart_note_mode_result.json", {
    ok: true,
    code,
    as_of_date: asOfDate,
    region_id: regionId,
    paragraph_count: note?.paragraphs?.length ?? 0,
    linked_objects: note?.paragraphs?.[0]?.linked_objects ?? [],
    reload_persistence_checked: true,
    bundle_checked: true,
  });
  writeArtifact("sample_bundle_with_paragraph_links.json", sample);
});
