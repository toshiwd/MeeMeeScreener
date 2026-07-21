import playwright from "../app/frontend/node_modules/playwright/index.js";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";

const { chromium } = playwright;
const args = process.argv.slice(2);
const value = (name, fallback = null) => {
  const index = args.indexOf(name);
  return index >= 0 ? args[index + 1] : fallback;
};
const configPath = resolve(value("--config"));
const outputRoot = resolve(value("--output"));
const baseUrl = value("--base-url", "http://127.0.0.1:5174").replace(/\/$/, "");
const apiBase = value("--api-base", "http://127.0.0.1:8000/api");
const timeoutMs = Number(value("--timeout-ms", "120000"));
const config = JSON.parse(readFileSync(configPath, "utf8"));
mkdirSync(outputRoot, { recursive: true });

const rows = [];
const failures = [];
const browser = await chromium.launch({ headless: true });
try {
  const page = await browser.newPage({ viewport: { width: 2048, height: 1100 }, deviceScaleFactor: 1 });
  await page.exposeFunction("__saveDirectionMarkerScreenshot", async (dataUri, filename) => {
    const base64 = String(dataUri).split(",", 2)[1] || String(dataUri);
    const savedPath = resolve(outputRoot, `${filename.replace(/\.png$/i, "")}_direction_markers.png`);
    mkdirSync(dirname(savedPath), { recursive: true });
    writeFileSync(savedPath, Buffer.from(base64, "base64"));
    return { success: true, savedPath, savedDir: outputRoot, fileName: savedPath.split(/[\\/]/).pop() };
  });
  await page.addInitScript((configuredApiBase) => {
    window.MEEMEE_API_BASE = configuredApiBase;
    window.pywebview = { api: { save_screenshot: (dataUri, filename) => window.__saveDirectionMarkerScreenshot(dataUri, filename), open_screenshot_dir: async () => true } };
  }, apiBase);
  for (const example of config.examples) {
    const params = new URLSearchParams({
      cleanScreenshot: "1", reviewTimeframes: "daily-monthly", mainAsOf: example.as_of,
      researchMarkers: JSON.stringify(example.markers), batch: String(Date.now()),
    });
    if (example.research_band) params.set("researchBand", JSON.stringify(example.research_band));
    const url = `${baseUrl}/detail-shot/${encodeURIComponent(example.code)}?${params.toString()}`;
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: timeoutMs });
      const handle = await page.waitForFunction(() => window.__meemeeDetailScreenshotResult, null, { timeout: timeoutMs });
      const result = await handle.jsonValue();
      if (!result?.success || !result?.savedPath) throw new Error(JSON.stringify(result));
      rows.push({ ...example, url, saved_path: result.savedPath });
    } catch (error) {
      failures.push({ case_id: example.case_id, code: example.code, error: error instanceof Error ? error.message : String(error) });
    }
  }
} finally {
  await browser.close();
}
const audit = {
  schema_version: "meemee_sideways_direction_marker_capture_v1",
  capture_owner: "MeeMee", research_owner: "TRADEX", review_only: true,
  requested: config.examples.length, captured: rows.length, failed: failures.length,
  runtime_db_write: false, rows, failures,
};
writeFileSync(resolve(outputRoot, "capture_audit.json"), `${JSON.stringify(audit, null, 2)}\n`, "utf8");
console.log(JSON.stringify(audit, null, 2));
