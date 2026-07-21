import playwright from "../app/frontend/node_modules/playwright/index.js";
import { appendFileSync, mkdirSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";

const AXIS_ID = "meemee_detail_clean_screenshot_dataset_v1";
const { chromium } = playwright;

const parseArgs = () => {
  const args = process.argv.slice(2);
  const parsed = {
    baseUrl: "http://127.0.0.1:5174",
    apiBase: "http://127.0.0.1:28888/api",
    outputRoot: "G:/Tradex/meemee_detail_clean_screenshot_dataset_v1",
    codes: [],
    samples: [],
    viewportWidth: 1440,
    viewportHeight: 1000,
    timeoutMs: 90000,
    viewportFallback: false,
    centered: false,
    centerLookbackMonths: 8,
    centerLookaheadMonths: 3,
    dailyMonthlyOnly: false,
  };
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    const next = args[index + 1];
    if (arg === "--base-url" && next) {
      parsed.baseUrl = next;
      index += 1;
    } else if (arg === "--api-base" && next) {
      parsed.apiBase = next;
      index += 1;
    } else if (arg === "--output-root" && next) {
      parsed.outputRoot = next;
      index += 1;
    } else if (arg === "--codes" && next) {
      parsed.codes = next.split(",").map((code) => code.trim()).filter(Boolean);
      index += 1;
    } else if (arg === "--samples" && next) {
      parsed.samples = next
        .split(",")
        .map((sample) => sample.trim())
        .filter(Boolean)
        .map((sample) => {
          const [code, asOf] = sample.split(":");
          if (!code || !asOf) {
            throw new Error(`Invalid sample '${sample}'. Expected code:YYYY-MM-DD`);
          }
          return { code: code.trim(), asOf: asOf.trim() };
        });
      index += 1;
    } else if (arg === "--viewport" && next) {
      const [width, height] = next.split("x").map((value) => Number.parseInt(value, 10));
      if (Number.isFinite(width) && Number.isFinite(height)) {
        parsed.viewportWidth = width;
        parsed.viewportHeight = height;
      }
      index += 1;
    } else if (arg === "--timeout-ms" && next) {
      const timeoutMs = Number.parseInt(next, 10);
      if (Number.isFinite(timeoutMs)) parsed.timeoutMs = timeoutMs;
      index += 1;
    } else if (arg === "--viewport-fallback") {
      parsed.viewportFallback = true;
    } else if (arg === "--centered") {
      parsed.centered = true;
    } else if (arg === "--center-lookback-months" && next) {
      const months = Number.parseInt(next, 10);
      if (Number.isFinite(months) && months > 0) parsed.centerLookbackMonths = months;
      index += 1;
    } else if (arg === "--center-lookahead-months" && next) {
      const months = Number.parseInt(next, 10);
      if (Number.isFinite(months) && months > 0) parsed.centerLookaheadMonths = months;
      index += 1;
    } else if (arg === "--daily-monthly-only") {
      parsed.dailyMonthlyOnly = true;
    }
  }
  if (parsed.codes.length === 0 && parsed.samples.length === 0) {
    throw new Error("Pass --codes 7327,2809,... or --samples 7327:2025-12-30,2809:2025-12-30");
  }
  if (parsed.samples.length === 0) {
    parsed.samples = parsed.codes.map((code) => ({ code, asOf: null }));
  }
  return parsed;
};

const tag = () => {
  const now = new Date();
  return now.toISOString().replace(/[-:]/g, "").replace(/\.\d{3}Z$/, "Z");
};

const writeJson = (path, payload) => {
  writeFileSync(path, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
};

const writeJsonl = (path, rows) => {
  writeFileSync(path, rows.map((row) => JSON.stringify(row)).join("\n") + "\n", "utf8");
};

const appendJsonl = (path, row) => {
  appendFileSync(path, `${JSON.stringify(row)}\n`, "utf8");
};

const auditPayload = (options, rows, failures) => ({
  schema_version: "meemee_detail_clean_screenshot_dataset_v1_audit",
  generated_at: new Date().toISOString(),
  boundary_owner: "MeeMee",
  purpose: "clean chart screenshot export for TRADEX shadow dataset comparison",
  base_url: options.baseUrl,
  api_base: options.apiBase,
  requested_code_count: new Set(options.samples.map((sample) => sample.code)).size,
  requested_sample_count: options.samples.length,
  exported_image_count: rows.length,
  failed_capture_count: failures.length,
  clean_ui_policy: {
    route: "/detail-shot/:code",
    query: options.centered ? "cleanScreenshot=1&centerAsOf=YYYY-MM-DD" : "cleanScreenshot=1",
    excludes_ui_chrome: true,
    chart_only: true,
    retains: options.dailyMonthlyOnly
      ? ["stock_code", "stock_name", "daily_chart", "monthly_chart"]
      : ["stock_code", "stock_name", "daily_chart", "weekly_chart", "monthly_chart"],
    review_timeframes: options.dailyMonthlyOnly ? ["monthly", "daily"] : ["monthly", "weekly", "daily"],
    excludes_chart_overlays: ["right_rail", "memo_panel", "manual_annotations", "box_overlay", "battle_zones", "gap_bands", "trade_markers"],
    centered_on_as_of: options.centered,
  },
  non_scope: [
    "model training",
    "prediction adoption",
    "production ranking mutation",
    "runtime DB write",
  ],
  judgment: failures.length === 0 && rows.length > 0 ? "pass_clean_detail_screenshot_export" : "hold_incomplete_clean_detail_screenshot_export",
});

const main = async () => {
  const options = parseArgs();
  const runDir = resolve(options.outputRoot, `${tag()}-${AXIS_ID}`);
  const imageDir = resolve(runDir, "images");
  const manifestPath = resolve(runDir, "image_manifest.jsonl");
  const failuresPath = resolve(runDir, "failed_captures.jsonl");
  const auditPath = resolve(runDir, "export_audit.json");
  mkdirSync(imageDir, { recursive: true });
  writeFileSync(manifestPath, "", "utf8");
  writeFileSync(failuresPath, "", "utf8");

  const rows = [];
  const failures = [];
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage({
      viewport: { width: options.viewportWidth, height: options.viewportHeight },
      deviceScaleFactor: 1,
    });
    await page.exposeFunction("__saveDetailScreenshotForBatch", async (dataUri, filename) => {
      const base64 = String(dataUri).split(",", 2)[1] || String(dataUri);
      const savedPath = resolve(imageDir, filename);
      mkdirSync(dirname(savedPath), { recursive: true });
      writeFileSync(savedPath, Buffer.from(base64, "base64"));
      return { success: true, savedPath, savedDir: imageDir, fileName: filename };
    });
    await page.addInitScript((apiBase) => {
      window.MEEMEE_API_BASE = apiBase;
      window.pywebview = {
        api: {
          save_screenshot: (dataUri, filename) => window.__saveDetailScreenshotForBatch(dataUri, filename),
          open_screenshot_dir: async () => true,
        },
      };
    }, options.apiBase);

    for (const sample of options.samples) {
      const { code, asOf } = sample;
      const params = new URLSearchParams({ cleanScreenshot: "1", batch: String(Date.now()) });
      if (options.dailyMonthlyOnly) {
        params.set("reviewTimeframes", "daily-monthly");
      }
      if (asOf) {
        if (options.centered) {
          params.set("centerAsOf", asOf);
          params.set("centerLookbackMonths", String(options.centerLookbackMonths));
          params.set("centerLookaheadMonths", String(options.centerLookaheadMonths));
        } else {
          params.set("mainAsOf", asOf);
        }
      }
      const url = `${options.baseUrl.replace(/\/$/, "")}/detail-shot/${encodeURIComponent(code)}?${params.toString()}`;
      try {
        await page.goto(url, { waitUntil: "domcontentloaded", timeout: options.timeoutMs });
        const handle = await page.waitForFunction(
          () => window.__meemeeDetailScreenshotResult,
          null,
          { timeout: options.timeoutMs },
        );
        const result = await handle.jsonValue();
        if (!result?.success || !result?.savedPath) {
          const failure = { code, as_of: asOf, status: "save_failed", result };
          failures.push(failure);
          appendJsonl(failuresPath, failure);
          writeJson(auditPath, auditPayload(options, rows, failures));
          continue;
        }
        const manifestRow = {
          code,
          as_of: asOf,
          route: `/detail-shot/${code}`,
          url,
          clean_screenshot: true,
          centered_screenshot: options.centered,
          review_timeframes: options.dailyMonthlyOnly ? ["monthly", "daily"] : ["monthly", "weekly", "daily"],
          center_lookback_months: options.centered ? options.centerLookbackMonths : null,
          center_lookahead_months: options.centered ? options.centerLookaheadMonths : null,
          viewport: `${options.viewportWidth}x${options.viewportHeight}`,
          image_relpath: `images/${result.filename}`,
          saved_path: result.savedPath,
          copied: Boolean(result.copied),
        };
        rows.push(manifestRow);
        appendJsonl(manifestPath, manifestRow);
        writeJson(auditPath, auditPayload(options, rows, failures));
      } catch (error) {
        if (options.viewportFallback) {
          try {
            await page.waitForTimeout(1500);
            const safeAsOf = asOf ? asOf.replace(/[^0-9]/g, "") : "latest";
            const filename = `MeeMee_Detail_${code}_${tag().replace(/[^0-9]/g, "").slice(0, 14)}_${safeAsOf}_viewport_fallback.png`;
            const savedPath = resolve(imageDir, filename);
            await page.screenshot({ path: savedPath, fullPage: false });
            const manifestRow = {
              code,
              as_of: asOf,
              route: `/detail-shot/${code}`,
              url,
              clean_screenshot: true,
              centered_screenshot: options.centered,
              review_timeframes: options.dailyMonthlyOnly ? ["monthly", "daily"] : ["monthly", "weekly", "daily"],
              center_lookback_months: options.centered ? options.centerLookbackMonths : null,
              center_lookahead_months: options.centered ? options.centerLookaheadMonths : null,
              viewport_fallback: true,
              viewport: `${options.viewportWidth}x${options.viewportHeight}`,
              image_relpath: `images/${filename}`,
              saved_path: savedPath,
              original_error: error instanceof Error ? error.message : String(error),
            };
            rows.push(manifestRow);
            appendJsonl(manifestPath, manifestRow);
            writeJson(auditPath, auditPayload(options, rows, failures));
            continue;
          } catch (fallbackError) {
            const failure = {
              code,
              as_of: asOf,
              status: "capture_failed_after_viewport_fallback",
              error: fallbackError instanceof Error ? fallbackError.message : String(fallbackError),
              original_error: error instanceof Error ? error.message : String(error),
            };
            failures.push(failure);
            appendJsonl(failuresPath, failure);
            writeJson(auditPath, auditPayload(options, rows, failures));
            continue;
          }
        }
        const failure = { code, as_of: asOf, status: "capture_failed", error: error instanceof Error ? error.message : String(error) };
        failures.push(failure);
        appendJsonl(failuresPath, failure);
        writeJson(auditPath, auditPayload(options, rows, failures));
      }
    }
  } finally {
    await browser.close();
  }

  writeJsonl(manifestPath, rows);
  writeJsonl(failuresPath, failures);
  const audit = auditPayload(options, rows, failures);
  writeJson(auditPath, audit);
  console.log(JSON.stringify({ runDir, ...audit }, null, 2));
};

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
