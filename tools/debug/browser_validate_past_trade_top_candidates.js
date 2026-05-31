const fs = require("fs");
const path = require("path");
const { spawnSync } = require("child_process");

const frontendNodeModules = path.resolve(__dirname, "..", "..", "app", "frontend", "node_modules");
process.env.NODE_PATH = [frontendNodeModules, process.env.NODE_PATH].filter(Boolean).join(path.delimiter);
require("module").Module._initPaths();

const { chromium } = require("playwright");

function parseArgs(argv) {
  const options = {
    baseUrl: "http://localhost:8000",
    input: path.resolve(__dirname, "..", "..", "artifacts", "actionability", "past-trade-top-validation-20260522", "past_trade_top_candidate_validation.json"),
    outputDir: path.resolve(__dirname, "..", "..", "artifacts", "actionability", "past-trade-top-browser-validation-20260522"),
    mode: "all",
    max: 0,
  };
  for (let index = 2; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--base-url") options.baseUrl = argv[++index] || options.baseUrl;
    else if (arg === "--input") options.input = path.resolve(argv[++index] || options.input);
    else if (arg === "--output-dir") options.outputDir = path.resolve(argv[++index] || options.outputDir);
    else if (arg === "--mode") options.mode = argv[++index] || options.mode;
    else if (arg === "--max") options.max = Number(argv[++index] || 0);
  }
  return options;
}

function ymdToIso(value) {
  const text = String(value || "").replace(/[^0-9]/g, "");
  if (text.length !== 8) return null;
  return `${text.slice(0, 4)}-${text.slice(4, 6)}-${text.slice(6, 8)}`;
}

function isoFromUnixSeconds(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  return new Date(num * 1000).toISOString().slice(0, 10);
}

function numberOrNull(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function pct(value) {
  const num = numberOrNull(value);
  return num == null ? null : num * 100;
}

function runVisualAnalysis(screenshotPath) {
  const scriptPath = path.resolve(__dirname, "analyze_detail_chart_screenshot.py");
  const result = spawnSync("python", [scriptPath, screenshotPath], {
    cwd: path.resolve(__dirname, "..", ".."),
    encoding: "utf8",
    maxBuffer: 1024 * 1024 * 16,
  });
  if (result.status !== 0) {
    return { confirmed: false, reason: (result.stderr || result.stdout || "analysis_failed").trim() };
  }
  try {
    return JSON.parse(result.stdout);
  } catch (error) {
    return { confirmed: false, reason: `analysis_json_parse_failed:${error.message}` };
  }
}

async function fetchJson(baseUrl, endpoint, init = undefined) {
  const url = `${baseUrl.replace(/\/$/, "")}${endpoint}`;
  const response = await fetch(url, init);
  if (!response.ok) {
    throw new Error(`HTTP ${response.status} ${response.statusText}: ${url}`);
  }
  return response.json();
}

async function fetchChartRightEdge(baseUrl, code, asofIso) {
  const payload = {
    codes: [String(code)],
    timeframes: ["daily"],
    limit: 2000,
    timeframeLimits: { daily: 2000 },
    includeProvisional: true,
    includeBoxes: true,
    asof: asofIso,
  };
  const body = await fetchJson(baseUrl, "/api/batch_bars_v3", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
  });
  const bars = body?.items?.[String(code)]?.daily?.bars || [];
  const last = bars.length ? bars[bars.length - 1] : null;
  const lastIso = last ? isoFromUnixSeconds(last[0]) : null;
  return {
    chart_right_edge_date: lastIso,
    browser_asof_matches_eval_date: lastIso === asofIso,
    daily_bar_count: bars.length,
    data_version: body?.meta?.data_version || null,
  };
}

function selectObservations(source, mode, max) {
  const observations = Array.isArray(source.observations) ? source.observations : [];
  if (mode === "stratified") {
    const bad = observations.filter((row) => row.prognosis === "bad");
    const good = observations.filter((row) => row.prognosis === "good").slice(0, 20);
    const neutral = observations.filter((row) => row.prognosis === "neutral").slice(0, 20);
    return [...bad, ...good, ...neutral].slice(0, max > 0 ? max : Number.MAX_SAFE_INTEGER);
  }
  return observations.slice(0, max > 0 ? max : Number.MAX_SAFE_INTEGER);
}

function issueTags(row, visualAi) {
  const tags = [];
  const side = row.side;
  const sideReturn = numberOrNull(row.side_forward_return);
  const change = numberOrNull(row.changePct);
  const upper = numberOrNull(row.candleUpperWickRatio);
  const lower = numberOrNull(row.candleLowerWickRatio);
  const distMa20 = numberOrNull(row.distMa20Signed);
  const monthlyBoxPos = numberOrNull(row.monthlyBoxPos);
  const monthlyBoxState = String(row.monthlyBoxState || "");
  const entryClass = String(row.tradeEntryClass || "");
  const setupType = String(row.setupType || "");
  const visualReview = side === "short" ? visualAi?.short_visual_review : visualAi?.long_visual_review;
  const visualReasons = Array.isArray(visualReview?.reasons) ? visualReview.reasons : [];

  if (side === "buy") {
    if ((monthlyBoxPos != null && monthlyBoxPos >= 0.95) || monthlyBoxState === "box_upper" || visualReasons.includes("visual_high_zone")) {
      tags.push("buy_chase_at_box_top");
    }
    if ((distMa20 != null && distMa20 >= 0.075) || visualReasons.includes("visual_chase_risk")) {
      tags.push("buy_extended_without_pullback");
    }
    if (sideReturn != null && sideReturn < 0) tags.push("buy_failed_followthrough");
  } else {
    if ((change != null && change <= -0.03) || setupType === "breakdown") tags.push("short_late_breakdown_chase");
    if ((lower != null && lower >= 0.45) || visualReasons.includes("visual_low_zone")) tags.push("short_lower_wick_rebound");
    if (!["failed_high_retest_short", "upper_rejection_primary"].includes(entryClass)) tags.push("short_failed_retest_absent");
    if (distMa20 != null && distMa20 <= -0.10) tags.push("short_too_extended_below_ma");
    if (entryClass === "failed_high_retest_short" && upper != null && upper < 0.35 && !(change != null && change <= -0.0075)) {
      tags.push("short_failed_retest_absent");
    }
  }
  return [...new Set(tags)];
}

function summarizeBySide(rows) {
  const summary = {};
  for (const side of ["buy", "short"]) {
    const sideRows = rows.filter((row) => row.side === side);
    summary[side] = { good: 0, neutral: 0, bad: 0, missing: 0 };
    for (const row of sideRows) {
      const key = ["good", "neutral", "bad"].includes(row.prognosis) ? row.prognosis : "missing";
      summary[side][key] += 1;
    }
  }
  return summary;
}

async function main() {
  const options = parseArgs(process.argv);
  const source = JSON.parse(fs.readFileSync(options.input, "utf8"));
  const observations = selectObservations(source, options.mode, options.max);
  const screenshotsDir = path.join(options.outputDir, "screenshots");
  fs.mkdirSync(screenshotsDir, { recursive: true });

  const health = await fetchJson(options.baseUrl, "/api/health");
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({
    viewport: { width: 1920, height: 900 },
    deviceScaleFactor: 1,
    locale: "ja-JP",
    timezoneId: "Asia/Tokyo",
  });

  const records = [];
  const missing = [];
  for (let index = 0; index < observations.length; index += 1) {
    const row = observations[index];
    const asofIso = row.as_of_iso || ymdToIso(row.as_of);
    const code = String(row.code || "");
    const side = String(row.side || "");
    const rank = Number(row.rank || 0);
    const prognosis = String(row.prognosis || "missing");
    const filename = `${String(index + 1).padStart(3, "0")}_${side}_${row.as_of}_${String(rank).padStart(2, "0")}_${code}_${prognosis}.png`;
    const screenshotPath = path.join(screenshotsDir, filename);
    const url = `${options.baseUrl.replace(/\/$/, "")}/detail/${encodeURIComponent(code)}?mainAsOf=${encodeURIComponent(asofIso)}`;
    let edge = null;
    let visualAi = null;
    let error = null;
    try {
      edge = await fetchChartRightEdge(options.baseUrl, code, asofIso);
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 45_000 });
      await page.waitForTimeout(1800);
      await page.waitForFunction(
        () => {
          const text = document.body.innerText || "";
          return text.includes("7MA") || text.includes("20MA") || document.querySelector("canvas");
        },
        null,
        { timeout: 20_000 },
      ).catch(() => null);
      await page.waitForTimeout(700);
      await page.screenshot({ path: screenshotPath, fullPage: false });
      visualAi = runVisualAnalysis(screenshotPath);
    } catch (err) {
      error = err && err.stack ? err.stack : String(err);
      missing.push({ code, side, rank, as_of: row.as_of, error });
    }
    records.push({
      eval_date: asofIso,
      code,
      name: row.name || null,
      side,
      rank,
      outcome_10d: numberOrNull(row.side_forward_return),
      prognosis,
      screenshot_path: fs.existsSync(screenshotPath) ? screenshotPath : null,
      chart_right_edge_date: edge?.chart_right_edge_date || null,
      browser_asof_date_matches_eval_date: edge?.browser_asof_matches_eval_date === true,
      daily_bar_count: edge?.daily_bar_count || 0,
      visual_ai: visualAi,
      visual_issue_tags: issueTags(row, visualAi),
      source_fields: {
        tradePriorityScore: numberOrNull(row.tradePriorityScore),
        tradeEntryClass: row.tradeEntryClass || null,
        setupType: row.setupType || null,
        changePct: numberOrNull(row.changePct),
        candleUpperWickRatio: numberOrNull(row.candleUpperWickRatio),
        candleLowerWickRatio: numberOrNull(row.candleLowerWickRatio),
        distMa20Signed: numberOrNull(row.distMa20Signed),
        monthlyBoxState: row.monthlyBoxState || null,
        monthlyBoxPos: numberOrNull(row.monthlyBoxPos),
      },
      error,
    });
    process.stdout.write(JSON.stringify({ progress: index + 1, total: observations.length, code, side, asof: asofIso, error: error ? "yes" : "no" }) + "\n");
  }

  await browser.close();

  const asofMatchCount = records.filter((row) => row.browser_asof_date_matches_eval_date).length;
  const asofMismatchCount = records.length - asofMatchCount;
  const tagCounts = {};
  for (const row of records) {
    for (const tag of row.visual_issue_tags || []) tagCounts[tag] = (tagCounts[tag] || 0) + 1;
  }
  const badRecords = records.filter((row) => row.prognosis === "bad" && row.screenshot_path);
  const goodRecords = records.filter((row) => row.prognosis === "good" && row.screenshot_path);
  const summary = {
    schema_version: "meemee_past_trade_top_browser_validation_v1",
    generated_at: new Date().toISOString(),
    source_artifact: options.input,
    runtime_health_ready: health.ready === true,
    browser_historical_asof_rendering_possible: true,
    expected_observation_count: observations.length,
    screenshot_count: records.filter((row) => row.screenshot_path).length,
    missing_screenshots: missing,
    asof_match_count: asofMatchCount,
    asof_mismatch_count: asofMismatchCount,
    side_level_summary: summarizeBySide(records),
    visual_issue_tag_distribution: tagCounts,
    representative_bad_browser_screenshots: badRecords.slice(0, 10),
    representative_good_browser_screenshots: goodRecords.slice(0, 10),
    records,
    final_decision:
      records.filter((row) => row.screenshot_path).length === observations.length && asofMismatchCount === 0
        ? "browser_validation_complete"
        : "browser_validation_incomplete",
  };
  fs.mkdirSync(options.outputDir, { recursive: true });
  const outPath = path.join(options.outputDir, "browser_validation_summary.json");
  fs.writeFileSync(outPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
  console.log(JSON.stringify({ summaryPath: outPath, screenshotCount: summary.screenshot_count, finalDecision: summary.final_decision }, null, 2));
}

main().catch((error) => {
  console.error(error && error.stack ? error.stack : String(error));
  process.exit(1);
});
