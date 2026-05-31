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
    outputDir: path.resolve(__dirname, "..", "..", "artifacts", "screenshots", "trade-candidate-review"),
    limit: 50,
    top: 8,
    inputReviewJson: null,
  };
  for (let index = 2; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--base-url") {
      options.baseUrl = argv[++index] || options.baseUrl;
    } else if (arg === "--output-dir") {
      options.outputDir = path.resolve(argv[++index] || options.outputDir);
    } else if (arg === "--limit") {
      options.limit = Number(argv[++index] || options.limit);
    } else if (arg === "--top") {
      options.top = Number(argv[++index] || options.top);
    } else if (arg === "--input-review-json") {
      options.inputReviewJson = path.resolve(argv[++index] || "");
    }
  }
  options.limit = Math.max(1, Math.min(Number(options.limit) || 50, 200));
  options.top = Math.max(1, Math.min(Number(options.top) || 8, 30));
  return options;
}

async function fetchJson(baseUrl, endpoint, init = undefined) {
  const url = `${baseUrl.replace(/\/$/, "")}${endpoint}`;
  let lastError = null;
  for (let attempt = 1; attempt <= 4; attempt += 1) {
    try {
      const response = await fetch(url, init);
      if (response.ok) {
        return response.json();
      }
      lastError = new Error(`GET ${url} failed: ${response.status} ${response.statusText}`);
      if (![429, 500, 502, 503, 504].includes(response.status)) {
        throw lastError;
      }
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolve) => setTimeout(resolve, 1_500 * attempt));
  }
  throw lastError || new Error(`GET ${url} failed`);
}

function compactItem(item, rank, side) {
  return {
    rank,
    side,
    code: String(item.code || ""),
    name: item.name || "",
    asOf: item.asOf || null,
    entryQualified: item.entryQualified === true,
    entryQualifiedByFallback: item.entryQualifiedByFallback === true,
    entryQualifiedFallbackStage: item.entryQualifiedFallbackStage || null,
    tradePriorityScore: numberOrNull(item.tradePriorityScore),
    tradePriorityProfitScore: numberOrNull(item.tradePriorityProfitScore),
    tradePriorityHitScore: numberOrNull(item.tradePriorityHitScore),
    entryScore: numberOrNull(item.entryScore),
    setupType: item.setupType || null,
    tradeEntryClass: item.tradeEntryClass || null,
    changePct: numberOrNull(item.changePct),
  };
}

function numberOrNull(value) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function pct(value) {
  return typeof value === "number" ? value * 100 : null;
}

function reviewLong(item) {
  const score = item.tradePriorityScore || 0;
  const change = pct(item.changePct);
  const fallback = item.entryQualifiedByFallback || item.entryQualifiedFallbackStage;
  const reasons = [];
  let decision = "watch";
  let entryMethod = "wait_for_chart_confirmation";

  if (fallback) reasons.push("fallback_entry_rejected_for_clear_entry");
  if (score >= 0.95) reasons.push("very_high_trade_priority_score");
  else if (score >= 0.9) reasons.push("high_trade_priority_score");
  if (item.tradeEntryClass) reasons.push(`entry_class:${item.tradeEntryClass}`);

  if (change !== null && change >= 2.5) {
    decision = "probe_only_or_wait";
    entryMethod = "small_probe_only_after_high_hold_or_wait_for_7ma_pullback";
    reasons.push("same_day_chase_risk");
  } else if (score >= 0.95 && !fallback) {
    decision = "probe_candidate";
    entryMethod = "small_probe_then_add_only_if_high_hold_continues";
  } else if (score >= 0.9 && !fallback) {
    decision = "watch_or_probe_small";
    entryMethod = "probe_small_or_wait_for_breakout_retest";
  }

  if (change !== null && change <= -1.5 && score >= 0.9) {
    decision = "pullback_probe_candidate";
    entryMethod = "probe_only_if_intraday_reclaims_prior_close_or_7ma";
    reasons.push("pullback_after_buy_signal");
  }
  if (fallback) {
    decision = "reject_for_clear_entry";
    entryMethod = "no_entry_until_non_fallback_signal";
  }
  return { decision, entryMethod, reasons };
}

function reviewShort(item) {
  const score = item.tradePriorityScore || 0;
  const change = pct(item.changePct);
  const fallback = item.entryQualifiedByFallback || item.entryQualifiedFallbackStage;
  const reasons = [];
  let decision = "watch";
  let entryMethod = "wait_for_failed_retest_or_breakdown_continuation";

  if (fallback) reasons.push("fallback_entry_rejected_for_clear_entry");
  if (score >= 0.85) reasons.push("high_short_priority_score");
  if (item.tradeEntryClass) reasons.push(`entry_class:${item.tradeEntryClass}`);

  if (change !== null && change <= -3.0) {
    decision = "short_probe_only_or_wait";
    entryMethod = "avoid_late_breakdown_chase_wait_for_retest";
    reasons.push("same_day_breakdown_chase_risk");
  } else if (score >= 0.85 && !fallback) {
    decision = "short_probe_candidate";
    entryMethod = "small_short_probe_then_add_on_failed_retest";
  } else if (score >= 0.75 && !fallback) {
    decision = "watch_short";
  }

  if (change !== null && change >= 1.5 && score >= 0.75) {
    decision = "wait_for_upper_rejection";
    entryMethod = "short_only_if_rebound_fails_below_recent_high";
    reasons.push("rebound_against_short_signal");
  }
  if (fallback) {
    decision = "reject_for_clear_entry";
    entryMethod = "no_entry_until_non_fallback_signal";
  }
  return { decision, entryMethod, reasons };
}

function runVisualAnalysis(screenshotPath, side) {
  const scriptPath = path.resolve(__dirname, "analyze_detail_chart_screenshot.py");
  const result = spawnSync("python", [scriptPath, screenshotPath], {
    cwd: path.resolve(__dirname, "..", ".."),
    encoding: "utf8",
    maxBuffer: 1024 * 1024 * 8,
  });
  if (result.status !== 0) {
    return {
      confirmed: false,
      error: (result.stderr || result.stdout || "visual_analysis_failed").trim(),
    };
  }
  try {
    const parsed = JSON.parse(result.stdout);
    const sideReview = side === "short" ? parsed.short_visual_review : parsed.long_visual_review;
    return {
      ...parsed,
      side_review: sideReview || null,
    };
  } catch (error) {
    return {
      confirmed: false,
      error: `visual_analysis_json_parse_failed:${error.message}`,
    };
  }
}

function runShapeAnalysis(bars) {
  const scriptPath = path.resolve(__dirname, "trade_shape_classifier.py");
  const result = spawnSync("python", [scriptPath], {
    cwd: path.resolve(__dirname, "..", ".."),
    input: JSON.stringify(bars || []),
    encoding: "utf8",
    maxBuffer: 1024 * 1024 * 8,
  });
  if (result.status !== 0) {
    return {
      confirmed: false,
      error: (result.stderr || result.stdout || "shape_analysis_failed").trim(),
    };
  }
  try {
    return JSON.parse(result.stdout);
  } catch (error) {
    return {
      confirmed: false,
      error: `shape_analysis_json_parse_failed:${error.message}`,
    };
  }
}

function mergeVisualReview(candidate) {
  const visual = candidate.visual_ai;
  const shape = candidate.shape_context;
  if (!visual || visual.confirmed !== true || !visual.side_review) {
    candidate.review.reasons.push("visual_ai_unavailable");
  } else {
    candidate.review.visualDecision = visual.side_review.decision;
    candidate.review.visualEntryMethod = visual.side_review.entryMethod;
    candidate.review.reasons.push(...(visual.side_review.reasons || []));
  }

  if (candidate.side === "buy") {
    if (shape && shape.confirmed === true) {
      candidate.review.shapeIntent = shape.shape_intent;
      candidate.review.marketScene = shape.market_scene;
      candidate.review.tradeSide = shape.trade_side;
      candidate.review.actionBias = shape.action_bias;
      candidate.review.riskNote = shape.risk_note;
      candidate.review.reasons.push(...(shape.reasons || []));
      if (shape.trade_side === "long" && shape.action_bias === "buy_breakout") {
        candidate.review.decision = "buy_probe_candidate";
        candidate.review.entryMethod = shape.entry_timing || "b_phase_buy_breakout";
      } else if (shape.trade_side === "long" && shape.action_bias === "hold_or_add_long") {
        candidate.review.decision = "watch_or_probe_small";
        candidate.review.entryMethod = shape.entry_timing || "c_phase_hold_or_add_long";
      } else if (shape.trade_side === "long_probe_or_short_cover") {
        candidate.review.decision = "probe_only_or_wait";
        candidate.review.entryMethod = shape.entry_timing || "bottom_probe_only_after_ma_reclaim";
      } else if (shape.action_bias === "avoid_chase" || shape.trade_side === "short") {
        candidate.review.decision = "wait";
        candidate.review.entryMethod = "buy_blocked_by_shape_context";
      }
    }
    if (visual && visual.confirmed === true && visual.side_review && visual.side_review.decision === "avoid_chase") {
      candidate.review.decision = "wait";
      candidate.review.entryMethod = "wait_for_reclaim_after_visual_rejection";
    } else if (visual && visual.confirmed === true && visual.side_review && visual.side_review.decision === "probe_only_or_wait") {
      candidate.review.decision = "probe_only_or_wait";
      candidate.review.entryMethod = "small_probe_only_or_wait_for_visual_pullback";
    } else if (visual && visual.confirmed === true && visual.side_review && candidate.review.decision === "watch" && visual.side_review.decision === "probe_candidate") {
      candidate.review.decision = "watch_or_probe_small";
      candidate.review.entryMethod = "probe_small_if_visual_high_hold_continues";
    }
  } else if (candidate.side === "short") {
    if (shape && shape.confirmed === true) {
      candidate.review.shapeIntent = shape.shape_intent;
      candidate.review.marketScene = shape.market_scene;
      candidate.review.tradeSide = shape.trade_side;
      candidate.review.actionBias = shape.action_bias;
      candidate.review.riskNote = shape.risk_note;
      candidate.review.reasons.push(...(shape.reasons || []));
      if (shape.shape_intent === "mature_uptrend_crash_setup_second_60ma_break") {
        candidate.review.decision = "short_probe_candidate";
        candidate.review.entryMethod = "small_short_probe_or_add_on_mature_uptrend_second_60ma_break";
      } else if (shape.shape_intent === "mature_uptrend_failed_high_retest_distribution") {
        candidate.review.decision = "short_probe_candidate";
        candidate.review.entryMethod = "small_short_probe_on_mature_uptrend_failed_high_retest";
      } else if (shape.shape_intent === "crash_warning_round_level_upper_wick") {
        if (shape.entry_timing && shape.entry_timing.includes("trigger")) {
          candidate.review.decision = "short_probe_candidate";
          candidate.review.entryMethod = shape.entry_timing;
        } else {
          candidate.review.decision = "watch_short_or_probe_small";
          candidate.review.entryMethod = "probe_only_if_upper_wick_high_holds_as_resistance";
        }
      } else if (shape.shape_intent === "crash_warning_round_level_failed_high_retest") {
        if (shape.entry_timing && shape.entry_timing.includes("trigger")) {
          candidate.review.decision = "short_probe_candidate";
          candidate.review.entryMethod = shape.entry_timing;
        } else {
          candidate.review.decision = "watch_short_or_probe_small";
          candidate.review.entryMethod = "probe_only_if_round_level_failed_retest_confirms";
        }
      } else if (shape.shape_intent === "crash_initial_mountain_failed_high_retest") {
        candidate.review.decision = "short_probe_candidate";
        candidate.review.entryMethod = shape.entry_timing || "small_short_probe_on_mountain_top_failed_high_retest";
      } else if (shape.shape_intent === "crash_initial_mountain_upper_wick") {
        if (shape.entry_timing && shape.entry_timing.includes("trigger")) {
          candidate.review.decision = "short_probe_candidate";
          candidate.review.entryMethod = shape.entry_timing;
        } else {
          candidate.review.decision = "watch_short_or_probe_small";
          candidate.review.entryMethod = "probe_only_if_mountain_upper_wick_high_holds_as_resistance";
        }
      } else if (shape.shape_intent === "failed_high_retest_7ma_break") {
        candidate.review.decision = "short_probe_candidate";
        candidate.review.entryMethod = "small_short_probe_below_failed_high_retest_and_7ma_break";
      } else if (shape.shape_intent === "failed_high_retest_wait_for_7ma_break") {
        candidate.review.decision = "watch_short";
        candidate.review.entryMethod = "wait_for_7ma_break_after_failed_high_retest";
      } else if (shape.shape_intent === "range_lower_drift") {
        candidate.review.decision = "watch_short";
        candidate.review.entryMethod = "do_not_call_failed_retest_wait_for_rebound_failure";
      } else if (shape.shape_intent === "late_breakdown_chase") {
        candidate.review.decision = "short_probe_only_or_wait";
        candidate.review.entryMethod = "avoid_late_breakdown_chase_wait_for_retest";
      } else if (shape.trade_side === "short" && shape.entry_timing) {
        candidate.review.decision = "short_probe_candidate";
        candidate.review.entryMethod = shape.entry_timing;
      } else if (shape.trade_side === "short_cover_watch") {
        candidate.review.decision = "watch_short";
        candidate.review.entryMethod = "avoid_new_short_after_recovery_20ma_hold";
      }
    } else if (visual && visual.confirmed === true && visual.side_review && visual.side_review.decision === "short_probe_candidate") {
      candidate.review.decision = "short_probe_candidate";
      candidate.review.entryMethod = visual.side_review.entryMethod;
    } else if (visual && visual.confirmed === true && visual.side_review && visual.side_review.decision === "watch") {
      candidate.review.decision = "watch_short";
      candidate.review.entryMethod = "wait_for_clearer_visual_short_entry";
    }
  }
  candidate.review.reasons = [...new Set(candidate.review.reasons)];
}

async function attachShapeContext(baseUrl, candidates) {
  const targetCodes = [...new Set(candidates.map((candidate) => candidate.code))];
  if (!targetCodes.length) return;
  try {
    const response = await fetchJson(baseUrl, "/api/batch_bars_v3", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        codes: targetCodes,
        timeframes: ["daily"],
        limit: 90,
        includeProvisional: true,
        includeBoxes: false,
        forceRefresh: false,
      }),
    });
    for (const candidate of candidates) {
      if (candidate.side !== "short") continue;
      const bars = response.items && response.items[candidate.code] && response.items[candidate.code].daily
        ? response.items[candidate.code].daily.bars
        : [];
      candidate.shape_context = runShapeAnalysis(bars);
    }
  } catch (error) {
    for (const candidate of candidates) {
      if (candidate.side === "short") {
        candidate.shape_context = {
          confirmed: false,
          error: `shape_context_fetch_failed:${error.message}`,
        };
      }
    }
  }
}

async function captureScreenshots(baseUrl, outputDir, candidates) {
  fs.mkdirSync(outputDir, { recursive: true });
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({
    viewport: { width: 1920, height: 900 },
    deviceScaleFactor: 1,
    locale: "ja-JP",
    timezoneId: "Asia/Tokyo",
  });
  try {
    for (const candidate of candidates) {
      const url = `${baseUrl.replace(/\/$/, "")}/detail/${encodeURIComponent(candidate.code)}`;
      const screenshotPath = path.join(outputDir, `${candidate.side}_${candidate.rank}_${candidate.code}.png`);
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 45_000 });
      await page.waitForTimeout(2_000);
      await page.screenshot({ path: screenshotPath, fullPage: false });
      candidate.detailUrl = url;
      candidate.screenshotPath = screenshotPath;
      candidate.screenshotBytes = fs.statSync(screenshotPath).size;
      candidate.visual_ai = runVisualAnalysis(screenshotPath, candidate.side);
      mergeVisualReview(candidate);
    }
  } finally {
    await browser.close();
  }
}

async function main() {
  const options = parseArgs(process.argv);
  fs.mkdirSync(options.outputDir, { recursive: true });
  const baseUrl = options.baseUrl.replace(/\/$/, "");

  if (options.inputReviewJson) {
    const existing = JSON.parse(fs.readFileSync(options.inputReviewJson, "utf8"));
    const candidates = Array.isArray(existing.candidates) ? existing.candidates : [];
    for (const candidate of candidates) {
      if (!candidate.screenshotPath || !fs.existsSync(candidate.screenshotPath)) {
        candidate.visual_ai = {
          confirmed: false,
          error: "screenshot_missing_for_visual_reanalysis",
        };
        continue;
      }
      candidate.visual_ai = runVisualAnalysis(candidate.screenshotPath, candidate.side);
      if (!candidate.review) {
        candidate.review = candidate.side === "short" ? reviewShort(candidate) : reviewLong(candidate);
      }
      mergeVisualReview(candidate);
    }
    existing.schema_version = "meemee_trade_candidate_screenshot_review_v1";
    existing.generated_at = new Date().toISOString();
    existing.visual_reanalysis_source = options.inputReviewJson;
    existing.selection_policy = {
      ...(existing.selection_policy || {}),
      visual_ai_policy: "Pillow-based screenshot inspection of daily chart area; visual rejection/chase risk can downgrade numeric ranking entries",
      silent_fallback_used: false,
    };
    const reviewPath = path.join(options.outputDir, "candidate_review.json");
    fs.writeFileSync(reviewPath, `${JSON.stringify(existing, null, 2)}\n`, "utf8");
    console.log(JSON.stringify({ reviewPath, outputDir: options.outputDir, candidateCount: candidates.length, mode: "visual_reanalysis" }, null, 2));
    return;
  }

  const [buySession, sellSession] = await Promise.all([
    fetchJson(baseUrl, `/api/rankings/session?tf=D&which=latest&dir=up&mode=trade&risk_mode=balanced&limit=${options.limit}`),
    fetchJson(baseUrl, `/api/rankings/session?tf=D&which=latest&dir=down&mode=trade&risk_mode=balanced&limit=${options.limit}`),
  ]);

  const buyItems = (buySession.confirmed_actionable_buy_candidates || buySession.confirmed_items || [])
    .slice(0, options.top)
    .map((item, index) => compactItem(item, index + 1, "buy"));
  const shortItems = (sellSession.confirmed_actionable_short_candidates || sellSession.confirmed_items || [])
    .slice(0, options.top)
    .map((item, index) => compactItem(item, index + 1, "short"));

  const reviewed = [
    ...buyItems.map((item) => ({ ...item, review: reviewLong(item) })),
    ...shortItems.map((item) => ({ ...item, review: reviewShort(item) })),
  ];
  await attachShapeContext(baseUrl, reviewed);
  await captureScreenshots(baseUrl, options.outputDir, reviewed);

  const result = {
    schema_version: "meemee_trade_candidate_screenshot_review_v1",
    generated_at: new Date().toISOString(),
    scope: {
      meemee_ui_changed: false,
      ranking_logic_changed: false,
      tradex_research_logic_changed: false,
      screenshot_review_only: true,
    },
    source: {
      baseUrl,
      buy_confirmed_snapshot_as_of: buySession.confirmed_snapshot_as_of || null,
      buy_provisional_snapshot_as_of: buySession.provisional_snapshot_as_of || null,
      buy_provisional_freshness_state: buySession.provisional_freshness_state || null,
      sell_confirmed_snapshot_as_of: sellSession.confirmed_snapshot_as_of || null,
      sell_provisional_snapshot_as_of: sellSession.provisional_snapshot_as_of || null,
      sell_provisional_freshness_state: sellSession.provisional_freshness_state || null,
    },
    selection_policy: {
      clear_buy_threshold: "tradePriorityScore>=0.95 and no fallback, then screenshot-gated probe only if not same-day chase",
      clear_short_threshold: "tradePriorityScore>=0.85 and no fallback, then screenshot-gated short probe only if not late breakdown chase",
      visual_ai_policy: "Pillow-based screenshot inspection of daily chart area; visual rejection/chase risk can downgrade numeric ranking entries",
      short_shape_policy: "OHLC shape gate requires prior-high retest failure plus 7MA break; lower-range drift is not labeled failed-high-retest",
      crash_shape_policy: "Mature uptrend crash setup adds long prior rise plus repeated MA20/MA60 fatigue to the failed-high-retest gate",
      crash_warning_policy: "Early crash warning labels mature uptrend plus round level with either long upper wick or failed high retest",
      concentration_policy: "start small across at most 2-3 candidates; cut invalidated names; add only to candidates that hold high/retest cleanly",
      silent_fallback_used: false,
    },
    candidates: reviewed,
  };

  const reviewPath = path.join(options.outputDir, "candidate_review.json");
  fs.writeFileSync(reviewPath, `${JSON.stringify(result, null, 2)}\n`, "utf8");
  console.log(JSON.stringify({ reviewPath, outputDir: options.outputDir, candidateCount: reviewed.length }, null, 2));
}

main().catch((error) => {
  console.error(error && error.stack ? error.stack : String(error));
  process.exit(1);
});
