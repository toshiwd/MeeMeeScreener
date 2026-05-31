const fs = require("fs");
const path = require("path");
const { spawnSync } = require("child_process");

const frontendNodeModules = path.resolve(__dirname, "..", "..", "app", "frontend", "node_modules");
process.env.NODE_PATH = [frontendNodeModules, process.env.NODE_PATH].filter(Boolean).join(path.delimiter);
require("module").Module._initPaths();

const { chromium } = require("playwright");

function analyzeScreenshot(screenshotPath) {
  const scriptPath = path.resolve(__dirname, "analyze_detail_chart_screenshot.py");
  const result = spawnSync("python", [scriptPath, screenshotPath], {
    cwd: path.resolve(__dirname, "..", ".."),
    encoding: "utf8",
    maxBuffer: 1024 * 1024 * 8,
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

function parseArgs(argv) {
  const options = {
    baseUrl: "http://localhost:8000",
    outputDir: path.resolve(__dirname, "..", "..", "artifacts", "screenshots", "detail-current"),
    codes: [],
  };

  for (let index = 2; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--base-url") {
      options.baseUrl = argv[++index] || options.baseUrl;
    } else if (arg === "--output-dir") {
      options.outputDir = path.resolve(argv[++index] || options.outputDir);
    } else if (arg === "--codes") {
      options.codes.push(
        ...(argv[++index] || "")
          .split(",")
          .map((code) => code.trim())
          .filter(Boolean),
      );
    } else if (arg && !arg.startsWith("--")) {
      options.codes.push(arg.trim());
    }
  }

  options.codes = [...new Set(options.codes.filter(Boolean))];
  if (options.codes.length === 0) {
    throw new Error("Provide at least one code, for example: node tools/debug/capture_detail_screenshots.js 8358 9041");
  }
  return options;
}

async function main() {
  const options = parseArgs(process.argv);
  fs.mkdirSync(options.outputDir, { recursive: true });

  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({
    viewport: { width: 1920, height: 900 },
    deviceScaleFactor: 1,
    locale: "ja-JP",
    timezoneId: "Asia/Tokyo",
  });

  const results = [];
  for (const code of options.codes) {
    const url = `${options.baseUrl.replace(/\/$/, "")}/detail/${encodeURIComponent(code)}`;
    const screenshotPath = path.join(options.outputDir, `detail_${code}.png`);
    let analysis = null;
    for (let attempt = 1; attempt <= 4; attempt += 1) {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 45_000 });
      await page.waitForFunction(
        () => !document.body.innerText.includes("バックエンド起動待ち") && !document.body.innerText.includes("画面を準備中"),
        null,
        { timeout: 20_000 },
      ).catch(() => null);
      await page.waitForTimeout(2_000 * attempt);
      await page.screenshot({ path: screenshotPath, fullPage: false });
      analysis = analyzeScreenshot(screenshotPath);
      if (analysis.confirmed === true) break;
    }
    if (!analysis || analysis.confirmed !== true) {
      throw new Error(`detail screenshot did not contain chart candles: code=${code} path=${screenshotPath} reason=${analysis ? analysis.reason : "unknown"}`);
    }
    results.push({ code, url, screenshotPath, visualConfirmed: true, candlePixelCount: analysis.candle_pixel_count });
  }

  await browser.close();
  console.log(JSON.stringify({ outputDir: options.outputDir, screenshots: results }, null, 2));
}

main().catch((error) => {
  console.error(error && error.stack ? error.stack : String(error));
  process.exit(1);
});
