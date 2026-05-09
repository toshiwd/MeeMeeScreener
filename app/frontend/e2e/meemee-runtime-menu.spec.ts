import { expect, test, type Page, type TestInfo } from "@playwright/test";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

type RuntimeMenuManifest = {
  artifactsDir: string;
  baselineFile: string;
  warningPolicy: {
    baselineRegressionRatio: number;
    absoluteTimingWarnMs: number;
    functionalTimeoutMs: number;
    settledStateTimeoutMs: number;
  };
  abnormalTextGuard: {
    settledForbiddenText: string[];
  };
};

type TimingMetrics = {
  appFirstUsableRenderMs?: number;
  rankingFirstContentVisibleMs?: number;
  rankingVisibleCardCount?: number;
  rankingClickToDetailUrlMs?: number;
  detailUrlToDailyVisibleMs?: number;
  detailUrlToWeeklyVisibleMs?: number;
  detailUrlToMonthlyVisibleMs?: number;
  nextInteractionLatencyMs?: number;
  previousInteractionLatencyMs?: number;
  backToRankingLatencyMs?: number;
};

type TabTimingMetrics = {
  list_first_visible?: number;
  ranking_first_visible?: number;
  market_first_visible?: number;
  positions_first_visible?: number;
  favorites_first_visible?: number;
  candidates_first_visible?: number;
};

type DetailTimingMetrics = {
  url_changed?: number;
  first_useful_chart_visible?: number;
  daily_visible?: number;
  weekly_visible?: number;
  monthly_visible?: number;
  all_charts_visible?: number;
};

type CheckResult = {
  id: string;
  status: "pass" | "pass_empty_state" | "warn" | "fail" | "fail_blank" | "skip";
  detail?: string;
};

type TabResult = {
  id: string;
  route: string;
  status: CheckResult["status"];
  urlChangedMs: number;
  firstVisibleMs: number;
  firstChartOrCardVisibleMs: number | null;
  settledUsableMs: number;
  visibleItemCount: number;
  detail?: string;
};

type RuntimeMenuReport = {
  generatedAt: string;
  baseURL: string;
  symbols: {
    originalCode: string | null;
    nextCode: string | null;
    directCode: string | null;
    highRankCode: string | null;
  };
  checks: CheckResult[];
  metrics: TimingMetrics;
  tab_timings_ms: TabTimingMetrics;
  detail_timings_ms: DetailTimingMetrics;
  tab_results: TabResult[];
  warnings: string[];
  screenshotPath: string | null;
};

const repoRoot = resolve(fileURLToPath(new URL("../../..", import.meta.url)));
const manifestPath = resolve(fileURLToPath(new URL("meemee-runtime-test-menu.json", import.meta.url)));
const manifest = JSON.parse(readFileSync(manifestPath, "utf-8")) as RuntimeMenuManifest;
const artifactsDir = resolve(repoRoot, manifest.artifactsDir);
const baselinePath = resolve(repoRoot, manifest.baselineFile);
const configuredBaseURL = process.env.MEEMEE_RUNTIME_BASE_URL?.trim();
const directDetailCode = process.env.MEEMEE_RUNTIME_DIRECT_DETAIL_CODE?.trim() || "4691";

if (configuredBaseURL) {
  test.use({ baseURL: configuredBaseURL });
}

const nowStamp = () => new Date().toISOString().replace(/[:.]/g, "-");

const elapsedSince = (start: number) => Math.round(performance.now() - start);

const firstCodeFromText = (text: string) => text.match(/\b\d{4}\b/)?.[0] ?? null;

const tabConfigs = [
  {
    id: "list",
    route: "/",
    firstVisibleMetric: "list_first_visible",
    meaningfulSelectors: [".tile", ".grid-empty-state", ".grid-skeleton"],
    itemSelector: ".tile",
    emptySelectors: [".grid-empty-state"],
    loadingSelectors: [".grid-skeleton", ".rank-status"],
    loadingText: ["読み込み中", "Loading"],
  },
  {
    id: "ranking",
    route: "/ranking",
    firstVisibleMetric: "ranking_first_visible",
    meaningfulSelectors: [".rank-tile", ".rank-status"],
    itemSelector: ".rank-tile",
    emptySelectors: [".rank-status"],
    loadingSelectors: [".rank-status"],
    loadingText: ["読み込み中", "Loading"],
  },
  {
    id: "market",
    route: "/market",
    firstVisibleMetric: "market_first_visible",
    meaningfulSelectors: [
      ".market-grid-tile",
      ".market-heatmap-panel",
      ".market-side-list",
      ".market-side-empty",
      ".heatmap-empty-sub",
    ],
    itemSelector: ".market-grid-tile",
    emptySelectors: [".market-side-empty", ".heatmap-empty-sub"],
    loadingSelectors: [".heatmap-empty-sub"],
    loadingText: ["読み込んでいます", "Loading"],
  },
  {
    id: "positions",
    route: "/positions",
    firstVisibleMetric: "positions_first_visible",
    meaningfulSelectors: [".tile", ".positions-empty", ".rank-status"],
    itemSelector: ".tile",
    emptySelectors: [".positions-empty", ".rank-status"],
    loadingSelectors: [".rank-status"],
    loadingText: ["読み込み中", "Loading"],
  },
  {
    id: "favorites",
    route: "/favorites",
    firstVisibleMetric: "favorites_first_visible",
    meaningfulSelectors: [".tile", ".rank-status"],
    itemSelector: ".tile",
    emptySelectors: [".rank-status"],
    loadingSelectors: [".rank-status"],
    loadingText: ["読み込み中", "Loading"],
  },
  {
    id: "candidates",
    route: "/candidates",
    firstVisibleMetric: "candidates_first_visible",
    meaningfulSelectors: [".tile", ".rank-status"],
    itemSelector: ".tile",
    emptySelectors: [".rank-status"],
    loadingSelectors: [".rank-status"],
    loadingText: ["読み込み中", "Loading"],
  },
] as const;

type TabConfig = (typeof tabConfigs)[number];

const reportCheck = (
  checks: CheckResult[],
  id: string,
  status: CheckResult["status"],
  detail?: string
) => {
  checks.push({ id, status, ...(detail ? { detail } : {}) });
};

const waitForAnyVisible = async (page: Page, selectors: string[], timeout: number) => {
  await page.waitForFunction(
    (candidateSelectors) =>
      candidateSelectors.some((selector) => {
        const element = document.querySelector(selector);
        if (!element) return false;
        const rect = element.getBoundingClientRect();
        return rect.width > 0 && rect.height > 0;
      }),
    selectors,
    { timeout }
  );
};

const waitForStartupOverlayGone = async (page: Page, timeout: number) => {
  await page.waitForFunction(
    () => {
      const overlay = document.querySelector(".startup-overlay");
      if (!overlay) return true;
      const rect = overlay.getBoundingClientRect();
      const style = window.getComputedStyle(overlay);
      return (
        rect.width === 0 ||
        rect.height === 0 ||
        style.visibility === "hidden" ||
        style.display === "none" ||
        style.pointerEvents === "none" ||
        !overlay.classList.contains("is-visible")
      );
    },
    undefined,
    { timeout }
  );
};

const waitForSettledWithoutForbiddenText = async (
  page: Page,
  forbiddenText: string[],
  timeout: number
) => {
  const visibleForbidden = await collectSettledForbiddenText(page, forbiddenText, timeout);
  expect(visibleForbidden, `settled forbidden text visible: ${visibleForbidden.join(", ")}`).toEqual([]);
};

const collectSettledForbiddenText = async (
  page: Page,
  forbiddenText: string[],
  timeout: number
) => {
  const deadline = performance.now() + timeout;
  let bodyText = "";
  while (performance.now() < deadline) {
    bodyText = await page.locator("body").innerText();
    const visibleForbidden = forbiddenText.filter((text) => bodyText.includes(text));
    if (!visibleForbidden.length) return [];
    await page.waitForTimeout(500);
  }
  return forbiddenText.filter((text) => bodyText.includes(text));
};

const visibleCount = async (page: Page, selector: string) =>
  page.locator(selector).evaluateAll((elements) =>
    elements.filter((element) => {
      const rect = element.getBoundingClientRect();
      return rect.width > 0 && rect.height > 0;
    }).length
  );

const hasVisibleSelector = async (page: Page, selectors: readonly string[]) => {
  for (const selector of selectors) {
    if ((await visibleCount(page, selector)) > 0) return true;
  }
  return false;
};

const waitForNoStuckLoading = async (page: Page, config: TabConfig, timeout: number) => {
  const deadline = performance.now() + timeout;
  while (performance.now() < deadline) {
    const itemCount = await visibleCount(page, config.itemSelector);
    const hasEmpty = await hasVisibleSelector(page, config.emptySelectors);
    if (itemCount > 0 || hasEmpty) return;
    await page.waitForTimeout(250);
  }
  const visibleLoadingTexts: string[] = [];
  for (const selector of config.loadingSelectors) {
    const texts = await page.locator(selector).evaluateAll((elements) =>
      elements
        .filter((element) => {
          const rect = element.getBoundingClientRect();
          return rect.width > 0 && rect.height > 0;
        })
        .map((element) => element.textContent ?? "")
    );
    visibleLoadingTexts.push(
      ...texts.filter((text) => config.loadingText.some((loadingText) => text.includes(loadingText)))
    );
  }
  expect(visibleLoadingTexts, `${config.id} stuck loading`).toEqual([]);
};

const measureTopTab = async (
  page: Page,
  config: TabConfig,
  timeout: number,
  forbiddenText: string[]
): Promise<TabResult> => {
  const start = performance.now();
  const link = page.locator(`.list-tabs a[href="${config.route}"]`).first();
  await link.waitFor({ state: "visible", timeout });
  await waitForStartupOverlayGone(page, timeout);
  await link.click();
  await page.waitForURL(config.route === "/" ? "**/" : `**${config.route}`, { timeout });
  const urlChangedMs = elapsedSince(start);

  const visibleStart = performance.now();
  await waitForAnyVisible(page, [...config.meaningfulSelectors], timeout);
  const firstVisibleMs = elapsedSince(visibleStart);
  const firstChartOrCardVisibleMs =
    (await visibleCount(page, config.itemSelector)) > 0 ? firstVisibleMs : null;

  const settledStart = performance.now();
  await waitForNoStuckLoading(page, config, Math.min(timeout, 15_000));
  await waitForSettledWithoutForbiddenText(page, forbiddenText, Math.min(timeout, 15_000));
  const settledUsableMs = elapsedSince(settledStart);
  const visibleItemCount = await visibleCount(page, config.itemSelector);
  const hasEmptyState = await hasVisibleSelector(page, config.emptySelectors);
  const status: CheckResult["status"] =
    visibleItemCount > 0 ? "pass" : hasEmptyState ? "pass_empty_state" : "fail_blank";
  const currentPathname = await page.evaluate(() => window.location.pathname);
  expect(currentPathname, `${config.id} URL should match the selected tab`).toBe(config.route);
  if (status === "fail_blank") {
    throw new Error(`${config.id} rendered no items and no intentional empty state`);
  }
  return {
    id: config.id,
    route: config.route,
    status,
    urlChangedMs,
    firstVisibleMs,
    firstChartOrCardVisibleMs,
    settledUsableMs,
    visibleItemCount,
    detail: visibleItemCount > 0 ? `${visibleItemCount} visible items` : "intentional empty state",
  };
};

const recordForbiddenTextGuard = async (
  page: Page,
  checks: CheckResult[],
  id: string,
  forbiddenText: string[],
  timeout: number
) => {
  const visibleForbidden = await collectSettledForbiddenText(page, forbiddenText, timeout);
  reportCheck(
    checks,
    id,
    visibleForbidden.length ? "fail" : "pass",
    visibleForbidden.length ? `forbidden text: ${visibleForbidden.join(", ")}` : undefined
  );
  return visibleForbidden;
};

const waitForDetailChrome = async (
  page: Page,
  metrics: TimingMetrics,
  detailTimings: DetailTimingMetrics | null,
  timeout: number,
  prefix: "detailUrlTo" | "directDetailUrlTo" = "detailUrlTo"
) => {
  const startedAt = performance.now();
  await page.getByText("Daily").first().waitFor({ state: "visible", timeout });
  const dailyVisible = elapsedSince(startedAt);
  if (prefix === "detailUrlTo") {
    metrics.detailUrlToDailyVisibleMs = dailyVisible;
    if (detailTimings) {
      detailTimings.first_useful_chart_visible = dailyVisible;
      detailTimings.daily_visible = dailyVisible;
    }
  }
  await page.getByText("Weekly").first().waitFor({ state: "visible", timeout });
  const weeklyVisible = elapsedSince(startedAt);
  if (prefix === "detailUrlTo") {
    metrics.detailUrlToWeeklyVisibleMs = weeklyVisible;
    if (detailTimings) detailTimings.weekly_visible = weeklyVisible;
  }
  await page.getByText("Monthly").first().waitFor({ state: "visible", timeout });
  const monthlyVisible = elapsedSince(startedAt);
  if (prefix === "detailUrlTo") {
    metrics.detailUrlToMonthlyVisibleMs = monthlyVisible;
    if (detailTimings) {
      detailTimings.monthly_visible = monthlyVisible;
      detailTimings.all_charts_visible = monthlyVisible;
    }
  }
};

const collectComparableMetrics = (
  metrics: TimingMetrics,
  tabTimings: TabTimingMetrics,
  detailTimings: DetailTimingMetrics
) => ({
  ...metrics,
  ...Object.fromEntries(
    Object.entries(tabTimings).map(([key, value]) => [`tab_timings_ms.${key}`, value])
  ),
  ...Object.fromEntries(
    Object.entries(detailTimings).map(([key, value]) => [`detail_timings_ms.${key}`, value])
  ),
});

const compareWithBaseline = (
  metrics: TimingMetrics,
  tabTimings: TabTimingMetrics,
  detailTimings: DetailTimingMetrics,
  baselineFile: string,
  warnRatio: number,
  absoluteWarnMs: number
) => {
  const warnings: string[] = [];
  if (!existsSync(baselineFile)) {
    warnings.push("baseline_missing_created_from_this_run");
    return warnings;
  }
  const baseline = JSON.parse(readFileSync(baselineFile, "utf-8")) as {
    metrics?: TimingMetrics;
    tab_timings_ms?: TabTimingMetrics;
    detail_timings_ms?: DetailTimingMetrics;
  };
  const comparableMetrics = collectComparableMetrics(metrics, tabTimings, detailTimings);
  const baselineMetrics = collectComparableMetrics(
    baseline.metrics ?? {},
    baseline.tab_timings_ms ?? {},
    baseline.detail_timings_ms ?? {}
  );
  for (const [key, value] of Object.entries(comparableMetrics)) {
    if (typeof value !== "number") continue;
    if (key === "rankingVisibleCardCount") continue;
    if (value > absoluteWarnMs) {
      warnings.push(`${key}_absolute_warn:${value}ms`);
    }
    const baselineValue = baselineMetrics[key as keyof typeof baselineMetrics];
    if (typeof baselineValue === "number" && baselineValue > 0 && value > baselineValue * warnRatio) {
      warnings.push(`${key}_baseline_warn:${value}ms>${Math.round(baselineValue * warnRatio)}ms`);
    }
  }
  return warnings;
};

test.describe("MeeMee runtime test menu", () => {
  test.setTimeout(manifest.warningPolicy.functionalTimeoutMs * 3);

  test("runs layered runtime UI regression checks", async ({ page, baseURL }, testInfo: TestInfo) => {
    const timeout = manifest.warningPolicy.functionalTimeoutMs;
    const checks: CheckResult[] = [];
    const metrics: TimingMetrics = {};
    const tabTimings: TabTimingMetrics = {};
    const detailTimings: DetailTimingMetrics = {};
    const tabResults: TabResult[] = [];
    const symbols = {
      originalCode: null as string | null,
      nextCode: null as string | null,
      directCode: directDetailCode,
      highRankCode: null as string | null,
    };

    mkdirSync(artifactsDir, { recursive: true });
    const runId = nowStamp();
    const screenshotPath = resolve(artifactsDir, `meemee-runtime-detail-${runId}.png`);
    const reportPath = resolve(artifactsDir, `meemee-runtime-report-${runId}.json`);

    const appStart = performance.now();
    await page.goto("/ranking", { waitUntil: "domcontentloaded" });
    await waitForAnyVisible(page, [".rank-tile", ".rank-status", ".grid-empty-state"], timeout);
    await waitForStartupOverlayGone(page, timeout);
    metrics.appFirstUsableRenderMs = elapsedSince(appStart);

    for (const config of tabConfigs) {
      const tabResult = await measureTopTab(
        page,
        config,
        timeout,
        manifest.abnormalTextGuard.settledForbiddenText
      );
      tabResults.push(tabResult);
      tabTimings[config.firstVisibleMetric] = tabResult.firstVisibleMs;
      reportCheck(
        checks,
        `top-tab-${config.id}-visible`,
        tabResult.status,
        `${tabResult.detail}; url ${tabResult.urlChangedMs}ms; visible ${tabResult.firstVisibleMs}ms; settled ${tabResult.settledUsableMs}ms`
      );
    }

    await page.goto("/ranking", { waitUntil: "domcontentloaded" });
    await waitForAnyVisible(page, [".rank-tile", ".rank-status"], timeout);
    await waitForStartupOverlayGone(page, timeout);

    const rankingStart = performance.now();
    await page.locator(".rank-tile").first().waitFor({ state: "visible", timeout });
    metrics.rankingFirstContentVisibleMs = elapsedSince(rankingStart);
    metrics.rankingVisibleCardCount = await page.locator(".rank-tile").count();
    reportCheck(checks, "ranking-first-card-visible", "pass", `${metrics.rankingVisibleCardCount} cards`);

    const firstTile = page.locator(".rank-tile").first();
    symbols.originalCode = firstCodeFromText(await firstTile.innerText());
    expect(symbols.originalCode, "first ranking tile should expose a 4-digit code").toBeTruthy();

    const clickStart = performance.now();
    await waitForStartupOverlayGone(page, timeout);
    await firstTile.click();
    await page.waitForURL(`**/detail-v2/${symbols.originalCode}`, { timeout });
    metrics.rankingClickToDetailUrlMs = elapsedSince(clickStart);
    detailTimings.url_changed = metrics.rankingClickToDetailUrlMs;
    reportCheck(checks, "ranking-click-opens-detail-v2", "pass", symbols.originalCode ?? undefined);

    await waitForDetailChrome(page, metrics, detailTimings, timeout);
    await recordForbiddenTextGuard(
      page,
      checks,
      "detail-v2-abnormal-text-guard",
      manifest.abnormalTextGuard.settledForbiddenText,
      manifest.warningPolicy.settledStateTimeoutMs
    );
    reportCheck(checks, "detail-v2-three-charts-visible", "pass");

    const navButtons = page.locator("button.nav-button:not(.nav-primary)");
    const navCount = await navButtons.count();
    if (navCount >= 2) {
      const nextStart = performance.now();
      await navButtons.nth(1).click();
      await page.waitForURL(/\/detail-v2\/\d{4}/, { timeout });
      metrics.nextInteractionLatencyMs = elapsedSince(nextStart);
      symbols.nextCode = page.url().match(/\/detail-v2\/(\d{4})/)?.[1] ?? null;
      expect(symbols.nextCode, "next should navigate to a detail-v2 code").toBeTruthy();
      expect(symbols.nextCode, "next should change symbol").not.toBe(symbols.originalCode);
      await waitForDetailChrome(page, metrics, null, timeout, "directDetailUrlTo");
      await recordForbiddenTextGuard(
        page,
        checks,
        "detail-v2-next-abnormal-text-guard",
        manifest.abnormalTextGuard.settledForbiddenText,
        manifest.warningPolicy.settledStateTimeoutMs
      );
      reportCheck(checks, "detail-v2-next-stays-v2", "pass", symbols.nextCode ?? undefined);

      const previousStart = performance.now();
      await page.locator("button.nav-button:not(.nav-primary)").nth(0).click();
      await page.waitForURL(`**/detail-v2/${symbols.originalCode}`, { timeout });
      metrics.previousInteractionLatencyMs = elapsedSince(previousStart);
      await waitForDetailChrome(page, metrics, null, timeout, "directDetailUrlTo");
      await recordForbiddenTextGuard(
        page,
        checks,
        "detail-v2-previous-abnormal-text-guard",
        manifest.abnormalTextGuard.settledForbiddenText,
        manifest.warningPolicy.settledStateTimeoutMs
      );
      reportCheck(checks, "detail-v2-previous-stays-v2", "pass", symbols.originalCode ?? undefined);
    } else {
      reportCheck(checks, "detail-v2-prev-next-available", "skip", "nav buttons not visible");
    }

    const backStart = performance.now();
    await page.locator("button.nav-button.nav-primary").first().click();
    await page.waitForURL("**/ranking**", { timeout });
    await page.locator(".rank-tile").first().waitFor({ state: "visible", timeout });
    metrics.backToRankingLatencyMs = elapsedSince(backStart);
    reportCheck(checks, "detail-v2-back-to-ranking", "pass");

    await page.goto(`/detail-v2/${directDetailCode}`, { waitUntil: "domcontentloaded" });
    await waitForDetailChrome(page, metrics, null, timeout, "directDetailUrlTo");
    const directForbiddenText = await collectSettledForbiddenText(
      page,
      manifest.abnormalTextGuard.settledForbiddenText,
      manifest.warningPolicy.settledStateTimeoutMs
    );
    reportCheck(
      checks,
      "direct-detail-v2-renders",
      directForbiddenText.length ? "fail" : "pass",
      directForbiddenText.length
        ? `${directDetailCode}; forbidden text: ${directForbiddenText.join(", ")}`
        : directDetailCode
    );
    await page.screenshot({ path: screenshotPath, fullPage: true });

    await page.goto("/ranking", { waitUntil: "domcontentloaded" });
    await page.locator(".rank-tile").first().waitFor({ state: "visible", timeout });
    const secondTile = page.locator(".rank-tile").nth(1);
    if ((await secondTile.count()) > 0) {
      symbols.highRankCode = firstCodeFromText(await secondTile.innerText());
      if (symbols.highRankCode) {
        await secondTile.click();
        await page.waitForURL(`**/detail-v2/${symbols.highRankCode}`, { timeout });
        await waitForDetailChrome(page, metrics, null, timeout, "directDetailUrlTo");
        await recordForbiddenTextGuard(
          page,
          checks,
          "second-high-rank-abnormal-text-guard",
          manifest.abnormalTextGuard.settledForbiddenText,
          manifest.warningPolicy.settledStateTimeoutMs
        );
        reportCheck(checks, "second-high-rank-detail-v2-renders", "pass", symbols.highRankCode);
      }
    }

    const warnings = compareWithBaseline(
      metrics,
      tabTimings,
      detailTimings,
      baselinePath,
      manifest.warningPolicy.baselineRegressionRatio,
      manifest.warningPolicy.absoluteTimingWarnMs
    );
    warnings.forEach((warning) => reportCheck(checks, warning, "warn"));

    const report: RuntimeMenuReport = {
      generatedAt: new Date().toISOString(),
      baseURL: configuredBaseURL ?? baseURL ?? "http://127.0.0.1:4173",
      symbols,
      checks,
      metrics,
      tab_timings_ms: tabTimings,
      detail_timings_ms: detailTimings,
      tab_results: tabResults,
      warnings,
      screenshotPath,
    };
    writeFileSync(reportPath, JSON.stringify(report, null, 2), "utf-8");
    if (!existsSync(baselinePath) || process.env.MEEMEE_RUNTIME_UPDATE_BASELINE === "1") {
      mkdirSync(dirname(baselinePath), { recursive: true });
      writeFileSync(baselinePath, JSON.stringify(report, null, 2), "utf-8");
    }

    await testInfo.attach("meemee-runtime-report.json", {
      body: JSON.stringify(report, null, 2),
      contentType: "application/json",
    });
    await testInfo.attach("meemee-runtime-screenshot-path.txt", {
      body: screenshotPath,
      contentType: "text/plain",
    });
    console.log(JSON.stringify(report, null, 2));
    const failures = checks.filter((check) => check.status === "fail" || check.status === "fail_blank");
    expect(failures, "runtime menu functional failures").toEqual([]);
  });
});
