import { expect, test, type Page } from "@playwright/test";
import { mkdirSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

type ApiCall = {
  method: string;
  url: string;
  postDataJson?: unknown;
  startedAtMs: number;
  endedAtMs: number | null;
  durationMs: number | null;
  status: number | null;
};

type TimelineMark = {
  label: string;
  atMs: number;
};

type DiagnosisArtifact = {
  summary: {
    status: "diagnosed" | "inconclusive";
    primary_bottleneck:
      | "route"
      | "api"
      | "db"
      | "state_lifecycle"
      | "chart_render"
      | "provider_init"
      | "unknown";
    reason?: string;
  };
  timings_ms: {
    click_to_url_changed: number | null;
    url_changed_to_detail_mounted: number | null;
    detail_mounted_to_first_api_start: number | null;
    first_api_start_to_first_api_response: number | null;
    first_api_response_to_daily_visible: number | null;
    daily_visible_to_all_charts_visible: number | null;
    click_to_first_useful_chart: number | null;
    click_to_all_charts_visible: number | null;
  };
  api_calls: ApiCall[];
  cache: {
    prefetch_hit: boolean | null;
    partial_cache_used: boolean | null;
  };
  detail: {
    symbol: string | null;
    timeline: TimelineMark[];
    perf_events: unknown[];
    chart_state_ready: {
      daily: number | null;
      weekly: number | null;
      monthly: number | null;
    };
  };
  recommendations: string[];
};

const repoRoot = resolve(fileURLToPath(new URL("../../..", import.meta.url)));
const artifactsDir = resolve(repoRoot, "artifacts/runtime-ui");
const directDetailCode = process.env.MEEMEE_RUNTIME_DIRECT_DETAIL_CODE?.trim() || "4691";

const nowStamp = () => new Date().toISOString().replace(/[:.]/g, "-");

const elapsed = (start: number, end: number | null | undefined) =>
  typeof end === "number" ? Math.round(end - start) : null;

const firstCodeFromText = (text: string) => text.match(/\b\d{4}\b/)?.[0] ?? null;

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

const waitForFooterBars = async (
  page: Page,
  timeframe: "daily" | "weekly" | "monthly",
  timeout: number
) => {
  await page.waitForFunction(
    (targetTimeframe) => {
      const footer = document.querySelector('[data-testid="detail-footer"]');
      const text = footer?.textContent ?? "";
      const match = text.match(/日足\s+(\d+)本\s+\|\s+週足\s+(\d+)本\s+\|\s+月足\s+(\d+)本/);
      if (!match) return false;
      const [, daily, weekly, monthly] = match;
      const value =
        targetTimeframe === "daily" ? daily : targetTimeframe === "weekly" ? weekly : monthly;
      return Number(value) > 0;
    },
    timeframe,
    { timeout }
  );
};

const waitForChartCanvas = async (
  page: Page,
  timeframe: "daily" | "weekly" | "monthly",
  timeout: number
) => {
  const selector =
    timeframe === "daily"
      ? ".detail-row-top .detail-chart-inner canvas"
      : timeframe === "weekly"
        ? ".detail-row-bottom .detail-pane:nth-child(1) .detail-chart-inner canvas"
        : ".detail-row-bottom .detail-pane:nth-child(3) .detail-chart-inner canvas";
  await page.waitForFunction(
    (candidateSelector) =>
      Array.from(document.querySelectorAll(candidateSelector)).some((element) => {
        const rect = element.getBoundingClientRect();
        const canvas = element as HTMLCanvasElement;
        return rect.width > 0 && rect.height > 0 && canvas.width > 0 && canvas.height > 0;
      }),
    selector,
    { timeout }
  );
};

const classifyBottleneck = (artifact: DiagnosisArtifact) => {
  const timings = artifact.timings_ms;
  const apiCalls = artifact.api_calls.filter((call) => call.url.includes("/batch_bars_v3"));
  const slowestApi = apiCalls.reduce<ApiCall | null>(
    (current, call) =>
      (call.durationMs ?? 0) > (current?.durationMs ?? 0) ? call : current,
    null
  );
  const clickToUrl = timings.click_to_url_changed ?? 0;
  const apiDuration = slowestApi?.durationMs ?? 0;
  const responseToDaily = timings.first_api_response_to_daily_visible ?? 0;
  const dailyToAll = timings.daily_visible_to_all_charts_visible ?? 0;

  if (apiCalls.some((call) => call.status === 503 || call.url.includes("database"))) {
    return {
      status: "diagnosed" as const,
      primary_bottleneck: "db" as const,
      reason: "detail open observed backend/database failure",
    };
  }
  if (clickToUrl >= 300) {
    return {
      status: "diagnosed" as const,
      primary_bottleneck: "route" as const,
      reason: `click_to_url_changed is ${clickToUrl}ms before DetailView can render`,
    };
  }
  if (apiDuration >= 500) {
    return {
      status: "diagnosed" as const,
      primary_bottleneck: "api" as const,
      reason: `slowest /batch_bars_v3 response is ${apiDuration}ms`,
    };
  }
  if (responseToDaily >= 300) {
    return {
      status: "diagnosed" as const,
      primary_bottleneck: "state_lifecycle" as const,
      reason: `first chart appears ${responseToDaily}ms after first chart API response`,
    };
  }
  if (dailyToAll >= 300) {
    return {
      status: "diagnosed" as const,
      primary_bottleneck: "chart_render" as const,
      reason: `remaining charts appear ${dailyToAll}ms after Daily`,
    };
  }
  return {
    status: "inconclusive" as const,
    primary_bottleneck: "unknown" as const,
    reason: "no single segment exceeded the current diagnostic threshold",
  };
};

test.describe("DetailView performance diagnosis", () => {
  test.setTimeout(180_000);

  test("writes a runtime detail timing diagnosis artifact", async ({ page }) => {
    const timeout = 60_000;
    mkdirSync(artifactsDir, { recursive: true });
    const artifactPath = resolve(
      artifactsDir,
      `detail-performance-diagnosis-${nowStamp()}.json`
    );
    const requests = new Map<object, ApiCall>();
    const apiCalls: ApiCall[] = [];
    const timeline: TimelineMark[] = [];

    page.on("request", (request) => {
      const url = request.url();
      if (!url.includes("/api/")) return;
      const call: ApiCall = {
        method: request.method(),
        url,
        postDataJson: (() => {
          if (request.method() !== "POST") return undefined;
          try {
            return request.postDataJSON();
          } catch {
            return request.postData();
          }
        })(),
        startedAtMs: performance.now(),
        endedAtMs: null,
        durationMs: null,
        status: null,
      };
      requests.set(request, call);
      apiCalls.push(call);
    });
    page.on("response", (response) => {
      const request = response.request();
      const url = request.url();
      if (!url.includes("/api/")) return;
      const matching = requests.get(request);
      if (!matching) return;
      matching.endedAtMs = performance.now();
      matching.durationMs = Math.round(matching.endedAtMs - matching.startedAtMs);
      matching.status = response.status();
    });

    await page.addInitScript(() => {
      window.localStorage.setItem("meemeePerfDiagnosticsEnabled", "1");
    });

    await page.goto("/ranking", { waitUntil: "domcontentloaded" });
    await waitForStartupOverlayGone(page, timeout);
    await page.locator(".rank-tile").first().waitFor({ state: "visible", timeout });
    const firstTile = page.locator(".rank-tile").first();
    const symbol = firstCodeFromText(await firstTile.innerText()) ?? directDetailCode;

    const clickAt = performance.now();
    timeline.push({ label: "ranking_symbol_click", atMs: 0 });
    await firstTile.click();
    await page.waitForURL(`**/detail-v2/${symbol}`, { timeout });
    const urlChangedAt = performance.now();
    timeline.push({ label: "detail_url_changed", atMs: elapsed(clickAt, urlChangedAt) ?? 0 });

    await page.locator(".detail-shell").waitFor({ state: "visible", timeout });
    const mountedAt = performance.now();
    timeline.push({ label: "detail_view_mounted", atMs: elapsed(clickAt, mountedAt) ?? 0 });

    await waitForFooterBars(page, "daily", timeout);
    const dailyStateReadyAt = performance.now();
    timeline.push({ label: "daily_state_ready", atMs: elapsed(clickAt, dailyStateReadyAt) ?? 0 });
    await waitForChartCanvas(page, "daily", timeout);
    const dailyVisibleAt = performance.now();
    timeline.push({ label: "daily_visible", atMs: elapsed(clickAt, dailyVisibleAt) ?? 0 });

    await waitForFooterBars(page, "weekly", timeout);
    const weeklyStateReadyAt = performance.now();
    timeline.push({ label: "weekly_state_ready", atMs: elapsed(clickAt, weeklyStateReadyAt) ?? 0 });
    await waitForChartCanvas(page, "weekly", timeout);
    const weeklyVisibleAt = performance.now();
    timeline.push({ label: "weekly_visible", atMs: elapsed(clickAt, weeklyVisibleAt) ?? 0 });

    await waitForFooterBars(page, "monthly", timeout);
    const monthlyStateReadyAt = performance.now();
    timeline.push({ label: "monthly_state_ready", atMs: elapsed(clickAt, monthlyStateReadyAt) ?? 0 });
    await waitForChartCanvas(page, "monthly", timeout);
    const monthlyVisibleAt = performance.now();
    timeline.push({ label: "monthly_visible", atMs: elapsed(clickAt, monthlyVisibleAt) ?? 0 });

    await page.waitForTimeout(500);
    const perfEvents = await page.evaluate(() => window.MeeMeePerfDiagnostics?.getEvents?.() ?? []);
    const detailApiCalls = apiCalls.filter((call) => call.startedAtMs >= clickAt);
    const chartApiCalls = detailApiCalls.filter((call) => call.url.includes("/batch_bars_v3"));
    const firstApiStart = chartApiCalls[0]?.startedAtMs ?? null;
    const firstApiResponse =
      chartApiCalls.find((call) => call.endedAtMs != null)?.endedAtMs ?? null;
    const hasSeedHit = perfEvents.some((event: unknown) => {
      const item = event as { eventType?: string; route?: string };
      return item.eventType === "detail_chart_seed_hit" && item.route?.includes("/detail");
    });
    const prefetchHit = hasSeedHit || chartApiCalls.length === 0;
    const partialCacheUsed =
      hasSeedHit && chartApiCalls.some((call) => call.endedAtMs != null && call.endedAtMs > dailyVisibleAt);

    const artifact: DiagnosisArtifact = {
      summary: {
        status: "inconclusive",
        primary_bottleneck: "unknown",
      },
      timings_ms: {
        click_to_url_changed: elapsed(clickAt, urlChangedAt),
        url_changed_to_detail_mounted: elapsed(urlChangedAt, mountedAt),
        detail_mounted_to_first_api_start:
          firstApiStart == null ? null : Math.round(firstApiStart - mountedAt),
        first_api_start_to_first_api_response:
          firstApiStart == null || firstApiResponse == null
            ? null
            : Math.round(firstApiResponse - firstApiStart),
        first_api_response_to_daily_visible:
          firstApiResponse == null ? null : Math.round(dailyVisibleAt - firstApiResponse),
        daily_visible_to_all_charts_visible: Math.round(monthlyVisibleAt - dailyVisibleAt),
        click_to_first_useful_chart: elapsed(clickAt, dailyVisibleAt),
        click_to_all_charts_visible: elapsed(clickAt, monthlyVisibleAt),
      },
      api_calls: detailApiCalls.map((call) => ({
        ...call,
        startedAtMs: Math.round(call.startedAtMs - clickAt),
        endedAtMs: call.endedAtMs == null ? null : Math.round(call.endedAtMs - clickAt),
      })),
      cache: {
        prefetch_hit: prefetchHit,
        partial_cache_used: partialCacheUsed,
      },
      detail: {
        symbol,
        timeline,
        perf_events: perfEvents,
        chart_state_ready: {
          daily: elapsed(clickAt, dailyStateReadyAt),
          weekly: elapsed(clickAt, weeklyStateReadyAt),
          monthly: elapsed(clickAt, monthlyStateReadyAt),
        },
      },
      recommendations: [],
    };

    artifact.summary = classifyBottleneck(artifact);
    if (artifact.summary.primary_bottleneck === "route") {
      artifact.recommendations.push(
        "Inspect openDetailWithPrefetch pre-navigation wait and target/neighborhood prefetch behavior."
      );
    } else if (artifact.summary.primary_bottleneck === "api") {
      artifact.recommendations.push(
        "Inspect /batch_bars_v3 backend duration and whether DetailView can display cached frames while the refresh continues."
      );
    } else if (artifact.summary.primary_bottleneck === "state_lifecycle") {
      artifact.recommendations.push(
        "Inspect DetailView main chart fetch effect and lifecycle state updates between /batch_bars_v3 response and first chart render."
      );
    } else if (artifact.summary.primary_bottleneck === "chart_render") {
      artifact.recommendations.push(
        "Inspect DetailChart initialization and canvas draw timing for weekly/monthly panels."
      );
    }

    writeFileSync(artifactPath, JSON.stringify(artifact, null, 2), "utf-8");
    console.log(JSON.stringify({ artifactPath, summary: artifact.summary, timings_ms: artifact.timings_ms }, null, 2));

    expect(artifact.timings_ms.click_to_first_useful_chart).toBeGreaterThan(0);
    expect(artifact.api_calls.length).toBeGreaterThan(0);
  });
});
