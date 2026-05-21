import { expect, test } from "@playwright/test";
import { mkdirSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

type BatchBarsCall = {
  url: string;
  startedAtMs: number;
  endedAtMs: number | null;
  durationMs: number | null;
  status: number | null;
  postDataJson: unknown;
};

type ComparePerformanceArtifact = {
  url: string;
  status: "passed" | "failed";
  failure_reason?: string;
  timings_ms: {
    domcontentloaded: number | null;
    compare_shell_visible: number | null;
    charts_ready: number | null;
  };
  batch_bars_calls: BatchBarsCall[];
  observed_limits: Array<{
    codes: unknown;
    timeframes: unknown;
    timeframeLimits: unknown;
  }>;
};

const repoRoot = resolve(fileURLToPath(new URL("../../..", import.meta.url)));
const artifactsDir = resolve(repoRoot, "artifacts/runtime-ui");
const liveBaseUrl = process.env.MEEMEE_E2E_BASE_URL?.replace(/\/$/, "") ?? null;
const targetMainCode = process.env.MEEMEE_COMPARE_MAIN_CODE ?? "6326";
const targetCompareCode = process.env.MEEMEE_COMPARE_CODE ?? "6302";
const targetMainAsOf = process.env.MEEMEE_COMPARE_MAIN_ASOF ?? "2026-05-20";
const targetCompareAsOf = process.env.MEEMEE_COMPARE_ASOF ?? "2026-05-31";

const nowStamp = () => new Date().toISOString().replace(/[:.]/g, "-");

const elapsed = (startMs: number) => Math.round(performance.now() - startMs);

const getBatchBarsPayloadSummary = (postDataJson: unknown) => {
  if (!postDataJson || typeof postDataJson !== "object") {
    return {
      codes: null,
      timeframes: null,
      timeframeLimits: null,
    };
  }
  const payload = postDataJson as {
    codes?: unknown;
    timeframes?: unknown;
    timeframeLimits?: unknown;
  };
  return {
    codes: payload.codes ?? null,
    timeframes: payload.timeframes ?? null,
    timeframeLimits: payload.timeframeLimits ?? null,
  };
};

test.describe("Detail compare chart performance", () => {
  test("renders the compare charts within the initial budget", async ({ page }) => {
    test.setTimeout(45_000);

    mkdirSync(artifactsDir, { recursive: true });
    const artifactPath = resolve(
      artifactsDir,
      `detail-compare-performance-${nowStamp()}.json`
    );
    const batchBarsCalls: BatchBarsCall[] = [];
    const requests = new Map<object, BatchBarsCall>();

    page.on("request", (request) => {
      if (!request.url().includes("batch_bars_v3")) return;
      const call: BatchBarsCall = {
        url: request.url(),
        startedAtMs: performance.now(),
        endedAtMs: null,
        durationMs: null,
        status: null,
        postDataJson: (() => {
          try {
            return request.postDataJSON();
          } catch {
            return request.postData();
          }
        })(),
      };
      requests.set(request, call);
      batchBarsCalls.push(call);
    });

    page.on("response", (response) => {
      const request = response.request();
      const call = requests.get(request);
      if (!call) return;
      call.endedAtMs = performance.now();
      call.durationMs = Math.round(call.endedAtMs - call.startedAtMs);
      call.status = response.status();
    });

    const targetPath =
      `/detail/${targetMainCode}?compare=${targetCompareCode}` +
      `&mainAsOf=${targetMainAsOf}&compareAsOf=${targetCompareAsOf}`;
    const targetUrl = liveBaseUrl ? `${liveBaseUrl}${targetPath}` : targetPath;
    const startedAtMs = performance.now();
    let domcontentloaded: number | null = null;
    let compareShellVisible: number | null = null;
    let chartsReady: number | null = null;

    try {
      await page.goto(targetUrl, { waitUntil: "domcontentloaded" });
      domcontentloaded = elapsed(startedAtMs);
      await expect(page.locator(".detail-compare")).toBeVisible({ timeout: 10_000 });
      compareShellVisible = elapsed(startedAtMs);

      await page.waitForFunction(
        () => {
          const compareRoot = document.querySelector(".detail-compare");
          if (!compareRoot) return false;
          const charts = compareRoot.querySelectorAll(".detail-compare-chart");
          if (charts.length !== 4) return false;
          const pendingNotices = compareRoot.querySelectorAll(".detail-chart-empty");
          if (pendingNotices.length > 0) return false;
          return Array.from(charts).every((chart) =>
            Array.from(chart.querySelectorAll("canvas")).some((canvas) => {
              const rect = canvas.getBoundingClientRect();
              return rect.width > 0 && rect.height > 0 && canvas.width > 0 && canvas.height > 0;
            })
          );
        },
        undefined,
        { timeout: 15_000 }
      );
      chartsReady = elapsed(startedAtMs);
    } catch (error) {
      const artifact: ComparePerformanceArtifact = {
        url: targetUrl,
        status: "failed",
        failure_reason: error instanceof Error ? error.message : String(error),
        timings_ms: {
          domcontentloaded,
          compare_shell_visible: compareShellVisible,
          charts_ready: chartsReady,
        },
        batch_bars_calls: batchBarsCalls,
        observed_limits: batchBarsCalls.map((call) =>
          getBatchBarsPayloadSummary(call.postDataJson)
        ),
      };
      writeFileSync(artifactPath, `${JSON.stringify(artifact, null, 2)}\n`, "utf8");
      test.info().attach("detail-compare-performance", {
        path: artifactPath,
        contentType: "application/json",
      });
      throw error;
    }

    const observedLimits = batchBarsCalls.map((call) =>
      getBatchBarsPayloadSummary(call.postDataJson)
    );
    const compareInitialCall = observedLimits.find((summary) => {
      const codes = summary.codes;
      return Array.isArray(codes) && codes.includes(targetCompareCode);
    });

    expect(compareInitialCall).toBeTruthy();
    expect(compareInitialCall?.timeframes).toEqual(["daily", "monthly"]);
    expect(compareInitialCall?.timeframeLimits).toMatchObject({
      daily: 420,
      monthly: 240,
    });
    expect(chartsReady).toBeLessThan(15_000);

    const artifact: ComparePerformanceArtifact = {
      url: targetUrl,
      status: "passed",
      timings_ms: {
        domcontentloaded,
        compare_shell_visible: compareShellVisible,
        charts_ready: chartsReady,
      },
      batch_bars_calls: batchBarsCalls,
      observed_limits: observedLimits,
    };
    writeFileSync(artifactPath, `${JSON.stringify(artifact, null, 2)}\n`, "utf8");
    test.info().attach("detail-compare-performance", {
      path: artifactPath,
      contentType: "application/json",
    });
  });
});
