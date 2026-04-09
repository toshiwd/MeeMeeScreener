import { expect, test, type Page, type Request } from "@playwright/test";

type RequestEntry = {
  url: string;
  method: string;
};

type FlowSummary = {
  flow: string;
  elapsedMs: number;
  totalRequests: number;
  apiRequests: number;
  pathCounts: Record<string, number>;
  note?: string;
};

const perfBaseURL = process.env.MEEMEE_PERF_BASE_URL?.trim();
if (perfBaseURL) {
  test.use({ baseURL: perfBaseURL });
}

const waitForAnySelector = async (page: Page, selectors: string[]) => {
  await page.waitForFunction(
    (candidateSelectors) =>
      candidateSelectors.some((selector) => Boolean(document.querySelector(selector))),
    selectors
  );
};

const summarizeRequests = (
  entries: RequestEntry[],
  startIndex: number,
  flow: string,
  elapsedMs: number
): FlowSummary => {
  const slice = entries.slice(startIndex);
  const pathCounts: Record<string, number> = {};
  let apiRequests = 0;

  for (const entry of slice) {
    const pathname = new URL(entry.url).pathname;
    pathCounts[pathname] = (pathCounts[pathname] ?? 0) + 1;
    if (pathname.startsWith("/api/")) {
      apiRequests += 1;
    }
  }

  return {
    flow,
    elapsedMs,
    totalRequests: slice.length,
    apiRequests,
    pathCounts
  };
};

test.describe("MeeMee perf smoke", () => {
  test("captures request counts for key flows", async ({ page }, testInfo) => {
    const requests: RequestEntry[] = [];
    const onRequest = (request: Request) => {
      requests.push({
        url: request.url(),
        method: request.method()
      });
    };

    page.on("request", onRequest);

    const summaries: FlowSummary[] = [];

    const measureFlow = async (
      flow: string,
      run: () => Promise<void>,
      settleSelectors: string[],
      settleMs = 250
    ) => {
      const startIndex = requests.length;
      const startedAt = performance.now();
      await run();
      await waitForAnySelector(page, settleSelectors);
      await page.waitForTimeout(settleMs);
      summaries.push(
        summarizeRequests(requests, startIndex, flow, Math.round(performance.now() - startedAt))
      );
    };

    try {
      await measureFlow(
        "grid initial",
        async () => {
          await page.goto("/");
        },
        [".tile-code", ".grid-empty-state"]
      );

      await measureFlow(
        "ranking initial",
        async () => {
          await page.goto("/ranking");
        },
        [".tile-code", ".rank-status", ".grid-empty-state"]
      );

      await measureFlow(
        "list scroll",
        async () => {
          const scroller = page.locator(".virtualized-card-grid-scroller").first();
          if ((await scroller.count()) === 0) return;
          if (!(await scroller.isVisible().catch(() => false))) return;
          await scroller.evaluate((element) => {
            element.scrollTo({ top: 0 });
          });
          for (let index = 0; index < 4; index += 1) {
            await scroller.evaluate((element) => {
              element.scrollBy(0, Math.max(480, element.clientHeight));
            });
            await page.waitForTimeout(120);
          }
          await scroller.evaluate((element) => {
            element.scrollTo({ top: element.scrollHeight });
          });
        },
        [".tile-code", ".rank-status", ".grid-empty-state"],
        350
      );

      await measureFlow(
        "favorites initial",
        async () => {
          await page.goto("/favorites");
        },
        [".tile-code", ".rank-status"]
      );

      await page.goto("/");
      await waitForAnySelector(page, [".tile-code", ".grid-empty-state"]);

      const firstTileDetailButton = page
        .locator(".tile .tile-actions button.tile-action:last-child")
        .first();
      await expect(firstTileDetailButton).toBeVisible();

      await measureFlow(
        "detail initial",
        async () => {
          await firstTileDetailButton.click();
        },
        [".detail-summary-code"]
      );

      const detailCode = page.locator(".detail-summary-code");
      const detailNavButtons = page.locator(".detail-summary-actions .nav-button");
      const navCount = await detailNavButtons.count();
      if (navCount >= 2) {
        const nextButton = detailNavButtons.nth(1);
        await expect(nextButton).toBeEnabled();

        await measureFlow(
          "next hops",
          async () => {
            for (let index = 0; index < 5; index += 1) {
              if (!(await nextButton.isVisible()) || !(await nextButton.isEnabled())) {
                break;
              }
              const before = await detailCode.textContent();
              try {
                await nextButton.click({ timeout: 5_000 });
                await expect(detailCode).not.toHaveText(before ?? "", { timeout: 10_000 });
              } catch {
                break;
              }
            }
          },
          [".detail-summary-code"]
        );
      } else {
        summaries.push({
          flow: "next hops",
          elapsedMs: 0,
          totalRequests: 0,
          apiRequests: 0,
          pathCounts: {},
          note: "detail nav buttons not available in this snapshot"
        });
      }

      await measureFlow(
        "roundtrip",
        async () => {
          const backButton = page.locator(".detail-summary-back .nav-button").first();
          if (await backButton.isVisible().catch(() => false)) {
            await backButton.click({ timeout: 5_000 });
          } else {
            await page.goto("/");
          }
          await waitForAnySelector(page, [".tile-code", ".grid-empty-state"]);
          const favoritesLink = page.locator('a[href="/favorites"]').first();
          if (await favoritesLink.isVisible().catch(() => false)) {
            await favoritesLink.click({ timeout: 5_000 });
          } else {
            await page.goto("/favorites");
          }
        },
        [".tile-code", ".rank-status"]
      );

      const report = {
        baseURL: perfBaseURL ?? "http://127.0.0.1:4173",
        summaries
      };

      await testInfo.attach("meemee-perf-request-counts.json", {
        body: JSON.stringify(report, null, 2),
        contentType: "application/json"
      });

      console.log(JSON.stringify(report, null, 2));
    } finally {
      page.off("request", onRequest);
    }
  });
});
