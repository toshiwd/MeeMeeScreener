import { expect, test, type Locator, type Page } from "@playwright/test";

const TICKER_COUNT = 120;
const BATCH_RESPONSE_DELAY_MS = 900;

const responseJson = (status: number, payload: unknown) => ({
  status,
  contentType: "application/json",
  body: JSON.stringify(payload),
});

const buildTickers = () =>
  Array.from({ length: TICKER_COUNT }, (_, index) => ({
    code: String(1001 + index),
    name: `Scroll Stability ${index + 1}`,
    chg1D: (TICKER_COUNT - index) / 1000,
    lastClose: 100,
  }));

const buildBars = (code: string) => {
  const codeIndex = Number(code) - 1000;
  return Array.from({ length: 60 }, (_, index) => {
    const close = index === 59 ? 200 + codeIndex : 100;
    const volume = index === 59 ? (1000 + index) * codeIndex : 1000 + index;
    return [20260101 + index, 100, close + 1, 99, close, volume];
  });
};

const installMockApi = async (page: Page) => {
  const batchRequests: string[][] = [];
  await page.route("**/api/**", async (route) => {
    const url = new URL(route.request().url());
    const method = route.request().method();
    const path = url.pathname;

    if (path === "/api/health" && method === "GET") {
      return route.fulfill(
        responseJson(200, { ready: true, phase: "ready", message: "ok", status: "healthy" })
      );
    }
    if (path === "/api/health/live" && method === "GET") {
      return route.fulfill(
        responseJson(200, { ready: true, phase: "live", message: "ok", status: "healthy" })
      );
    }
    if (path === "/api/health/deep" && method === "GET") {
      return route.fulfill(responseJson(200, { ready: true, txt_count: 1 }));
    }
    if (path === "/api/grid/screener" && method === "GET") {
      return route.fulfill(responseJson(200, { items: buildTickers() }));
    }
    if (path === "/api/watchlist" && method === "GET") {
      return route.fulfill(responseJson(200, { codes: [] }));
    }
    if (path === "/api/favorites" && method === "GET") {
      return route.fulfill(responseJson(200, { codes: [] }));
    }
    if (path === "/api/jobs/current" && method === "GET") {
      return route.fulfill(responseJson(200, { job: null }));
    }
    if (path === "/api/events/meta" && method === "GET") {
      return route.fulfill(responseJson(200, {}));
    }
    if (path === "/api/ticker/tradex/summary" && method === "POST") {
      return route.fulfill(responseJson(200, { items: [] }));
    }
    if (path === "/api/batch_bars_v3" && method === "POST") {
      const request = JSON.parse(route.request().postData() ?? "{}") as {
        codes?: string[];
        timeframes?: string[];
      };
      batchRequests.push(request.codes ?? []);
      const timeframe = request.timeframes?.[0] ?? "daily";
      await new Promise((resolve) => setTimeout(resolve, BATCH_RESPONSE_DELAY_MS));
      const items = Object.fromEntries(
        (request.codes ?? []).map((code) => [
          code,
          {
            [timeframe]: {
              bars: buildBars(code),
              boxes: [],
              provenance: null,
            },
          },
        ])
      );
      return route.fulfill(
        responseJson(200, {
          meta: { data_version: "grid-scroll-stability-v1" },
          items,
        })
      );
    }

    return route.fulfill(responseJson(200, {}));
  });
  return batchRequests;
};

const readVisibleCodes = (scroller: Locator) =>
  scroller.evaluate((element) => {
    const viewport = element.getBoundingClientRect();
    return Array.from(element.querySelectorAll(".tile"))
      .filter((tile) => {
        const rect = tile.getBoundingClientRect();
        return rect.bottom > viewport.top && rect.top < viewport.bottom;
      })
      .map((tile) => tile.querySelector(".tile-code")?.textContent ?? "");
  });

const setInitialGridSort = (page: Page, sortKey: string) =>
  page.addInitScript((key) => {
    window.localStorage.clear();
    window.localStorage.setItem("sortKey", key);
    window.localStorage.setItem("sortDir", "desc");
    window.localStorage.setItem("gridPreset", "3");
  }, sortKey);

test.describe("grid scroll stability", () => {
  test("keeps snapshot change ordering stable while chart bars arrive during scrolling", async ({
    page,
  }) => {
    await installMockApi(page);
    await setInitialGridSort(page, "chg1D");

    await page.goto("/");
    await expect(page.locator(".tile-code").first()).toHaveText("1001");

    const scroller = page.locator(".grid-inner > div").first();
    await expect(scroller).toBeVisible();
    await scroller.evaluate((element) => {
      element.scrollBy(0, Math.max(480, element.clientHeight));
    });
    await page.waitForTimeout(50);
    const renderedCodesBeforeBars = await readVisibleCodes(scroller);
    await page.waitForTimeout(BATCH_RESPONSE_DELAY_MS + 500);
    const renderedCodesAfterBars = await readVisibleCodes(scroller);

    expect(renderedCodesAfterBars).toEqual(renderedCodesBeforeBars);

    await scroller.evaluate((element) => {
      element.scrollTo({ top: 0 });
    });

    await expect(page.locator(".tile-code").first()).toHaveText("1001");
  });

  test("waits for complete derived sort data before committing volume surge ordering", async ({
    page,
  }) => {
    const batchRequests = await installMockApi(page);
    await setInitialGridSort(page, "volumeSurge");

    await page.goto("/");
    await expect(page.locator(".tile-code").first()).toHaveText("1001");
    await expect(page.getByText("並び: 出来高急増順 (準備中)")).toBeVisible();

    const scroller = page.locator(".grid-inner > div").first();
    await expect(scroller).toBeVisible();
    await scroller.evaluate((element) => {
      element.scrollBy(0, Math.max(480, element.clientHeight));
    });
    await page.waitForTimeout(50);
    const renderedCodesBeforeBars = await readVisibleCodes(scroller);
    await page.waitForTimeout(BATCH_RESPONSE_DELAY_MS + 500);
    const renderedCodesAfterBars = await readVisibleCodes(scroller);

    expect(renderedCodesAfterBars).toEqual(renderedCodesBeforeBars);

    await scroller.evaluate((element) => {
      element.scrollTo({ top: 0 });
    });
    await expect(page.getByText("並び: 出来高急増順 (準備中)")).toHaveCount(0, {
      timeout: 10_000,
    });
    await expect(page.locator(".tile-code").first()).toHaveText("1120");
    expect(batchRequests.flat()).toEqual(expect.arrayContaining(["1001", "1120"]));
  });
});
