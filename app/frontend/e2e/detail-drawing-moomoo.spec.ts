import { expect, test, type Page } from "@playwright/test";

const responseJson = (status: number, payload: unknown) => ({
  status,
  contentType: "application/json",
  body: JSON.stringify(payload),
});

const buildBars = () =>
  Array.from({ length: 90 }, (_, index) => {
    const date = 20260101 + index;
    const close = 1000 + index * 4;
    return [date, close - 3, close + 8, close - 10, close, 1000 + index * 10];
  });

const installMockApi = async (page: Page) => {
  await page.route("**/api/**", async (route) => {
    const url = new URL(route.request().url());
    const method = route.request().method();
    const path = url.pathname;

    if (path === "/api/health" && method === "GET") {
      return route.fulfill(responseJson(200, { ready: true, phase: "ready", message: "ok", status: "healthy" }));
    }
    if (path === "/api/health/live" && method === "GET") {
      return route.fulfill(responseJson(200, { ready: true, phase: "live", message: "ok", status: "healthy" }));
    }
    if (path === "/api/health/deep" && method === "GET") {
      return route.fulfill(responseJson(200, { ready: true, txt_count: 1 }));
    }
    if (path === "/api/grid/screener" && method === "GET") {
      return route.fulfill(responseJson(200, { items: [{ code: "1802", name: "Drawing Browser Test", lastClose: 1356 }] }));
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
    if (path === "/api/batch_bars_v3" && method === "POST") {
      const request = JSON.parse(route.request().postData() ?? "{}") as {
        codes?: string[];
        timeframes?: string[];
      };
      const frames = Object.fromEntries(
        (request.timeframes ?? ["daily", "weekly", "monthly"]).map((timeframe) => [
          timeframe,
          { bars: buildBars(), boxes: [], provenance: null },
        ])
      );
      return route.fulfill(
        responseJson(200, {
          meta: { data_version: "detail-drawing-moomoo-v1" },
          items: Object.fromEntries((request.codes ?? ["1802"]).map((code) => [code, frames])),
        })
      );
    }
    if (path === "/api/chart-annotations" && method === "GET") {
      return route.fulfill(responseJson(200, { annotations: [] }));
    }
    if (path === "/api/chart-notes" && method === "GET") {
      return route.fulfill(responseJson(200, { notes: [] }));
    }
    if (path === "/api/events/meta" && method === "GET") {
      return route.fulfill(responseJson(200, {}));
    }
    if (path === "/api/ticker/tradex/summary" && method === "POST") {
      return route.fulfill(responseJson(200, { items: [] }));
    }
    return route.fulfill(responseJson(200, {}));
  });
};

const dragInside = async (page: Page, selector: string) => {
  const box = await page.locator(selector).first().boundingBox();
  if (!box) throw new Error(`missing chart box for ${selector}`);
  const start = { x: box.x + box.width * 0.32, y: box.y + box.height * 0.34 };
  const end = { x: box.x + box.width * 0.68, y: box.y + box.height * 0.64 };
  await page.mouse.move(start.x, start.y);
  await page.mouse.down();
  await page.mouse.move(end.x, end.y, { steps: 8 });
  await page.mouse.up();
  return { start, end, center: { x: (start.x + end.x) / 2, y: (start.y + end.y) / 2 } };
};

test("uses MOOMOO-style candle count arrows and BOX editing handles", async ({ page }, testInfo) => {
  await page.addInitScript(() => {
    localStorage.clear();
    const state = {
      labels: [] as string[],
      measurementDashes: 0,
      arcs: [] as Array<{ strokeStyle: string | CanvasGradient | CanvasPattern }>,
    };
    (window as typeof window & { __drawingProbe: typeof state }).__drawingProbe = state;
    const originalFillText = CanvasRenderingContext2D.prototype.fillText;
    CanvasRenderingContext2D.prototype.fillText = function (text, x, y, maxWidth) {
      state.labels.push(String(text));
      return maxWidth == null
        ? originalFillText.call(this, text, x, y)
        : originalFillText.call(this, text, x, y, maxWidth);
    };
    const originalSetLineDash = CanvasRenderingContext2D.prototype.setLineDash;
    CanvasRenderingContext2D.prototype.setLineDash = function (segments) {
      if (segments.join(",") === "7,5") state.measurementDashes += 1;
      return originalSetLineDash.call(this, segments);
    };
    const originalArc = CanvasRenderingContext2D.prototype.arc;
    CanvasRenderingContext2D.prototype.arc = function (...args) {
      state.arcs.push({ strokeStyle: this.strokeStyle });
      return originalArc.apply(this, args);
    };
  });
  await installMockApi(page);
  await page.goto("/detail/1802");

  await expect(page.getByTestId("detail-draw-toolbar")).toBeVisible();
  await expect(page.getByRole("button", { name: "選択 / 描画編集" })).toBeVisible();
  await expect(page.getByRole("button", { name: "連続描画 ON" })).toBeVisible();

  const chartSelector = ".detail-row-top .detail-chart-wrapper";
  await expect(page.locator(chartSelector).first()).toBeVisible();

  await page.getByRole("button", { name: "ローソク足カウント" }).click();
  await dragInside(page, chartSelector);
  await expect
    .poll(() =>
      page.evaluate(() => {
        const probe = (window as typeof window & { __drawingProbe: { labels: string[] } }).__drawingProbe;
        return probe.labels.some((label) => /^\d+bars,\d+d$/.test(label));
      })
    )
    .toBe(true);

  await page.getByRole("button", { name: "BOX" }).click();
  const boxDrag = await dragInside(page, chartSelector);
  await page.getByRole("button", { name: "選択 / 描画編集" }).click();
  const arcsBeforeSelection = await page.evaluate(
    () => (window as typeof window & { __drawingProbe: { arcs: unknown[] } }).__drawingProbe.arcs.length
  );
  await page.mouse.click(boxDrag.center.x, boxDrag.center.y);

  await expect
    .poll(() =>
      page.evaluate(
        (before) =>
          (window as typeof window & { __drawingProbe: { arcs: unknown[] } }).__drawingProbe.arcs.length - before,
        arcsBeforeSelection
      )
    )
    .toBeGreaterThanOrEqual(4);
  await expect
    .poll(() =>
      page.evaluate(
        () => (window as typeof window & { __drawingProbe: { measurementDashes: number } }).__drawingProbe.measurementDashes
      )
    )
    .toBeGreaterThan(1);

  const drawingsBeforeDrag = await page.evaluate(() =>
    Object.fromEntries(
      Object.keys(localStorage)
        .filter((key) => key.startsWith("drawings:v1:"))
        .map((key) => [key, localStorage.getItem(key)])
    )
  );
  await page.mouse.move(boxDrag.center.x, boxDrag.center.y);
  await page.mouse.down();
  await page.mouse.move(boxDrag.center.x + 48, boxDrag.center.y + 24, { steps: 8 });
  await page.mouse.up();
  await expect
    .poll(() =>
      page.evaluate(
        (before) =>
          JSON.stringify(
            Object.fromEntries(
              Object.keys(localStorage)
                .filter((key) => key.startsWith("drawings:v1:"))
                .map((key) => [key, localStorage.getItem(key)])
            )
          ) !== JSON.stringify(before),
        drawingsBeforeDrag
      )
    )
    .toBe(true);
  await page.screenshot({ path: testInfo.outputPath("detail-drawing-moomoo.png"), fullPage: true });

  await page.getByRole("button", { name: "連続描画 ON" }).click();
  await expect(page.getByRole("button", { name: "連続描画 OFF" })).toBeVisible();
  await page.getByRole("button", { name: "水平ライン" }).click();
  await page.mouse.click(boxDrag.center.x, boxDrag.center.y);
  await expect(page.getByRole("button", { name: "選択 / 描画編集" })).toHaveClass(/is-selected/);
});
