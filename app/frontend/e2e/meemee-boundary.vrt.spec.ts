import { expect, test, type Locator, type Page, type Route } from "@playwright/test";

type AnyRecord = Record<string, unknown>;
type ApiMethod = "GET" | "POST";

type BarRow = [number, number, number, number, number, number];

const RANKING_NORMAL_ASOF = "2026-03-10";
const RANKING_PROVISIONAL_ASOF = "2026-03-05";
const BASE_TIME = "2026-03-19T09:00:00+09:00";
const EVENT_META_TIME = "2099-01-01T09:00:00+09:00";
const EVENT_RIGHTS_MAX_DATE = "2099-01-01";

function formatYmd(date: Date) {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return Number(`${year}${month}${day}`);
}

function buildBars(start: { year: number; month: number; day: number }, closes: number[]): BarRow[] {
  const base = new Date(Date.UTC(start.year, start.month - 1, start.day));
  return closes.map((close, index) => {
    const date = new Date(base);
    date.setUTCDate(base.getUTCDate() + index);
    const open = close - (index % 2 === 0 ? 1 : -1);
    const high = Math.max(open, close) + 3;
    const low = Math.min(open, close) - 3;
    const volume = 1000 + index * 100;
    return [formatYmd(date), open, high, low, close, volume];
  });
}

function responseJson(status: number, payload: unknown) {
  return {
    status,
    contentType: "application/json",
    body: JSON.stringify(payload)
  };
}

function buildBaseBars() {
  return {
    "1301": buildBars(
      { year: 2026, month: 3, day: 1 },
      [100, 103, 104, 108, 109, 111, 113, 114, 117, 120]
    ),
    "7203": buildBars(
      { year: 2026, month: 3, day: 1 },
      [240, 238, 237, 236, 234, 233, 231, 230, 228, 227]
    )
  } satisfies Record<string, BarRow[]>;
}

function buildCommonScreenerItems() {
  return {
    items: [
      {
        code: "1301",
        name: "MeeMee Electric",
        stage: "confirmed",
        score: 0.91,
        reason: "confirmed momentum",
        asOf: RANKING_NORMAL_ASOF,
        lastClose: 120,
        chg1D: 0.03,
        chg1W: 0.08,
        chg1M: 0.14,
        displayScore: 0.91,
        displayScoreSource: "ranking_entry",
        is_favorite: false
      },
      {
        code: "7203",
        name: "Toyota",
        stage: "confirmed",
        score: 0.77,
        reason: "confirmed mean reversion",
        asOf: RANKING_NORMAL_ASOF,
        lastClose: 227,
        chg1D: -0.01,
        chg1W: -0.04,
        chg1M: -0.09,
        displayScore: 0.77,
        displayScoreSource: "ranking_entry",
        is_favorite: false
      }
    ],
    stale: false,
    asOf: RANKING_NORMAL_ASOF,
    updatedAt: BASE_TIME,
    generation: "vrt",
    lastError: null
  };
}

function buildRankingPayload(provisional: boolean) {
  return {
    items: [
      {
        code: "1301",
        name: "MeeMee Electric",
        asOf: RANKING_NORMAL_ASOF,
        score: 0.91,
        changePct: 0.03,
        changeAbs: 30,
        setupType: "breakout",
        reason: "confirmed momentum",
        series: buildBaseBars()["1301"],
        is_favorite: false
      },
      {
        code: "7203",
        name: "Toyota",
        asOf: provisional ? RANKING_PROVISIONAL_ASOF : RANKING_NORMAL_ASOF,
        score: 0.77,
        changePct: -0.01,
        changeAbs: -25,
        setupType: "pullback",
        reason: "confirmed mean reversion",
        series: buildBaseBars()["7203"],
        is_favorite: false
      }
    ]
  };
}

function buildTradeSummary() {
  return {
    dominant_direction: "up",
    difference_score: 0.2,
    buy: {
      count: 1,
      avg_trade_priority_score: 0.9,
      avg_profit_expectancy: 0.1,
      avg_hit_score: 0.2
    },
    sell: {
      count: 0,
      avg_trade_priority_score: null,
      avg_profit_expectancy: null,
      avg_hit_score: null
    }
  };
}

function buildPositionsHeldItems() {
  return {
    items: [
      {
        symbol: "7203",
        name: "Toyota",
        sell_buy_text: "買い",
        opened_at: "2026-03-19",
        has_issue: false,
        buy_qty: 100,
        sell_qty: 0
      }
    ]
  };
}

function buildPositionsHistoryItems() {
  return {
    items: [
      {
        round_id: "round-7203-1",
        symbol: "7203",
        name: "Toyota",
        opened_at: "2026-03-10",
        closed_at: null,
        round_no: 1,
        has_issue: false
      }
    ]
  };
}

function buildBatchBarsPayload() {
  const baseBars = buildBaseBars();
  return (request: { codes?: string[]; timeframes?: string[]; limit?: number; timeframeLimits?: AnyRecord }) => {
    const codes = Array.isArray(request.codes) ? request.codes.map((code) => String(code)) : [];
    const timeframes = Array.isArray(request.timeframes) ? request.timeframes.map((tf) => String(tf)) : ["daily"];
    const items: AnyRecord = {};

    codes.forEach((code) => {
      const bars = baseBars[code] ?? [];
      const limitByTf = request.timeframeLimits ?? {};
      const codeItem: AnyRecord = {};
      timeframes.forEach((timeframe) => {
        const limit =
          typeof limitByTf[timeframe] === "number" && Number.isFinite(limitByTf[timeframe])
            ? Number(limitByTf[timeframe])
            : typeof request.limit === "number" && Number.isFinite(request.limit)
              ? Number(request.limit)
              : bars.length;
        codeItem[timeframe] = {
          bars: bars.slice(-limit),
          boxes: []
        };
      });
      items[code] = codeItem;
    });

    return {
      items,
      meta: {
        data_version: "vrt",
        fetched_at: BASE_TIME
      }
    };
  };
}

async function setMeeMeeTestState(page: Page) {
  await page.addInitScript(() => {
    window.localStorage.clear();
    window.sessionStorage.clear();
    window.localStorage.setItem("meemee-theme", "light");
    class ImmediateIntersectionObserver implements IntersectionObserver {
      readonly root: Element | Document | null = null;
      readonly rootMargin = "0px";
      readonly thresholds = [0];
      constructor(
        private readonly callback: IntersectionObserverCallback,
        _options?: IntersectionObserverInit
      ) {}
      observe(target: Element) {
        const entry = {
          time: performance.now(),
          target,
          isIntersecting: true,
          intersectionRatio: 1,
          boundingClientRect: target.getBoundingClientRect(),
          intersectionRect: target.getBoundingClientRect(),
          rootBounds: null
        } as IntersectionObserverEntry;
        this.callback([entry], this);
      }
      unobserve() {}
      disconnect() {}
      takeRecords() {
        return [];
      }
    }
    window.IntersectionObserver = ImmediateIntersectionObserver as unknown as typeof IntersectionObserver;
  });
}

async function seedPersistentChartFrame(
  page: Page,
  seed: {
    code: string;
    timeframe: "daily" | "weekly" | "monthly";
    limit: number;
    asof?: string | null;
    includeBoxes: boolean;
    dataVersion: string;
    bars: number[][];
    boxes: AnyRecord[];
  }
) {
  await page.addInitScript(async (seedData) => {
    const dbName = "meemee-chart-cache";
    const storeName = "frames";
    const activeVersionKey = "meemee.chart-cache.active-version";
    window.localStorage.setItem(activeVersionKey, seedData.dataVersion);

    await new Promise<void>((resolve, reject) => {
      const request = indexedDB.open(dbName, 1);
      request.onupgradeneeded = () => {
        const db = request.result;
        if (db.objectStoreNames.contains(storeName)) return;
        const store = db.createObjectStore(storeName, { keyPath: "id" });
        store.createIndex("baseKey", "baseKey", { unique: false });
        store.createIndex("dataVersion", "dataVersion", { unique: false });
        store.createIndex("touchedAt", "touchedAt", { unique: false });
      };
      request.onerror = () => reject(request.error ?? new Error("IndexedDB open failed"));
      request.onblocked = () => reject(new Error("IndexedDB open blocked"));
      request.onsuccess = () => {
        const db = request.result;
        const tx = db.transaction(storeName, "readwrite");
        const store = tx.objectStore(storeName);
        const normalizedAsof =
          typeof seedData.asof === "string" && seedData.asof.trim() ? seedData.asof.trim() : "";
        const limit = Math.max(1, Math.floor(seedData.limit));
        const baseKey = `${seedData.code}|${seedData.timeframe}|${limit}|${normalizedAsof}|boxes:${seedData.includeBoxes ? 1 : 0}`;
        const now = Date.now();
        store.put({
          id: `${baseKey}|version:${seedData.dataVersion}`,
          baseKey,
          code: seedData.code,
          timeframe: seedData.timeframe,
          limit,
          asof: normalizedAsof,
          includeBoxes: seedData.includeBoxes,
          dataVersion: seedData.dataVersion,
          bars: seedData.bars,
          boxes: seedData.boxes,
          fetchedAt: now,
          touchedAt: now
        });
        tx.oncomplete = () => {
          db.close();
          resolve();
        };
        tx.onerror = () => {
          db.close();
          reject(tx.error ?? new Error("IndexedDB seed transaction failed"));
        };
        tx.onabort = () => {
          db.close();
          reject(tx.error ?? new Error("IndexedDB seed transaction aborted"));
        };
      };
    });
  }, seed);
}

async function waitForFontsAndPaint(page: Page) {
  await page.evaluate(async () => {
    const ready = (document as Document & { fonts?: FontFaceSet }).fonts?.ready;
    if (ready) {
      await ready;
    }
    await new Promise<void>((resolve) => requestAnimationFrame(() => requestAnimationFrame(() => resolve())));
  });
}

async function waitForScreenReady(page: Page, container: Locator, loadingSelectors: Locator[]) {
  for (const loading of loadingSelectors) {
    await expect(loading).toHaveCount(0);
  }
  await expect(page.locator(".startup-overlay.is-visible")).toHaveCount(0);
  await expect(container).toBeVisible();
  await waitForFontsAndPaint(page);
}

async function assertNoBoundaryLeaks(page: Page) {
  const text = (await page.locator("body").innerText()).replace(/\s+/g, " ");
  const forbidden = [
    "logic_family",
    "candidate_id",
    "comparison_snapshot",
    "published_logic_artifact",
    "validation_summary",
    "Top 3 candidate comparisons",
    "run_id",
    "research-only",
    "research only",
    "debug"
  ];

  for (const token of forbidden) {
    expect(text, `unexpected visible boundary leak: ${token}`).not.toContain(token);
  }
}

async function routeMeemeeApi(
  page: Page,
  options: { provisionalRanking: boolean }
) {
  const barsPayload = buildBatchBarsPayload();

  await page.route("**/api/**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const path = url.pathname;
    const method = request.method() as ApiMethod;

    if (path === "/api/health" && method === "GET") {
      return route.fulfill(responseJson(200, { ready: true, phase: "ready", message: "ok", status: "healthy" }));
    }

    if (path === "/api/health/live" && method === "GET") {
      return route.fulfill(responseJson(200, { ready: true, phase: "live", message: "ok", status: "healthy" }));
    }

    if (path === "/api/grid/screener" && method === "GET") {
      return route.fulfill(responseJson(200, buildCommonScreenerItems()));
    }

    if (path === "/api/watchlist" && method === "GET") {
      return route.fulfill(responseJson(200, { codes: [] }));
    }

    if (path === "/api/events/meta" && method === "GET") {
      return route.fulfill(
        responseJson(200, {
          earnings_last_success_at: EVENT_META_TIME,
          rights_last_success_at: EVENT_META_TIME,
          is_refreshing: false,
          refresh_job_id: null,
          last_error: null,
          last_attempt_at: EVENT_META_TIME,
          data_coverage: {
            rights_max_date: EVENT_RIGHTS_MAX_DATE
          }
        })
      );
    }

    if (path === "/api/events/refresh") {
      return route.fulfill(responseJson(200, { jobId: "events-refresh-vrt-1" }));
    }

    if (path === "/api/ai-explain/settings" && method === "GET") {
      return route.fulfill(
        responseJson(200, {
          settings: {
            uiVisible: false,
            enabled: false,
            providerLabel: "sakura",
            providerType: "openai_compatible",
            endpointUrl: "",
            model: "",
            credentialName: "sakura",
            sendImages: true,
            answerLength: "short",
            dailyLimit: 20,
            compareEnabled: false,
            debugEnabled: false
          },
          providerReady: true,
          credentialConfigured: false,
          canShowUi: false,
          canUse: false
        })
      );
    }

    if (path === "/api/rankings" && method === "GET") {
      return route.fulfill(responseJson(200, buildRankingPayload(options.provisionalRanking)));
    }

    if (path === "/api/rankings/trade-summary" && method === "GET") {
      return route.fulfill(responseJson(200, buildTradeSummary()));
    }

    if (path === "/api/positions/held" && method === "GET") {
      return route.fulfill(responseJson(200, buildPositionsHeldItems()));
    }

    if (path === "/api/positions/history" && method === "GET") {
      return route.fulfill(responseJson(200, buildPositionsHistoryItems()));
    }

    if (path === "/api/positions/history/events" && method === "GET") {
      return route.fulfill(responseJson(200, { events: [] }));
    }

    if (path === "/api/batch_bars_v3" && method === "POST") {
      const body = request.postDataJSON() as {
        codes?: string[];
        timeframes?: string[];
        limit?: number;
        timeframeLimits?: AnyRecord;
      };
      return route.fulfill(responseJson(200, barsPayload(body)));
    }

    throw new Error(`Unexpected API request: ${method} ${path}`);
  });
}

async function openRanking(page: Page, provisionalRanking: boolean) {
  await setMeeMeeTestState(page);
  await routeMeemeeApi(page, { provisionalRanking });
  await page.goto("/ranking");
  const grid = page.locator(".rank-grid");
  await waitForScreenReady(page, grid, [
    page.locator(".rank-skeleton"),
    page.locator(".rank-status", { hasText: "読み込み中" }),
    page.locator(".startup-overlay.is-visible")
  ]);
  await expect(grid.locator(".rank-tile")).toHaveCount(2);
  await expect(grid.locator("canvas")).toHaveCount(2);
  return grid;
}

async function openPositions(page: Page) {
  await setMeeMeeTestState(page);
  await routeMeemeeApi(page, { provisionalRanking: false });
  // NOTE: これは positions の browser VRT を一時的に安定化するための test-only seed。
  // 本番挙動を変えず、IndexedDB 上の chart cache を先に用意して canvas 描画を再現している。
  await seedPersistentChartFrame(page, {
    code: "7203",
    timeframe: "daily",
    limit: 259,
    includeBoxes: true,
    dataVersion: "vrt",
    bars: buildBaseBars()["7203"],
    boxes: []
  });
  await page.goto("/positions");
  const grid = page.locator(".rank-grid");
  await waitForScreenReady(page, grid, [
    page.locator(".rank-status", { hasText: "読み込み中" }),
    page.locator(".startup-overlay.is-visible")
  ]);
  await expect(grid.locator(".rank-tile")).toHaveCount(1);
  await expect(grid.locator("canvas")).toHaveCount(1);
  return grid;
}

test.describe("MeeMee boundary VRT", () => {
  test("ranking normal published display", async ({ page }) => {
    const grid = await openRanking(page, false);
    await assertNoBoundaryLeaks(page);
    await expect(grid).toHaveScreenshot("meemee-ranking-normal.png", {
      animations: "disabled",
      caret: "hide",
      scale: "css"
    });
  });

  test("ranking provisional display", async ({ page }) => {
    const grid = await openRanking(page, true);
    await assertNoBoundaryLeaks(page);
    await expect(grid).toHaveScreenshot("meemee-ranking-provisional.png", {
      animations: "disabled",
      caret: "hide",
      scale: "css",
      maxDiffPixels: 300
    });
  });

  test("positions normal display", async ({ page }) => {
    const grid = await openPositions(page);
    await assertNoBoundaryLeaks(page);
    await expect(grid).toHaveScreenshot("meemee-positions-normal.png", {
      animations: "disabled",
      caret: "hide",
      scale: "css",
      maxDiffPixels: 300
    });
  });
});
