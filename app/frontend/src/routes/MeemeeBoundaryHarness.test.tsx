// @vitest-environment jsdom
import { act } from "react";
import type { ReactNode } from "react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { renderClient } from "../test/renderClient";

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

const mocks = vi.hoisted(() => ({
  apiGet: vi.fn(),
  apiPost: vi.fn(),
  apiDelete: vi.fn(),
  backendReadyRef: { value: true },
  storeState: {
    tickers: [
      { code: "1301", name: "MeeMee Electric", close: 1000, chg1D: 0.03 },
      { code: "7203", name: "Toyota", close: 2500, chg1D: -0.01 }
    ],
    barsCache: {
      daily: {},
      weekly: {},
      monthly: {}
    },
    barsStatus: {
      daily: {},
      weekly: {},
      monthly: {}
    },
    boxesCache: {
      daily: {},
      weekly: {},
      monthly: {}
    },
    maSettings: {
      daily: [],
      weekly: [],
      monthly: []
    },
    compareMaSettings: {
      daily: [],
      weekly: [],
      monthly: []
    },
    settings: {
      listTimeframe: "daily",
      listRangeBars: 120,
      columns: 3,
      rows: 3,
      showBoxes: false
    },
    eventsMeta: {
      isRefreshing: false,
      lastError: null,
      earningsLastSuccessAt: null,
      rightsLastSuccessAt: null,
      dataCoverage: { rightsMaxDate: null }
    },
    ensureListLoaded: vi.fn(async () => {}),
    ensureBarsForVisible: vi.fn(async () => {}),
    setListTimeframe: vi.fn(),
    setListRangeBars: vi.fn(),
    setColumns: vi.fn(),
    setRows: vi.fn(),
    setFavoriteLocal: vi.fn(),
    refreshEvents: vi.fn(),
  }
}));

function MockChrome(props: Record<string, unknown>) {
  return (
    <div data-testid="chrome">
      {props.children}
    </div>
  );
}

function MockHeader(props: Record<string, unknown>) {
  return (
    <div data-testid="list-header">
      {props.topRowLeftExtra}
      {props.children}
    </div>
  );
}

function MockChartListCard(props: Record<string, unknown>) {
  return (
    <section data-testid="chart-card">
      <div data-testid="chart-card-title">
        <span>{String(props.code ?? "")}</span>
        <span>{String(props.name ?? "")}</span>
      </div>
      <div data-testid="chart-card-body">
        {props.headerLeft}
        {props.headerRight}
        {props.annotation}
      </div>
    </section>
  );
}

function MockToast() {
  return null;
}

function MockConsultSummaryMount(props: Record<string, unknown>) {
  return (
    <>
      {typeof props.children === "function"
        ? props.children({
            loading: false,
            available: true,
            reason: null,
            scope: props.scope ?? null,
            itemsByKey: {},
          })
        : props.children}
    </>
  );
}

function MockPatternBadges() {
  return null;
}

function MockAiExplainDock() {
  return null;
}

function MockUseConsultScreenshot() {
  return {
    generateScreenshots: vi.fn(async () => ({ success: true, count: 1 })),
    isProcessing: false,
  };
}

vi.mock("../api", () => ({
  api: {
    get: mocks.apiGet,
    post: mocks.apiPost,
    delete: mocks.apiDelete,
  },
}));

vi.mock("../backendReady", () => ({
  useBackendReadyState: () => ({
    ready: mocks.backendReadyRef.value,
    backendAlive: mocks.backendReadyRef.value,
    backendReady: mocks.backendReadyRef.value,
    dbBusy: false,
    phase: mocks.backendReadyRef.value ? "ready" : "starting",
    message: mocks.backendReadyRef.value ? "ready" : "starting",
    error: null,
    errorDetails: null,
    attemptCount: 0,
    elapsedMs: 0,
    retry: () => undefined,
  }),
}));

vi.mock("../store", () => ({
  Box: class Box {},
  MaSetting: class MaSetting {},
  useStore: (selector: (state: typeof mocks.storeState) => unknown) => selector(mocks.storeState),
}));

vi.mock("../components/UnifiedListHeader", () => ({
  __esModule: true,
  default: MockHeader,
}));

vi.mock("../components/ChartListCard", () => ({
  __esModule: true,
  default: MockChartListCard,
}));

vi.mock("../components/Toast", () => ({
  __esModule: true,
  default: MockToast,
}));

vi.mock("../components/TradexListSummary", () => ({
  __esModule: true,
  default: () => null,
}));

vi.mock("../components/DetailHeaderChrome", () => ({
  __esModule: true,
  default: MockChrome,
}));

vi.mock("../components/DetailModeTabs", () => ({
  __esModule: true,
  default: () => null,
}));

vi.mock("../components/DetailTimeframeSwitcher", () => ({
  __esModule: true,
  default: () => null,
}));

vi.mock("../components/DetailDrawToolbar", () => ({
  __esModule: true,
  default: () => null,
}));

vi.mock("../components/ScreenPanel", () => ({
  __esModule: true,
  default: ({ children }: { children?: ReactNode }) => <>{children}</>,
}));

vi.mock("../components/SimilarSearchPanel", () => ({
  __esModule: true,
  default: () => null,
}));

vi.mock("../features/aiExplain/AiExplainDock", () => ({
  __esModule: true,
  default: MockAiExplainDock,
}));

vi.mock("../hooks/useConsultScreenshot", () => ({
  useConsultScreenshot: MockUseConsultScreenshot,
}));

vi.mock("../utils/consultation", () => ({
  buildConsultationPack: () => ({ text: "相談パック", omittedCount: 0 }),
}));

vi.mock("../features/aiExplain/aiExplainImages", () => ({
  buildAiExplainImages: () => ({})
}));

vi.mock("../routes/detail/openDetailWithPrefetch", () => ({
  openDetailWithPrefetch: async () => undefined,
}));

vi.mock("./list/TradexListSummaryMount", () => ({
  TradexListSummaryMount: MockConsultSummaryMount,
}));

vi.mock("./list/ResearchPatternBadges", () => ({
  ResearchPatternBadges: MockPatternBadges,
}));

vi.mock("../perfDiagnostics", () => ({
  recordPerfEvent: () => undefined,
}));

import RankingView from "./RankingView";
import PositionsView from "./PositionsView";

function resetMocks() {
  mocks.apiGet.mockReset();
  mocks.apiPost.mockReset();
  mocks.apiDelete.mockReset();
  mocks.storeState.ensureListLoaded.mockClear();
  mocks.storeState.ensureBarsForVisible.mockClear();
  mocks.storeState.setListTimeframe.mockClear();
  mocks.storeState.setListRangeBars.mockClear();
  mocks.storeState.setColumns.mockClear();
  mocks.storeState.setRows.mockClear();
  mocks.storeState.setFavoriteLocal.mockClear();
  mocks.storeState.refreshEvents.mockClear();
  mocks.backendReadyRef.value = true;
}

function renderRankingRoute() {
  return renderClient(
    <MemoryRouter initialEntries={["/ranking"]}>
      <Routes>
        <Route path="/ranking" element={<RankingView />} />
      </Routes>
    </MemoryRouter>
  );
}

function renderPositionsRoute() {
  return renderClient(
    <MemoryRouter initialEntries={["/positions"]}>
      <Routes>
        <Route path="/positions" element={<PositionsView />} />
      </Routes>
    </MemoryRouter>
  );
}

async function flush() {
  await act(async () => {
    await Promise.resolve();
    await Promise.resolve();
  });
}

async function waitForText(container: HTMLElement, text: string, timeoutMs = 2500) {
  const start = Date.now();
  while (!container.textContent?.includes(text)) {
    if (Date.now() - start > timeoutMs) {
      throw new Error(`Timed out waiting for text: ${text}`);
    }
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 50));
    });
  }
}

describe("MeeMee boundary harness", () => {
  beforeEach(() => {
    resetMocks();
    window.localStorage.clear();
    window.sessionStorage.clear();
    mocks.apiGet.mockImplementation((url: string, config?: { params?: Record<string, unknown> }) => {
      const isDownRanking = config?.params?.dir === "down";
      if (url === "/system/runtime-selection") {
        return Promise.resolve({
          data: {
            shadow_integration_available: true,
            shadow_only: true,
            shadow_integration_state: {
              acceptance_state: "accepted_for_shadow_integration_only",
              adoption_readiness: "shadow_only",
              compare_method: "boundary_local_rerank",
              outside_top20_locked: true,
              shadow_only: true,
            },
            shadow_rollout_boundary: {
              outside_top20_locked: true,
              shadow_only: true,
            },
          },
        });
      }
      if (url === "/rankings") {
        return Promise.resolve({
          data: {
            freshness_state: "stale",
            freshness_days: 33,
            snapshot_as_of: "2026-03-19",
            current_candidate_available: false,
            stale: true,
            actionable_buy_candidates: [
              ...(isDownRanking ? [] : [{
                code: "1301",
                name: "MeeMee Electric",
                asOf: "2026-03-19",
                score: 0.91,
                changePct: 0.03,
                changeAbs: 30,
                setupType: "breakout",
                reason: "confirmed momentum",
                is_favorite: false,
              }]),
            ],
            actionable_short_candidates: isDownRanking
              ? [
                  {
                    code: "2269",
                    name: "Meiji",
                    asOf: "2026-03-19",
                    score: 0.82,
                    changePct: -0.02,
                    changeAbs: -50,
                    setupType: "failed_high_retest",
                    reason: "confirmed short",
                    is_favorite: false,
                  },
                ]
              : [],
            caution_watch_candidates: isDownRanking ? [] : [
              {
                code: "7203",
                name: "Toyota",
                asOf: "2026-03-19",
                score: 0.77,
                changePct: -0.01,
                changeAbs: -25,
                setupType: "watch",
                reason: "confirmed caution",
                is_favorite: false,
              },
            ],
            items: isDownRanking ? [
              {
                code: "2269",
                name: "Meiji",
                asOf: "2026-03-19",
                score: 0.82,
                changePct: -0.02,
                changeAbs: -50,
                setupType: "failed_high_retest",
                reason: "confirmed short",
                is_favorite: false,
              },
            ] : [
              {
                code: "1301",
                name: "MeeMee Electric",
                asOf: "2026-03-19",
                score: 0.91,
                changePct: 0.03,
                changeAbs: 30,
                setupType: "breakout",
                reason: "confirmed momentum",
                is_favorite: false,
              },
            ],
          },
        });
      }
      if (url === "/rankings/trade-summary") {
        return Promise.resolve({
          data: {
            dominant_direction: "up",
            difference_score: 0.2,
            actionable_buy: { count: 1, avg_trade_priority_score: 0.9, avg_profit_expectancy: 0.1, avg_hit_score: 0.2 },
            actionable_short: { count: 0, avg_trade_priority_score: null, avg_profit_expectancy: null, avg_hit_score: null },
            caution_watch: { count: 1, top_n: 5, top_codes: ["7203"] },
            buy: { count: 1, avg_trade_priority_score: 0.9, avg_profit_expectancy: 0.1, avg_hit_score: 0.2 },
            sell: { count: 0, avg_trade_priority_score: null, avg_profit_expectancy: null, avg_hit_score: null },
          },
        });
      }
      if (url === "/rankings/session") {
        return Promise.resolve({
          data: {
            confirmed_snapshot_as_of: "2026-03-19",
            provisional_snapshot_as_of: "2026-04-21",
            provisional_source: "yahoo_intraday_unconfirmed_source",
            provisional_freshness_state: "partial",
            provisional_fetched_at: "2026-04-21T12:34:56+09:00",
            is_provisional: true,
            provisional_requested_symbols: 2,
            provisional_covered_symbols: 1,
            provisional_complete_ohlcv_symbols: 1,
            provisional_same_day_symbols: 1,
            provisional_missing_symbols: 1,
            provisional_missing_reason_summary: { fetch_none: 1 },
            provisional_coverage_ratio: 0.5,
            provisional_same_day_ratio: 0.5,
            provisional_min_coverage_ratio: 0.95,
            provisional_min_same_day_ratio: 0.95,
            provisional_allow_partial: true,
            provisional_render_mode: "practical_partial",
            confirmed_actionable_buy_candidates: [
              {
                code: "1301",
                name: "MeeMee Electric",
                asOf: "2026-03-19",
                score: 0.91,
                changePct: 0.03,
                changeAbs: 30,
                setupType: "breakout",
                reason: "confirmed momentum",
                is_favorite: false,
              },
            ],
            confirmed_actionable_short_candidates: [],
            confirmed_caution_watch_candidates: [
              {
                code: "7203",
                name: "Toyota",
                asOf: "2026-03-19",
                score: 0.77,
                changePct: -0.01,
                changeAbs: -25,
                setupType: "watch",
                reason: "confirmed caution",
                is_favorite: false,
              },
            ],
            provisional_actionable_buy_candidates: [
              {
                code: "1301",
                name: "MeeMee Electric",
                asOf: "2026-04-21",
                score: 0.95,
                changePct: 0.04,
                changeAbs: 40,
                setupType: "breakout",
                reason: "provisional momentum",
                is_favorite: false,
                is_provisional: true,
                confirmed_rank: 1,
                provisional_rank: 1,
                rank_delta: 0,
              },
            ],
            provisional_actionable_short_candidates: [],
            provisional_caution_watch_candidates: [
              {
                code: "7203",
                name: "Toyota",
                asOf: "2026-04-21",
                score: 0.77,
                changePct: -0.01,
                changeAbs: -25,
                setupType: "watch",
                reason: "provisional caution",
                is_favorite: false,
                is_provisional: true,
                confirmed_rank: null,
                provisional_rank: 1,
                rank_delta: null,
              },
            ],
            confirmed_trade_summary: {
              dominant_direction: "up",
              difference_score: 0.2,
              actionable_buy: { count: 1, avg_trade_priority_score: 0.9, avg_profit_expectancy: 0.1, avg_hit_score: 0.2 },
              actionable_short: { count: 0, avg_trade_priority_score: null, avg_profit_expectancy: null, avg_hit_score: null },
              caution_watch: { count: 1, top_n: 5, top_codes: ["7203"] },
            },
            provisional_trade_summary: {
              dominant_direction: "up",
              difference_score: 0.15,
              actionable_buy: { count: 1, avg_trade_priority_score: 0.95, avg_profit_expectancy: 0.12, avg_hit_score: 0.25 },
              actionable_short: { count: 0, avg_trade_priority_score: null, avg_profit_expectancy: null, avg_hit_score: null },
              caution_watch: { count: 1, top_n: 5, top_codes: ["7203"] },
            },
            confirmed_items: [
              {
                code: "1301",
                name: "MeeMee Electric",
                asOf: "2026-03-19",
                score: 0.91,
                changePct: 0.03,
                changeAbs: 30,
                setupType: "breakout",
                reason: "confirmed momentum",
                is_favorite: false,
              },
            ],
            provisional_items: [
              {
                code: "1301",
                name: "MeeMee Electric",
                asOf: "2026-04-21",
                score: 0.95,
                changePct: 0.04,
                changeAbs: 40,
                setupType: "breakout",
                reason: "provisional momentum",
                is_favorite: false,
                is_provisional: true,
                confirmed_rank: 1,
                provisional_rank: 1,
                rank_delta: 0,
              },
            ],
          },
        });
      }
      if (url === "/positions/held") {
        return Promise.resolve({
          data: {
            items: [
              {
                symbol: "7203",
                name: "Toyota",
                sell_buy_text: "売-買",
                opened_at: "2026-03-19",
                has_issue: false,
                buy_qty: 100,
                sell_qty: 0,
                spot_qty: 0,
                margin_long_qty: 100,
                margin_short_qty: 0,
              },
            ],
          },
        });
      }
      if (url === "/positions/history") {
        return Promise.resolve({ data: { items: [] } });
      }
      if (url === "/positions/history/events") {
        return Promise.resolve({ data: { events: [] } });
      }
      return Promise.resolve({ data: {} });
    });
  });

  afterEach(() => {
    document.body.innerHTML = "";
  });

  it("renders the ranking shell with confirmed-only rows", async () => {
    const render = await renderRankingRoute();
    await flush();
    await waitForText(render.container, "注意/監視 1件");
    await waitForText(render.container, "市場データ更新", 12000);

    expect(render.container.textContent).toContain("1301");
    expect(render.container.textContent).toContain("MeeMee Electric");
    expect(render.container.textContent).toContain("注意/監視 1件");
    expect(render.container.textContent).toContain("短期売り 0件");
    expect(render.container.textContent).toContain("検証表示: 有効");
    expect(render.container.textContent).toContain("当日反映");
    expect(render.container.textContent).toContain("市場データ更新");
    expect(render.container.textContent).toContain("当日データ 一部反映");
    expect(render.container.textContent).toContain("当日データ取得 1/2");
    expect(render.container.textContent).toContain("未取得 1");
    expect(render.container.textContent).toContain("当日一致 1/1");
    expect(render.container.textContent).toContain("当日候補 1件");
    expect(render.container.querySelectorAll("[data-testid='chart-card']")).toHaveLength(3);

    render.cleanup();
  }, 15000);

  it("renders actionable short candidates as enter-now sell chips", async () => {
    window.sessionStorage.setItem("rankingViewState", JSON.stringify({
      stateVersion: 6,
      dir: "down",
      filterSignalsOnly: false,
      filterDataOnly: false,
      filterBuySignalsOnly: false,
      filterSellSignalsOnly: false,
    }));

    const render = await renderRankingRoute();
    await flush();

    await waitForText(render.container, "\u4eca\u5165\u308b \u58f2\u308a", 12000);

    expect(render.container.textContent).toContain("2269");
    expect(render.container.textContent).toContain("\u4eca\u5165\u308b \u58f2\u308a");

    render.cleanup();
  }, 15000);

  it("renders the positions shell with a held position", async () => {
    const render = await renderPositionsRoute();
    await flush();

    expect(render.container.textContent).toContain("7203");
    expect(render.container.textContent).toContain("Toyota");
    expect(render.container.textContent).toContain("0-100");
    expect(render.container.querySelectorAll("[data-testid='chart-card']")).toHaveLength(1);

    render.cleanup();
  });
});
