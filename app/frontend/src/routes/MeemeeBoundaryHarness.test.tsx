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

describe("MeeMee boundary harness", () => {
  beforeEach(() => {
    resetMocks();
    window.localStorage.clear();
    window.sessionStorage.clear();
    mocks.apiGet.mockImplementation((url: string) => {
      if (url === "/rankings") {
        return Promise.resolve({
          data: {
            items: [
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
              {
                code: "7203",
                name: "Toyota",
                asOf: "2026-03-19",
                score: 0.77,
                changePct: -0.01,
                changeAbs: -25,
                setupType: "pullback",
                reason: "confirmed mean reversion",
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
            buy: { count: 1, avg_trade_priority_score: 0.9, avg_profit_expectancy: 0.1, avg_hit_score: 0.2 },
            sell: { count: 0, avg_trade_priority_score: null, avg_profit_expectancy: null, avg_hit_score: null },
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

    expect(render.container.textContent).toContain("1301");
    expect(render.container.textContent).toContain("MeeMee Electric");
    expect(render.container.textContent).toContain("7203");
    expect(render.container.querySelectorAll("[data-testid='chart-card']")).toHaveLength(2);

    render.cleanup();
  });

  it("renders the positions shell with a held position", async () => {
    const render = await renderPositionsRoute();
    await flush();

    expect(render.container.textContent).toContain("7203");
    expect(render.container.textContent).toContain("Toyota");
    expect(render.container.textContent).toContain("売-買");
    expect(render.container.querySelectorAll("[data-testid='chart-card']")).toHaveLength(1);

    render.cleanup();
  });
});
