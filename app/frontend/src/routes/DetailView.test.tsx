// @vitest-environment jsdom
import { act } from "react";
import { createRoot } from "react-dom/client";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

const mocks = vi.hoisted(() => {
  const baseMaSettings = [
    {
      key: "ma7",
      label: "MA7",
      period: 7,
      color: "#ff6b6b",
      visible: true,
      lineWidth: 1,
    },
  ];

  return {
    apiGet: vi.fn(),
    apiPost: vi.fn(),
    backendReadyRef: { value: false },
    storeState: {
      tickers: [
        {
          code: "7203",
          name: "Test Corp",
          close: 1000,
          chg1D: 0.01,
        },
      ],
      ensureListLoaded: vi.fn(async () => {}),
      loadingList: false,
      favorites: [],
      favoritesLoaded: true,
      loadFavorites: vi.fn(async () => {}),
      setFavoriteLocal: vi.fn(),
      settings: {
        showBoxes: true,
      },
      setShowBoxes: vi.fn(),
      maSettings: {
        daily: baseMaSettings,
        weekly: baseMaSettings,
        monthly: baseMaSettings,
      },
      compareMaSettings: {
        daily: baseMaSettings,
        weekly: baseMaSettings,
        monthly: baseMaSettings,
      },
      updateMaSetting: vi.fn(),
      updateCompareMaSetting: vi.fn(),
      resetMaSettings: vi.fn(),
      resetCompareMaSettings: vi.fn(),
    },
  };
});

function MockToast() {
  return null;
}

function MockIconButton(props: Record<string, unknown>) {
  const { ariaLabel, ...rest } = props as { ariaLabel?: string };
  return <button type="button" aria-label={ariaLabel} {...rest} />;
}

function MockDetailHeaderChrome(props: Record<string, any>) {
  return (
    <div data-testid="detail-header-chrome">
      {props.modeControls}
      {props.topbarActions}
      {props.children}
    </div>
  );
}

function MockDetailModeTabs(props: Record<string, any>) {
  return (
    <div data-testid="detail-mode-tabs">
      <button type="button" data-testid="chart-tab" onClick={props.onChart}>
        chart
      </button>
      <button type="button" data-testid="analysis-tab" onClick={props.onAnalysis}>
        analysis
      </button>
      <button type="button" data-testid="financial-tab" onClick={props.onFinancial}>
        financial
      </button>
      <button type="button" data-testid="positions-tab" onClick={props.onPositions}>
        positions
      </button>
    </div>
  );
}

function MockDetailTimeframeSwitcher() {
  return null;
}

function MockDetailDrawToolbar() {
  return null;
}

function MockScreenPanel(props: Record<string, any>) {
  return <div>{props.children}</div>;
}

function MockSimilarSearchPanel() {
  return null;
}

function MockAiExplainDock() {
  return null;
}

function MockDailyMemoPanel() {
  return null;
}

function MockDetailFinancialPanel() {
  return <div data-testid="financial-panel">financial-panel</div>;
}

function MockDetailAnalysisPanel() {
  return <div data-testid="legacy-analysis-panel">legacy-analysis-panel</div>;
}

function MockTradexAnalysisMount() {
  return <div data-testid="tradex-analysis-panel">tradex-analysis-panel</div>;
}

function MockDetailTdnetCard() {
  return null;
}

function MockDetailDebugBanner() {
  return null;
}

function MockDetailIndicatorOverlay() {
  return null;
}

function MockDetailPositionLedgerSheet() {
  return null;
}

vi.mock("../api", () => ({
  api: {
    get: mocks.apiGet,
    post: mocks.apiPost,
  },
}));

vi.mock("../backendReady", () => ({
  useBackendReadyState: () => ({
    ready: mocks.backendReadyRef.value,
    backendAlive: mocks.backendReadyRef.value,
    backendReady: mocks.backendReadyRef.value,
    dbBusy: false,
    phase: mocks.backendReadyRef.value ? "ready" : "starting",
    message: mocks.backendReadyRef.value ? "準備完了" : "バックエンド起動待ち",
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

vi.mock("../components/DetailChart", async () => {
  const React = await import("react");
  return {
    __esModule: true,
    default: React.forwardRef(function MockDetailChart() {
      return React.createElement("div", { "data-testid": "detail-chart" });
    }),
    DetailChartHandle: class DetailChartHandle {},
  };
});

vi.mock("../components/Toast", () => ({
  __esModule: true,
  default: MockToast,
}));

vi.mock("../components/IconButton", () => ({
  __esModule: true,
  default: MockIconButton,
}));

vi.mock("../components/DetailHeaderChrome", () => ({
  __esModule: true,
  default: MockDetailHeaderChrome,
}));

vi.mock("../components/DetailModeTabs", () => ({
  __esModule: true,
  default: MockDetailModeTabs,
}));

vi.mock("../components/DetailTimeframeSwitcher", () => ({
  __esModule: true,
  default: MockDetailTimeframeSwitcher,
}));

vi.mock("../components/DetailDrawToolbar", () => ({
  __esModule: true,
  default: MockDetailDrawToolbar,
}));

vi.mock("../components/ScreenPanel", () => ({
  __esModule: true,
  default: MockScreenPanel,
}));

vi.mock("../components/SimilarSearchPanel", () => ({
  __esModule: true,
  default: MockSimilarSearchPanel,
}));

vi.mock("../features/aiExplain/AiExplainDock", () => ({
  __esModule: true,
  default: MockAiExplainDock,
}));

vi.mock("../components/DailyMemoPanel", () => ({
  __esModule: true,
  default: MockDailyMemoPanel,
}));

vi.mock("./detail/DetailFinancialPanel", () => ({
  DetailFinancialPanel: MockDetailFinancialPanel,
}));

vi.mock("./detail/DetailAnalysisPanel", () => ({
  DetailAnalysisPanel: MockDetailAnalysisPanel,
}));

vi.mock("./detail/DetailTdnetCard", () => ({
  DetailTdnetCard: MockDetailTdnetCard,
}));

vi.mock("./detail/TradexAnalysisMount", () => ({
  __esModule: true,
  default: MockTradexAnalysisMount,
}));

vi.mock("./detail/components/DetailDebugBanner", () => ({
  __esModule: true,
  default: MockDetailDebugBanner,
}));

vi.mock("./detail/components/DetailIndicatorOverlay", () => ({
  __esModule: true,
  default: MockDetailIndicatorOverlay,
}));

vi.mock("./detail/components/DetailPositionLedgerSheet", () => ({
  __esModule: true,
  default: MockDetailPositionLedgerSheet,
}));

vi.mock("../hooks/useChartSync", () => ({
  useChartSync: () => ({
    hoverTime: null,
    setHoverTime: () => undefined,
    handleDailyVisibleRangeChange: () => undefined,
    handleMonthlyVisibleRangeChange: () => undefined,
    handleWeeklyVisibleRangeChange: () => undefined,
    handleDailyCrosshair: () => undefined,
    handleMonthlyCrosshair: () => undefined,
    handleWeeklyCrosshair: () => undefined,
  }),
}));

vi.mock("../hooks/useDetailInfo", () => ({
  useDetailInfo: () => null,
}));

vi.mock("./detail/hooks/useExactDecisionRange", () => ({
  useExactDecisionRange: () => ({
    items: [],
  }),
}));

vi.mock("./detail/hooks/useAsOfItemFetch", () => ({
  useAsOfItemFetch: () => ({
    item: null,
    loading: false,
  }),
}));

vi.mock("./detail/hooks/useDetailDrawings", () => ({
  useDetailDrawings: () => ({
    dailyDrawingKey: null,
    weeklyDrawingKey: null,
    monthlyDrawingKey: null,
    compareDailyDrawingKey: null,
    compareMonthlyDrawingKey: null,
    dailyDrawings: { timeZones: [], priceBands: [], drawBoxes: [], horizontalLines: [] },
    weeklyDrawings: { timeZones: [], priceBands: [], drawBoxes: [], horizontalLines: [] },
    monthlyDrawings: { timeZones: [], priceBands: [], drawBoxes: [], horizontalLines: [] },
    compareDailyDrawings: { timeZones: [], priceBands: [], drawBoxes: [], horizontalLines: [] },
    compareMonthlyDrawings: { timeZones: [], priceBands: [], drawBoxes: [], horizontalLines: [] },
    addTimeZone: () => () => undefined,
    updateTimeZone: () => () => undefined,
    addPriceBand: () => () => undefined,
    updatePriceBand: () => () => undefined,
    addDrawBox: () => () => undefined,
    updateDrawBox: () => () => undefined,
    addHorizontalLine: () => () => undefined,
    updateHorizontalLine: () => () => undefined,
    deleteTimeZone: () => () => undefined,
    deletePriceBand: () => () => undefined,
    deleteDrawBox: () => () => undefined,
    deleteHorizontalLine: () => () => undefined,
    resetAllDrawings: () => undefined,
  }),
}));

import DetailView from "./DetailView";

const flushMicrotasks = async () => {
  await Promise.resolve();
  await Promise.resolve();
};

const renderDetailView = async () => {
  const container = document.createElement("div");
  document.body.appendChild(container);
  const root = createRoot(container);
  await act(async () => {
    root.render(
      <MemoryRouter initialEntries={["/detail/7203"]}>
        <Routes>
          <Route path="/detail/:code" element={<DetailView />} />
        </Routes>
      </MemoryRouter>
    );
  });
  return { container, root };
};

describe("DetailView", () => {
  beforeEach(() => {
    mocks.backendReadyRef.value = false;
    mocks.apiGet.mockReset();
    mocks.apiPost.mockReset();
    mocks.storeState.ensureListLoaded.mockClear();
    mocks.storeState.loadFavorites.mockClear();
    mocks.storeState.setFavoriteLocal.mockClear();
    mocks.storeState.setShowBoxes.mockClear();
    mocks.storeState.updateMaSetting.mockClear();
    mocks.storeState.updateCompareMaSetting.mockClear();
    mocks.storeState.resetMaSettings.mockClear();
    mocks.storeState.resetCompareMaSettings.mockClear();
    window.localStorage.clear();
    window.sessionStorage.clear();
    if (!window.requestAnimationFrame) {
      Object.assign(window, {
        requestAnimationFrame: (callback: FrameRequestCallback) => window.setTimeout(() => callback(0), 0),
        cancelAnimationFrame: (handle: number) => window.clearTimeout(handle),
      });
    }
  });

  afterEach(() => {
    document.body.innerHTML = "";
  });

  it("renders the detail route without throwing when the EDINET request is null", async () => {
    const { container, root } = await renderDetailView();

    expect(container.querySelector("[data-testid='detail-header-chrome']")).not.toBeNull();
    expect(mocks.apiPost).not.toHaveBeenCalledWith("/jobs/edinet/official-backfill", expect.anything(), expect.anything());

    act(() => {
      root.unmount();
    });
    container.remove();
  });

  it("stays mounted after switching to financial mode and requesting official EDINET backfill", async () => {
    mocks.backendReadyRef.value = true;
    mocks.apiGet.mockImplementation((url: string) => {
      if (url === "/ticker/edinet/financials") {
        return Promise.resolve({
          data: {
            item: {
              status: "ok",
              statusDetail: null,
              mapped: true,
              fetchedAt: "2026-03-29T09:00:00.000Z",
              lastCheckedAt: "2026-03-29T09:00:00.000Z",
              bootstrapState: null,
              summary: null,
              series: [],
              analysisSummary: null,
              textHighlights: [],
              officialFilings: [],
            },
          },
        });
      }
      if (url === "/ticker/taisyaku/snapshot") {
        return Promise.resolve({ data: { item: null } });
      }
      if (url === "/ticker/tdnet/disclosures") {
        return Promise.resolve({ data: { items: [] } });
      }
      if (url.startsWith("/jobs/")) {
        return Promise.resolve({ data: { status: "done" } });
      }
      return Promise.resolve({ data: {} });
    });
    mocks.apiPost.mockImplementation((url: string) => {
      if (url === "/jobs/edinet/official-backfill") {
        return Promise.resolve({ data: { job_id: "job-123" } });
      }
      if (url === "/batch_bars_v3") {
        return Promise.resolve({ data: { items: {} } });
      }
      return Promise.resolve({ data: {} });
    });

    const { container, root } = await renderDetailView();
    const financialTab = container.querySelector("[data-testid='financial-tab']");
    expect(financialTab).not.toBeNull();

    await act(async () => {
      financialTab?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
      await flushMicrotasks();
    });

    expect(mocks.apiGet).toHaveBeenCalledWith("/ticker/edinet/financials", { params: { code: "7203" } });
    expect(mocks.apiPost).toHaveBeenCalledWith(
      "/jobs/edinet/official-backfill",
      null,
      expect.objectContaining({
        params: { code: "7203" },
      })
    );
    expect(container.querySelector("[data-testid='detail-header-chrome']")).not.toBeNull();

    act(() => {
      root.unmount();
    });
    container.remove();
  });

  it("renders both TRADEX and legacy analysis panels on analysis mode", async () => {
    const { container, root } = await renderDetailView();
    const analysisTab = container.querySelector("[data-testid='analysis-tab']");
    expect(analysisTab).not.toBeNull();

    await act(async () => {
      analysisTab?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
      await flushMicrotasks();
    });

    expect(container.querySelector("[data-testid='tradex-analysis-panel']")).not.toBeNull();
    expect(container.querySelector("[data-testid='legacy-analysis-panel']")).not.toBeNull();

    act(() => {
      root.unmount();
    });
    container.remove();
  });
});
