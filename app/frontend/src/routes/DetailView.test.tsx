// @vitest-environment jsdom
import { act } from "react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { installCanvasMock, type CanvasMockHandle } from "../test/canvasMock";
import { renderClient, type RenderClientHandle } from "../test/renderClient";

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
    aiExplainDockProps: [] as Array<Record<string, unknown>>,
    detailDebugBannerProps: [] as Array<Record<string, unknown>>,
    storeState: {
      tickers: [
        {
          code: "7203",
          name: "Test Corp",
          close: 1000,
          chg1D: 0.01,
        },
        {
          code: "6758",
          name: "Next Corp",
          close: 2000,
          chg1D: -0.02,
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
        showBattleZones: true,
      },
      setShowBoxes: vi.fn(),
      setShowBattleZones: vi.fn(),
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
      {props.summaryBack}
      {props.summaryMain}
      {props.summaryStatus}
      {props.summaryCenter}
      {props.summaryActions}
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

function MockAiExplainDock(props: Record<string, any>) {
  mocks.aiExplainDockProps.push(props);
  return <div data-testid="ai-explain-dock" data-inline={props.inline ? "1" : ""} />;
}

function MockDailyMemoPanel() {
  return null;
}

function MockDetailFinancialPanel() {
  return <div data-testid="financial-panel">financial-panel</div>;
}

function MockTradexAnalysisMount() {
  return <div data-testid="tradex-analysis-panel">tradex-analysis-panel</div>;
}

function MockDetailTdnetCard() {
  return null;
}

function MockDetailDebugBanner(props: Record<string, any>) {
  mocks.detailDebugBannerProps.push(props);
  if (!props.hasIssues) return null;
  return <div data-testid="detail-debug-banner" data-inline={props.inline ? "1" : ""} />;
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
      default: React.forwardRef(function MockDetailChart(props: Record<string, unknown>, _ref) {
        const candles = Array.isArray(props.candles) ? props.candles.length : 0;
        const visibleRange = props.visibleRange as { from?: number; to?: number } | undefined;
        const lastCandleTime = Array.isArray(props.candles) && props.candles.length > 0
          ? (props.candles[props.candles.length - 1] as { time?: number } | undefined)?.time ?? null
          : null;
        const positionOverlay = props.positionOverlay as
          | { dailyPositions?: Array<{ posText?: string }>; tradeMarkers?: unknown[] }
          | undefined;
        return React.createElement("div", {
          "data-testid": "detail-chart",
          "data-candles": candles,
          "data-price-bands": Array.isArray(props.priceBands) ? props.priceBands.length : 0,
          "data-horizontal-lines": Array.isArray(props.horizontalLines) ? props.horizontalLines.length : 0,
          "data-event-markers": Array.isArray(props.eventMarkers) ? props.eventMarkers.length : 0,
          "data-visible-from": visibleRange?.from ?? "",
          "data-visible-to": visibleRange?.to ?? "",
          "data-last-time": lastCandleTime ?? "",
          "data-trade-markers": positionOverlay?.tradeMarkers?.length ?? 0,
          "data-position-text": positionOverlay?.dailyPositions?.[0]?.posText ?? "",
          "data-detail-chrome": props.meeMeeDetailChrome ? "on" : "",
          "data-detail-chrome-timeframe": props.meeMeeDetailChrome?.timeframe ?? "",
        });
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

import DetailView, { parseResearchReviewBand, parseResearchReviewMarkers, resolveCursorPannedRange } from "./DetailView";

describe("parseResearchReviewMarkers", () => {
  it("accepts only bounded research marker payloads", () => {
    expect(parseResearchReviewMarkers(JSON.stringify([
      { date: "2025-01-06", label: "base", kind: "research-neutral" },
      { date: "bad", label: "ignored", kind: "research-up" },
      { date: "2025-01-07", label: "ignored", kind: "decision-buy" },
    ]))).toEqual([{ date: "2025-01-06", label: "base", kind: "research-neutral" }]);
    expect(parseResearchReviewMarkers("not-json")).toEqual([]);
  });
});

describe("parseResearchReviewBand", () => {
  it("accepts one bounded positive price band", () => {
    expect(parseResearchReviewBand(JSON.stringify({ startDate: "2025-01-01", endDate: "2025-01-31", lower: 980, upper: 1020 })))
      .toEqual({ startDate: "2025-01-01", endDate: "2025-01-31", lower: 980, upper: 1020 });
    expect(parseResearchReviewBand(JSON.stringify({ startDate: "2025-01-01", endDate: "2025-01-31", lower: 1020, upper: 980 }))).toBeNull();
  });
});

const flushMicrotasks = async () => {
  await Promise.resolve();
  await Promise.resolve();
};

const waitForSelector = async (container: HTMLElement, selector: string, attempts = 10) => {
  for (let index = 0; index < attempts; index += 1) {
    const found = container.querySelector(selector);
    if (found) return found;
    await act(async () => {
      await flushMicrotasks();
      await new Promise((resolve) => setTimeout(resolve, 0));
    });
  }
  return container.querySelector(selector);
};

const createDeferred = <T,>() => {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
};

const createTimeoutError = (message = "timeout of 15000ms exceeded") => {
  const error = new Error(message) as Error & { code?: string };
  error.code = "ECONNABORTED";
  return error;
};

const createChartDbBusyError = () => {
  return Object.assign(new Error("Request failed with status code 503"), {
    response: {
      status: 503,
      data: {
        detail: {
          error: "chart_db_busy",
          message: "database is temporarily busy",
        },
      },
    },
  });
};

const createBarsFrame = (time: number, base: number) => ({
  bars: [[time, base, base + 2, base - 2, base + 1, base * 10]],
});

const createBarsResponse = (code: string, seed = 1000) => ({
  data: {
    items: {
      [code]: {
        daily: createBarsFrame(1_710_000_000, seed),
        weekly: createBarsFrame(1_709_395_200, seed + 10),
        monthly: {
          ...createBarsFrame(1_707_264_000, seed + 20),
          boxes: [],
        },
      },
    },
  },
});

const createDatedBarsResponse = (code: string, seed = 1000) => {
  const dailyBars = Array.from({ length: 15 }, (_, index) => {
    const time = Date.UTC(2025, 5 + index, 14) / 1000;
    const base = seed + index;
    return [time, base, base + 2, base - 2, base + 1, base * 10];
  });
  const monthlyBars = Array.from({ length: 15 }, (_, index) => {
    const time = Date.UTC(2025, 5 + index, 1) / 1000;
    const base = seed + 100 + index;
    return [time, base, base + 2, base - 2, base + 1, base * 10];
  });
  return {
    data: {
      items: {
        [code]: {
          daily: { bars: dailyBars },
          weekly: createBarsFrame(Date.UTC(2026, 4, 11) / 1000, seed + 10),
          monthly: {
            bars: monthlyBars,
            boxes: [],
          },
        },
      },
    },
  };
};

const createAsOfMismatchBarsResponse = (code: string) => {
  const marchDay = Date.UTC(2026, 2, 31) / 1000;
  const aprilDay = Date.UTC(2026, 3, 1) / 1000;
  const marchMonth = Date.UTC(2026, 2, 1) / 1000;
  return {
    data: {
      items: {
        [code]: {
          daily: {
            bars: [
              [Date.UTC(2026, 2, 30) / 1000, 100, 105, 99, 104, 1000],
              [marchDay, 104, 106, 103, 105, 1200],
            ],
          },
          weekly: {
            bars: [
              [Date.UTC(2026, 2, 24) / 1000, 98, 106, 97, 104, 3000],
              [marchDay, 104, 107, 103, 106, 2800],
            ],
          },
          monthly: {
            bars: [
              [marchMonth, 90, 110, 88, 104, 12000],
              [aprilDay, 104, 112, 102, 110, 13000],
            ],
            boxes: [],
          },
        },
      },
    },
  };
};

const createReplayBarsResponse = (code: string) => {
  const daily = {
    bars: [
      [Date.UTC(2026, 0, 8) / 1000, 100, 102, 99, 100, 1000],
      [Date.UTC(2026, 0, 20) / 1000, 108, 109, 107, 108, 1200],
    ],
  };
  const weekly = {
    bars: [
      [Date.UTC(2026, 0, 5) / 1000, 99, 103, 98, 101, 2000],
      [Date.UTC(2026, 0, 19) / 1000, 101, 110, 100, 108, 2200],
    ],
  };
  const monthly = {
    bars: [
      [Date.UTC(2026, 0, 1) / 1000, 98, 110, 97, 108, 4200],
    ],
    boxes: [],
  };
  return {
    data: {
      items: {
        [code]: { daily, weekly, monthly },
      },
    },
  };
};

const renderDetailView = async (initialEntry = "/detail/7203"): Promise<RenderClientHandle> =>
  renderClient(
    <MemoryRouter initialEntries={[initialEntry]}>
      <Routes>
        <Route path="/detail/:code" element={<DetailView />} />
      </Routes>
    </MemoryRouter>
  );

const findNextButton = (container: HTMLElement) =>
  container.querySelectorAll("button.back.nav-button")[1] ?? null;

describe("DetailView", () => {
  beforeEach(() => {
    mocks.backendReadyRef.value = false;
    mocks.apiGet.mockReset();
    mocks.apiPost.mockReset();
    mocks.apiGet.mockImplementation((url: string) => {
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
      return Promise.resolve({ data: {} });
    });
    mocks.storeState.tickers = [
      {
        code: "7203",
        name: "Test Corp",
        close: 1000,
        chg1D: 0.01,
      },
      {
        code: "6758",
        name: "Next Corp",
        close: 2000,
        chg1D: -0.02,
      },
    ];
    mocks.storeState.ensureListLoaded.mockClear();
    mocks.storeState.loadFavorites.mockClear();
    mocks.storeState.setFavoriteLocal.mockClear();
    mocks.storeState.setShowBoxes.mockClear();
    mocks.storeState.setShowBattleZones.mockClear();
    mocks.storeState.updateMaSetting.mockClear();
    mocks.storeState.updateCompareMaSetting.mockClear();
    mocks.storeState.resetMaSettings.mockClear();
    mocks.storeState.resetCompareMaSettings.mockClear();
    mocks.aiExplainDockProps = [];
    mocks.detailDebugBannerProps = [];
    window.localStorage.clear();
    window.sessionStorage.clear();
    if (!window.requestAnimationFrame) {
      Object.assign(window, {
        requestAnimationFrame: (callback: FrameRequestCallback) => window.setTimeout(() => callback(0), 0),
        cancelAnimationFrame: (handle: number) => window.clearTimeout(handle),
      });
    }
  });

  let canvasMock: CanvasMockHandle | null = null;

  beforeEach(() => {
    canvasMock = installCanvasMock();
  });

  afterEach(() => {
    document.body.innerHTML = "";
    canvasMock?.restore();
    canvasMock = null;
    vi.unstubAllEnvs();
    vi.useRealTimers();
  });

  it("renders the detail route without throwing when the EDINET request is null", async () => {
    const render = await renderDetailView();
    const { container } = render;
    await flushMicrotasks();

    expect(container.querySelector("[data-testid='detail-header-chrome']")).not.toBeNull();
    expect(container.textContent).toContain("検証表示: 有効");
    expect(mocks.apiPost).not.toHaveBeenCalledWith("/jobs/edinet/official-backfill", expect.anything(), expect.anything());

    render.cleanup();
  });

  it("navigates to a directly entered ticker code on submit", async () => {
    const render = await renderDetailView();
    const input = render.container.querySelector<HTMLInputElement>("[aria-label='銘柄コードを入力']");
    const form = input?.closest("form");

    expect(input?.value).toBe("7203");
    expect(form).not.toBeNull();

    act(() => {
      if (!input || !form) return;
      Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")?.set?.call(input, "６７５８");
      input.dispatchEvent(new Event("input", { bubbles: true }));
    });
    expect(input?.value).toBe("6758");
    act(() => {
      if (!form) return;
      form.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
    });
    await flushMicrotasks();

    expect(render.container.querySelector<HTMLInputElement>("[aria-label='銘柄コードを入力']")?.value).toBe("6758");
    expect(render.container.textContent).toContain("Next Corp");

    render.cleanup();
  });

  it("keeps overwrite observability hidden on the normal detail route", async () => {
    vi.stubEnv("VITE_SHOW_OPERATOR_CONSOLE", "1");
    mocks.backendReadyRef.value = true;
    mocks.apiPost.mockImplementation((url: string) => {
      if (url === "/batch_bars_v3") {
        return Promise.resolve(createBarsResponse("7203"));
      }
      return Promise.resolve({ data: {} });
    });

    const render = await renderDetailView();
    const { container } = render;

    expect(await waitForSelector(container, "[data-testid='detail-chart']")).not.toBeNull();
    expect(container.querySelector("[data-testid='detail-overwrite-observability']")).toBeNull();

    render.cleanup();
  });

  it("shows chart db busy errors as retryable user-facing chart messages", async () => {
    mocks.backendReadyRef.value = true;
    mocks.apiPost.mockImplementation((url: string) => {
      if (url === "/batch_bars_v3") {
        return Promise.reject(createChartDbBusyError());
      }
      return Promise.resolve({ data: {} });
    });

    const render = await renderDetailView();

    await act(async () => {
      await flushMicrotasks();
      await new Promise((resolve) => setTimeout(resolve, 2200));
      await flushMicrotasks();
    });

    expect(render.container.textContent).toContain("チャートDBが一時的に混み合っています");
    expect(render.container.textContent).not.toContain("Request failed with status code 503");
    expect(render.container.textContent).not.toContain("timeout of 30000ms exceeded");

    render.cleanup();
  });

  it("places the AI explain dock in the detail footer row", async () => {
    mocks.backendReadyRef.value = true;
    mocks.apiPost.mockImplementation((url: string) => {
      if (url === "/batch_bars_v3") {
        return Promise.resolve(createBarsResponse("7203"));
      }
      return Promise.resolve({ data: {} });
    });

    const render = await renderDetailView();
    const { container } = render;

    expect(await waitForSelector(container, "[data-testid='ai-explain-dock']")).not.toBeNull();
    expect(container.querySelector("[data-testid='detail-footer'] [data-testid='ai-explain-dock']")).not.toBeNull();
    expect(mocks.aiExplainDockProps.at(-1)?.inline).toBe(true);

    render.cleanup();
  });

  it("shows overwrite observability only in overwrite live validation mode", async () => {
    vi.stubEnv("VITE_SHOW_OPERATOR_CONSOLE", "1");
    mocks.backendReadyRef.value = true;
    mocks.apiPost.mockImplementation((url: string) => {
      if (url === "/batch_bars_v3") {
        return Promise.resolve(createBarsResponse("7203"));
      }
      return Promise.resolve({ data: {} });
    });

    const render = await renderDetailView("/detail/7203?overwriteLiveValidation=1");
    const { container } = render;

    expect(await waitForSelector(container, "[data-testid='detail-overwrite-observability']")).not.toBeNull();

    render.cleanup();
  });

  it("keeps monthly charts capped by the daily as-of instead of snapping into the future month", async () => {
    mocks.backendReadyRef.value = true;
    mocks.apiPost.mockImplementation((url: string, payload?: Record<string, unknown>) => {
      if (url !== "/batch_bars_v3") {
        return Promise.resolve({ data: {} });
      }
      const code = Array.isArray(payload?.codes) ? String(payload.codes[0]) : "";
      if (code === "7203") {
        return Promise.resolve(createAsOfMismatchBarsResponse("7203"));
      }
      return Promise.resolve({ data: { items: {} } });
    });

    const render = await renderDetailView("/detail/7203?mainAsOf=2026-04-01");
    const { container } = render;

    await act(async () => {
      await flushMicrotasks();
      await new Promise((resolve) => setTimeout(resolve, 450));
      await flushMicrotasks();
    });

    const chartNodes = Array.from(container.querySelectorAll("[data-testid='detail-chart']"));
    expect(chartNodes.length).toBeGreaterThanOrEqual(3);
    expect(chartNodes.every((node) => node.getAttribute("data-detail-chrome") === "on")).toBe(true);

    const dailyChart = chartNodes[0] as HTMLElement;
    const monthlyChart = chartNodes[2] as HTMLElement;
    expect(dailyChart.getAttribute("data-last-time")).toBe(String(Date.UTC(2026, 2, 31) / 1000));
    expect(monthlyChart.getAttribute("data-candles")).toBe("1");
    expect(monthlyChart.getAttribute("data-last-time")).toBe(String(Date.UTC(2026, 2, 1) / 1000));

    render.cleanup();
  });

  it("resolves selected candle cursor edge panning one visible candle at a time", () => {
    const candles = Array.from({ length: 8 }, (_, index) => ({
      time: 1000 + index * 100,
      open: 1,
      high: 2,
      low: 0,
      close: 1,
    }));

    expect(resolveCursorPannedRange(candles, { from: 1200, to: 1500 }, 1100)).toEqual({
      from: 1100,
      to: 1400,
    });
    expect(resolveCursorPannedRange(candles, { from: 1200, to: 1500 }, 1600)).toEqual({
      from: 1300,
      to: 1600,
    });
    expect(resolveCursorPannedRange(candles, { from: 1200, to: 1500 }, 1400)).toBeNull();
  });

  it("stays mounted after switching to financial mode", async () => {
    vi.useFakeTimers();
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

    const render = await renderDetailView();
    const { container } = render;
    const financialTab = container.querySelector("[data-testid='financial-tab']");
    expect(financialTab).not.toBeNull();

    await act(async () => {
      financialTab?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
      await flushMicrotasks();
      await vi.advanceTimersByTimeAsync(7000);
      await vi.runAllTimersAsync();
      await flushMicrotasks();
    });

    expect(mocks.apiGet).toHaveBeenCalledWith("/ticker/edinet/financials", { params: { code: "7203" } });
    expect(container.querySelector("[data-testid='detail-header-chrome']")).not.toBeNull();

    render.cleanup();
  });

  it("renders the consolidated analysis panel on analysis mode", async () => {
    const render = await renderDetailView();
    const { container } = render;
    const analysisTab = container.querySelector("[data-testid='analysis-tab']");
    expect(analysisTab).not.toBeNull();

    await act(async () => {
      analysisTab?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
      await flushMicrotasks();
      await new Promise((resolve) => setTimeout(resolve, 350));
      await flushMicrotasks();
    });

    expect(container.querySelector("[data-testid='tradex-analysis-panel']")).toBeNull();
    expect(container.textContent).toContain("TRADEX詳細を開く");
    const tradexDetails = Array.from(container.querySelectorAll("summary")).find((node) =>
      node.textContent?.includes("TRADEX詳細を開く")
    );
    await act(async () => {
      tradexDetails?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
      await flushMicrotasks();
    });
    expect(await waitForSelector(container, "[data-testid='tradex-analysis-panel']")).not.toBeNull();

    render.cleanup();
  });

  it("shows MA candle read-only review in annotation mode when the bundle provides it", async () => {
    mocks.backendReadyRef.value = true;
    mocks.apiPost.mockImplementation((url: string) => {
      if (url === "/batch_bars_v3") {
        return Promise.resolve(createBarsResponse("7203"));
      }
      return Promise.resolve({ data: {} });
    });
    mocks.apiGet.mockImplementation((url: string) => {
      if (url === "/system/runtime-selection") {
        return Promise.resolve({ data: {} });
      }
      if (url === "/chart-annotations") {
        return Promise.resolve({ data: { items: [] } });
      }
      if (url === "/chart-reading/bundle") {
        return Promise.resolve({
          data: {
            notes: [],
            ma_role_review: {
              schema_version: "ma_role_readonly_review_v1",
              available: true,
              read_only: true,
              display_only: true,
              ranking_effect: false,
              automatic_trade_action: false,
              matches: [
                {
                  rule_id: "ma_candle_review_001",
                  display_label: "candle:normal_bear+three_bar_falling / MA7/20:below,below,below",
                  actionability: "watch_context_not_trade_signal",
                },
              ],
              chart_markers: [
                {
                  date: "2024-03-09",
                  kind: "ranking-up",
                  label: "MA",
                  rule_id: "ma_candle_review_001",
                  actionability: "watch_context_not_trade_signal",
                },
              ],
            },
          },
        });
      }
      return Promise.resolve({ data: {} });
    });

    const render = await renderDetailView("/detail/7203?mainAsOf=2026-03-31");
    const { container } = render;

    expect(await waitForSelector(container, "[data-testid='detail-chart']")).not.toBeNull();
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 450));
      await flushMicrotasks();
    });
    expect(await waitForSelector(container, "[data-testid='annotation-toolbar']")).not.toBeNull();
    const annotationToggle = container.querySelector<HTMLButtonElement>("[data-testid='annotation-toolbar'] button");
    await act(async () => {
      annotationToggle?.click();
      await flushMicrotasks();
      await new Promise((resolve) => setTimeout(resolve, 0));
      await flushMicrotasks();
      await new Promise((resolve) => setTimeout(resolve, 50));
      await flushMicrotasks();
    });

    expect(mocks.apiGet.mock.calls.map(([url]) => url)).toContain("/chart-reading/bundle");
    const review = await waitForSelector(container, "[data-testid='ma-role-review-panel']", 30);
    expect(review).not.toBeNull();
    expect(review?.textContent).toContain("MA/Candle Review");
    expect(review?.textContent).toContain("Showing 1 important historical MA/candle markers");
    expect(review?.textContent).toContain("Research evidence stays out of the main UI");
    const chartWithMarker = Array.from(container.querySelectorAll("[data-testid='detail-chart']")).find(
      (node) => Number((node as HTMLElement).getAttribute("data-event-markers") || "0") > 0
    );
    expect(chartWithMarker).not.toBeNull();

    render.cleanup();
  });

  it("prioritizes current daily bars before loading secondary charts and next-code prefetch", async () => {
    vi.useFakeTimers();
    mocks.backendReadyRef.value = true;
    mocks.storeState.tickers = [
      { code: "9101", name: "Alpha Corp", close: 1000, chg1D: 0.01 },
      { code: "9102", name: "Beta Corp", close: 1100, chg1D: -0.01 },
    ];
    window.sessionStorage.setItem("detailListCodes", JSON.stringify(["9101", "9102"]));
    mocks.apiPost.mockImplementation((url: string, payload?: Record<string, unknown>) => {
      if (url !== "/batch_bars_v3") {
        return Promise.resolve({ data: {} });
      }
      const code = Array.isArray(payload?.codes) ? String(payload.codes[0]) : "";
      if (code === "9101") {
        return Promise.resolve(createBarsResponse("9101", 1000));
      }
      if (code === "9102") {
        return Promise.resolve(createBarsResponse("9102", 2000));
      }
      return Promise.resolve({ data: { items: {} } });
    });

    const render = await renderDetailView("/detail/9101");
    const { container } = render;

    await act(async () => {
      await flushMicrotasks();
      await vi.advanceTimersByTimeAsync(200);
      await flushMicrotasks();
    });

    const batchBarsCalls = mocks.apiPost.mock.calls.filter(([url]) => url === "/batch_bars_v3");
    expect(batchBarsCalls).toHaveLength(3);
    expect(batchBarsCalls[0]?.[1]).toMatchObject({
      codes: ["9101"],
      timeframes: ["daily"],
      timeframeLimits: {
        daily: 2000,
        weekly: 520,
        monthly: 240,
      },
    });
    expect(batchBarsCalls[1]?.[1]).toMatchObject({
      codes: ["9101"],
      timeframes: ["weekly", "monthly"],
      timeframeLimits: {
        daily: 2000,
        weekly: 520,
        monthly: 240,
      },
    });
    expect(batchBarsCalls[2]?.[1]).toMatchObject({
      codes: ["9102"],
      timeframes: ["daily", "weekly", "monthly"],
      timeframeLimits: {
        daily: 2000,
        weekly: 520,
        monthly: 240,
      },
    });

    const nextButton = findNextButton(container);
    expect(nextButton).not.toBeNull();

    await act(async () => {
      nextButton?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
      await flushMicrotasks();
    });

    const batchBarsCallsAfterNavigate = mocks.apiPost.mock.calls.filter(([url]) => url === "/batch_bars_v3");
    expect(batchBarsCallsAfterNavigate).toHaveLength(3);

    render.cleanup();
  });

  it("uses compareAsOf for the compare chart request so the displayed period matches the similar date", async () => {
    mocks.backendReadyRef.value = true;
    mocks.storeState.tickers = [
      { code: "9111", name: "Alpha Corp", close: 1000, chg1D: 0.01 },
      { code: "9112", name: "Beta Corp", close: 1100, chg1D: -0.01 },
    ];
    mocks.apiPost.mockImplementation((url: string, payload?: Record<string, unknown>) => {
      if (url !== "/batch_bars_v3") {
        return Promise.resolve({ data: {} });
      }
      const code = Array.isArray(payload?.codes) ? String(payload.codes[0]) : "";
      return Promise.resolve(createDatedBarsResponse(code || "9101", code === "9102" ? 2000 : 1000));
    });

    const render = await renderDetailView("/detail/9101?compare=9102&compareAsOf=2026-05-31");

    await act(async () => {
      await flushMicrotasks();
      await flushMicrotasks();
      await flushMicrotasks();
      await flushMicrotasks();
    });

    const compareCall = mocks.apiPost.mock.calls.find(
      ([url, payload]) =>
        url === "/batch_bars_v3" &&
        Array.isArray((payload as { codes?: unknown[] } | undefined)?.codes) &&
        (payload as { codes?: unknown[] }).codes?.[0] === "9102"
    );
    expect(compareCall?.[1]).toMatchObject({
      codes: ["9102"],
      timeframes: ["daily", "monthly"],
      asof: "2026-05-31",
      forwardBars: {
        daily: 60,
        monthly: 2,
      },
      timeframeLimits: {
        daily: 420,
        monthly: 240,
      },
    });
    const charts = Array.from(render.container.querySelectorAll('[data-testid="detail-chart"]'));
    const compareDailyChart = charts[3];
    expect(compareDailyChart?.getAttribute("data-visible-from")).toBe(String(Date.UTC(2025, 6, 31) / 1000));
    expect(compareDailyChart?.getAttribute("data-visible-to")).toBe(String(Date.UTC(2026, 6, 31) / 1000));

    render.cleanup();
  });

  it("moves to the next similar comparison using the stored compare index", async () => {
    mocks.backendReadyRef.value = true;
    mocks.storeState.tickers = [
      { code: "9101", name: "Alpha Corp", close: 1000, chg1D: 0.01 },
      { code: "9102", name: "Beta Corp", close: 1100, chg1D: -0.01 },
      { code: "9103", name: "Gamma Corp", close: 1200, chg1D: 0.02 },
    ];
    window.sessionStorage.setItem(
      "similarCompareList",
      JSON.stringify({
        queryTicker: "9101",
        mainAsOf: "2026-05-31",
        items: [
          { ticker: "9102", asof: "2020-01-31" },
          { ticker: "9103", asof: "2021-01-31" },
        ],
      })
    );
    mocks.apiPost.mockImplementation((url: string, payload?: Record<string, unknown>) => {
      if (url !== "/batch_bars_v3") {
        return Promise.resolve({ data: {} });
      }
      const code = Array.isArray(payload?.codes) ? String(payload.codes[0]) : "";
      return Promise.resolve(createBarsResponse(code || "9101", code === "9103" ? 3000 : 1000));
    });

    const render = await renderDetailView(
      "/detail/9101?mainAsOf=2026-05-31&compare=9102&compareAsOf=2020-01-31&compareIndex=0"
    );
    const { container } = render;

    await act(async () => {
      await flushMicrotasks();
      await flushMicrotasks();
      await flushMicrotasks();
    });

    const compareNextButton = container.querySelector(".detail-compare-actions button") as HTMLButtonElement | null;
    expect(compareNextButton).not.toBeNull();
    expect(compareNextButton?.disabled).toBe(false);

    await act(async () => {
      compareNextButton?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
      await flushMicrotasks();
      await flushMicrotasks();
      await flushMicrotasks();
    });

    const compareCalls = mocks.apiPost.mock.calls.filter(
      ([url, payload]) =>
        url === "/batch_bars_v3" &&
        Array.isArray((payload as { codes?: unknown[] } | undefined)?.codes) &&
        (payload as { codes?: unknown[] }).codes?.[0] === "9103"
    );
    expect(compareCalls.at(-1)?.[1]).toMatchObject({
      codes: ["9103"],
      asof: "2021-01-31",
      forwardBars: {
        daily: 60,
        monthly: 2,
      },
    });

    render.cleanup();
  });

  it("shows loading with empty charts when the detail bars request is still loading", async () => {
    mocks.backendReadyRef.value = true;
    mocks.storeState.tickers = [
      { code: "9202", name: "Delta Corp", close: 1300, chg1D: -0.04 },
    ];
    const nextDeferred = createDeferred<{ data: { items: Record<string, unknown> } }>();
    mocks.apiPost.mockImplementation((url: string, payload?: Record<string, unknown>) => {
      if (url !== "/batch_bars_v3") {
        return Promise.resolve({ data: {} });
      }
      const code = Array.isArray(payload?.codes) ? String(payload.codes[0]) : "";
      if (code === "9202") {
        return nextDeferred.promise;
      }
      return Promise.resolve({ data: { items: {} } });
    });

    const render = await renderDetailView("/detail/9202");
    const { container } = render;

    await act(async () => {
      await flushMicrotasks();
      await flushMicrotasks();
    });

    expect(container.querySelector<HTMLInputElement>("[aria-label='銘柄コードを入力']")?.value).toBe("9202");
    expect(container.textContent).toContain("日足: 読み込み中...");
    expect(container.textContent).toContain("週足: 読み込み中...");
    expect(container.textContent).toContain("月足: 読み込み中...");
    const chartNodes = Array.from(container.querySelectorAll("[data-testid='detail-chart']"));
    expect(chartNodes.length).toBeGreaterThan(0);
    expect(chartNodes.every((node) => node.getAttribute("data-detail-chrome") === "on")).toBe(true);
    for (const chartNode of chartNodes) {
      expect(chartNode.getAttribute("data-candles")).toBe("0");
    }

    await act(async () => {
      nextDeferred.resolve(createBarsResponse("9202", 2000));
      await flushMicrotasks();
    });

    render.cleanup();
  });

  it("keeps charts empty for the target code when the detail bars request times out", async () => {
    mocks.backendReadyRef.value = true;
    mocks.storeState.tickers = [
      { code: "9302", name: "Zeta Corp", close: 1500, chg1D: -0.03 },
    ];
    mocks.apiPost.mockImplementation((url: string, payload?: Record<string, unknown>) => {
      if (url !== "/batch_bars_v3") {
        return Promise.resolve({ data: {} });
      }
      const code = Array.isArray(payload?.codes) ? String(payload.codes[0]) : "";
      if (code === "9302") {
        return Promise.reject(createTimeoutError());
      }
      return Promise.resolve({ data: { items: {} } });
    });

    const render = await renderDetailView("/detail/9302");
    const { container } = render;

    await act(async () => {
      await flushMicrotasks();
      await flushMicrotasks();
    });

    const chartNodes = Array.from(container.querySelectorAll("[data-testid='detail-chart']"));
    expect(chartNodes.length).toBeGreaterThan(0);
    expect(container.querySelector<HTMLInputElement>("[aria-label='銘柄コードを入力']")?.value).toBe("9302");
    for (const chartNode of chartNodes) {
      expect(chartNode.getAttribute("data-candles")).toBe("0");
    }

    const timedOutCalls = mocks.apiPost.mock.calls.filter(
      ([url, payload]) =>
        url === "/batch_bars_v3" &&
        Array.isArray((payload as { codes?: unknown[] } | undefined)?.codes) &&
        (payload as { codes?: unknown[] }).codes?.[0] === "9302"
    );
    expect(timedOutCalls.length).toBeGreaterThanOrEqual(1);

    render.cleanup();
  });

  it("resolves an incomplete detail bars response into explicit chart lifecycle states", async () => {
    mocks.backendReadyRef.value = true;
    mocks.storeState.tickers = [
      { code: "9402", name: "Partial Corp", close: 1600, chg1D: 0.02 },
    ];
    mocks.apiPost.mockImplementation((url: string, payload?: Record<string, unknown>) => {
      if (url !== "/batch_bars_v3") {
        return Promise.resolve({ data: {} });
      }
      const code = Array.isArray(payload?.codes) ? String(payload.codes[0]) : "";
      if (code === "9402") {
        return Promise.resolve({
          data: {
            items: {
              "9402": {
                daily: createBarsFrame(1_710_000_000, 1600),
                monthly: {
                  ...createBarsFrame(1_707_264_000, 1620),
                  boxes: [],
                },
              },
            },
          },
        });
      }
      return Promise.resolve({ data: { items: {} } });
    });

    const render = await renderDetailView("/detail/9402");
    const { container } = render;

    await act(async () => {
      await flushMicrotasks();
      await flushMicrotasks();
      await new Promise((resolve) => setTimeout(resolve, 250));
      await flushMicrotasks();
    });

    expect(container.textContent).toContain("週足: 表示できるデータがありません");
    expect(container.textContent).not.toContain("週足: 読み込み中...");
    const chartNodes = Array.from(container.querySelectorAll("[data-testid='detail-chart']"));
    expect(chartNodes.length).toBeGreaterThanOrEqual(3);
    expect((chartNodes[0] as HTMLElement).getAttribute("data-candles")).toBe("1");
    expect((chartNodes[1] as HTMLElement).getAttribute("data-candles")).toBe("0");
    expect((chartNodes[2] as HTMLElement).getAttribute("data-candles")).toBe("1");

    render.cleanup();
  });

  it("loads replay mode on the detail route and renders replay markers and ledger rows", async () => {
    mocks.backendReadyRef.value = true;
    mocks.storeState.tickers = [
      { code: "9999", name: "Replay Corp", close: 1000, chg1D: 0.01 },
    ];
    mocks.apiGet.mockImplementation((url: string) => {
      if (url === "/tradex/replay/runs/phase2_contract_check") {
        return Promise.resolve({
          data: {
            run_id: "phase2_contract_check",
            window_summary: {
              run_id: "phase2_contract_check",
              policy_id: "tradex_policy_replay_v1",
              policy_version: "v1",
              window_start_date: "2026-01-01",
              window_end_date: "2026-03-31",
              portfolio_return_3m: 0.0523,
              excess_vs_universe: 0.0184,
              final_score: 0.421,
              weekly_activity_pass_rate: 1,
              weeks_with_no_trade: 0,
              weekly_trade_count_map: { "2026-W01": 2 },
            },
            trade_ledger: [
              {
                date: "2026-01-08",
                symbol: "9999",
                state_from: "flat",
                state_to: "entered_1",
                position_notation_before: "0-0",
                position_notation_after: "0-2",
                action_taken: "enter_long",
                trigger_reason_code: "entry_signal",
                trigger_reason_text: "long entry signal",
                units_changed: 2,
                close_price: 100.0,
                avg_entry_price_after: 100.0,
                realized_pnl_delta: 0,
                unrealized_pnl_after: 0,
                cash_after: 9800000,
                equity_after: 10000000,
                position_id: "pos-1",
                feature_state_id: "feature-1",
                holding_days_open: 0,
              },
              {
                date: "2026-01-20",
                symbol: "9999",
                state_from: "entered_1",
                state_to: "full_exit",
                position_notation_before: "0-2",
                position_notation_after: "0-0",
                action_taken: "full_exit",
                trigger_reason_code: "opposite_signal",
                trigger_reason_text: "opposite signal triggered exit",
                units_changed: 2,
                close_price: 108.0,
                avg_entry_price_after: 100.0,
                realized_pnl_delta: 1600,
                unrealized_pnl_after: 0,
                cash_after: 10001600,
                equity_after: 10001600,
                position_id: "pos-1",
                feature_state_id: "feature-2",
                holding_days_open: 9,
              },
            ],
            positions_timeline: [
              {
                date: "2026-01-08",
                symbol: "9999",
                position_notation: "0-2",
                long_units: 2,
                short_units: 0,
                avg_entry_price: 100,
                close_price: 100,
                market_value: 20000,
                unrealized_pnl: 0,
                realized_pnl_cum: 0,
                holding_days_open: 1,
                cash_after: 9800000,
                equity_after: 10000000,
                action_taken: "enter_long",
                trigger_reason_code: "entry_signal",
                trigger_reason_text: "long entry signal",
                feature_state_id: "feature-1",
              },
              {
                date: "2026-01-20",
                symbol: "9999",
                position_notation: "0-0",
                long_units: 0,
                short_units: 0,
                avg_entry_price: null,
                close_price: 108,
                market_value: 0,
                unrealized_pnl: 0,
                realized_pnl_cum: 1600,
                holding_days_open: 0,
                cash_after: 10001600,
                equity_after: 10001600,
                action_taken: "full_exit",
                trigger_reason_code: "opposite_signal",
                trigger_reason_text: "opposite signal triggered exit",
                feature_state_id: "feature-2",
              },
            ],
          },
        });
      }
      if (url.startsWith("/jobs/")) {
        return Promise.resolve({ data: { status: "done" } });
      }
      return Promise.resolve({ data: {} });
    });
    mocks.apiPost.mockImplementation((url: string, payload?: Record<string, unknown>) => {
      if (url === "/batch_bars_v3") {
        const code = Array.isArray(payload?.codes) ? String(payload.codes[0]) : "";
        if (code === "9999") {
          return Promise.resolve(createReplayBarsResponse("9999"));
        }
      }
      return Promise.resolve({ data: {} });
    });

    const render = await renderDetailView("/detail/9999?replayRunId=phase2_contract_check");
    const { container } = render;

    await act(async () => {
      await flushMicrotasks();
      await flushMicrotasks();
    });

    const chartNode = container.querySelector("[data-testid='detail-chart']");
    expect(chartNode).not.toBeNull();
    expect(chartNode?.getAttribute("data-trade-markers")).toBe("2");
    expect(chartNode?.getAttribute("data-position-text")).toContain("0-");
    expect(container.textContent).toContain("enter_long");
    expect(container.textContent).toContain("full_exit");

    render.cleanup();
  });
});
