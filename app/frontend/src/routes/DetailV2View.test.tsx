// @vitest-environment jsdom
import { act } from "react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { renderClient, type RenderClientHandle } from "../test/renderClient";

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

const mocks = vi.hoisted(() => {
  const frame = (rightEdge: string) => ({
    rows: [
      [Date.parse(`${rightEdge}T00:00:00Z`) / 1000, 100, 110, 90, 105, 1000],
    ],
    boxes: [],
    fetchedAt: Date.now(),
    dataVersion: "test",
    provenance: null,
    cacheSource: "memory" as const,
  });
  const contract = {
    contract_version: "meemee_data_freshness_v1",
    generated_at: "2026-05-06T00:00:00Z",
    ranking: {
      source: null,
      source_path_or_adapter: null,
      classification: "confirmed",
      status: "ready",
      snapshot_as_of: "2026-05-01",
      snapshot_id: "test",
      freshness_state: "fresh",
    },
    detail: {
      source: "batch_bars_v3",
      source_path_or_adapter: null,
      classification: "confirmed",
      status: "ready",
    },
    charts: {
      daily: {
        source: "batch_bars_v3",
        source_path_or_adapter: null,
        classification: "confirmed",
        status: "ready",
        right_edge_date: "2026-05-01",
        freshness_state: "fresh",
      },
      weekly: {
        source: "batch_bars_v3",
        source_path_or_adapter: null,
        classification: "confirmed",
        status: "ready",
        right_edge_date: "2026-04-27",
        freshness_state: "fresh",
      },
      monthly: {
        source: "batch_bars_v3",
        source_path_or_adapter: null,
        classification: "confirmed",
        status: "ready",
        right_edge_date: "2026-05-01",
        freshness_state: "fresh",
      },
    },
    research: {
      normal_ui_exposure_allowed: false,
      classification: "research-only",
    },
  };
  const frames = {
    daily: frame("2026-05-01"),
    weekly: frame("2026-04-27"),
    monthly: frame("2026-05-01"),
    dataFreshnessContract: contract,
  };
  const baseMaSettings = [
    {
      key: "ma7",
      label: "MA7",
      period: 7,
      color: "#f87171",
      visible: true,
      lineWidth: 1,
    },
  ];
  return {
    frames,
    storeState: {
      backendReady: true,
      tickers: [
        { code: "7203", name: "Toyota" },
        { code: "6758", name: "Sony" },
      ],
      loadingList: false,
      ensureListLoaded: vi.fn(async () => undefined),
      maSettings: {
        daily: baseMaSettings,
        weekly: baseMaSettings,
        monthly: baseMaSettings,
      },
      settings: {
        showBoxes: true,
      },
    },
    loadDetailChartPrefetch: vi.fn(async () => frames),
    prefetchDetailChartFrames: vi.fn(async () => frames),
    readDetailChartPrefetchSync: vi.fn(() => ({
      daily: null,
      weekly: null,
      monthly: null,
      dataFreshnessContract: null,
    })),
  };
});

vi.mock("../store", () => ({
  useStore: (selector: (state: typeof mocks.storeState) => unknown) => selector(mocks.storeState),
}));

vi.mock("../components/DetailChart", () => ({
  default: (props: { candles: unknown[]; meeMeeDetailChrome?: { timeframe?: string } }) => (
    <div data-testid={`mock-detail-chart-${props.meeMeeDetailChrome?.timeframe}`}>
      {props.candles.length}
    </div>
  ),
}));

vi.mock("./detail/detailChartPrefetch", () => ({
  loadDetailChartPrefetch: (...args: unknown[]) => mocks.loadDetailChartPrefetch(...args),
  prefetchDetailChartFrames: (...args: unknown[]) => mocks.prefetchDetailChartFrames(...args),
  readDetailChartPrefetchSync: (...args: unknown[]) => mocks.readDetailChartPrefetchSync(...args),
}));

import DetailV2View from "./DetailV2View";

describe("DetailV2View", () => {
  let rendered: RenderClientHandle | null = null;

  beforeEach(() => {
    sessionStorage.clear();
    mocks.loadDetailChartPrefetch.mockClear();
    mocks.prefetchDetailChartFrames.mockClear();
    mocks.readDetailChartPrefetchSync.mockClear();
  });

  afterEach(() => {
    rendered?.cleanup();
    rendered = null;
    sessionStorage.clear();
  });

  it("renders the minimal D/W/M shell with freshness badges", async () => {
    sessionStorage.setItem("detailListBack", "/ranking");
    sessionStorage.setItem("detailListCodes", JSON.stringify(["7203", "6758"]));
    rendered = await renderClient(
      <MemoryRouter initialEntries={["/detail-v2/7203"]}>
        <Routes>
          <Route path="/detail-v2/:code" element={<DetailV2View />} />
        </Routes>
      </MemoryRouter>
    );

    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(rendered.container.querySelector("[data-testid='detail-v2-shell']")).not.toBeNull();
    expect(rendered.container.querySelector("[data-testid='mock-detail-chart-monthly']")).not.toBeNull();
    expect(rendered.container.querySelector("[data-testid='mock-detail-chart-weekly']")).not.toBeNull();
    expect(rendered.container.querySelector("[data-testid='mock-detail-chart-daily']")).not.toBeNull();
    expect(rendered.container.textContent).toContain("right edge 2026-05-01");
    expect(rendered.container.textContent).toContain("confirmed");
    expect(rendered.container.textContent).not.toContain("internal source blocked");
  });

  it("keeps v2 next and back navigation on the v2 route", async () => {
    sessionStorage.setItem("detailListBack", "/ranking");
    sessionStorage.setItem("detailListCodes", JSON.stringify(["7203", "6758"]));
    rendered = await renderClient(
      <MemoryRouter initialEntries={["/detail-v2/7203"]}>
        <Routes>
          <Route path="/ranking" element={<div data-testid="ranking-return">ranking</div>} />
          <Route path="/detail-v2/:code" element={<DetailV2View />} />
        </Routes>
      </MemoryRouter>
    );

    await act(async () => {
      await Promise.resolve();
    });

    const next = Array.from(rendered.container.querySelectorAll("button")).find(
      (button) => button.textContent === "Next"
    ) as HTMLButtonElement | undefined;
    expect(next?.disabled).toBe(false);
    await act(async () => {
      next?.click();
    });
    expect(rendered.container.textContent).toContain("6758");

    const back = Array.from(rendered.container.querySelectorAll("button")).find(
      (button) => button.textContent === "Back to list"
    ) as HTMLButtonElement | undefined;
    await act(async () => {
      back?.click();
    });
    expect(rendered.container.querySelector("[data-testid='ranking-return']")).not.toBeNull();
  });

  it("keeps old detail available from the v2 shell", async () => {
    sessionStorage.setItem("detailListBack", "/ranking");
    sessionStorage.setItem("detailListCodes", JSON.stringify(["7203", "6758"]));
    rendered = await renderClient(
      <MemoryRouter initialEntries={["/detail-v2/7203"]}>
        <Routes>
          <Route path="/detail/:code" element={<div data-testid="old-detail-fallback">old detail</div>} />
          <Route path="/detail-v2/:code" element={<DetailV2View />} />
        </Routes>
      </MemoryRouter>
    );

    await act(async () => {
      await Promise.resolve();
    });

    const oldDetail = Array.from(rendered.container.querySelectorAll("button")).find(
      (button) => button.textContent === "Open old detail"
    ) as HTMLButtonElement | undefined;
    expect(oldDetail).toBeTruthy();
    await act(async () => {
      oldDetail?.click();
    });
    expect(rendered.container.querySelector("[data-testid='old-detail-fallback']")).not.toBeNull();
  });
});
