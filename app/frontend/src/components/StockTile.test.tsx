import { renderToStaticMarkup } from "react-dom/server";
import { beforeEach, describe, expect, it, vi } from "vitest";
import StockTile from "./StockTile";

const mocks = vi.hoisted(() => {
  const state = {
    favorites: [] as string[],
    setFavoriteLocal: vi.fn(),
    barsCache: {
      daily: {
        "7203": {
          bars: [
            [20260318, 100, 102, 98, 100, 10],
            [20260319, 100, 112, 99, 110, 100]
          ],
          ma: { ma7: [], ma20: [], ma60: [] }
        }
      },
      weekly: {},
      monthly: {}
    },
    boxesCache: { daily: { "7203": [] }, weekly: {}, monthly: {} },
    barsStatus: { daily: { "7203": "success" }, weekly: {}, monthly: {} },
    maSettings: { daily: [], weekly: [], monthly: [] },
    settings: { showBoxes: true }
  } as any;

  const useStore = ((selector: (current: typeof state) => unknown) => selector(state)) as any;
  useStore.getState = () => state;
  useStore.setState = (patch: Record<string, unknown>) => {
    Object.assign(state, patch);
  };

  return {
    state,
    useStore,
    apiPost: vi.fn(),
    apiDelete: vi.fn()
  };
});

vi.mock("../store", () => ({
  useStore: mocks.useStore,
  Ticker: {}
}));

vi.mock("../api", () => ({
  api: {
    post: mocks.apiPost,
    delete: mocks.apiDelete,
    get: vi.fn()
  },
  setApiErrorReporter: vi.fn()
}));

vi.mock("../backendReady", () => ({
  useBackendReadyState: () => ({ ready: true })
}));

vi.mock("./ThumbnailCanvas", () => ({
  default: () => <div data-testid="thumbnail-canvas" />
}));

describe("StockTile", () => {
  beforeEach(() => {
    mocks.apiPost.mockReset();
    mocks.apiDelete.mockReset();
    mocks.state.setFavoriteLocal.mockReset();
    mocks.state.favorites = [];
    mocks.state.barsCache = {
      daily: {
        "7203": {
          bars: [
            [20260318, 100, 102, 98, 100, 10],
            [20260319, 100, 112, 99, 110, 100]
          ],
          ma: { ma7: [], ma20: [], ma60: [] }
        }
      },
      weekly: {},
      monthly: {}
    };
    mocks.state.boxesCache = { daily: { "7203": [] }, weekly: {}, monthly: {} };
    mocks.state.barsStatus = { daily: { "7203": "success" }, weekly: {}, monthly: {} };
    mocks.state.maSettings = { daily: [], weekly: [], monthly: [] };
    mocks.state.settings = { showBoxes: true };
  });

  it("hides heavy analysis tags and shows the compact cell metadata", () => {
    const markup = renderToStaticMarkup(
      <StockTile
        ticker={
          {
            code: "7203",
            name: "\u30c8\u30e8\u30bf",
            lastClose: 110,
            chg1D: 0.1,
            eventRightsDate: "2026-03-10",
            eventEarningsDate: "2026-01-01",
            dataStatus: null
          } as any
        }
        timeframe="daily"
        maxBars={60}
        kept
        asofLabel="2026/03/19"
        asofTooltip="\u57fa\u6e96\u65e5 2026/03/19 \u306e\u8db3\u304c\u7121\u3044\u306e\u3067 2026/03/19 \u3092\u4f7f\u7528"
        onOpenDetail={vi.fn()}
        onToggleKeep={vi.fn()}
        onExclude={vi.fn()}
      />
    );

    expect(markup).toContain("\u65e5\u4ed8 26/03/19");
    expect(markup).toContain("\u7d42\u5024 110");
    expect(markup).toContain("\u524d\u65e5\u6bd4 +10.0%");
    expect(markup).toContain("\u66ab\u5b9a 2026/03/19");
    expect(markup).toContain("\u5019\u88dc\u304b\u3089\u5916\u3059");
    expect(markup).toContain("\u2713");
    expect(markup).toContain("\u6a29\u5229 3/10");
    expect(markup).not.toContain("\u6c7a\u7b97 1/1");
    expect(markup).not.toContain("\u4ed5\u8fbc");
    expect(markup).not.toContain("\u58f2\u308a");
    expect(markup).not.toContain("\u8cb7\u3044:");
  });
});
