import { beforeEach, describe, expect, it, vi } from "vitest";

const apiGet = vi.fn();
const apiPost = vi.fn();
const setApiErrorReporter = vi.fn();

vi.mock("./api", () => ({
  api: {
    get: apiGet,
    post: apiPost
  },
  setApiErrorReporter
}));

describe("store.loadList", () => {
  beforeEach(() => {
    vi.resetModules();
    apiGet.mockReset();
    apiPost.mockReset();
    setApiErrorReporter.mockReset();
    const storage = new Map<string, string>();
    const windowStub = {
      MEEMEE_API_BASE: "/api",
      localStorage: {
        getItem: (key: string) => storage.get(key) ?? null,
        setItem: (key: string, value: string) => {
          storage.set(key, value);
        },
        removeItem: (key: string) => {
          storage.delete(key);
        },
        clear: () => {
          storage.clear();
        }
      }
    };
    vi.stubGlobal("window", windowStub);
  });

  it("loads screener snapshot without legacy /list fallback", async () => {
    apiGet.mockImplementation((url: string) => {
      if (url === "/grid/screener") {
        return Promise.resolve({
          data: {
            items: [{ code: "1001", name: "Nikkei", stage: "WATCH", score: 10, reason: "" }],
            stale: true,
            asOf: "2026-03-13",
            updatedAt: "2026-03-13T03:00:00Z",
            generation: "g1",
            lastError: "previous refresh failed"
          }
        });
      }
      if (url === "/watchlist") {
        return Promise.resolve({ data: { codes: [] } });
      }
      return Promise.reject(new Error(`unexpected url ${url}`));
    });

    const { useStore } = await import("./store");
    useStore.setState({
      tickers: [],
      loadingList: false,
      listLoadError: null,
      listSnapshotMeta: null,
      listLoadedAt: null,
    });

    await useStore.getState().loadList();

    const state = useStore.getState();
    expect(apiGet).toHaveBeenCalledWith("/grid/screener");
    expect(apiGet).toHaveBeenCalledWith("/watchlist");
    expect(apiGet).not.toHaveBeenCalledWith("/list");
    expect(state.tickers).toHaveLength(1);
    expect(state.listSnapshotMeta).toEqual({
      stale: true,
      asOf: "2026-03-13",
      updatedAt: "2026-03-13T03:00:00Z",
      generation: "g1",
      lastError: "previous refresh failed"
    });
    expect(state.listLoadError).toBeNull();
  });

  it("reuses a fresh screener list for ensureListLoaded", async () => {
    const { useStore } = await import("./store");
    useStore.setState({
      tickers: [{ code: "1001", name: "Nikkei", stage: "WATCH", score: 10, reason: "" }],
      loadingList: false,
      listLoadError: null,
      listSnapshotMeta: null,
      listLoadedAt: Date.now(),
    });

    await useStore.getState().ensureListLoaded();

    expect(apiGet).not.toHaveBeenCalled();
  });
});
