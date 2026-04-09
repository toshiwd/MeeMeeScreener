import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const apiGet = vi.fn();
const apiPost = vi.fn();
const setApiErrorReporter = vi.fn();
let consoleErrorSpy: ReturnType<typeof vi.spyOn> | null = null;

vi.mock("./api", () => ({
  api: {
    get: apiGet,
    post: apiPost
  },
  setApiErrorReporter
}));

describe("store.loadList", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.resetModules();
    apiGet.mockReset();
    apiPost.mockReset();
    setApiErrorReporter.mockReset();
    consoleErrorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const storage = new Map<string, string>();
    const windowStub = {
      MEEMEE_API_BASE: "/api",
      setTimeout,
      clearTimeout,
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

  afterEach(() => {
    consoleErrorSpy?.mockRestore();
    consoleErrorSpy = null;
    vi.runOnlyPendingTimers();
    vi.useRealTimers();
  });

  it("keeps favorites unloaded and retries after a temporary favorites failure", async () => {
    let watchlistFailures = 0;
    apiGet.mockImplementation((url: string) => {
      if (url === "/grid/screener") {
        return Promise.resolve({
          data: {
            items: [{ code: "1001", name: "Nikkei", stage: "WATCH", score: 10, reason: "" }],
          },
        });
      }
      if (url === "/watchlist") {
        return Promise.resolve({ data: { codes: [] } });
      }
      if (url === "/favorites") {
        watchlistFailures += 1;
        if (watchlistFailures === 1) {
          return Promise.reject(new Error("temporary favorites error"));
        }
        return Promise.resolve({ data: [{ code: "1001" }] });
      }
      return Promise.reject(new Error(`unexpected url ${url}`));
    });

    const { useStore } = await import("./store");
    useStore.setState({
      favorites: ["1301"],
      favoritesLoaded: false,
      favoritesLoading: false,
    });

    await useStore.getState().loadFavorites();

    expect(useStore.getState().favorites).toEqual(["1301"]);
    expect(useStore.getState().favoritesLoaded).toBe(false);
    expect(consoleErrorSpy).toHaveBeenCalledWith("[favorites] load failed", {
      status: null,
      data: null,
      message: "temporary favorites error"
    });

    await vi.advanceTimersByTimeAsync(5_000);

    expect(useStore.getState().favorites).toEqual(["1001"]);
    expect(useStore.getState().favoritesLoaded).toBe(true);
  });

  it("skips /favorites request when favorites are already loaded or loading", async () => {
    const { useStore } = await import("./store");
    useStore.setState({
      favorites: ["1001"],
      favoritesLoaded: true,
      favoritesLoading: false,
    });

    await useStore.getState().loadFavorites();
    expect(apiGet).not.toHaveBeenCalledWith("/favorites");

    useStore.setState({
      favoritesLoaded: false,
      favoritesLoading: true,
    });
    await useStore.getState().loadFavorites();
    expect(apiGet).not.toHaveBeenCalledWith("/favorites");
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

  it("hydrates a cached screener list before the fresh network response arrives", async () => {
    let resolveGrid: ((value: unknown) => void) | null = null;
    apiGet.mockImplementation((url: string) => {
      if (url === "/grid/screener") {
        return new Promise((resolve) => {
          resolveGrid = resolve;
        });
      }
      if (url === "/watchlist") {
        return Promise.resolve({ data: { codes: [] } });
      }
      return Promise.reject(new Error(`unexpected url ${url}`));
    });

    window.localStorage.setItem(
      "screenerListCache",
      JSON.stringify({
        cacheVersion: 1,
        tickers: [{ code: "1001", name: "Cached", stage: "WATCH", score: 10, reason: "" }],
        listSnapshotMeta: { stale: false, asOf: "2026-04-03", updatedAt: null, generation: "cached", lastError: null },
        listLoadedAt: Date.now() - 60_000,
      })
    );

    const { useStore } = await import("./store");
    const pending = useStore.getState().loadList();

    expect(useStore.getState().tickers).toHaveLength(1);
    expect(useStore.getState().tickers[0]?.code).toBe("1001");
    expect(useStore.getState().loadingList).toBe(false);

    resolveGrid?.({
      data: {
        items: [{ code: "2002", name: "Fresh", stage: "WATCH", score: 20, reason: "" }],
      },
    });

    await pending;

    expect(useStore.getState().tickers).toHaveLength(1);
    expect(useStore.getState().tickers[0]?.code).toBe("2002");
  });

  it("keeps cached screener rows when the refresh request fails", async () => {
    apiGet.mockImplementation((url: string) => {
      if (url === "/grid/screener") {
        return Promise.reject(new Error("grid unavailable"));
      }
      return Promise.reject(new Error(`unexpected url ${url}`));
    });

    window.localStorage.setItem(
      "screenerListCache",
      JSON.stringify({
        cacheVersion: 1,
        tickers: [{ code: "1001", name: "Cached", stage: "WATCH", score: 10, reason: "" }],
        listSnapshotMeta: { stale: false, asOf: "2026-04-03", updatedAt: "2026-04-03T03:00:00Z", generation: "cached", lastError: null },
        listLoadedAt: Date.now() - 60_000,
      })
    );

    const { useStore } = await import("./store");
    await useStore.getState().loadList();

    expect(useStore.getState().tickers).toHaveLength(1);
    expect(useStore.getState().tickers[0]?.code).toBe("1001");
    expect(useStore.getState().listLoadError).toBeNull();
    expect(useStore.getState().listSnapshotMeta).toMatchObject({
      stale: true,
      lastError: "grid unavailable",
    });
  });
});
