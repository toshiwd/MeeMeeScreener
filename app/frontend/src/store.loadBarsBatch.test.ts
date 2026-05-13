import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { BatchBarsV3Response } from "./routes/detail/batchBarsRequest";

const apiPost = vi.fn();
const setApiErrorReporter = vi.fn();
const applyChartDataVersion = vi.fn(async () => ({ changed: false, current: null, previous: null }));
const getActiveChartDataVersion = vi.fn(() => null);
const versionListeners = new Set<(nextDataVersion: string, previousDataVersion: string | null) => void>();

vi.mock("./api", () => ({
  api: {
    get: vi.fn(),
    post: apiPost,
    delete: vi.fn()
  },
  setApiErrorReporter
}));

vi.mock("./persistentChartCache", () => ({
  applyChartDataVersion,
  getActiveChartDataVersion,
  getPersistentChartFrame: vi.fn(async () => null),
  setPersistentChartFrame: vi.fn(async () => undefined),
  subscribeToChartDataVersionChange: vi.fn((listener) => {
    versionListeners.add(listener);
    return () => {
      versionListeners.delete(listener);
    };
  }),
}));

describe("store.loadBarsBatch", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.resetModules();
    apiPost.mockReset();
    setApiErrorReporter.mockReset();
    applyChartDataVersion.mockReset();
    applyChartDataVersion.mockImplementation(async () => ({ changed: false, current: null, previous: null }));
    getActiveChartDataVersion.mockReset();
    getActiveChartDataVersion.mockReturnValue(null);
    versionListeners.clear();
    const storage = new Map<string, string>();
    vi.stubGlobal("window", {
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
    } as Window);
  });

  afterEach(() => {
    vi.runOnlyPendingTimers();
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("clears loading and returns the tile to idle when a batch request is aborted", async () => {
    apiPost.mockImplementation((_url: string, _payload: unknown, options?: { signal?: AbortSignal }) => {
      return new Promise<{ status: number; data?: BatchBarsV3Response }>((_resolve, reject) => {
        options?.signal?.addEventListener("abort", () => {
          const abortError = new Error("canceled") as Error & { name?: string; code?: string };
          abortError.name = "CanceledError";
          abortError.code = "ERR_CANCELED";
          reject(abortError);
        });
      });
    });

    const { useStore } = await import("./store");
    const request = useStore.getState().loadBarsBatch("daily", ["1001"], 60, "test");

    await vi.advanceTimersByTimeAsync(15_000);
    await request;

    const state = useStore.getState();
    expect(state.barsLoading.daily["1001"]).toBeUndefined();
    expect(state.barsStatus.daily["1001"]).toBe("idle");
  });

  it("keeps the first visible chart request alive when the visible range changes", async () => {
    const pending: Array<{
      payload: { codes?: string[]; timeframes?: string[] };
      signal?: AbortSignal;
      resolve: (value: { status: number; data?: BatchBarsV3Response }) => void;
    }> = [];
    const makePayload = (code: string) => ({
      daily: {
        bars: [[20260513, 100, 110, 90, 105, 1000]],
        ma: { ma7: [], ma20: [], ma60: [] },
        boxes: [],
        provenance: { code }
      }
    });
    apiPost.mockImplementation(
      (_url: string, payload: { codes?: string[]; timeframes?: string[] }, options?: { signal?: AbortSignal }) =>
        new Promise<{ status: number; data?: BatchBarsV3Response }>((resolve) => {
          pending.push({ payload, signal: options?.signal, resolve });
        })
    );

    const { useStore } = await import("./store");
    const first = useStore
      .getState()
      .ensureBarsForVisible("daily", ["1001", "1002"], "initial-visible");
    await vi.advanceTimersByTimeAsync(16);
    expect(pending).toHaveLength(1);

    const second = useStore
      .getState()
      .ensureBarsForVisible("daily", ["1002", "1003"], "scroll");
    await vi.advanceTimersByTimeAsync(16);

    expect(pending[0].signal?.aborted).toBe(false);
    expect(pending).toHaveLength(2);
    expect(pending[0].payload.codes).toEqual(["1001", "1002"]);
    expect(pending[1].payload.codes).toEqual(["1003"]);

    pending[0].resolve({
      status: 200,
      data: {
        items: {
          "1001": makePayload("1001"),
          "1002": makePayload("1002")
        },
        meta: {}
      }
    });
    pending[1].resolve({
      status: 200,
      data: {
        items: {
          "1003": makePayload("1003")
        },
        meta: {}
      }
    });

    await first;
    await second;

    const state = useStore.getState();
    expect(state.barsCache.daily["1001"]?.bars).toHaveLength(1);
    expect(state.barsCache.daily["1002"]?.bars).toHaveLength(1);
    expect(state.barsCache.daily["1003"]?.bars).toHaveLength(1);
    expect(state.barsStatus.daily["1001"]).toBe("success");
    expect(state.barsStatus.daily["1002"]).toBe("success");
    expect(state.barsStatus.daily["1003"]).toBe("success");
  });

  it("keeps returned visible bars when the response advances the chart data version", async () => {
    applyChartDataVersion.mockImplementation(async (dataVersion?: string | null) => {
      const next = dataVersion?.trim() || "unknown";
      versionListeners.forEach((listener) => listener(next, null));
      return { changed: true, current: next, previous: null };
    });
    apiPost.mockResolvedValue({
      status: 200,
      data: {
        items: {
          "1001": {
            daily: {
              bars: [[20260513, 100, 110, 90, 105, 1000]],
              ma: { ma7: [], ma20: [], ma60: [] },
              boxes: [],
              provenance: { code: "1001" }
            }
          }
        },
        meta: { data_version: "duckdb-mtime:test|yf-live:test" }
      }
    });

    const { useStore } = await import("./store");
    const request = useStore
      .getState()
      .ensureBarsForVisible("daily", ["1001"], "initial-visible");
    await vi.advanceTimersByTimeAsync(16);
    await request;

    const state = useStore.getState();
    expect(state.barsCache.daily["1001"]?.bars).toHaveLength(1);
    expect(state.barsStatus.daily["1001"]).toBe("success");
  });
});
