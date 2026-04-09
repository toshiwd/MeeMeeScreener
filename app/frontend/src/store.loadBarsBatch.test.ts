import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { BatchBarsV3Response } from "./routes/detail/batchBarsRequest";

const apiPost = vi.fn();
const setApiErrorReporter = vi.fn();

vi.mock("./api", () => ({
  api: {
    get: vi.fn(),
    post: apiPost,
    delete: vi.fn()
  },
  setApiErrorReporter
}));

vi.mock("./persistentChartCache", () => ({
  applyChartDataVersion: vi.fn(),
  getActiveChartDataVersion: vi.fn(() => null),
  getPersistentChartFrame: vi.fn(async () => null),
  setPersistentChartFrame: vi.fn(async () => undefined),
  subscribeToChartDataVersionChange: vi.fn(() => () => undefined),
}));

describe("store.loadBarsBatch", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.resetModules();
    apiPost.mockReset();
    setApiErrorReporter.mockReset();
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
});
