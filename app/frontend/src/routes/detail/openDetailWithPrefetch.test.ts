// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const prefetchDetailChartFramesBatch = vi.fn(async () => ({}));
const readDetailChartPrefetchSync = vi.fn(() => ({
  daily: null,
  weekly: null,
  monthly: null,
}));
const hasCompleteDetailChartPrefetch = vi.fn(() => false);

vi.mock("./detailChartPrefetch", () => ({
  prefetchDetailChartFramesBatch,
  readDetailChartPrefetchSync,
  hasCompleteDetailChartPrefetch,
}));

describe("openDetailWithPrefetch", () => {
  beforeEach(() => {
    vi.useRealTimers();
    sessionStorage.clear();
    prefetchDetailChartFramesBatch.mockClear();
    readDetailChartPrefetchSync.mockClear();
    hasCompleteDetailChartPrefetch.mockClear();
    hasCompleteDetailChartPrefetch.mockReturnValue(false);
  });

  afterEach(() => {
    sessionStorage.clear();
  });

  it("stores detail list context, fires target and neighbor prewarm, and navigates immediately", async () => {
    const { openDetailWithPrefetch } = await import("./openDetailWithPrefetch");
    const navigate = vi.fn();

    await openDetailWithPrefetch({
      navigate,
      code: "7203",
      listCodes: ["7203", "6758"],
      backPath: "/ranking",
      asof: "2026-04-04",
      backendReady: true,
      prefetchWaitMs: 0,
    });

    expect(sessionStorage.getItem("detailListBack")).toBe("/ranking");
    expect(sessionStorage.getItem("detailListCodes")).toBe(JSON.stringify(["7203", "6758"]));
    expect(prefetchDetailChartFramesBatch).toHaveBeenCalledWith([
      {
        code: "7203",
        dailyLimit: 2000,
        weeklyLimit: 520,
        monthlyLimit: 240,
        asof: "2026-04-04",
      },
      {
        code: "6758",
        dailyLimit: 2000,
        weeklyLimit: 520,
        monthlyLimit: 240,
        asof: "2026-04-04",
      },
    ]);
    expect(navigate).toHaveBeenCalledWith("/detail/7203", { state: { from: "/ranking" } });
  });

  it("tolerates missing list codes and still navigates", async () => {
    const { openDetailWithPrefetch } = await import("./openDetailWithPrefetch");
    const navigate = vi.fn();

    await openDetailWithPrefetch({
      navigate,
      code: "7203",
      backPath: "/ranking",
      backendReady: true,
      prefetchWaitMs: 0,
    });

    expect(sessionStorage.getItem("detailListCodes")).toBe(JSON.stringify([]));
    expect(prefetchDetailChartFramesBatch).toHaveBeenCalledWith([
      {
        code: "7203",
        dailyLimit: 2000,
        weeklyLimit: 520,
        monthlyLimit: 240,
        asof: undefined,
      },
    ]);
    expect(navigate).toHaveBeenCalledWith("/detail/7203", { state: { from: "/ranking" } });
  });

  it("normalizes stored list context and navigation state", async () => {
    const { openDetailWithPrefetch } = await import("./openDetailWithPrefetch");
    const navigate = vi.fn();

    await openDetailWithPrefetch({
      navigate,
      code: "7203",
      listCodes: [" 7203 ", "6758", "7203", ""],
      backPath: "/positions?tab=held",
      backendReady: false,
    });

    expect(sessionStorage.getItem("detailListBack")).toBe("/positions?tab=held");
    expect(sessionStorage.getItem("detailListCodes")).toBe(JSON.stringify(["7203", "6758"]));
    expect(navigate).toHaveBeenCalledWith("/detail/7203", { state: { from: "/positions?tab=held" } });
    expect(prefetchDetailChartFramesBatch).not.toHaveBeenCalled();
  });

  it("falls back to root for unsafe return paths", async () => {
    const { openDetailWithPrefetch } = await import("./openDetailWithPrefetch");
    const navigate = vi.fn();

    await openDetailWithPrefetch({
      navigate,
      code: "7203",
      listCodes: ["7203"],
      backPath: "https://example.com/ranking",
      backendReady: false,
    });

    expect(sessionStorage.getItem("detailListBack")).toBe("/");
    expect(navigate).toHaveBeenCalledWith("/detail/7203", { state: { from: "/" } });
  });

  it("can navigate to the opt-in v2 detail route while preserving list context", async () => {
    const { openDetailWithPrefetch } = await import("./openDetailWithPrefetch");
    const navigate = vi.fn();

    await openDetailWithPrefetch({
      navigate,
      code: "7203",
      listCodes: ["7203", "6758"],
      backPath: "/ranking?detailV2=1",
      backendReady: false,
      targetRoute: "detail-v2",
    });

    expect(sessionStorage.getItem("detailListBack")).toBe("/ranking?detailV2=1");
    expect(sessionStorage.getItem("detailListCodes")).toBe(JSON.stringify(["7203", "6758"]));
    expect(navigate).toHaveBeenCalledWith("/detail-v2/7203", { state: { from: "/ranking?detailV2=1" } });
  });

  it("waits for the configured prefetch budget before navigating when prefetch is still pending", async () => {
    vi.useFakeTimers();
    const { openDetailWithPrefetch } = await import("./openDetailWithPrefetch");
    const navigate = vi.fn();
    let resolvePrefetch: (() => void) | null = null;
    prefetchDetailChartFramesBatch.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          resolvePrefetch = () =>
            resolve({});
        })
    );

    const pending = openDetailWithPrefetch({
      navigate,
      code: "6758",
      listCodes: ["6758"],
      backPath: "/favorites",
      backendReady: true,
      prefetchWaitMs: 250,
    });

    await vi.advanceTimersByTimeAsync(200);
    expect(navigate).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(60);
    await pending;
    expect(navigate).toHaveBeenCalledWith("/detail/6758", { state: { from: "/favorites" } });

    resolvePrefetch?.();
  });

  it("skips the wait budget when the target seed is already in memory", async () => {
    vi.useFakeTimers();
    hasCompleteDetailChartPrefetch.mockReturnValue(true);
    const { openDetailWithPrefetch } = await import("./openDetailWithPrefetch");
    const navigate = vi.fn();

    const pending = openDetailWithPrefetch({
      navigate,
      code: "8306",
      listCodes: ["7203", "8306", "9984"],
      backPath: "/ranking",
      backendReady: true,
      prefetchWaitMs: 250,
    });

    await vi.advanceTimersByTimeAsync(10);
    await pending;

    expect(navigate).toHaveBeenCalledWith("/detail/8306", { state: { from: "/ranking" } });
    expect(prefetchDetailChartFramesBatch).toHaveBeenCalledWith([
      {
        code: "8306",
        dailyLimit: 2000,
        weeklyLimit: 520,
        monthlyLimit: 240,
        asof: undefined,
      },
      {
        code: "7203",
        dailyLimit: 2000,
        weeklyLimit: 520,
        monthlyLimit: 240,
        asof: undefined,
      },
      {
        code: "9984",
        dailyLimit: 2000,
        weeklyLimit: 520,
        monthlyLimit: 240,
        asof: undefined,
      },
    ]);
  });
});
