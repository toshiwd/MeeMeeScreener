import { beforeEach, describe, expect, it, vi } from "vitest";

const applyChartDataVersion = vi.fn(async () => undefined);
const getPersistentChartFrame = vi.fn(async () => null);
const setPersistentChartFrame = vi.fn(async () => undefined);
const subscribeToChartDataVersionChange = vi.fn(() => () => undefined);
const postDetailBatchBarsRequest = vi.fn();

vi.mock("../../persistentChartCache", () => ({
  applyChartDataVersion,
  getPersistentChartFrame,
  setPersistentChartFrame,
  subscribeToChartDataVersionChange,
}));

vi.mock("./batchBarsRequest", () => ({
  buildDetailBatchBarsRequestPayload: ({ code, dailyLimit, weeklyLimit, monthlyLimit, asof }: any) => ({
    codes: [code],
    timeframes: ["daily", "weekly", "monthly"],
    limit: Math.max(dailyLimit, weeklyLimit, monthlyLimit),
    timeframeLimits: {
      daily: dailyLimit,
      weekly: weeklyLimit,
      monthly: monthlyLimit,
    },
    includeProvisional: true,
    includeBoxes: true,
    ...(asof ? { asof } : {}),
  }),
  buildScopedDetailBatchBarsRequestPayload: ({
    code,
    dailyLimit,
    weeklyLimit,
    monthlyLimit,
    asof,
    timeframes,
    includeBoxes,
  }: any) => ({
    codes: [code],
    timeframes: timeframes ?? ["daily", "weekly", "monthly"],
    limit: Math.max(
      ...(timeframes ?? ["daily", "weekly", "monthly"]).map((timeframe: string) =>
        timeframe === "daily" ? dailyLimit : timeframe === "weekly" ? weeklyLimit : monthlyLimit
      )
    ),
    timeframeLimits: {
      daily: dailyLimit,
      weekly: weeklyLimit,
      monthly: monthlyLimit,
    },
    includeProvisional: true,
    includeBoxes: includeBoxes ?? true,
    ...(asof ? { asof } : {}),
  }),
  postDetailBatchBarsRequest,
}));

describe("detailChartPrefetch", () => {
  beforeEach(() => {
    vi.resetModules();
    applyChartDataVersion.mockClear();
    getPersistentChartFrame.mockClear();
    setPersistentChartFrame.mockClear();
    subscribeToChartDataVersionChange.mockClear();
    postDetailBatchBarsRequest.mockReset();
  });

  it("reuses an in-flight batch request when the same code is fetched individually", async () => {
    let resolveBatch: ((value: unknown) => void) | null = null;
    postDetailBatchBarsRequest.mockImplementation(
      () =>
        new Promise((resolve) => {
          resolveBatch = resolve;
        })
    );

    const mod = await import("./detailChartPrefetch");
    const request1332 = {
      code: "1332",
      dailyLimit: 2000,
      weeklyLimit: 520,
      monthlyLimit: 240,
      asof: null,
    };
    const request1357 = {
      code: "1357",
      dailyLimit: 2000,
      weeklyLimit: 520,
      monthlyLimit: 240,
      asof: null,
    };

    const batchPromise = mod.prefetchDetailChartFramesBatch([request1332, request1357]);
    await vi.waitFor(() => {
      expect(postDetailBatchBarsRequest).toHaveBeenCalledTimes(1);
    });

    const singlePromise = mod.prefetchDetailChartFrames(request1332);

    expect(postDetailBatchBarsRequest).toHaveBeenCalledTimes(1);

    resolveBatch?.({
      data: {
        items: {
          "1332": {
            daily: { bars: [[1, 1, 1, 1, 10]] },
            weekly: { bars: [[2, 2, 2, 2, 20]] },
            monthly: { bars: [[3, 3, 3, 3, 30]], boxes: [] },
          },
          "1357": {
            daily: { bars: [[4, 4, 4, 4, 40]] },
            weekly: { bars: [[5, 5, 5, 5, 50]] },
            monthly: { bars: [[6, 6, 6, 6, 60]], boxes: [] },
          },
        },
        meta: {
          data_version: "v1",
        },
      },
    });

    const [batchResult, singleResult] = await Promise.all([batchPromise, singlePromise]);

    expect(postDetailBatchBarsRequest).toHaveBeenCalledTimes(1);
    expect(batchResult["1332"]?.daily?.rows).toEqual([[1, 1, 1, 1, 10]]);
    expect(singleResult.daily?.rows).toEqual([[1, 1, 1, 1, 10]]);
    expect(singleResult.weekly?.rows).toEqual([[2, 2, 2, 2, 20]]);
    expect(singleResult.monthly?.rows).toEqual([[3, 3, 3, 3, 30]]);
  });

  it("allows compare prefetch to complete with daily and monthly frames only", async () => {
    postDetailBatchBarsRequest.mockResolvedValue({
      data: {
        items: {
          "7203": {
            daily: { bars: [[1, 1, 1, 1, 10]] },
            monthly: { bars: [[3, 3, 3, 3, 30]], boxes: [] },
          },
        },
        meta: {
          data_version: "v2",
        },
      },
    });

    const mod = await import("./detailChartPrefetch");
    const request = {
      code: "7203",
      dailyLimit: 240,
      weeklyLimit: 120,
      monthlyLimit: 90,
      asof: "2026-03-19",
    };

    const result = await mod.prefetchDetailChartFrames(request, {
      timeframes: ["daily", "monthly"],
    });

    expect(postDetailBatchBarsRequest).toHaveBeenCalledWith(
      {
        codes: ["7203"],
        timeframes: ["daily", "monthly"],
        limit: 240,
        timeframeLimits: {
          daily: 240,
          weekly: 120,
          monthly: 90,
        },
        includeProvisional: true,
        includeBoxes: true,
        asof: "2026-03-19",
      },
      undefined
    );
    expect(mod.hasCompleteDetailChartPrefetch(result, ["daily", "monthly"])).toBe(true);
    expect(result.daily?.rows).toEqual([[1, 1, 1, 1, 10]]);
    expect(result.monthly?.rows).toEqual([[3, 3, 3, 3, 30]]);
    expect(result.weekly).toBeNull();
  });
});
